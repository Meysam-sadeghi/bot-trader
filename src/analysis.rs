use crate::{
    db::{Database, Quote},
    model::{Exchange, MarketPoint, Prediction, now_ms},
    paper::{self, Diagnostics, HORIZONS, LabConfig, LaneDiagnostic, STRATEGIES},
};
use anyhow::Context;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::RwLock, task::AbortHandle};
use uuid::Uuid;

#[derive(Clone)]
pub struct AnalysisManager {
    db: Database,
    tasks: Arc<RwLock<HashMap<Exchange, AbortHandle>>>,
    diagnostics: Arc<RwLock<HashMap<Exchange, Diagnostics>>>,
    pub config: LabConfig,
}

impl AnalysisManager {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            diagnostics: Arc::new(RwLock::new(HashMap::new())),
            config: LabConfig::from_env(),
        }
    }

    pub async fn start(&self, exchange: Exchange, symbol: String) -> bool {
        // Serialize check+insert; simultaneous API requests must not spawn duplicate engines.
        let mut tasks = self.tasks.write().await;
        if tasks.contains_key(&exchange) {
            return false;
        }
        let manager = self.clone();
        let task = tokio::spawn(async move {
            loop {
                if let Err(error) = manager.analyze_once(exchange, &symbol).await {
                    tracing::warn!(%exchange, %error, "analysis waiting");
                    manager.diagnostics.write().await.insert(
                        exchange,
                        Diagnostics {
                            evaluated_at: Some(now_ms()),
                            message: error.to_string(),
                            ..Default::default()
                        },
                    );
                }
                tokio::time::sleep(Duration::from_secs(manager.config.interval_secs)).await;
            }
        });
        tasks.insert(exchange, task.abort_handle());
        true
    }
    pub async fn stop(&self, exchange: Exchange) -> bool {
        if let Some(handle) = self.tasks.write().await.remove(&exchange) {
            handle.abort();
            true
        } else {
            false
        }
    }
    pub async fn is_running(&self, exchange: Exchange) -> bool {
        self.tasks.read().await.contains_key(&exchange)
    }
    pub async fn diagnostics(&self, exchange: Exchange) -> Diagnostics {
        self.diagnostics
            .read()
            .await
            .get(&exchange)
            .cloned()
            .unwrap_or_else(|| Diagnostics {
                message: "Start the lab to collect fresh history and evaluate all six strategies."
                    .into(),
                ..Default::default()
            })
    }
    pub fn spawn_resolver(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(error) = manager.resolve_open_predictions().await {
                    tracing::error!(%error, "paper resolver failed");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    async fn analyze_once(&self, exchange: Exchange, symbol: &str) -> anyhow::Result<()> {
        self.db.flush().await?;
        let at = now_ms();
        let points = self
            .db
            .load_points(
                exchange,
                symbol,
                at - self.config.lookback_hours * 3_600_000,
                self.config.max_points,
                at,
            )
            .await?;
        let peer = if exchange == Exchange::Binance {
            Exchange::Bybit
        } else {
            Exchange::Binance
        };
        let peer_points = self
            .db
            .load_points(peer, symbol, at - 900_000, 120_000, at)
            .await?;
        let config = self.config.clone();
        // Feature/analog computation is CPU work, kept off the async ingestion executor.
        let (signals, mut diagnostic) = tokio::task::spawn_blocking(move || {
            build_signals(&points, &peer_points, exchange, at, &config)
        })
        .await??;
        let config_id = self.config.id();
        let positions = self
            .db
            .experiment_predictions(exchange, symbol, &config_id)
            .await?;
        for signal in signals {
            let lane: Vec<_> = positions
                .iter()
                .filter(|p| p.strategy == signal.strategy && p.horizon_secs == signal.horizon)
                .collect();
            let reason = if lane.iter().any(|p| p.status == "OPEN") {
                "Position already open".to_string()
            } else if lane.iter().any(|p| {
                p.resolved_at
                    .is_some_and(|ts| at - ts < self.config.cooldown_secs * 1000)
            }) {
                "Cooldown after previous exit".to_string()
            } else if paper::stats(&lane, self.config.initial_capital, &self.config).net_pnl
                + self.config.initial_capital
                < self.config.notional * 1.02
            {
                "Paper account has insufficient equity".to_string()
            } else if now_ms() - at > self.config.max_gap_ms {
                "Signal computation exceeded the freshness budget; waiting for next cycle".into()
            } else {
                // Do not backdate entries to the beginning of an expensive analysis pass.
                let decision_at = now_ms();
                let quote = self.db.latest_quote(exchange, symbol, decision_at).await?;
                if let Some(quote) = quote
                    .filter(|q| q.valid() && decision_at - q.ts <= self.config.max_quote_age_ms)
                {
                    let spread = (quote.ask - quote.bid) / ((quote.ask + quote.bid) / 2.) * 10_000.;
                    let entry = paper::entry_fill(
                        signal.direction,
                        quote.bid,
                        quote.ask,
                        self.config.slippage_bps,
                    );
                    if spread > self.config.max_spread_bps {
                        "Spread widened before entry".into()
                    } else if quote.entry_qty(signal.direction) < self.config.notional / entry {
                        "Insufficient visible top-book size".into()
                    } else {
                        let sign = if signal.direction == "LONG" { 1. } else { -1. };
                        let p = Prediction {
                            id: Uuid::new_v4().to_string(),
                            exchange,
                            symbol: symbol.into(),
                            created_at: decision_at,
                            horizon_secs: signal.horizon,
                            direction: signal.direction.into(),
                            entry_price: entry,
                            target_price: entry
                                * (1. + sign * signal.stop_move * self.config.risk_reward),
                            stop_price: entry * (1. - sign * signal.stop_move),
                            confidence: signal.evidence,
                            score: signal.score,
                            expected_return: signal.expected_return,
                            strategy: signal.strategy.into(),
                            status: "OPEN".into(),
                            resolved_at: None,
                            exit_price: None,
                            pnl_bps: None,
                            config_id: config_id.clone(),
                            fee_bps: self.config.fee(exchange),
                            slippage_bps: self.config.slippage_bps,
                            notional: self.config.notional,
                            gross_pnl_bps: None,
                            cursor_id: quote.id,
                            last_quote_ts: quote.ts,
                            mark_price: (quote.exit_qty(signal.direction)
                                >= self.config.notional / entry)
                                .then(|| quote.exit_price(signal.direction)),
                            max_gap_ms: self.config.max_gap_ms,
                            entry_reason: signal.reason,
                        };
                        self.db.insert_prediction(&p).await?;
                        "Paper position opened".into()
                    }
                } else {
                    "Waiting for a fresh, valid executable quote".into()
                }
            };
            if let Some(d) = diagnostic
                .lanes
                .iter_mut()
                .find(|d| d.strategy == signal.strategy && d.horizon_secs == signal.horizon)
            {
                d.reason = reason;
            }
        }
        self.diagnostics.write().await.insert(exchange, diagnostic);
        Ok(())
    }

    async fn resolve_open_predictions(&self) -> anyhow::Result<()> {
        self.db.flush().await?;
        let now = now_ms();
        for mut p in self.db.open_predictions().await? {
            if p.config_id == "legacy" {
                // Preserve the old model for already-open legacy trades only.
                let deadline = p.created_at + p.horizon_secs.min(3600) * 1000;
                if let Some((ts, price, won)) =
                    self.db.first_crossing(&p, now.min(deadline)).await?
                {
                    self.db
                        .resolve_prediction(
                            &p.id,
                            if won { "WIN" } else { "LOSS" },
                            ts,
                            price,
                            paper::pnl(&p.direction, p.entry_price, price, 0.).0,
                        )
                        .await?;
                } else if now >= deadline
                    && let Some(price) = self
                        .db
                        .price_at_or_before(p.exchange, &p.symbol, deadline)
                        .await?
                {
                    self.db
                        .resolve_prediction(
                            &p.id,
                            "TIMEOUT",
                            deadline,
                            price,
                            paper::pnl(&p.direction, p.entry_price, price, 0.).0,
                        )
                        .await?;
                }
                continue;
            }
            let deadline = p.created_at + p.horizon_secs * 1000;
            let quotes = self.db.subsequent_quotes(&p, now.min(deadline)).await?;
            let drained = quotes.len() < 5000;
            advance_position(&mut p, &quotes, now, drained);
            if p.status == "OPEN" {
                self.db.checkpoint(&p).await?;
            } else {
                self.db.finish(&p).await?;
            }
        }
        Ok(())
    }
}

/// Same ordered quote logic is used by deterministic replay tests and the live resolver.
fn advance_position(p: &mut Prediction, quotes: &[Quote], now: i64, drained: bool) {
    let deadline = p.created_at + p.horizon_secs * 1000;
    for q in quotes {
        p.cursor_id = q.id;
        if q.ts <= p.created_at || q.ts > deadline {
            continue;
        }
        if q.ts < p.last_quote_ts || !q.valid() || q.ts - p.last_quote_ts > p.max_gap_ms {
            invalidate(p, q.ts.min(deadline));
            return;
        }
        // Thin liquidity while holding is not an exit. Only a requested fill
        // becomes unverifiable; an unfillable mark is explicitly left unknown.
        let liquid = q.exit_qty(&p.direction) >= p.notional / p.entry_price;
        p.last_quote_ts = q.ts;
        p.mark_price = liquid.then(|| q.exit_price(&p.direction));
        let fill = paper::exit_fill(&p.direction, q.exit_price(&p.direction), p.slippage_bps);
        let (target_hit, stop_hit) = if p.direction == "LONG" {
            (fill >= p.target_price, fill <= p.stop_price)
        } else {
            (fill <= p.target_price, fill >= p.stop_price)
        };
        if (target_hit || stop_hit) && !liquid {
            invalidate(p, q.ts);
            return;
        }
        if stop_hit {
            close(p, "LOSS", q.ts, fill);
            return;
        }
        // Cap favorable gaps at target; adverse stop gaps retain the worse executable fill.
        if target_hit {
            close(p, "WIN", q.ts, p.target_price);
            return;
        }
    }
    if drained {
        if now >= deadline {
            if deadline - p.last_quote_ts > p.max_gap_ms {
                invalidate(p, deadline);
            } else if let Some(mark) = p.mark_price {
                close(
                    p,
                    "TIMEOUT",
                    deadline,
                    paper::exit_fill(&p.direction, mark, p.slippage_bps),
                );
            } else {
                invalidate(p, deadline);
            }
        } else if now - p.last_quote_ts > p.max_gap_ms {
            invalidate(p, now);
        }
    }
}
fn invalidate(p: &mut Prediction, ts: i64) {
    p.status = "DATA_GAP".into();
    p.resolved_at = Some(ts);
    p.pnl_bps = None;
    p.gross_pnl_bps = None;
    p.exit_price = None;
    p.mark_price = None;
}
fn close(p: &mut Prediction, status: &str, ts: i64, fill: f64) {
    let (gross, net) = paper::pnl(&p.direction, p.entry_price, fill, p.fee_bps);
    p.status = status.into();
    p.resolved_at = Some(ts);
    p.exit_price = Some(fill);
    p.gross_pnl_bps = Some(gross);
    p.pnl_bps = Some(net);
}

struct Signal {
    strategy: &'static str,
    horizon: i64,
    direction: &'static str,
    stop_move: f64,
    evidence: f64,
    score: f64,
    expected_return: f64,
    reason: String,
}

fn build_signals(
    points: &[MarketPoint],
    peer_points: &[MarketPoint],
    exchange: Exchange,
    at: i64,
    c: &LabConfig,
) -> anyhow::Result<(Vec<Signal>, Diagnostics)> {
    // Closed 5-second windows only. A partially formed bucket cannot become a signal.
    let cutoff = at / 5000 * 5000;
    let closed = &points[..points.partition_point(|p| p.ts < cutoff)];
    let buckets = build_buckets(closed, c.max_gap_ms);
    anyhow::ensure!(
        buckets.len() >= 120,
        "Waiting for 10 minutes of fresh quote/trade history"
    );
    let i = buckets.len() - 1;
    anyhow::ensure!(
        at - buckets[i].quote_ts.unwrap_or(0) <= c.max_quote_age_ms + 5000,
        "Market feed is stale; no new paper entries"
    );
    let f = feature_at(&buckets, i)
        .context("Recent history contains missing or invalid quotes; waiting for continuity")?;
    anyhow::ensure!(
        f.spread_bps <= c.max_spread_bps,
        "Spread exceeds the configured limit"
    );
    let peer_closed = &peer_points[..peer_points.partition_point(|p| p.ts < cutoff)];
    let peer_buckets = build_buckets(peer_closed, c.max_gap_ms);
    let peer_score = peer_buckets
        .last()
        .filter(|b| at - b.quote_ts.unwrap_or(0) <= c.max_quote_age_ms + 5000)
        .and_then(|_| feature_at(&peer_buckets, peer_buckets.len() - 1))
        .map(|f| microstructure_score(&f));
    let score = microstructure_score(&f);
    let mut signals = Vec::new();
    let mut diagnostic = Diagnostics {
        evaluated_at: Some(at),
        message: "All six strategies evaluated on closed windows. No setup means no trade.".into(),
        history_minutes: (buckets[i].ts - buckets[0].ts) as f64 / 60_000.,
        history_truncated: points.len() as i64 >= c.max_points,
        lanes: Vec::new(),
    };
    let features: Vec<_> = (0..buckets.len())
        .map(|n| feature_at(&buckets, n))
        .collect();
    for horizon in HORIZONS {
        let steps = (horizon / 5) as usize;
        let sigma = (0.65 * f.volatility_60s + 0.35 * f.volatility_300s).max(0.000_005);
        // The reward must exceed round-trip costs by a margin; do not increase win rate by shrinking TP.
        let costs = 2. * c.fee(exchange) + 2. * c.slippage_bps + f.spread_bps;
        let stop_move = (sigma * (steps as f64).sqrt() * 0.55 / 3.)
            .max(sigma * 1.35)
            .max((costs + c.min_edge_bps) * 1.25 / 30_000.)
            .max(f.spread_bps * 4. / 10_000.)
            .clamp(0.0001, 0.015);
        let analog = pattern_forecast(&buckets, &features, i, steps, &f, c.analog_neighbors);
        for (strategy, _, _) in STRATEGIES {
            let mut evidence = 0.;
            let mut expected = 0.;
            let mut reason = "Waiting for this strategy's market setup".to_string();
            let flow_setup = score.abs() >= 0.30
                && score * f.flow_60s > 0.02
                && score * f.book_imbalance_60s > 0.01
                && score * f.ret_15s > 0.;
            let mut sign = match strategy {
                "flow_follow_v3" if flow_setup => score.signum(),
                "flow_reverse_v3" if flow_setup => -score.signum(),
                "trend_pullback_v3"
                    if f.trend_efficiency >= 0.30
                        && f.ret_300s * f.ret_60s > 0.
                        && f.ret_15s * f.ret_300s < 0.
                        && f.turn * f.ret_300s > 0.
                        && f.flow_300s * f.ret_300s > 0. =>
                {
                    f.ret_300s.signum()
                }
                "breakout_v3"
                    if f.breakout != 0.
                        && f.volume_ratio >= 1.25
                        && f.trade_intensity_ratio >= 1.10
                        && score * f.breakout > 0.20
                        && f.flow_60s * f.breakout > 0.15 =>
                {
                    f.breakout
                }
                "range_reversion_v3"
                    if f.trend_efficiency <= 0.25
                        && f.range_position.abs() >= 0.75
                        && f.flow_15s * f.range_position < -0.10
                        && f.acceleration * f.range_position < 0. =>
                {
                    -f.range_position.signum()
                }
                _ => 0.,
            };
            if strategy == "consensus_v3" {
                reason = "Waiting for independent historical analogs".into();
                if let Some(a) = &analog {
                    let direction_sign = a.expected_return.signum();
                    if a.neighbors.len() >= c.min_analog_samples {
                        reason = "Waiting for fresh agreement from both exchanges and trend".into();
                        if score * direction_sign > 0.2
                            && f.ret_300s * direction_sign > 0.
                            && peer_score.is_some_and(|p| p * direction_sign > 0.2)
                        {
                            let direction = if direction_sign > 0. { "LONG" } else { "SHORT" };
                            let b = barrier_stats(
                                &buckets,
                                &a.neighbors,
                                steps,
                                direction,
                                stop_move * 3.,
                                stop_move,
                                c.fee(exchange),
                                c.slippage_bps,
                            );
                            evidence = b.weighted_strict_win_rate;
                            expected = b.expected_net_bps / 10_000.;
                            reason = format!(
                                "Analog evidence: {} independent samples, {:.1}% net TP wins, lower bound {:.1}%, net edge {:.2} bps",
                                b.samples,
                                evidence * 100.,
                                b.wilson_lower_bound * 100.,
                                b.expected_net_bps
                            );
                            if b.samples >= c.min_analog_samples
                                && evidence >= c.target_win_rate
                                && b.wins as f64 / b.samples as f64 >= c.target_win_rate
                                && b.wilson_lower_bound >= c.min_win_lower_bound
                                && b.expected_net_bps >= c.min_edge_bps
                            {
                                sign = direction_sign;
                            }
                        }
                    }
                }
            }
            diagnostic.lanes.push(LaneDiagnostic {
                strategy: strategy.into(),
                horizon_secs: horizon,
                reason: reason.clone(),
            });
            if sign != 0. {
                let direction = if sign > 0. { "LONG" } else { "SHORT" };
                signals.push(Signal { strategy, horizon, direction, stop_move, evidence,
                    score: if strategy == "flow_reverse_v3" { -score } else { score }, expected_return: expected,
                    reason: if strategy == "consensus_v3" { reason } else { format!("{strategy}: closed-window setup; rule score {:.3} (not a win probability)", score * sign) } });
            }
        }
    }
    Ok((signals, diagnostic))
}
#[derive(Debug, Clone, Default)]
struct Bucket {
    ts: i64,
    price: Option<f64>,
    high_price: Option<f64>,
    low_price: Option<f64>,
    buy_qty: f64,
    sell_qty: f64,
    trade_count: u64,
    bid_price: Option<f64>,
    bid_qty: Option<f64>,
    ask_price: Option<f64>,
    ask_qty: Option<f64>,
    quote_ts: Option<i64>,
    bid_high: Option<f64>,
    bid_low: Option<f64>,
    ask_high: Option<f64>,
    ask_low: Option<f64>,
}

#[derive(Debug, Clone)]
struct Feature {
    ret_15s: f64,
    ret_60s: f64,
    ret_300s: f64,
    acceleration: f64,
    flow_15s: f64,
    flow_60s: f64,
    flow_300s: f64,
    volatility_60s: f64,
    volatility_300s: f64,
    book_imbalance: f64,
    book_imbalance_60s: f64,
    spread_bps: f64,
    volume_ratio: f64,
    trade_intensity_ratio: f64,
    trend_efficiency: f64,
    range_position: f64,
    breakout: f64,
    turn: f64,
}

#[derive(Debug, Clone)]
struct Neighbor {
    index: usize,
    future_return: f64,
    weight: f64,
}

#[derive(Debug, Clone)]
struct AnalogForecast {
    expected_return: f64,
    neighbors: Vec<Neighbor>,
}

#[derive(Debug, Clone, Default)]
struct BarrierStats {
    samples: usize,
    wins: usize,
    losses: usize,
    timeouts: usize,
    weighted_strict_win_rate: f64,
    wilson_lower_bound: f64,
    expected_net_bps: f64,
}

fn build_buckets(points: &[MarketPoint], max_gap_ms: i64) -> Vec<Bucket> {
    let mut map: BTreeMap<i64, Bucket> = BTreeMap::new();
    for point in points {
        let ts = point.ts / 5000 * 5000;
        let b = map.entry(ts).or_insert_with(|| Bucket {
            ts,
            ..Default::default()
        });
        if matches!(point.kind.as_str(), "trade" | "public_trade") {
            if let Some(qty) = point.qty.filter(|q| q.is_finite() && *q > 0.) {
                match point.side.as_deref() {
                    Some("BUY") => b.buy_qty += qty,
                    Some("SELL") => b.sell_qty += qty,
                    _ => {}
                }
                b.trade_count += 1;
            }
        } else if point.kind == "book_ticker"
            && let (Some(bid), Some(ask), Some(bq), Some(aq)) = (
                point.bid_price,
                point.ask_price,
                point.bid_qty,
                point.ask_qty,
            )
            && [bid, ask, bq, aq].iter().all(|v| v.is_finite() && *v > 0.)
            && ask >= bid
        {
            let mid = (bid + ask) / 2.;
            b.price = Some(mid);
            b.high_price = Some(b.high_price.unwrap_or(mid).max(mid));
            b.low_price = Some(b.low_price.unwrap_or(mid).min(mid));
            b.bid_high = Some(b.bid_high.unwrap_or(bid).max(bid));
            b.bid_low = Some(b.bid_low.unwrap_or(bid).min(bid));
            b.ask_high = Some(b.ask_high.unwrap_or(ask).max(ask));
            b.ask_low = Some(b.ask_low.unwrap_or(ask).min(ask));
            b.bid_price = Some(bid);
            b.ask_price = Some(ask);
            b.bid_qty = Some(bq);
            b.ask_qty = Some(aq);
            b.quote_ts = Some(point.ts);
        }
    }
    let (Some(first), Some(last)) = (map.keys().next().copied(), map.keys().next_back().copied())
    else {
        return Vec::new();
    };
    let mut buckets = Vec::new();
    let mut previous: Option<Bucket> = None;
    for ts in (first..=last).step_by(5000) {
        let mut b = map.remove(&ts).unwrap_or_else(|| Bucket {
            ts,
            ..Default::default()
        });
        if b.price.is_none()
            && let Some(prev) = &previous
            && prev.quote_ts.is_some_and(|q| ts + 4999 - q <= max_gap_ms)
        {
            b.price = prev.price;
            b.bid_price = prev.bid_price;
            b.ask_price = prev.ask_price;
            b.bid_qty = prev.bid_qty;
            b.ask_qty = prev.ask_qty;
            b.quote_ts = prev.quote_ts;
            b.high_price = b.price;
            b.low_price = b.price;
            b.bid_high = b.bid_price;
            b.bid_low = b.bid_price;
            b.ask_high = b.ask_price;
            b.ask_low = b.ask_price;
        }
        previous = Some(b.clone());
        buckets.push(b);
    }
    buckets
}

fn feature_at(buckets: &[Bucket], index: usize) -> Option<Feature> {
    if index < 60 {
        return None;
    }

    if buckets[index - 60..=index]
        .iter()
        .any(|b| b.price.is_none())
    {
        return None;
    }
    let price = buckets[index].price?;
    let price_15 = buckets[index - 3].price?;
    let price_60 = buckets[index - 12].price?;
    let price_300 = buckets[index - 60].price?;

    if price <= 0.0 || price_15 <= 0.0 || price_60 <= 0.0 || price_300 <= 0.0 {
        return None;
    }

    let ret_15s = (price / price_15).ln();
    let ret_60s = (price / price_60).ln();
    let ret_300s = (price / price_300).ln();
    let acceleration = ret_15s - ret_60s / 4.0;

    let (buy_15, sell_15) = flow_sum(&buckets[index - 2..=index]);
    let (buy_60, sell_60) = flow_sum(&buckets[index - 11..=index]);
    let (buy_300, sell_300) = flow_sum(&buckets[index - 59..=index]);
    let flow_15s = imbalance(buy_15, sell_15);
    let flow_60s = imbalance(buy_60, sell_60);
    let flow_300s = imbalance(buy_300, sell_300);

    let returns_60 = log_returns(&buckets[index - 12..=index]);
    let returns_300 = log_returns(&buckets[index - 60..=index]);
    let volatility_60s = standard_deviation(&returns_60);
    let volatility_300s = standard_deviation(&returns_300);

    let book_imbalance = bucket_book_imbalance(&buckets[index]).unwrap_or(0.0);
    let book_imbalance_60s = mean_book_imbalance(&buckets[index - 11..=index]);

    let spread_bps = match (buckets[index].bid_price, buckets[index].ask_price) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask >= bid => {
            ((ask - bid) / ((ask + bid) / 2.0)) * 10_000.0
        }
        _ => return None,
    };

    let recent_volume = volume_sum(&buckets[index - 11..=index]);
    let previous_volume = volume_sum(&buckets[index - 59..=index - 12]) / 4.0;
    let volume_ratio = if previous_volume > 0.0 {
        (recent_volume / previous_volume).clamp(0.0, 8.0)
    } else {
        1.0
    };

    let recent_trades = trade_count_sum(&buckets[index - 11..=index]) as f64;
    let previous_trades = trade_count_sum(&buckets[index - 59..=index - 12]) as f64 / 4.0;
    let trade_intensity_ratio = if previous_trades > 0.0 {
        (recent_trades / previous_trades).clamp(0.0, 8.0)
    } else {
        1.0
    };

    let mut path_length = 0.0;
    let mut low = price;
    let mut high = price;
    for i in (index - 60)..=index {
        if let Some(p) = buckets[i].price {
            low = low.min(p);
            high = high.max(p);
        }
        if i > index - 60
            && let (Some(previous), Some(current)) = (buckets[i - 1].price, buckets[i].price)
        {
            path_length += (current - previous).abs();
        }
    }

    let trend_efficiency = if path_length > 0.0 {
        ((price - price_300).abs() / path_length).clamp(0.0, 1.0)
    } else {
        0.0
    };

    let range_position = if high > low {
        (((price - low) / (high - low)) * 2.0 - 1.0).clamp(-1.0, 1.0)
    } else {
        0.0
    };

    let prior_high = buckets[index - 60..index]
        .iter()
        .filter_map(|b| b.high_price)
        .fold(f64::NEG_INFINITY, f64::max);
    let prior_low = buckets[index - 60..index]
        .iter()
        .filter_map(|b| b.low_price)
        .fold(f64::INFINITY, f64::min);
    let breakout = if price > prior_high {
        1.
    } else if price < prior_low {
        -1.
    } else {
        0.
    };
    let turn = (price / buckets[index - 1].price?).ln();
    Some(Feature {
        breakout,
        turn,
        ret_15s,
        ret_60s,
        ret_300s,
        acceleration,
        flow_15s,
        flow_60s,
        flow_300s,
        volatility_60s,
        volatility_300s,
        book_imbalance,
        book_imbalance_60s,
        spread_bps,
        volume_ratio,
        trade_intensity_ratio,
        trend_efficiency,
        range_position,
    })
}

fn pattern_forecast(
    buckets: &[Bucket],
    features: &[Option<Feature>],
    current_index: usize,
    horizon_steps: usize,
    current: &Feature,
    max_neighbors: usize,
) -> Option<AnalogForecast> {
    let latest_candidate = current_index.checked_sub(horizon_steps + 60)?;
    if latest_candidate <= 61 {
        return None;
    }

    let mut missing_prefix = vec![0usize; buckets.len() + 1];
    for (i, bucket) in buckets.iter().enumerate() {
        missing_prefix[i + 1] = missing_prefix[i] + usize::from(bucket.price.is_none());
    }
    let mut candidates = Vec::new();
    for index in 60..latest_candidate {
        if missing_prefix[index + horizon_steps + 1] != missing_prefix[index] {
            continue;
        }
        let Some(feature) = &features[index] else {
            continue;
        };
        let (Some(entry), Some(future)) =
            (buckets[index].price, buckets[index + horizon_steps].price)
        else {
            continue;
        };
        if entry <= 0.0 || future <= 0.0 {
            continue;
        }

        let distance = feature_distance(current, feature);
        let future_return = (future / entry).ln();
        candidates.push((distance, index, future_return));
    }

    candidates.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));

    // Adjacent 5-second samples are highly correlated and must not be treated as
    // independent evidence. De-correlate analogs in time before estimating odds.
    let separation_steps = horizon_steps + 60; // Purge both feature and outcome windows.
    let mut neighbors: Vec<Neighbor> = Vec::with_capacity(max_neighbors);

    for (distance, index, future_return) in candidates {
        if neighbors
            .iter()
            .any(|neighbor| neighbor.index.abs_diff(index) < separation_steps)
        {
            continue;
        }

        let weight = 1.0 / (0.20 + distance);
        neighbors.push(Neighbor {
            index,
            future_return,
            weight,
        });

        if neighbors.len() >= max_neighbors {
            break;
        }
    }

    if neighbors.len() < 8 {
        return None;
    }

    let weight_sum = neighbors.iter().map(|item| item.weight).sum::<f64>();
    if weight_sum <= 0.0 {
        return None;
    }

    let expected_return = neighbors
        .iter()
        .map(|item| item.weight * item.future_return)
        .sum::<f64>()
        / weight_sum;
    Some(AnalogForecast {
        expected_return,
        neighbors,
    })
}

#[allow(clippy::too_many_arguments)]
fn barrier_stats(
    buckets: &[Bucket],
    neighbors: &[Neighbor],
    horizon_steps: usize,
    direction: &str,
    target_move: f64,
    stop_move: f64,
    fee_bps: f64,
    slip_bps: f64,
) -> BarrierStats {
    let mut stats = BarrierStats::default();
    let mut win_weight = 0.;
    let mut total_weight = 0.;
    let mut net_weight = 0.;
    for neighbor in neighbors {
        let start = &buckets[neighbor.index];
        let Some(end) = neighbor
            .index
            .checked_add(horizon_steps)
            .filter(|e| *e < buckets.len())
        else {
            continue;
        };
        // Never invent future paths across a capture outage, or use incomplete labels.
        if buckets[neighbor.index..=end]
            .iter()
            .any(|b| b.price.is_none())
        {
            continue;
        }
        let (Some(bid), Some(ask)) = (start.bid_price, start.ask_price) else {
            continue;
        };
        let entry = paper::entry_fill(direction, bid, ask, slip_bps);
        let sign = if direction == "LONG" { 1. } else { -1. };
        let target = entry * (1. + sign * target_move);
        let stop = entry * (1. - sign * stop_move);
        let mut outcome = "TIMEOUT";
        let last = &buckets[end];
        let Some(mark) = (if direction == "LONG" {
            last.bid_price
        } else {
            last.ask_price
        }) else {
            continue;
        };
        let mut exit = paper::exit_fill(direction, mark, slip_bps);
        for b in &buckets[neighbor.index + 1..=end] {
            let (Some(high), Some(low)) = (if direction == "LONG" {
                (b.bid_high, b.bid_low)
            } else {
                (b.ask_high, b.ask_low)
            }) else {
                continue;
            };
            let high = paper::exit_fill(direction, high, slip_bps);
            let low = paper::exit_fill(direction, low, slip_bps);
            let (target_hit, stop_hit) = if direction == "LONG" {
                (high >= target, low <= stop)
            } else {
                (low <= target, high >= stop)
            };
            // Intrabucket order is unknown: charge the adverse extreme on ambiguous bars.
            if stop_hit {
                outcome = "LOSS";
                exit = if direction == "LONG" { low } else { high };
                break;
            }
            if target_hit {
                outcome = "WIN";
                exit = target;
                break;
            }
        }
        let net = paper::pnl(direction, entry, exit, fee_bps).1;
        stats.samples += 1;
        total_weight += neighbor.weight;
        net_weight += net * neighbor.weight;
        match outcome {
            "WIN" if net > 0. => {
                stats.wins += 1;
                win_weight += neighbor.weight;
            }
            "TIMEOUT" => stats.timeouts += 1,
            _ => stats.losses += 1,
        }
    }
    if total_weight > 0. {
        stats.weighted_strict_win_rate = win_weight / total_weight;
        stats.expected_net_bps = net_weight / total_weight;
    }
    stats.wilson_lower_bound = paper::win_interval(stats.wins as i64, stats.samples as i64).0;
    stats
}

fn feature_distance(left: &Feature, right: &Feature) -> f64 {
    let values = [
        (left.ret_15s - right.ret_15s) / 0.0015,
        (left.ret_60s - right.ret_60s) / 0.0030,
        (left.ret_300s - right.ret_300s) / 0.0060,
        (left.acceleration - right.acceleration) / 0.0010,
        left.flow_15s - right.flow_15s,
        left.flow_60s - right.flow_60s,
        left.flow_300s - right.flow_300s,
        (left.volatility_60s - right.volatility_60s) / 0.0005,
        (left.volatility_300s - right.volatility_300s) / 0.0005,
        left.book_imbalance - right.book_imbalance,
        left.book_imbalance_60s - right.book_imbalance_60s,
        (left.spread_bps - right.spread_bps) / 3.0,
        (left.volume_ratio - right.volume_ratio) / 2.0,
        (left.trade_intensity_ratio - right.trade_intensity_ratio) / 2.0,
        (left.trend_efficiency - right.trend_efficiency) / 0.5,
        left.range_position - right.range_position,
    ];

    (values.iter().map(|value| value * value).sum::<f64>() / values.len() as f64).sqrt()
}

fn microstructure_score(feature: &Feature) -> f64 {
    let sigma_60 = (feature.volatility_60s * (12.0_f64).sqrt()).max(0.000_001);
    let sigma_15 = (feature.volatility_60s * (3.0_f64).sqrt()).max(0.000_001);
    let sigma_300 = (feature.volatility_300s * (60.0_f64).sqrt()).max(0.000_001);

    let momentum = (feature.ret_60s / (2.5 * sigma_60)).clamp(-1.0, 1.0);
    let short_momentum = (feature.ret_15s / (2.2 * sigma_15)).clamp(-1.0, 1.0);
    let trend_momentum = (feature.ret_300s / (2.8 * sigma_300)).clamp(-1.0, 1.0);
    let acceleration = (feature.acceleration / (1.5 * sigma_15)).clamp(-1.0, 1.0);

    let activity_boost = (0.75
        + 0.125 * feature.volume_ratio.min(2.0)
        + 0.125 * feature.trade_intensity_ratio.min(2.0))
    .clamp(0.75, 1.25);

    (0.17 * feature.flow_15s * activity_boost
        + 0.15 * feature.flow_60s * activity_boost
        + 0.08 * feature.flow_300s
        + 0.15 * feature.book_imbalance
        + 0.08 * feature.book_imbalance_60s
        + 0.14 * momentum
        + 0.08 * short_momentum
        + 0.08 * trend_momentum
        + 0.04 * acceleration
        + 0.03 * feature.range_position * feature.trend_efficiency)
        .clamp(-1.0, 1.0)
}

fn flow_sum(buckets: &[Bucket]) -> (f64, f64) {
    buckets.iter().fold((0.0, 0.0), |(buy, sell), bucket| {
        (buy + bucket.buy_qty, sell + bucket.sell_qty)
    })
}

fn volume_sum(buckets: &[Bucket]) -> f64 {
    buckets
        .iter()
        .map(|bucket| bucket.buy_qty + bucket.sell_qty)
        .sum()
}

fn trade_count_sum(buckets: &[Bucket]) -> u64 {
    buckets.iter().map(|bucket| bucket.trade_count).sum()
}

fn log_returns(buckets: &[Bucket]) -> Vec<f64> {
    let mut values = Vec::with_capacity(buckets.len().saturating_sub(1));
    for pair in buckets.windows(2) {
        if let (Some(previous), Some(current)) = (pair[0].price, pair[1].price)
            && previous > 0.0
            && current > 0.0
        {
            values.push((current / previous).ln());
        }
    }
    values
}

fn bucket_book_imbalance(bucket: &Bucket) -> Option<f64> {
    match (bucket.bid_qty, bucket.ask_qty) {
        (Some(bid), Some(ask)) => Some(imbalance(bid, ask)),
        _ => None,
    }
}

fn mean_book_imbalance(buckets: &[Bucket]) -> f64 {
    let values: Vec<f64> = buckets.iter().filter_map(bucket_book_imbalance).collect();
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn imbalance(left: f64, right: f64) -> f64 {
    let total = left + right;
    if total > 0.0 {
        ((left - right) / total).clamp(-1.0, 1.0)
    } else {
        0.0
    }
}

fn standard_deviation(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|value| {
            let difference = value - mean;
            difference * difference
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{event, prediction, quote};

    #[test]
    fn first_executable_crossing_wins_and_later_stop_is_ignored() {
        let mut p = prediction();
        advance_position(
            &mut p,
            &[quote(1, 2000, 104., 104.01), quote(2, 3000, 98., 98.01)],
            3000,
            true,
        );
        assert_eq!(p.status, "WIN");
        assert_eq!(p.resolved_at, Some(2000));
        assert_eq!(p.exit_price, Some(103.));
        assert!(p.pnl_bps.unwrap() < 300.);
    }
    #[test]
    fn short_uses_ask_and_pays_costs() {
        let mut p = prediction();
        p.direction = "SHORT".into();
        p.target_price = 97.;
        p.stop_price = 101.;
        advance_position(&mut p, &[quote(1, 2000, 96.9, 97.01)], 2000, true);
        assert_eq!(p.status, "OPEN"); // A trade/bid below target is not an executable ask fill.
        advance_position(&mut p, &[quote(2, 3000, 96., 96.1)], 3000, true);
        assert_eq!(p.status, "WIN");
        assert!((p.pnl_bps.unwrap() - 280.3).abs() < 1e-8);
    }
    #[test]
    fn adverse_stop_gap_keeps_the_worse_fill() {
        let mut p = prediction();
        advance_position(&mut p, &[quote(1, 2000, 97., 97.01)], 2000, true);
        assert_eq!(p.status, "LOSS");
        assert!(p.exit_price.unwrap() < 97.);
        assert!(p.pnl_bps.unwrap() < -300.);
    }
    #[test]
    fn positive_timeout_counts_as_profit_but_not_target_hit() {
        let mut p = prediction();
        p.last_quote_ts = 60000;
        p.mark_price = Some(100.5);
        advance_position(&mut p, &[], 61000, true);
        assert_eq!(p.status, "TIMEOUT");
        assert!(p.pnl_bps.unwrap() > 0.);
        let s = paper::stats(&[&p], 10000., &LabConfig::default());
        assert_eq!(s.wins, 1);
        assert_eq!(s.target_hits, 0);
    }
    #[test]
    fn disconnected_feed_is_invalid_not_a_fabricated_timeout() {
        let mut p = prediction();
        advance_position(&mut p, &[], 62000, true);
        assert_eq!(p.status, "DATA_GAP");
        assert_eq!(p.pnl_bps, None);
        assert_eq!(p.exit_price, None);
    }
    #[test]
    fn gap_before_a_target_hit_does_not_become_a_win() {
        let mut p = prediction();
        advance_position(&mut p, &[quote(1, 25000, 110., 110.1)], 25000, true);
        assert_eq!(p.status, "DATA_GAP");
    }
    #[test]
    fn insufficient_book_liquidity_is_flagged() {
        let mut p = prediction();
        let mut q = quote(1, 2000, 104., 104.01);
        q.bid_qty = 0.001;
        advance_position(&mut p, &[q], 2000, true);
        assert_eq!(p.status, "DATA_GAP");
    }
    #[test]
    fn thin_book_during_hold_does_not_invent_a_fill_or_close_the_trade() {
        let mut p = prediction();
        let mut q = quote(1, 2000, 100., 100.01);
        q.bid_qty = 0.001;
        advance_position(&mut p, &[q], 2000, true);
        assert_eq!(p.status, "OPEN");
        assert_eq!(p.mark_price, None);
        let s = paper::stats(&[&p], 10000., &LabConfig::default());
        assert_eq!(s.unmarked_open, 1);
        advance_position(&mut p, &[quote(2, 3000, 104., 104.01)], 3000, true);
        assert_eq!(p.status, "WIN");
    }
    #[test]
    fn delayed_quote_is_not_used_as_an_executable_fill() {
        let mut p = prediction();
        let mut q = quote(1, 12000, 104., 104.01);
        q.event_ts = 1000;
        advance_position(&mut p, &[q], 12000, true);
        assert_eq!(p.status, "DATA_GAP");
    }
    #[test]
    fn paginated_resolver_does_not_timeout_before_consuming_backlog() {
        let mut p = prediction();
        advance_position(&mut p, &[quote(1, 2000, 100., 100.01)], 62000, false);
        assert_eq!(p.status, "OPEN");
        assert_eq!(p.cursor_id, 1);
        advance_position(&mut p, &[quote(2, 3000, 104., 104.01)], 62000, true);
        assert_eq!(p.status, "WIN");
    }
    #[test]
    fn data_after_deadline_cannot_create_a_win() {
        let mut p = prediction();
        p.last_quote_ts = 60000;
        p.mark_price = Some(100.);
        advance_position(&mut p, &[quote(1, 62000, 104., 104.01)], 62000, true);
        assert_eq!(p.status, "TIMEOUT");
    }
    fn point(ts: i64, price: f64) -> MarketPoint {
        MarketPoint {
            kind: "book_ticker".into(),
            ts,
            qty: None,
            side: None,
            bid_price: Some(price - 0.005),
            ask_price: Some(price + 0.005),
            bid_qty: Some(200.),
            ask_qty: Some(40.),
        }
    }
    #[test]
    fn missing_history_is_not_filled_indefinitely() {
        let buckets = build_buckets(&[point(0, 100.), point(60000, 101.)], 15000);
        assert_eq!(buckets.len(), 13);
        assert!(buckets[6].price.is_none());
    }
    #[test]
    fn zero_or_crossed_book_is_not_a_free_spread() {
        let mut p = point(1000, 100.);
        p.ask_price = Some(90.);
        assert!(build_buckets(&[p], 15000)[0].price.is_none());
    }
    #[test]
    fn analogs_have_disjoint_feature_and_outcome_windows() {
        let points: Vec<_> = (0..3000)
            .map(|n| point(n * 5000, 100. + (n as f64 * 0.01).sin()))
            .collect();
        let buckets = build_buckets(&points, 15000);
        let features: Vec<_> = (0..buckets.len())
            .map(|i| feature_at(&buckets, i))
            .collect();
        let a = pattern_forecast(
            &buckets,
            &features,
            2999,
            60,
            features[2999].as_ref().unwrap(),
            80,
        )
        .unwrap();
        for left in &a.neighbors {
            assert!(left.index + 60 < 2999 - 60);
            for right in &a.neighbors {
                assert!(left.index == right.index || left.index.abs_diff(right.index) >= 120);
            }
        }
    }
    #[test]
    fn ambiguous_analog_bar_is_a_cost_aware_loss() {
        let mut buckets = build_buckets(&[point(0, 100.), point(5000, 100.)], 15000);
        buckets[1].bid_high = Some(104.);
        buckets[1].bid_low = Some(98.);
        let n = vec![Neighbor {
            index: 0,
            future_return: 0.,
            weight: 1.,
        }];
        let b = barrier_stats(&buckets, &n, 1, "LONG", 0.03, 0.01, 10., 1.);
        assert_eq!(b.losses, 1);
        assert_eq!(b.wins, 0);
        assert!(b.expected_net_bps < 0.);
    }
    #[test]
    fn incomplete_historical_path_is_not_an_analog_win() {
        let buckets = build_buckets(&[point(0, 100.), point(60000, 104.)], 15000);
        let n = vec![Neighbor {
            index: 0,
            future_return: 0.04,
            weight: 1.,
        }];
        let b = barrier_stats(&buckets, &n, 12, "LONG", 0.03, 0.01, 10., 1.);
        assert_eq!(b.samples, 0);
    }
    #[test]
    fn partial_or_future_buckets_do_not_change_the_signal() {
        let at = 655000;
        let mut points: Vec<_> = (0..655)
            .map(|n| point(n * 1000, 100. + n as f64 * 0.002))
            .collect();
        let c = LabConfig::default();
        let (a, _) = build_signals(&points, &points, Exchange::Binance, at, &c).unwrap();
        points.push(point(at + 1000, 10000.));
        let (b, _) = build_signals(&points, &points, Exchange::Binance, at, &c).unwrap();
        let shape = |items: Vec<Signal>| {
            items
                .into_iter()
                .map(|s| (s.strategy, s.horizon, s.direction, s.stop_move))
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(a), shape(b));
    }
    #[tokio::test]
    async fn both_exchanges_open_independent_forward_and_reverse_lanes_without_duplicates() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let manager = AnalysisManager::new(db.clone());
        db.register_config(&manager.config).await.unwrap();
        let at = now_ms();
        let start = (at - 650000) / 1000 * 1000;
        for exchange in [Exchange::Binance, Exchange::Bybit] {
            for ts in (start..at).step_by(1000) {
                let price = 100. + (ts - start) as f64 / 500000.;
                db.insert_event(event(exchange, "book_ticker", ts, price))
                    .await
                    .unwrap();
                db.insert_event(event(exchange, "trade", ts, price))
                    .await
                    .unwrap();
            }
        }
        db.flush().await.unwrap();
        for exchange in [Exchange::Binance, Exchange::Bybit] {
            manager.analyze_once(exchange, "BTCUSDT").await.unwrap();
            manager.analyze_once(exchange, "BTCUSDT").await.unwrap();
            let rows = db
                .experiment_predictions(exchange, "BTCUSDT", &manager.config.id())
                .await
                .unwrap();
            assert_eq!(rows.len(), 8);
            assert_eq!(
                rows.iter()
                    .filter(|p| p.strategy == "flow_follow_v3" && p.direction == "LONG")
                    .count(),
                4
            );
            assert_eq!(
                rows.iter()
                    .filter(|p| p.strategy == "flow_reverse_v3" && p.direction == "SHORT")
                    .count(),
                4
            );
            for p in rows {
                assert!(
                    ((p.target_price - p.entry_price).abs() / (p.stop_price - p.entry_price).abs()
                        - 3.)
                        .abs()
                        < 1e-8
                );
                assert!(p.created_at >= at);
                assert!(p.fee_bps > 0.);
            }
            assert_eq!(manager.diagnostics(exchange).await.lanes.len(), 24);
        }
    }
}
