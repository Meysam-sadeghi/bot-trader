use crate::{
    db::Database,
    model::{Exchange, MarketPoint, Prediction, now_ms, strategy_mode},
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
    risk_reward: f64,
    interval_secs: u64,
    max_position_secs: i64,
    lookback_hours: i64,
    max_analysis_points: i64,
    target_strict_win_rate: f64,
    min_signal_confidence: f64,
    min_win_lower_bound: f64,
    min_edge_bps: f64,
    max_spread_bps: f64,
    min_analog_samples: usize,
    analog_neighbors: usize,
    reentry_cooldown_secs: i64,
}

impl AnalysisManager {
    pub fn new(db: Database) -> Self {
        // Paper trades intentionally keep a fixed 1:3 risk/reward ratio.
        // V2 improves selectivity; it never changes the payoff ratio to inflate wins.
        let risk_reward = 3.0;

        let interval_secs = env_u64("ANALYSIS_INTERVAL_SECS", 30).clamp(5, 3_600);
        let max_position_secs = env_i64("MAX_POSITION_SECS", 3_600).clamp(60, 3_600);
        let lookback_hours = env_i64("ANALYSIS_LOOKBACK_HOURS", 24).clamp(4, 168);
        let max_analysis_points =
            env_i64("ANALYSIS_MAX_POINTS", 750_000).clamp(50_000, 2_000_000);

        // This is a target/gate for historical analog performance, not a guarantee
        // that live forward trades will win at this rate.
        let target_strict_win_rate =
            env_f64("TARGET_STRICT_WIN_RATE", 0.80).clamp(0.26, 0.95);
        let min_signal_confidence =
            env_f64("MIN_SIGNAL_CONFIDENCE", 0.72).clamp(0.50, 0.99);
        let min_win_lower_bound =
            env_f64("MIN_WIN_LOWER_BOUND", 0.55).clamp(0.25, 0.90);
        let min_edge_bps = env_f64("MIN_SIGNAL_EDGE_BPS", 2.0).clamp(0.0, 100.0);
        let max_spread_bps = env_f64("MAX_SPREAD_BPS", 3.0).clamp(0.1, 50.0);
        let min_analog_samples = env_usize("MIN_ANALOG_SAMPLES", 24).clamp(8, 200);
        let analog_neighbors = env_usize("ANALOG_NEIGHBORS", 80)
            .clamp(min_analog_samples, 250);
        let reentry_cooldown_secs =
            env_i64("REENTRY_COOLDOWN_SECS", 180).clamp(0, 3_600);

        Self {
            db,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            risk_reward,
            interval_secs,
            max_position_secs,
            lookback_hours,
            max_analysis_points,
            target_strict_win_rate,
            min_signal_confidence,
            min_win_lower_bound,
            min_edge_bps,
            max_spread_bps,
            min_analog_samples,
            analog_neighbors,
            reentry_cooldown_secs,
        }
    }

    pub async fn start(&self, exchange: Exchange, symbol: String) -> bool {
        {
            let tasks = self.tasks.read().await;
            if tasks.contains_key(&exchange) {
                return false;
            }
        }

        let manager = self.clone();
        let task = tokio::spawn(async move {
            loop {
                if let Err(error) = manager.analyze_once(exchange, &symbol).await {
                    tracing::warn!(exchange = %exchange, %error, "analysis iteration skipped");
                }
                tokio::time::sleep(Duration::from_secs(manager.interval_secs)).await;
            }
        });

        self.tasks.write().await.insert(exchange, task.abort_handle());
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

    pub fn spawn_resolver(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(error) = manager.resolve_open_predictions().await {
                    tracing::error!(%error, "prediction resolver failed");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    async fn analyze_once(&self, exchange: Exchange, symbol: &str) -> anyhow::Result<()> {
        let now = now_ms();
        let since = now - self.lookback_hours * 60 * 60 * 1_000;
        let points = self
            .db
            .load_points(exchange, symbol, since, self.max_analysis_points)
            .await
            .context("load market history")?;

        let buckets = build_buckets(&points);
        if buckets.len() < 120 {
            anyhow::bail!("need at least 10 minutes of continuous 5-second history");
        }

        let current_index = buckets.len() - 1;
        let current = feature_at(&buckets, current_index)
            .context("insufficient feature history")?;
        let entry_price = buckets[current_index]
            .price
            .context("no current price")?;

        if current.spread_bps > self.max_spread_bps {
            tracing::debug!(
                exchange = %exchange,
                symbol,
                spread_bps = current.spread_bps,
                max_spread_bps = self.max_spread_bps,
                "selective gate: spread too wide"
            );
            return Ok(());
        }

        let micro_score = microstructure_score(&current);
        let peer_score = self.peer_micro_score(exchange, symbol, now).await;
        let peer_alignment = peer_alignment(micro_score, peer_score);

        // One active paper position per horizon prevents uncontrolled position stacking.
        // The 60-minute horizon is also the hard maximum holding time.
        let horizons = [60_i64, 300_i64, 900_i64, 3_600_i64];

        for horizon_secs in horizons {
            if self
                .db
                .has_open_prediction(exchange, symbol, horizon_secs)
                .await?
            {
                continue;
            }

            if self.reentry_cooldown_secs > 0
                && self
                    .db
                    .has_recent_prediction(
                        exchange,
                        symbol,
                        horizon_secs,
                        now - self.reentry_cooldown_secs * 1_000,
                    )
                    .await?
            {
                continue;
            }

            let horizon_steps = (horizon_secs / 5) as usize;
            let Some(analog) = pattern_forecast(
                &buckets,
                current_index,
                horizon_steps,
                &current,
                self.analog_neighbors,
            ) else {
                continue;
            };

            if analog.neighbors.len() < self.min_analog_samples {
                continue;
            }

            // Feature volatility is the standard deviation of 5-second returns.
            // Scale it by sqrt(number of 5-second steps), not sqrt(minutes).
            let sigma_5s =
                (0.65 * current.volatility_60s + 0.35 * current.volatility_300s)
                    .max(0.000_005);
            let horizon_sigma = sigma_5s * (horizon_steps as f64).sqrt();

            let micro_expected = micro_score * horizon_sigma * 0.35;
            let expected_return = 0.78 * analog.expected_return + 0.22 * micro_expected;

            let micro_prob_up = 1.0 / (1.0 + (-2.8 * micro_score).exp());
            let trend_z = if current.volatility_300s > 0.0 {
                current.ret_300s
                    / (current.volatility_300s * (60.0_f64).sqrt()).max(0.000_001)
            } else {
                0.0
            };
            let trend_prob_up = 1.0 / (1.0 + (-1.4 * trend_z.clamp(-4.0, 4.0)).exp());
            let probability_up = (0.72 * analog.probability_up
                + 0.18 * micro_prob_up
                + 0.10 * trend_prob_up)
                .clamp(0.01, 0.99);

            let model_direction = if expected_return >= 0.0 { "LONG" } else { "SHORT" };
            let raw_directional_probability = if model_direction == "LONG" {
                probability_up
            } else {
                1.0 - probability_up
            };

            // Binance remains the deliberate contrarian experiment requested earlier.
            // V2 changes *whether* a trade is allowed, not the inversion rule itself.
            let inverted = exchange == Exchange::Binance;
            let direction = strategy_direction(exchange, model_direction);
            let strategy_expected_return = if inverted {
                -expected_return
            } else {
                expected_return
            };
            let strategy_score = if inverted {
                -micro_score
            } else {
                micro_score
            };

            let forecast_target = expected_return.abs().max(horizon_sigma * 0.55);
            let spread_floor = (current.spread_bps / 10_000.0) * 4.0;
            let micro_noise_floor = sigma_5s * 1.35;
            let stop_move = (forecast_target / self.risk_reward)
                .max(spread_floor)
                .max(micro_noise_floor)
                .clamp(0.000_10, 0.015);
            let target_move = (stop_move * self.risk_reward).clamp(0.000_30, 0.045);

            let barrier = barrier_stats(
                &buckets,
                &analog.neighbors,
                horizon_steps,
                direction,
                target_move,
                stop_move,
            );

            if barrier.samples < self.min_analog_samples {
                continue;
            }

            let edge_bps = strategy_expected_return.abs() * 10_000.0;
            let confidence = (
                0.58 * barrier.weighted_strict_win_rate
                    + 0.16 * barrier.wilson_lower_bound
                    + 0.12 * analog.quality
                    + 0.09 * raw_directional_probability
                    + 0.05 * peer_alignment
            )
                .clamp(0.0, 0.99);

            let accepted = barrier.weighted_strict_win_rate >= self.target_strict_win_rate
                && barrier.wilson_lower_bound >= self.min_win_lower_bound
                && confidence >= self.min_signal_confidence
                && edge_bps >= self.min_edge_bps
                && stop_move * 10_000.0 >= current.spread_bps * 3.0;

            if !accepted {
                tracing::debug!(
                    exchange = %exchange,
                    symbol,
                    horizon_secs,
                    direction,
                    analog_samples = barrier.samples,
                    analog_win_rate = barrier.weighted_strict_win_rate,
                    analog_win_lb = barrier.wilson_lower_bound,
                    confidence,
                    edge_bps,
                    spread_bps = current.spread_bps,
                    peer_alignment,
                    "selective gate rejected paper trade"
                );
                continue;
            }

            let (target_price, stop_price) = if direction == "LONG" {
                (
                    entry_price * (1.0 + target_move),
                    entry_price * (1.0 - stop_move),
                )
            } else {
                (
                    entry_price * (1.0 - target_move),
                    entry_price * (1.0 + stop_move),
                )
            };

            tracing::info!(
                exchange = %exchange,
                symbol,
                horizon_secs,
                direction,
                confidence,
                analog_win_rate = barrier.weighted_strict_win_rate,
                analog_win_lb = barrier.wilson_lower_bound,
                analog_samples = barrier.samples,
                target_bps = target_move * 10_000.0,
                stop_bps = stop_move * 10_000.0,
                "selective V2 paper trade accepted"
            );

            let prediction = Prediction {
                id: Uuid::new_v4().to_string(),
                exchange,
                symbol: symbol.to_string(),
                created_at: now,
                horizon_secs,
                direction: direction.to_string(),
                entry_price,
                target_price,
                stop_price,
                confidence,
                score: strategy_score,
                expected_return: strategy_expected_return,
                strategy: strategy_mode(exchange).to_string(),
                status: "OPEN".to_string(),
                resolved_at: None,
                exit_price: None,
                pnl_bps: None,
            };
            self.db.insert_prediction(&prediction).await?;
        }

        Ok(())
    }

    async fn peer_micro_score(
        &self,
        exchange: Exchange,
        symbol: &str,
        now: i64,
    ) -> Option<f64> {
        let peer = match exchange {
            Exchange::Binance => Exchange::Bybit,
            Exchange::Bybit => Exchange::Binance,
        };

        let points = self
            .db
            .load_points(peer, symbol, now - 15 * 60 * 1_000, 120_000)
            .await
            .ok()?;
        let buckets = build_buckets(&points);
        if buckets.len() < 60 {
            return None;
        }
        let feature = feature_at(&buckets, buckets.len() - 1)?;
        Some(microstructure_score(&feature))
    }

    async fn resolve_open_predictions(&self) -> anyhow::Result<()> {
        let now = now_ms();
        for prediction in self.db.open_predictions().await? {
            let hold_secs = prediction.horizon_secs.min(self.max_position_secs);
            let deadline = prediction.created_at + hold_secs * 1_000;
            let evaluation_end = now.min(deadline);

            if let Some((ts, price, won)) = self
                .db
                .first_crossing(&prediction, evaluation_end)
                .await?
            {
                let pnl_bps = paper_pnl_bps(&prediction, price);
                self.db
                    .resolve_prediction(
                        &prediction.id,
                        if won { "WIN" } else { "LOSS" },
                        ts,
                        price,
                        pnl_bps,
                    )
                    .await?;
                continue;
            }

            if now >= deadline
                && let Some(price) = self
                    .db
                    .price_at_or_before(prediction.exchange, &prediction.symbol, deadline)
                    .await?
            {
                let pnl_bps = paper_pnl_bps(&prediction, price);
                self.db
                    .resolve_prediction(
                        &prediction.id,
                        "TIMEOUT",
                        deadline,
                        price,
                        pnl_bps,
                    )
                    .await?;
            }
        }
        Ok(())
    }
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
}

#[derive(Debug, Clone)]
struct Neighbor {
    index: usize,
    distance: f64,
    future_return: f64,
    weight: f64,
}

#[derive(Debug, Clone)]
struct AnalogForecast {
    expected_return: f64,
    probability_up: f64,
    quality: f64,
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
}

fn build_buckets(points: &[MarketPoint]) -> Vec<Bucket> {
    let mut map: BTreeMap<i64, Bucket> = BTreeMap::new();

    for point in points {
        let bucket_ts = (point.ts / 5_000) * 5_000;
        let bucket = map.entry(bucket_ts).or_insert_with(|| Bucket {
            ts: bucket_ts,
            ..Bucket::default()
        });

        if let Some(price) = point.price {
            bucket.price = Some(price);
        }

        // Binance emits both trade and aggTrade for overlapping executions.
        // Use the raw trade stream only so taker flow is not double counted.
        if matches!(point.kind.as_str(), "trade" | "public_trade") {
            let qty = point.qty.unwrap_or(0.0);
            match point.side.as_deref() {
                Some("BUY") => bucket.buy_qty += qty,
                Some("SELL") => bucket.sell_qty += qty,
                _ => {}
            }
            bucket.trade_count = bucket.trade_count.saturating_add(1);

            if let Some(price) = point.price {
                bucket.high_price = Some(
                    bucket
                        .high_price
                        .map(|current| current.max(price))
                        .unwrap_or(price),
                );
                bucket.low_price = Some(
                    bucket
                        .low_price
                        .map(|current| current.min(price))
                        .unwrap_or(price),
                );
            }
        }

        if point.kind == "book_ticker" {
            bucket.bid_price = point.bid_price.or(bucket.bid_price);
            bucket.bid_qty = point.bid_qty.or(bucket.bid_qty);
            bucket.ask_price = point.ask_price.or(bucket.ask_price);
            bucket.ask_qty = point.ask_qty.or(bucket.ask_qty);
        }
    }

    let Some(first_ts) = map.keys().next().copied() else {
        return Vec::new();
    };
    let Some(last_ts) = map.keys().next_back().copied() else {
        return Vec::new();
    };

    // Fill missing 5-second windows. Horizon steps must represent clock time,
    // not "number of buckets that happened to contain an event".
    let mut buckets = Vec::with_capacity(((last_ts - first_ts) / 5_000 + 1) as usize);
    let mut last_price = None;
    let mut last_bid = None;
    let mut last_bid_qty = None;
    let mut last_ask = None;
    let mut last_ask_qty = None;
    let mut ts = first_ts;

    while ts <= last_ts {
        let mut bucket = map.remove(&ts).unwrap_or_else(|| Bucket {
            ts,
            ..Bucket::default()
        });

        bucket.price = bucket.price.or(last_price);
        bucket.bid_price = bucket.bid_price.or(last_bid);
        bucket.bid_qty = bucket.bid_qty.or(last_bid_qty);
        bucket.ask_price = bucket.ask_price.or(last_ask);
        bucket.ask_qty = bucket.ask_qty.or(last_ask_qty);

        if bucket.high_price.is_none() {
            bucket.high_price = bucket.price;
        }
        if bucket.low_price.is_none() {
            bucket.low_price = bucket.price;
        }

        last_price = bucket.price;
        last_bid = bucket.bid_price;
        last_bid_qty = bucket.bid_qty;
        last_ask = bucket.ask_price;
        last_ask_qty = bucket.ask_qty;

        buckets.push(bucket);
        ts += 5_000;
    }

    buckets
}

fn feature_at(buckets: &[Bucket], index: usize) -> Option<Feature> {
    if index < 60 {
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

    let returns_60 = log_returns(&buckets[index - 11..=index]);
    let returns_300 = log_returns(&buckets[index - 59..=index]);
    let volatility_60s = standard_deviation(&returns_60);
    let volatility_300s = standard_deviation(&returns_300);

    let book_imbalance = bucket_book_imbalance(&buckets[index]).unwrap_or(0.0);
    let book_imbalance_60s = mean_book_imbalance(&buckets[index - 11..=index]);

    let spread_bps = match (buckets[index].bid_price, buckets[index].ask_price) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask >= bid => {
            ((ask - bid) / ((ask + bid) / 2.0)) * 10_000.0
        }
        _ => 0.0,
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
    for i in (index - 59)..=index {
        if let Some(p) = buckets[i].price {
            low = low.min(p);
            high = high.max(p);
        }
        if i > index - 59
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

    Some(Feature {
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
    current_index: usize,
    horizon_steps: usize,
    current: &Feature,
    max_neighbors: usize,
) -> Option<AnalogForecast> {
    let latest_candidate = current_index.checked_sub(horizon_steps)?;
    if latest_candidate <= 61 {
        return None;
    }

    let mut candidates = Vec::new();
    for index in 60..latest_candidate {
        let Some(feature) = feature_at(buckets, index) else {
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

        let distance = feature_distance(current, &feature);
        let future_return = (future / entry).ln();
        candidates.push((distance, index, future_return));
    }

    candidates.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
    });

    // Adjacent 5-second samples are highly correlated and must not be treated as
    // independent evidence. De-correlate analogs in time before estimating odds.
    let separation_steps = (horizon_steps / 4).clamp(6, 180);
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
            distance,
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
    let probability_up = neighbors
        .iter()
        .filter(|item| item.future_return > 0.0)
        .map(|item| item.weight)
        .sum::<f64>()
        / weight_sum;
    let mean_distance = neighbors
        .iter()
        .map(|item| item.weight * item.distance)
        .sum::<f64>()
        / weight_sum;
    let quality = (1.0 / (1.0 + mean_distance)).clamp(0.0, 1.0);

    Some(AnalogForecast {
        expected_return,
        probability_up,
        quality,
        neighbors,
    })
}

fn barrier_stats(
    buckets: &[Bucket],
    neighbors: &[Neighbor],
    horizon_steps: usize,
    direction: &str,
    target_move: f64,
    stop_move: f64,
) -> BarrierStats {
    let mut stats = BarrierStats::default();
    let mut win_weight = 0.0;
    let mut total_weight = 0.0;

    for neighbor in neighbors {
        let Some(entry) = buckets[neighbor.index].price else {
            continue;
        };
        if entry <= 0.0 {
            continue;
        }

        stats.samples += 1;
        total_weight += neighbor.weight;

        let (target, stop) = if direction == "LONG" {
            (entry * (1.0 + target_move), entry * (1.0 - stop_move))
        } else {
            (entry * (1.0 - target_move), entry * (1.0 + stop_move))
        };

        let mut outcome = None;
        let end = (neighbor.index + horizon_steps).min(buckets.len() - 1);
        for bucket in &buckets[(neighbor.index + 1)..=end] {
            let high = bucket.high_price.or(bucket.price).unwrap_or(entry);
            let low = bucket.low_price.or(bucket.price).unwrap_or(entry);

            let (target_hit, stop_hit) = if direction == "LONG" {
                (high >= target, low <= stop)
            } else {
                (low <= target, high >= stop)
            };

            // If both barriers fit inside the same 5-second bucket, event ordering
            // is unknown. Count it as a loss to avoid optimistic backtest bias.
            if stop_hit {
                outcome = Some(false);
                break;
            }
            if target_hit {
                outcome = Some(true);
                break;
            }
        }

        match outcome {
            Some(true) => {
                stats.wins += 1;
                win_weight += neighbor.weight;
            }
            Some(false) => stats.losses += 1,
            None => stats.timeouts += 1,
        }
    }

    stats.weighted_strict_win_rate = if total_weight > 0.0 {
        win_weight / total_weight
    } else {
        0.0
    };
    stats.wilson_lower_bound = wilson_lower_bound(stats.wins, stats.samples, 1.96);
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
    let acceleration =
        (feature.acceleration / (1.5 * sigma_15)).clamp(-1.0, 1.0);

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

fn wilson_lower_bound(successes: usize, trials: usize, z: f64) -> f64 {
    if trials == 0 {
        return 0.0;
    }

    let n = trials as f64;
    let p = successes as f64 / n;
    let z2 = z * z;
    let denominator = 1.0 + z2 / n;
    let center = p + z2 / (2.0 * n);
    let margin = z * ((p * (1.0 - p) + z2 / (4.0 * n)) / n).sqrt();
    ((center - margin) / denominator).clamp(0.0, 1.0)
}

fn peer_alignment(primary: f64, peer: Option<f64>) -> f64 {
    let Some(peer) = peer else {
        return 0.50;
    };
    if primary.abs() < 0.05 || peer.abs() < 0.05 {
        return 0.50;
    }
    if primary.signum() == peer.signum() {
        (0.70 + 0.30 * primary.abs().min(peer.abs())).clamp(0.0, 1.0)
    } else {
        (0.30 - 0.20 * primary.abs().min(peer.abs())).clamp(0.0, 1.0)
    }
}

fn strategy_direction(exchange: Exchange, model_direction: &'static str) -> &'static str {
    if exchange == Exchange::Binance {
        if model_direction == "LONG" { "SHORT" } else { "LONG" }
    } else {
        model_direction
    }
}

fn paper_pnl_bps(prediction: &Prediction, exit_price: f64) -> f64 {
    if prediction.entry_price <= 0.0 {
        return 0.0;
    }
    let raw = if prediction.direction == "LONG" {
        (exit_price - prediction.entry_price) / prediction.entry_price
    } else {
        (prediction.entry_price - exit_price) / prediction.entry_price
    };
    raw * 10_000.0
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::{
        BarrierStats, Bucket, Neighbor, barrier_stats, imbalance, peer_alignment,
        strategy_direction, wilson_lower_bound,
    };
    use crate::model::Exchange;

    #[test]
    fn imbalance_is_bounded() {
        assert_eq!(imbalance(10.0, 0.0), 1.0);
        assert_eq!(imbalance(0.0, 10.0), -1.0);
        assert_eq!(imbalance(0.0, 0.0), 0.0);
    }

    #[test]
    fn binance_is_contrarian_but_bybit_is_normal() {
        assert_eq!(strategy_direction(Exchange::Binance, "LONG"), "SHORT");
        assert_eq!(strategy_direction(Exchange::Binance, "SHORT"), "LONG");
        assert_eq!(strategy_direction(Exchange::Bybit, "LONG"), "LONG");
        assert_eq!(strategy_direction(Exchange::Bybit, "SHORT"), "SHORT");
    }

    #[test]
    fn wilson_bound_is_conservative() {
        let bound = wilson_lower_bound(24, 30, 1.96);
        assert!(bound > 0.60);
        assert!(bound < 0.80);
    }

    #[test]
    fn peer_alignment_rewards_same_direction() {
        assert!(peer_alignment(0.7, Some(0.6)) > 0.7);
        assert!(peer_alignment(0.7, Some(-0.6)) < 0.3);
        assert_eq!(peer_alignment(0.7, None), 0.5);
    }

    #[test]
    fn barrier_test_counts_target_before_stop() {
        let mut buckets = Vec::new();
        for i in 0..5 {
            buckets.push(Bucket {
                ts: i * 5_000,
                price: Some(100.0),
                high_price: Some(if i == 2 { 103.5 } else { 100.0 }),
                low_price: Some(100.0),
                ..Bucket::default()
            });
        }
        let neighbors = vec![Neighbor {
            index: 0,
            distance: 0.1,
            future_return: 0.03,
            weight: 1.0,
        }];
        let stats: BarrierStats = barrier_stats(&buckets, &neighbors, 4, "LONG", 0.03, 0.01);
        assert_eq!(stats.wins, 1);
        assert_eq!(stats.losses, 0);
        assert_eq!(stats.timeouts, 0);
    }

    #[test]
    fn ambiguous_same_bucket_is_counted_as_loss() {
        let mut buckets = Vec::new();
        for i in 0..3 {
            buckets.push(Bucket {
                ts: i * 5_000,
                price: Some(100.0),
                high_price: Some(if i == 1 { 104.0 } else { 100.0 }),
                low_price: Some(if i == 1 { 98.0 } else { 100.0 }),
                ..Bucket::default()
            });
        }
        let neighbors = vec![Neighbor {
            index: 0,
            distance: 0.1,
            future_return: 0.0,
            weight: 1.0,
        }];
        let stats = barrier_stats(&buckets, &neighbors, 2, "LONG", 0.03, 0.01);
        assert_eq!(stats.wins, 0);
        assert_eq!(stats.losses, 1);
    }
}
