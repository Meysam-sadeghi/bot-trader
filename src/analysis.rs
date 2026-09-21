use crate::{
    db::Database,
    model::{Exchange, MarketPoint, Prediction, now_ms},
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
}

impl AnalysisManager {
    pub fn new(db: Database) -> Self {
        // Paper trades intentionally use a fixed 1:3 risk/reward ratio.
        let risk_reward = 3.0;

        let interval_secs = std::env::var("ANALYSIS_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= 5)
            .unwrap_or(30);

        let max_position_secs = std::env::var("MAX_POSITION_SECS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(3_600)
            .clamp(60, 3_600);

        Self {
            db,
            tasks: Arc::new(RwLock::new(HashMap::new())),
            risk_reward,
            interval_secs,
            max_position_secs,
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
        let points = self
            .db
            .load_points(exchange, symbol, now - 4 * 60 * 60 * 1000, 300_000)
            .await
            .context("load market history")?;

        let buckets = build_buckets(&points);
        if buckets.len() < 30 {
            anyhow::bail!("need more captured history before analysis");
        }

        let current_index = buckets.len() - 1;
        let current = feature_at(&buckets, current_index)
            .context("insufficient feature history")?;
        let entry_price = buckets[current_index]
            .price
            .context("no current price")?;

        let micro_score = microstructure_score(&current);
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
            let horizon_steps = (horizon_secs / 5) as usize;
            let analog = pattern_forecast(&buckets, current_index, horizon_steps, &current);

            let volatility_scale = current.volatility.max(0.00015)
                * (horizon_secs as f64 / 60.0).sqrt();
            let micro_expected = micro_score * volatility_scale;
            let (expected_return, analog_prob_up) = match analog {
                Some((forecast, probability_up)) => {
                    (0.65 * forecast + 0.35 * micro_expected, Some(probability_up))
                }
                None => (micro_expected, None),
            };

            let micro_prob_up = 1.0 / (1.0 + (-2.5 * micro_score).exp());
            let probability_up = analog_prob_up
                .map(|probability| 0.7 * probability + 0.3 * micro_prob_up)
                .unwrap_or(micro_prob_up)
                .clamp(0.01, 0.99);

            let direction = if expected_return >= 0.0 { "LONG" } else { "SHORT" };
            let directional_probability = if direction == "LONG" {
                probability_up
            } else {
                1.0 - probability_up
            };
            let confidence = (0.75 * directional_probability
                + 0.25 * (0.5 + 0.5 * micro_score.abs()))
                .clamp(0.50, 0.99);

            let target_move = expected_return
                .abs()
                .max(volatility_scale * 0.75)
                .clamp(0.00035, 0.02);
            let stop_move = (target_move / self.risk_reward).max(0.0001);

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
                score: micro_score,
                expected_return,
                status: "OPEN".to_string(),
                resolved_at: None,
                exit_price: None,
                pnl_bps: None,
            };
            self.db.insert_prediction(&prediction).await?;
        }

        Ok(())
    }

    async fn resolve_open_predictions(&self) -> anyhow::Result<()> {
        let now = now_ms();
        for prediction in self.db.open_predictions().await? {
            let hold_secs = prediction.horizon_secs.min(self.max_position_secs);
            let deadline = prediction.created_at + hold_secs * 1000;
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
    buy_qty: f64,
    sell_qty: f64,
    bid_price: Option<f64>,
    bid_qty: Option<f64>,
    ask_price: Option<f64>,
    ask_qty: Option<f64>,
}

#[derive(Debug, Clone)]
struct Feature {
    ret_15s: f64,
    ret_60s: f64,
    flow_15s: f64,
    flow_60s: f64,
    volatility: f64,
    book_imbalance: f64,
    spread_bps: f64,
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

        if matches!(point.kind.as_str(), "trade" | "agg_trade" | "public_trade") {
            let qty = point.qty.unwrap_or(0.0);
            match point.side.as_deref() {
                Some("BUY") => bucket.buy_qty += qty,
                Some("SELL") => bucket.sell_qty += qty,
                _ => {}
            }
        }

        if point.kind == "book_ticker" {
            bucket.bid_price = point.bid_price.or(bucket.bid_price);
            bucket.bid_qty = point.bid_qty.or(bucket.bid_qty);
            bucket.ask_price = point.ask_price.or(bucket.ask_price);
            bucket.ask_qty = point.ask_qty.or(bucket.ask_qty);
        }
    }

    let mut buckets: Vec<Bucket> = map.into_values().collect();
    let mut last_price = None;
    let mut last_bid = None;
    let mut last_bid_qty = None;
    let mut last_ask = None;
    let mut last_ask_qty = None;

    for bucket in &mut buckets {
        bucket.price = bucket.price.or(last_price);
        bucket.bid_price = bucket.bid_price.or(last_bid);
        bucket.bid_qty = bucket.bid_qty.or(last_bid_qty);
        bucket.ask_price = bucket.ask_price.or(last_ask);
        bucket.ask_qty = bucket.ask_qty.or(last_ask_qty);

        last_price = bucket.price;
        last_bid = bucket.bid_price;
        last_bid_qty = bucket.bid_qty;
        last_ask = bucket.ask_price;
        last_ask_qty = bucket.ask_qty;
    }

    buckets
}

fn feature_at(buckets: &[Bucket], index: usize) -> Option<Feature> {
    if index < 12 {
        return None;
    }

    let price = buckets[index].price?;
    let price_15 = buckets[index - 3].price?;
    let price_60 = buckets[index - 12].price?;
    let ret_15s = (price / price_15).ln();
    let ret_60s = (price / price_60).ln();

    let (buy_15, sell_15) = flow_sum(&buckets[index - 2..=index]);
    let (buy_60, sell_60) = flow_sum(&buckets[index - 11..=index]);
    let flow_15s = imbalance(buy_15, sell_15);
    let flow_60s = imbalance(buy_60, sell_60);

    let mut returns = Vec::with_capacity(12);
    for i in (index - 11)..=index {
        if i == 0 {
            continue;
        }
        if let (Some(previous), Some(current)) = (buckets[i - 1].price, buckets[i].price)
            && previous > 0.0
            && current > 0.0
        {
            returns.push((current / previous).ln());
        }
    }
    let volatility = standard_deviation(&returns);

    let book_imbalance = match (buckets[index].bid_qty, buckets[index].ask_qty) {
        (Some(bid), Some(ask)) => imbalance(bid, ask),
        _ => 0.0,
    };

    let spread_bps = match (buckets[index].bid_price, buckets[index].ask_price) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask >= bid => {
            ((ask - bid) / ((ask + bid) / 2.0)) * 10_000.0
        }
        _ => 0.0,
    };

    Some(Feature {
        ret_15s,
        ret_60s,
        flow_15s,
        flow_60s,
        volatility,
        book_imbalance,
        spread_bps,
    })
}

fn pattern_forecast(
    buckets: &[Bucket],
    current_index: usize,
    horizon_steps: usize,
    current: &Feature,
) -> Option<(f64, f64)> {
    if current_index <= 12 + horizon_steps + 10 {
        return None;
    }

    let mut neighbors = Vec::new();
    for index in 12..(current_index - horizon_steps) {
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
        neighbors.push((distance, future_return));
    }

    neighbors.sort_by(|left, right| {
        left.0
            .partial_cmp(&right.0)
            .unwrap_or(Ordering::Equal)
    });

    let mut weight_sum = 0.0;
    let mut return_sum = 0.0;
    let mut up_weight = 0.0;
    let mut used = 0_usize;

    for (distance, future_return) in neighbors.into_iter().take(30) {
        let weight = 1.0 / (0.05 + distance);
        weight_sum += weight;
        return_sum += weight * future_return;
        if future_return > 0.0 {
            up_weight += weight;
        }
        used += 1;
    }

    if used < 8 || weight_sum <= 0.0 {
        return None;
    }

    Some((return_sum / weight_sum, up_weight / weight_sum))
}

fn feature_distance(left: &Feature, right: &Feature) -> f64 {
    let values = [
        (left.ret_15s - right.ret_15s) / 0.001,
        (left.ret_60s - right.ret_60s) / 0.002,
        left.flow_15s - right.flow_15s,
        left.flow_60s - right.flow_60s,
        (left.volatility - right.volatility) / 0.001,
        left.book_imbalance - right.book_imbalance,
        (left.spread_bps - right.spread_bps) / 5.0,
    ];
    values.iter().map(|value| value * value).sum::<f64>().sqrt()
}

fn microstructure_score(feature: &Feature) -> f64 {
    let momentum = if feature.volatility > 0.0 {
        (feature.ret_60s / (3.0 * feature.volatility)).clamp(-1.0, 1.0)
    } else {
        0.0
    };
    let short_momentum = if feature.volatility > 0.0 {
        (feature.ret_15s / (2.0 * feature.volatility)).clamp(-1.0, 1.0)
    } else {
        0.0
    };

    (0.25 * feature.flow_15s
        + 0.20 * feature.flow_60s
        + 0.25 * feature.book_imbalance
        + 0.20 * momentum
        + 0.10 * short_momentum)
        .clamp(-1.0, 1.0)
}

fn flow_sum(buckets: &[Bucket]) -> (f64, f64) {
    buckets.iter().fold((0.0, 0.0), |(buy, sell), bucket| {
        (buy + bucket.buy_qty, sell + bucket.sell_qty)
    })
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

#[cfg(test)]
mod tests {
    use super::imbalance;

    #[test]
    fn imbalance_is_bounded() {
        assert_eq!(imbalance(10.0, 0.0), 1.0);
        assert_eq!(imbalance(0.0, 10.0), -1.0);
        assert_eq!(imbalance(0.0, 0.0), 0.0);
    }
}
