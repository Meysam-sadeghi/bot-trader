//! Shared experiment definitions, execution costs and honest forward statistics.
use crate::model::{Exchange, Prediction, PredictionStats};
use serde::Serialize;

pub const HORIZONS: [i64; 4] = [60, 300, 900, 3600];
pub const STRATEGIES: [(&str, &str, &str); 6] = [
    (
        "flow_follow_v3",
        "Order-flow follow",
        "Follows aligned taker flow, book pressure and short momentum.",
    ),
    (
        "flow_reverse_v3",
        "Order-flow reverse",
        "Opposite-direction control using the same flow setup.",
    ),
    (
        "trend_pullback_v3",
        "Trend pullback",
        "Joins an efficient trend after a short pullback begins to recover.",
    ),
    (
        "breakout_v3",
        "Confirmed breakout",
        "Requires a five-minute breakout, volume expansion and aligned flow.",
    ),
    (
        "range_reversion_v3",
        "Range reversion",
        "Fades range extremes only after flow and acceleration turn inward.",
    ),
    (
        "consensus_v3",
        "Selective consensus",
        "Requires fresh peer agreement and cost-aware, non-overlapping historical analog evidence.",
    ),
];

#[derive(Debug, Clone, Serialize)]
pub struct LabConfig {
    pub engine_version: u32,
    pub risk_reward: f64,
    pub interval_secs: u64,
    pub lookback_hours: i64,
    pub max_points: i64,
    pub target_win_rate: f64,
    pub min_win_lower_bound: f64,
    pub min_edge_bps: f64,
    pub max_spread_bps: f64,
    pub min_analog_samples: usize,
    pub analog_neighbors: usize,
    pub cooldown_secs: i64,
    pub max_quote_age_ms: i64,
    pub max_gap_ms: i64,
    pub binance_fee_bps: f64,
    pub bybit_fee_bps: f64,
    pub slippage_bps: f64,
    pub notional: f64,
    pub initial_capital: f64,
    pub min_forward_trades: i64,
}

impl Default for LabConfig {
    fn default() -> Self {
        Self {
            engine_version: 3,
            risk_reward: 3.0,
            interval_secs: 30,
            lookback_hours: 24,
            max_points: 750_000,
            target_win_rate: 0.80,
            min_win_lower_bound: 0.55,
            min_edge_bps: 2.0,
            max_spread_bps: 3.0,
            min_analog_samples: 24,
            analog_neighbors: 80,
            cooldown_secs: 180,
            max_quote_age_ms: 5_000,
            max_gap_ms: 15_000,
            // Research assumptions, not a claim about an account's actual fee tier.
            binance_fee_bps: 10.0,
            bybit_fee_bps: 10.0,
            slippage_bps: 1.0,
            notional: 1_000.0,
            initial_capital: 10_000.0,
            min_forward_trades: 100,
        }
    }
}

impl LabConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        c.interval_secs =
            env_num("ANALYSIS_INTERVAL_SECS", c.interval_secs as f64).clamp(5., 3600.) as u64;
        c.lookback_hours =
            env_num("ANALYSIS_LOOKBACK_HOURS", c.lookback_hours as f64).clamp(4., 168.) as i64;
        c.max_points =
            env_num("ANALYSIS_MAX_POINTS", c.max_points as f64).clamp(50_000., 2_000_000.) as i64;
        c.target_win_rate = env_num("TARGET_STRICT_WIN_RATE", c.target_win_rate).clamp(0.26, 0.99);
        c.min_win_lower_bound =
            env_num("MIN_WIN_LOWER_BOUND", c.min_win_lower_bound).clamp(0.25, 0.95);
        c.min_edge_bps = env_num("MIN_SIGNAL_EDGE_BPS", c.min_edge_bps).clamp(0., 100.);
        c.max_spread_bps = env_num("MAX_SPREAD_BPS", c.max_spread_bps).clamp(0.1, 50.);
        c.min_analog_samples =
            env_num("MIN_ANALOG_SAMPLES", c.min_analog_samples as f64).clamp(8., 200.) as usize;
        c.analog_neighbors = env_num("ANALOG_NEIGHBORS", c.analog_neighbors as f64)
            .clamp(c.min_analog_samples as f64, 250.) as usize;
        c.cooldown_secs =
            env_num("REENTRY_COOLDOWN_SECS", c.cooldown_secs as f64).clamp(0., 3600.) as i64;
        c.max_quote_age_ms =
            env_num("MAX_QUOTE_AGE_MS", c.max_quote_age_ms as f64).clamp(1000., 10_000.) as i64;
        c.max_gap_ms = env_num("MAX_DATA_GAP_MS", c.max_gap_ms as f64)
            .clamp(c.max_quote_age_ms as f64, 60_000.) as i64;
        c.binance_fee_bps = env_num("BINANCE_TAKER_FEE_BPS", c.binance_fee_bps).clamp(0., 100.);
        c.bybit_fee_bps = env_num("BYBIT_TAKER_FEE_BPS", c.bybit_fee_bps).clamp(0., 100.);
        c.slippage_bps = env_num("PAPER_SLIPPAGE_BPS", c.slippage_bps).clamp(0., 100.);
        c.initial_capital =
            env_num("PAPER_INITIAL_CAPITAL", c.initial_capital).clamp(100., 1_000_000.);
        c.notional = env_num("PAPER_NOTIONAL", c.notional).clamp(10., c.initial_capital / 2.);
        c
    }

    pub fn fee(&self, exchange: Exchange) -> f64 {
        match exchange {
            Exchange::Binance => self.binance_fee_bps,
            Exchange::Bybit => self.bybit_fee_bps,
        }
    }

    pub fn id(&self) -> String {
        // Stable across restarts. The full JSON is also persisted and checked for collisions.
        let json = serde_json::to_string(self).expect("finite configuration");
        let hash = json.bytes().fold(0xcbf29ce484222325_u64, |h, b| {
            (h ^ b as u64).wrapping_mul(0x100000001b3)
        });
        format!("v3-{hash:016x}")
    }
}

fn env_num(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(default)
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct Diagnostics {
    pub evaluated_at: Option<i64>,
    pub message: String,
    pub history_minutes: f64,
    pub history_truncated: bool,
    pub lanes: Vec<LaneDiagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LaneDiagnostic {
    pub strategy: String,
    pub horizon_secs: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HorizonReport {
    pub horizon_secs: i64,
    pub stats: PredictionStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyReport {
    pub id: String,
    pub name: String,
    pub description: String,
    pub stats: PredictionStats,
    pub horizons: Vec<HorizonReport>,
}

/// Entry crosses the spread; adverse slippage is applied separately on each leg.
pub fn entry_fill(direction: &str, bid: f64, ask: f64, slip_bps: f64) -> f64 {
    if direction == "LONG" {
        ask * (1. + slip_bps / 10_000.)
    } else {
        bid * (1. - slip_bps / 10_000.)
    }
}

pub fn exit_fill(direction: &str, quote: f64, slip_bps: f64) -> f64 {
    if direction == "LONG" {
        quote * (1. - slip_bps / 10_000.)
    } else {
        quote * (1. + slip_bps / 10_000.)
    }
}

pub fn pnl(direction: &str, entry: f64, exit: f64, fee_bps: f64) -> (f64, f64) {
    let ratio = exit / entry;
    let gross = if direction == "LONG" {
        ratio - 1.
    } else {
        1. - ratio
    } * 10_000.;
    // Fees are charged on actual entry AND exit notionals, including shorts.
    (gross, gross - fee_bps * (1. + ratio))
}

pub fn win_interval(wins: i64, total: i64) -> (f64, f64) {
    if total == 0 {
        return (0., 1.);
    }
    let n = total as f64;
    let p = wins as f64 / n;
    let z = 1.96;
    let center = p + z * z / (2. * n);
    let margin = z * ((p * (1. - p) + z * z / (4. * n)) / n).sqrt();
    let denom = 1. + z * z / n;
    (
        ((center - margin) / denom).max(0.),
        ((center + margin) / denom).min(1.),
    )
}

pub fn stats(items: &[&Prediction], capital: f64, config: &LabConfig) -> PredictionStats {
    let mut s = PredictionStats {
        total: items.len() as i64,
        initial_capital: capital,
        ..Default::default()
    };
    let mut resolved: Vec<_> = items
        .iter()
        .copied()
        .filter(|p| p.status != "OPEN")
        .collect();
    resolved.sort_by(|a, b| (a.resolved_at, &a.id).cmp(&(b.resolved_at, &b.id)));
    let mut balance = capital;
    let mut peak = capital;
    let mut sum_bps = 0.;
    let mut priced = 0;
    for p in resolved {
        s.resolved += 1;
        s.timeouts += i64::from(p.status == "TIMEOUT");
        if let Some(net) = p.pnl_bps {
            priced += 1;
            sum_bps += net;
            let amount = p.notional * net / 10_000.;
            s.net_pnl += amount;
            if net > 0.000_001 {
                s.wins += 1;
                s.gross_profit += amount;
            } else if net < -0.000_001 {
                s.losses += 1;
                s.gross_loss -= amount;
            } else {
                s.breakeven += 1;
            }
            s.target_hits += i64::from(p.status == "WIN" && net > 0.);
            balance += amount;
            peak = peak.max(balance);
            s.closed_drawdown_pct = s.closed_drawdown_pct.max((peak - balance) / peak * 100.);
        } else {
            s.invalid += 1;
        }
    }
    for p in items.iter().filter(|p| p.status == "OPEN") {
        s.open += 1;
        if let Some(mark) = p.mark_price {
            let fill = exit_fill(&p.direction, mark, p.slippage_bps);
            s.unrealized_pnl +=
                p.notional * pnl(&p.direction, p.entry_price, fill, p.fee_bps).1 / 10_000.;
        } else {
            s.unmarked_open += 1;
        }
    }
    if s.resolved > 0 {
        // Invalid/data-gap exits remain in the denominator: never silently remove failures.
        s.win_rate = s.wins as f64 / s.resolved as f64;
        s.strict_win_rate = s.target_hits as f64 / s.resolved as f64;
    }
    if priced > 0 {
        s.avg_pnl_bps = sum_bps / priced as f64;
    }
    (s.win_rate_low, s.win_rate_high) = win_interval(s.wins, s.resolved);
    if s.gross_loss > 0. {
        s.profit_factor = Some(s.gross_profit / s.gross_loss);
    }
    s.return_pct = (s.net_pnl + s.unrealized_pnl) / capital * 100.;
    s.sample_ready = s.resolved >= config.min_forward_trades
        && s.invalid == 0
        && s.win_rate_low >= config.target_win_rate
        && s.net_pnl > 0.;
    s
}

pub fn reports(items: &[Prediction], config: &LabConfig) -> Vec<StrategyReport> {
    STRATEGIES
        .iter()
        .map(|(id, name, description)| {
            let strategy_items: Vec<_> = items.iter().filter(|p| p.strategy == *id).collect();
            let horizons = HORIZONS
                .iter()
                .map(|h| {
                    let lane: Vec<_> = strategy_items
                        .iter()
                        .copied()
                        .filter(|p| p.horizon_secs == *h)
                        .collect();
                    HorizonReport {
                        horizon_secs: *h,
                        stats: stats(&lane, config.initial_capital, config),
                    }
                })
                .collect();
            StrategyReport {
                id: id.to_string(),
                name: name.to_string(),
                description: description.to_string(),
                stats: stats(
                    &strategy_items,
                    config.initial_capital * HORIZONS.len() as f64,
                    config,
                ),
                horizons,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::prediction;

    #[test]
    fn long_and_short_pay_both_fees_and_adverse_slippage() {
        let long_entry = entry_fill("LONG", 99., 101., 10.);
        let short_entry = entry_fill("SHORT", 99., 101., 10.);
        assert!((long_entry - 101.101).abs() < 1e-9);
        assert!((short_entry - 98.901).abs() < 1e-9);
        assert!(pnl("LONG", long_entry, exit_fill("LONG", 99., 10.), 10.).1 < -200.);
        assert!(pnl("SHORT", short_entry, exit_fill("SHORT", 101., 10.), 10.).1 < -200.);
        assert!((pnl("LONG", 100., 103., 10.).1 - 279.7).abs() < 1e-8);
        assert!((pnl("SHORT", 100., 97., 10.).1 - 280.3).abs() < 1e-8);
    }

    #[test]
    fn target_hit_below_cost_is_not_a_net_win() {
        let mut p = prediction();
        p.status = "WIN".into();
        p.pnl_bps = Some(-2.);
        let s = stats(&[&p], 10000., &LabConfig::default());
        assert_eq!(s.wins, 0);
        assert_eq!(s.target_hits, 0);
        assert_eq!(s.losses, 1);
    }

    #[test]
    fn timeouts_and_data_gaps_cannot_disappear_from_the_denominator() {
        let mut win = prediction();
        win.status = "WIN".into();
        win.pnl_bps = Some(100.);
        let mut timeout = prediction();
        timeout.status = "TIMEOUT".into();
        timeout.pnl_bps = Some(5.);
        let mut gap = prediction();
        gap.status = "DATA_GAP".into();
        let s = stats(&[&win, &timeout, &gap], 10000., &LabConfig::default());
        assert_eq!(s.resolved, 3);
        assert_eq!(s.wins, 2);
        assert_eq!(s.timeouts, 1);
        assert_eq!(s.invalid, 1);
        assert!((s.win_rate - 2. / 3.).abs() < 1e-9);
        assert!((s.strict_win_rate - 1. / 3.).abs() < 1e-9);
        assert!(!s.sample_ready);
    }

    #[test]
    fn capital_profit_factor_and_closed_drawdown_use_actual_notional() {
        let mut a = prediction();
        a.status = "WIN".into();
        a.resolved_at = Some(10);
        a.pnl_bps = Some(1000.);
        let mut b = prediction();
        b.status = "LOSS".into();
        b.resolved_at = Some(20);
        b.pnl_bps = Some(-500.);
        let s = stats(&[&b, &a], 10000., &LabConfig::default());
        assert_eq!(s.net_pnl, 50.);
        assert_eq!(s.profit_factor, Some(2.));
        assert!((s.closed_drawdown_pct - 50. / 10100. * 100.).abs() < 1e-9);
    }

    #[test]
    fn small_perfect_sample_is_not_eighty_percent_evidence() {
        assert!(win_interval(5, 5).0 < 0.8);
        assert!(win_interval(80, 100).0 < 0.8);
        assert_eq!(win_interval(0, 0), (0., 1.));
    }

    #[test]
    fn configuration_change_creates_a_new_cohort() {
        let a = LabConfig::default();
        let mut b = a.clone();
        assert_eq!(a.id(), b.id());
        b.bybit_fee_bps += 1.;
        assert_ne!(a.id(), b.id());
    }

    #[test]
    fn every_exchange_has_identical_strategy_and_horizon_definitions() {
        let reports = reports(&[], &LabConfig::default());
        assert_eq!(reports.len(), 6);
        for report in reports {
            assert_eq!(report.horizons.len(), 4);
            assert_eq!(report.stats.initial_capital, 40000.);
        }
    }
}
