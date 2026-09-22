//! Observed evidence, not a retrospective claim about why a market moved.
use crate::{db::Quote, model::Prediction, paper};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mark {
    pub quote: Quote,
    pub executable: bool,
    pub gross_bps: Option<f64>,
    pub net_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitEvidence {
    pub code: String,
    pub at: i64,
    pub quote: Option<Quote>,
    pub last_valid_quote_at: i64,
    pub quote_age_ms: i64,
    pub required_base_qty: f64,
    pub available_base_qty: Option<f64>,
    pub fill_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionAudit {
    pub schema_version: u32,
    pub complete_from_entry: bool,
    pub tracking_started_at: i64,
    pub starting_cursor_id: i64,
    pub quotes_observed: u64,
    pub executable_quotes: u64,
    pub illiquid_quotes: u64,
    pub max_observed_gap_ms: i64,
    pub first: Option<Mark>,
    pub last: Option<Mark>,
    pub best: Option<Mark>,
    pub worst: Option<Mark>,
    pub exit: Option<ExitEvidence>,
}

impl ExecutionAudit {
    pub fn start(p: &Prediction, complete: bool, at: i64) -> Self {
        Self {
            schema_version: 1,
            complete_from_entry: complete,
            tracking_started_at: at,
            starting_cursor_id: p.cursor_id,
            quotes_observed: 0,
            executable_quotes: 0,
            illiquid_quotes: 0,
            max_observed_gap_ms: 0,
            first: None,
            last: None,
            best: None,
            worst: None,
            exit: None,
        }
    }
}

/// Call before advancing the cursor/last-quote timestamp. Unfillable marks have no PnL.
/// Only extrema and endpoint quotes are retained: this is NOT a full tick-path archive.
pub fn observe(p: &mut Prediction, q: &Quote, continuous: bool) {
    let liquid = q.valid() && q.exit_qty(&p.direction) >= p.notional / p.entry_price;
    let executable = continuous && liquid;
    let pnl = executable.then(|| {
        paper::pnl(
            &p.direction,
            p.entry_price,
            paper::exit_fill(&p.direction, q.exit_price(&p.direction), p.slippage_bps),
            p.fee_bps,
        )
    });
    let mark = Mark {
        quote: q.clone(),
        executable,
        gross_bps: pnl.map(|v| v.0),
        net_bps: pnl.map(|v| v.1),
    };
    let gap = (q.ts - p.last_quote_ts).max(0);
    let trace = p.execution_audit.as_mut().expect("audit initialized");
    trace.quotes_observed += 1;
    trace.executable_quotes += u64::from(executable);
    trace.illiquid_quotes += u64::from(q.valid() && !liquid);
    trace.max_observed_gap_ms = trace.max_observed_gap_ms.max(gap);
    trace.first.get_or_insert_with(|| mark.clone());
    trace.last = Some(mark.clone());
    if let Some(net) = mark.net_bps {
        if trace
            .best
            .as_ref()
            .and_then(|m| m.net_bps)
            .is_none_or(|v| net > v)
        {
            trace.best = Some(mark.clone());
        }
        if trace
            .worst
            .as_ref()
            .and_then(|m| m.net_bps)
            .is_none_or(|v| net < v)
        {
            trace.worst = Some(mark);
        }
    }
}

pub fn record_exit(p: &mut Prediction, code: &str, at: i64, quote: Option<&Quote>) {
    let Some(trace) = p.execution_audit.as_mut() else {
        return;
    };
    let quote = quote
        .cloned()
        .or_else(|| trace.last.as_ref().map(|m| m.quote.clone()));
    trace.max_observed_gap_ms = trace.max_observed_gap_ms.max((at - p.last_quote_ts).max(0));
    trace.exit = Some(ExitEvidence {
        code: code.into(),
        at,
        last_valid_quote_at: p.last_quote_ts,
        quote_age_ms: (at - p.last_quote_ts).max(0),
        required_base_qty: p.notional / p.entry_price,
        available_base_qty: quote.as_ref().map(|q| q.exit_qty(&p.direction)),
        quote,
        fill_price: p.exit_price,
    });
}

#[derive(Debug, Clone, Serialize)]
pub struct PositionDiagnosis {
    pub entry_evidence: &'static str,
    pub path_evidence: &'static str,
    pub exit_reason: String,
    pub exit_reason_source: &'static str,
    pub outcome: &'static str,
    pub fees_changed_profit_to_loss: bool,
    pub fees_paid_bps: Option<f64>,
    pub net_pnl_quote: Option<f64>,
    pub held_ms: Option<i64>,
    pub target_net_bps: Option<f64>,
    pub stop_net_bps_at_level: Option<f64>,
    pub planned_net_reward_risk: Option<f64>,
    pub best_observed_net_bps: Option<f64>,
    pub worst_observed_net_bps: Option<f64>,
    pub gave_back_observed_profit: bool,
}

pub fn diagnose(p: &Prediction) -> PositionDiagnosis {
    let trace = p.execution_audit.as_ref();
    let recorded_exit = trace.and_then(|t| t.exit.as_ref());
    let net = p.pnl_bps;
    let best = trace.and_then(|t| t.best.as_ref()).and_then(|m| m.net_bps);
    let worst = trace.and_then(|t| t.worst.as_ref()).and_then(|m| m.net_bps);
    let outcome = if p.status == "OPEN" {
        "open"
    } else if p.status == "DATA_GAP" || net.is_none() {
        "unverifiable"
    } else if net.is_some_and(|v| v > 0.) {
        "profitable"
    } else if net == Some(0.) {
        "breakeven"
    } else if p.status == "LOSS" {
        "stop_loss"
    } else if p.status == "TIMEOUT" {
        "timeout_loss"
    } else {
        "other_loss"
    };
    PositionDiagnosis {
        entry_evidence: if p.entry_snapshot.is_some() {
            "recorded_at_entry"
        } else {
            "not_recorded"
        },
        path_evidence: match trace {
            Some(t) if t.complete_from_entry => "observed_from_entry",
            Some(_) => "partial_after_upgrade",
            None => "not_recorded",
        },
        exit_reason: recorded_exit.map(|e| e.code.clone()).unwrap_or_else(|| {
            match p.status.as_str() {
                "OPEN" => "still_open",
                "WIN" => "target_hit_inferred",
                "LOSS" => "stop_hit_inferred",
                "TIMEOUT" => "horizon_timeout_inferred",
                "DATA_GAP" => "unspecified_data_or_fill_gap",
                _ => "unknown",
            }
            .into()
        }),
        exit_reason_source: if recorded_exit.is_some() {
            "recorded_at_exit"
        } else {
            "status_only"
        },
        outcome,
        fees_changed_profit_to_loss: net.is_some_and(|v| v < 0.)
            && p.gross_pnl_bps.is_some_and(|v| v >= 0.),
        fees_paid_bps: p.gross_pnl_bps.zip(net).map(|(gross, net)| gross - net),
        net_pnl_quote: net.map(|v| p.notional * v / 10_000.),
        held_ms: p.resolved_at.map(|ts| ts - p.created_at),
        target_net_bps: (p.config_id != "legacy")
            .then(|| paper::pnl(&p.direction, p.entry_price, p.target_price, p.fee_bps).1),
        stop_net_bps_at_level: (p.config_id != "legacy")
            .then(|| paper::pnl(&p.direction, p.entry_price, p.stop_price, p.fee_bps).1),
        planned_net_reward_risk: (p.config_id != "legacy")
            .then(|| {
                let target = paper::pnl(&p.direction, p.entry_price, p.target_price, p.fee_bps).1;
                let stop = paper::pnl(&p.direction, p.entry_price, p.stop_price, p.fee_bps).1;
                (stop < 0.).then_some(target / -stop)
            })
            .flatten(),
        best_observed_net_bps: best,
        worst_observed_net_bps: worst,
        gave_back_observed_profit: net.is_some_and(|v| v < 0.) && best.is_some_and(|v| v > 0.),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{prediction, quote};

    #[test]
    fn extrema_only_use_executable_quotes_and_include_both_sides_costs() {
        let mut p = prediction();
        p.execution_audit = Some(ExecutionAudit::start(&p, true, p.created_at));
        observe(&mut p, &quote(1, 2000, 100.5, 100.51), true);
        let best = p.execution_audit.as_ref().unwrap().best.clone().unwrap();
        assert!(best.net_bps.unwrap() < best.gross_bps.unwrap());
        let mut thin = quote(2, 3000, 104., 104.01);
        thin.bid_qty = 0.01;
        observe(&mut p, &thin, true);
        observe(&mut p, &quote(3, 4000, 97., 97.01), true);
        let t = p.execution_audit.as_ref().unwrap();
        assert_eq!(t.best.as_ref().unwrap().quote.id, 1);
        assert_eq!(t.worst.as_ref().unwrap().quote.id, 3);
        assert_eq!(t.quotes_observed, 3);
        assert_eq!(t.executable_quotes, 2);
        assert_eq!(t.illiquid_quotes, 1);
    }

    #[test]
    fn missing_historical_evidence_is_not_fabricated_and_cost_loss_is_distinguished() {
        let mut p = prediction();
        p.status = "TIMEOUT".into();
        p.gross_pnl_bps = Some(5.);
        p.pnl_bps = Some(-15.);
        let d = diagnose(&p);
        assert_eq!(d.entry_evidence, "not_recorded");
        assert_eq!(d.exit_reason_source, "status_only");
        assert_eq!(d.outcome, "timeout_loss");
        assert!(d.fees_changed_profit_to_loss);
        assert_eq!(d.fees_paid_bps, Some(20.));
        assert_eq!(d.best_observed_net_bps, None);
        p.status = "DATA_GAP".into();
        p.pnl_bps = None;
        assert_eq!(diagnose(&p).outcome, "unverifiable");
    }
}
