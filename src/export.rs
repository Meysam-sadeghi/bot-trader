use crate::{
    audit::{self, PositionDiagnosis},
    model::{Exchange, Prediction, now_ms},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};

pub const MAX_EXPORT_POSITIONS: i64 = 25_000;

#[derive(Debug)]
pub struct ExportTooLarge;
impl std::fmt::Display for ExportTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "More than {MAX_EXPORT_POSITIONS} positions match. Select a shorter date range, exchange or strategy; no partial file was exported."
        )
    }
}
impl std::error::Error for ExportTooLarge {}

#[derive(Debug, Default, Clone, Serialize)]
pub struct ExportFilters {
    pub exchange: Option<Exchange>,
    pub symbol: Option<String>,
    pub strategy: Option<String>,
    pub horizon_secs: Option<i64>,
    pub status: Option<String>,
    pub config_id: Option<String>,
    pub from_ms: Option<i64>,
    pub to_ms: Option<i64>,
}
impl ExportFilters {
    pub fn parse(query: &HashMap<String, String>, current_config: &str) -> Result<Self, String> {
        for key in query.keys() {
            if ![
                "format", "exchange", "symbol", "strategy", "horizon", "status", "config",
                "from_ms", "to_ms",
            ]
            .contains(&key.as_str())
            {
                return Err(format!("Unknown export filter: {key}"));
            }
        }
        let selected = |name: &str| {
            query
                .get(name)
                .filter(|v| !v.is_empty() && *v != "all")
                .cloned()
        };
        let exchange = selected("exchange")
            .map(|s| {
                s.parse::<Exchange>()
                    .map_err(|_| "Exchange must be all, binance or bybit")
            })
            .transpose()?;
        let symbol = selected("symbol").map(|s| s.trim().to_ascii_uppercase());
        if symbol.as_ref().is_some_and(|s| {
            !(5..=24).contains(&s.len()) || !s.chars().all(|c| c.is_ascii_alphanumeric())
        }) {
            return Err("Invalid symbol".into());
        }
        let strategy = selected("strategy");
        if strategy.as_ref().is_some_and(|s| {
            s.len() > 100
                || !s
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        }) {
            return Err("Invalid strategy".into());
        }
        let horizon_secs = selected("horizon")
            .map(|s| s.parse::<i64>().map_err(|_| "Invalid horizon"))
            .transpose()?;
        if horizon_secs.is_some_and(|h| !crate::paper::HORIZONS.contains(&h)) {
            return Err("Horizon must be all, 60, 300, 900 or 3600 seconds".into());
        }
        let status = selected("status").map(|s| s.to_ascii_uppercase());
        if status
            .as_ref()
            .is_some_and(|s| !["OPEN", "WIN", "LOSS", "TIMEOUT", "DATA_GAP"].contains(&s.as_str()))
        {
            return Err("Invalid position status".into());
        }
        let config_id = selected("config").map(|s| {
            if s == "current" {
                current_config.into()
            } else {
                s
            }
        });
        if config_id.as_ref().is_some_and(|s| {
            s.len() > 100
                || !s
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        }) {
            return Err("Invalid configuration ID".into());
        }
        let timestamp = |name| -> Result<Option<i64>, String> {
            selected(name)
                .map(|s| {
                    s.parse::<i64>()
                        .ok()
                        .filter(|v| {
                            *v >= 0 && chrono::DateTime::from_timestamp_millis(*v).is_some()
                        })
                        .ok_or_else(|| {
                            format!("{name} must be a non-negative UTC timestamp in milliseconds")
                        })
                })
                .transpose()
        };
        let from_ms = timestamp("from_ms")?;
        let to_ms = timestamp("to_ms")?;
        if from_ms.zip(to_ms).is_some_and(|(from, to)| from >= to) {
            return Err("Start must be earlier than end (end is exclusive)".into());
        }
        Ok(Self {
            exchange,
            symbol,
            strategy,
            horizon_secs,
            status,
            config_id,
            from_ms,
            to_ms,
        })
    }
}

pub struct ExportSnapshot {
    pub positions: Vec<Prediction>,
    pub configurations: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct ExportPosition {
    #[serde(flatten)]
    pub position: Prediction,
    pub diagnosis: PositionDiagnosis,
}
impl From<Prediction> for ExportPosition {
    fn from(position: Prediction) -> Self {
        let diagnosis = audit::diagnose(&position);
        Self {
            position,
            diagnosis,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct Counts {
    pub total: usize,
    pub open: usize,
    pub closed: usize,
    pub priced_closed: usize,
    pub profitable: usize,
    pub losing: usize,
    pub breakeven: usize,
    pub unverifiable: usize,
    pub stop_losses: usize,
    pub timeout_losses: usize,
    pub other_losses: usize,
    pub fees_changed_profit_to_loss: usize,
    pub gave_back_observed_profit: usize,
    pub missing_entry_snapshot: usize,
    pub missing_execution_audit: usize,
    pub partial_execution_audit: usize,
}
impl Counts {
    fn add(&mut self, p: &ExportPosition) {
        let d = &p.diagnosis;
        self.total += 1;
        self.open += usize::from(d.outcome == "open");
        self.closed += usize::from(d.outcome != "open");
        self.unverifiable += usize::from(d.outcome == "unverifiable");
        self.priced_closed += usize::from(d.outcome != "open" && d.outcome != "unverifiable");
        self.profitable += usize::from(d.outcome == "profitable");
        self.losing += usize::from(matches!(
            d.outcome,
            "stop_loss" | "timeout_loss" | "other_loss"
        ));
        self.breakeven += usize::from(d.outcome == "breakeven");
        self.stop_losses += usize::from(d.outcome == "stop_loss");
        self.timeout_losses += usize::from(d.outcome == "timeout_loss");
        self.other_losses += usize::from(d.outcome == "other_loss");
        self.fees_changed_profit_to_loss += usize::from(d.fees_changed_profit_to_loss);
        self.gave_back_observed_profit += usize::from(d.gave_back_observed_profit);
        self.missing_entry_snapshot += usize::from(d.entry_evidence == "not_recorded");
        self.missing_execution_audit += usize::from(d.path_evidence == "not_recorded");
        self.partial_execution_audit += usize::from(d.path_evidence == "partial_after_upgrade");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct GroupKey {
    exchange: String,
    symbol: String,
    config_id: String,
    strategy: String,
    horizon_secs: i64,
    direction: String,
}
#[derive(Debug, Serialize)]
pub struct GroupSummary {
    #[serde(flatten)]
    key: GroupKey,
    pub counts: Counts,
    pub net_win_rate_all_closed: Option<f64>,
    pub net_win_rate_priced_closed: Option<f64>,
    pub known_net_pnl_quote: f64,
    pub avg_known_net_bps: Option<f64>,
    pub exit_reasons: BTreeMap<String, usize>,
    #[serde(skip)]
    net_bps_sum: f64,
}

#[derive(Debug, Serialize)]
pub struct ExportReport {
    pub schema_version: u32,
    pub kind: &'static str,
    pub generated_at: i64,
    pub filters: ExportFilters,
    pub complete: bool,
    pub position_count: usize,
    pub counts: Counts,
    pub groups: Vec<GroupSummary>,
    pub configurations: Vec<Value>,
    pub unavailable_configuration_ids: Vec<String>,
    pub notes: Vec<&'static str>,
    pub positions: Vec<ExportPosition>,
}
pub fn report(snapshot: ExportSnapshot, filters: ExportFilters) -> ExportReport {
    let positions: Vec<ExportPosition> = snapshot.positions.into_iter().map(Into::into).collect();
    let mut counts = Counts::default();
    let mut groups = BTreeMap::new();
    let mut config_ids = HashSet::new();
    for p in &positions {
        let row = &p.position;
        config_ids.insert(row.config_id.clone());
        counts.add(p);
        let key = GroupKey {
            exchange: row.exchange.to_string(),
            symbol: row.symbol.clone(),
            config_id: row.config_id.clone(),
            strategy: row.strategy.clone(),
            horizon_secs: row.horizon_secs,
            direction: row.direction.clone(),
        };
        let group = groups.entry(key.clone()).or_insert_with(|| GroupSummary {
            key,
            counts: Counts::default(),
            net_win_rate_all_closed: None,
            net_win_rate_priced_closed: None,
            known_net_pnl_quote: 0.,
            avg_known_net_bps: None,
            exit_reasons: BTreeMap::new(),
            net_bps_sum: 0.,
        });
        group.counts.add(p);
        *group
            .exit_reasons
            .entry(p.diagnosis.exit_reason.clone())
            .or_default() += 1;
        if !matches!(p.diagnosis.outcome, "open" | "unverifiable") {
            group.known_net_pnl_quote += p.diagnosis.net_pnl_quote.unwrap_or(0.);
            group.net_bps_sum += row.pnl_bps.unwrap_or(0.);
        }
    }
    for g in groups.values_mut() {
        g.net_win_rate_all_closed =
            (g.counts.closed > 0).then(|| g.counts.profitable as f64 / g.counts.closed as f64);
        g.net_win_rate_priced_closed = (g.counts.priced_closed > 0)
            .then(|| g.counts.profitable as f64 / g.counts.priced_closed as f64);
        g.avg_known_net_bps =
            (g.counts.priced_closed > 0).then(|| g.net_bps_sum / g.counts.priced_closed as f64);
    }
    for c in &snapshot.configurations {
        if let Some(id) = c["id"].as_str() {
            config_ids.remove(id);
        }
    }
    let mut unavailable_configuration_ids: Vec<_> = config_ids.into_iter().collect();
    unavailable_configuration_ids.sort();
    ExportReport {
        schema_version: 1,
        kind: "market_lab_position_diagnostics",
        generated_at: now_ms(),
        filters,
        complete: true,
        position_count: positions.len(),
        counts,
        groups: groups.into_values().collect(),
        configurations: snapshot.configurations,
        unavailable_configuration_ids,
        notes: vec![
            "Paper trades only. Observations describe execution and outcomes; they do not establish the market cause of a loss or future profitability.",
            "All matching positions are included, oldest first, from one database read snapshot. Dates filter entry time: from inclusive, to exclusive. Times are UTC epoch milliseconds; bps means 0.01%.",
            "Entry snapshots are immutable and recorded before outcomes. Missing historical features and detailed exits are not reconstructed. Partial audits cover only observations processed after the upgrade.",
            "Groups keep exchange, symbol, config, strategy, horizon and direction separate. Legacy outcomes use the old gross-price model, even though the old column is named pnl_bps. Missing legacy notional/config values are migration defaults, not recovered account facts.",
            "Net win rate all closed includes unverifiable outcomes in the denominator, matching V3 dashboard accounting. Priced-closed win rate excludes them and is reported separately; neither counts an unknown outcome as a priced loss.",
            "Fees-change-profit-to-loss and profit-giveback counts are overlapping subsets of losses, not extra losses. Gross PnL already contains spread/slippage; fees_paid_bps is gross minus net. Do not subtract slippage again.",
            "Best/worst marks use observed executable exit quotes with modeled costs, including the entry quote. They are uncapped hypothetical marks; actual favorable target fills are capped at target. Sparse/unfillable quotes cannot establish a complete price path. No ticks after exit are included.",
            "Signal scores are not win probabilities. Feature returns are natural log returns; volatility is per five-second return; flow/book imbalances are signed ratios. Historical analog evidence is in-sample selection evidence, not forward win rate.",
            "V4 expected_return is the weighted historical NET estimate as a fraction. V4 plans net target profit at three times net stop loss, at the stored executable levels; adverse stop gaps and timeouts can change realized reward/risk. V3 used a gross-distance ratio. Check each configuration and planned_net_reward_risk.",
            "Raw market archives and credentials are not exported. JSON contains full decision/configuration evidence; CSV is a flat comparison with embedded evidence JSON.",
        ],
        positions,
    }
}

// Explicit, stable CSV columns. Numeric negatives remain numeric; only text cells are
// protected against formula execution. JSON retains original text without modification.
const CSV_COLUMNS: &[(&str, &str)] = &[
    ("id", "/id"),
    ("exchange", "/exchange"),
    ("symbol", "/symbol"),
    ("config_id", "/config_id"),
    ("strategy", "/strategy"),
    ("horizon_secs", "/horizon_secs"),
    ("direction", "/direction"),
    ("created_at_utc_ms", "/created_at"),
    ("resolved_at_utc_ms", "/resolved_at"),
    ("status", "/status"),
    ("outcome", "/diagnosis/outcome"),
    ("entry_price", "/entry_price"),
    ("target_price", "/target_price"),
    ("stop_price", "/stop_price"),
    ("exit_price", "/exit_price"),
    ("gross_pnl_bps", "/gross_pnl_bps"),
    ("net_pnl_bps_legacy_gross", "/pnl_bps"),
    ("net_pnl_quote", "/diagnosis/net_pnl_quote"),
    ("notional_quote", "/notional"),
    ("fee_bps_per_side", "/fee_bps"),
    ("slippage_bps_per_side", "/slippage_bps"),
    ("fees_paid_bps", "/diagnosis/fees_paid_bps"),
    (
        "fees_changed_profit_to_loss",
        "/diagnosis/fees_changed_profit_to_loss",
    ),
    ("held_ms", "/diagnosis/held_ms"),
    ("exit_reason", "/diagnosis/exit_reason"),
    ("exit_reason_source", "/diagnosis/exit_reason_source"),
    ("entry_reason", "/entry_reason"),
    ("score_not_probability", "/score"),
    ("expected_return_fraction_see_config", "/expected_return"),
    ("entry_evidence", "/diagnosis/entry_evidence"),
    ("path_evidence", "/diagnosis/path_evidence"),
    ("target_net_bps", "/diagnosis/target_net_bps"),
    ("stop_net_bps_at_level", "/diagnosis/stop_net_bps_at_level"),
    (
        "planned_net_reward_risk",
        "/diagnosis/planned_net_reward_risk",
    ),
    ("best_observed_net_bps", "/diagnosis/best_observed_net_bps"),
    (
        "worst_observed_net_bps",
        "/diagnosis/worst_observed_net_bps",
    ),
    (
        "gave_back_observed_profit",
        "/diagnosis/gave_back_observed_profit",
    ),
    (
        "max_observed_gap_ms",
        "/execution_audit/max_observed_gap_ms",
    ),
    ("quotes_observed", "/execution_audit/quotes_observed"),
    ("illiquid_quotes", "/execution_audit/illiquid_quotes"),
    ("entry_spread_bps", "/entry_snapshot/execution/spread_bps"),
    (
        "entry_quote_age_ms",
        "/entry_snapshot/execution/quote_age_ms",
    ),
    ("ret_15s", "/entry_snapshot/features/ret_15s"),
    ("ret_60s", "/entry_snapshot/features/ret_60s"),
    ("ret_300s", "/entry_snapshot/features/ret_300s"),
    ("flow_60s", "/entry_snapshot/features/flow_60s"),
    (
        "book_imbalance_60s",
        "/entry_snapshot/features/book_imbalance_60s",
    ),
    ("volatility_60s", "/entry_snapshot/features/volatility_60s"),
    (
        "trend_efficiency",
        "/entry_snapshot/features/trend_efficiency",
    ),
    ("volume_ratio", "/entry_snapshot/features/volume_ratio"),
    ("peer_score", "/entry_snapshot/peer/microstructure_score"),
    ("entry_snapshot_json", "/entry_snapshot"),
    ("execution_audit_json", "/execution_audit"),
];

fn csv_text(value: &str) -> String {
    let trimmed = value.trim_start();
    let protect =
        trimmed.starts_with(['=', '+', '-', '@']) || value.starts_with(['\t', '\r', '\n']);
    format!(
        "\"{}{}\"",
        if protect { "'" } else { "" },
        value.replace('"', "\"\"")
    )
}
pub fn csv(report: &ExportReport) -> anyhow::Result<String> {
    let mut out = String::from("\u{feff}"); // UTF-8 BOM for Excel.
    out.push_str(
        &CSV_COLUMNS
            .iter()
            .map(|c| c.0)
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push_str(",configuration_json\r\n");
    let configs: HashMap<_, _> = report
        .configurations
        .iter()
        .filter_map(|c| c["id"].as_str().map(|id| (id, &c["parameters"])))
        .collect();
    for position in &report.positions {
        let row = serde_json::to_value(position)?;
        let mut cells: Vec<_> = CSV_COLUMNS
            .iter()
            .map(|(_, path)| match row.pointer(path) {
                Some(Value::Null) | None => String::new(),
                Some(Value::String(s)) => csv_text(s),
                Some(v @ (Value::Number(_) | Value::Bool(_))) => v.to_string(),
                Some(v) => csv_text(&v.to_string()),
            })
            .collect();
        cells.push(
            configs
                .get(position.position.config_id.as_str())
                .map(|v| csv_text(&v.to_string()))
                .unwrap_or_default(),
        );
        out.push_str(&cells.join(","));
        out.push_str("\r\n");
    }
    Ok(out)
}

pub fn summary_json(report: &ExportReport) -> Value {
    json!({"generated_at": report.generated_at, "filters": report.filters, "complete": report.complete,
        "position_count": report.position_count, "counts": report.counts, "groups": report.groups,
        "unavailable_configuration_ids": report.unavailable_configuration_ids})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::Database, paper::LabConfig, test_support::prediction};

    #[test]
    fn filters_reject_invalid_or_silently_ignored_input() {
        let parse = |items: &[(&str, &str)]| {
            ExportFilters::parse(
                &items
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                "v3-current",
            )
        };
        assert!(parse(&[("exchange", "bad")]).is_err());
        assert!(parse(&[("limit", "500")]).is_err());
        assert!(parse(&[("from_ms", "2000"), ("to_ms", "1000")]).is_err());
        assert!(parse(&[("from_ms", "1.2")]).is_err());
        assert!(parse(&[("horizon", "20")]).is_err());
        assert_eq!(
            parse(&[("config", "current")])
                .unwrap()
                .config_id
                .as_deref(),
            Some("v3-current")
        );
        assert!(parse(&[]).unwrap().config_id.is_none());
    }

    #[test]
    fn summary_separates_cost_losses_unknown_outcomes_and_configurations() {
        let mut p = prediction();
        p.status = "TIMEOUT".into();
        p.gross_pnl_bps = Some(5.);
        p.pnl_bps = Some(-15.);
        let mut gap = p.clone();
        gap.id = "gap".into();
        gap.status = "DATA_GAP".into();
        gap.pnl_bps = None;
        gap.gross_pnl_bps = None;
        let mut old = p.clone();
        old.config_id = "legacy".into();
        old.status = "WIN".into();
        old.pnl_bps = Some(100.);
        let r = report(
            ExportSnapshot {
                positions: vec![p, gap, old],
                configurations: vec![],
            },
            ExportFilters::default(),
        );
        assert_eq!(r.counts.losing, 1);
        assert_eq!(r.counts.unverifiable, 1);
        assert_eq!(r.counts.profitable, 1);
        assert_eq!(r.counts.fees_changed_profit_to_loss, 1);
        assert_eq!(r.groups.len(), 2);
        let g = r
            .groups
            .iter()
            .find(|g| g.key.config_id != "legacy")
            .unwrap();
        assert_eq!(g.counts.closed, 2);
        assert_eq!(g.counts.priced_closed, 1);
        assert_eq!(g.avg_known_net_bps, Some(-15.));
    }

    #[test]
    fn csv_escapes_text_without_converting_negative_numbers_to_text() {
        assert_eq!(csv_text("=1+1"), "\"'=1+1\"");
        assert_eq!(csv_text("  @cmd"), "\"'  @cmd\"");
        assert_eq!(csv_text("a,\"b\"\nفارسی"), "\"a,\"\"b\"\"\nفارسی\"");
        let mut p = prediction();
        p.pnl_bps = Some(-15.5);
        p.entry_reason = "=HYPERLINK(\"x\")".into();
        let r = report(
            ExportSnapshot {
                positions: vec![p],
                configurations: vec![],
            },
            ExportFilters::default(),
        );
        let text = csv(&r).unwrap();
        assert!(text.starts_with('\u{feff}'));
        assert!(text.contains(",-15.5,"));
        assert!(text.contains("\"'=HYPERLINK(\"\"x\"\")\""));
    }

    #[tokio::test]
    async fn export_includes_more_than_dashboard_limit_and_filters_both_exchanges_and_archives() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.register_config(&LabConfig::default()).await.unwrap();
        for i in 0..510 {
            let mut p = prediction();
            p.created_at = 1000 + i;
            p.exchange = if i % 2 == 0 {
                Exchange::Binance
            } else {
                Exchange::Bybit
            };
            p.config_id = if i < 10 {
                "legacy".into()
            } else {
                LabConfig::default().id()
            };
            p.status = "TIMEOUT".into();
            db.insert_prediction(&p).await.unwrap();
        }
        let all = db
            .export_positions(&ExportFilters::default())
            .await
            .unwrap();
        assert_eq!(all.positions.len(), 510);
        assert_eq!(all.configurations.len(), 1);
        let filters = ExportFilters {
            exchange: Some(Exchange::Bybit),
            from_ms: Some(1010),
            to_ms: Some(1030),
            ..Default::default()
        };
        let subset = db.export_positions(&filters).await.unwrap();
        assert_eq!(subset.positions.len(), 10);
        assert!(
            subset
                .positions
                .iter()
                .all(|p| p.exchange == Exchange::Bybit
                    && p.created_at >= 1010
                    && p.created_at < 1030)
        );
        let filters = ExportFilters {
            config_id: Some("legacy".into()),
            ..Default::default()
        };
        assert_eq!(
            db.export_positions(&filters).await.unwrap().positions.len(),
            10
        );
    }
}
