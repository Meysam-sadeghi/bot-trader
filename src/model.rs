use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Bybit,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binance => write!(f, "binance"),
            Self::Bybit => write!(f, "bybit"),
        }
    }
}

impl FromStr for Exchange {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "binance" => Ok(Self::Binance),
            "bybit" => Ok(Self::Bybit),
            other => anyhow::bail!("unsupported exchange: {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub kind: String,
    pub event_ts: i64,
    pub received_ts: i64,
    pub price: Option<f64>,
    pub qty: Option<f64>,
    pub side: Option<String>,
    pub bid_price: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_price: Option<f64>,
    pub ask_qty: Option<f64>,
    pub raw_json: String,
}

#[derive(Debug, Clone)]
pub struct MarketPoint {
    pub kind: String,
    pub ts: i64,
    pub qty: Option<f64>,
    pub side: Option<String>,
    pub bid_price: Option<f64>,
    pub bid_qty: Option<f64>,
    pub ask_price: Option<f64>,
    pub ask_qty: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BusEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub kind: String,
    pub ts: i64,
    pub price: Option<f64>,
    pub qty: Option<f64>,
    pub side: Option<String>,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
}

impl From<&MarketEvent> for BusEvent {
    fn from(event: &MarketEvent) -> Self {
        Self {
            exchange: event.exchange,
            symbol: event.symbol.clone(),
            kind: event.kind.clone(),
            ts: event.received_ts,
            price: event.price,
            qty: event.qty,
            side: event.side.clone(),
            bid_price: event.bid_price,
            ask_price: event.ask_price,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CaptureStatus {
    pub exchange: Exchange,
    pub running: bool,
    pub symbol: String,
    pub started_at: Option<i64>,
    pub messages: u64,
    pub last_event_at: Option<i64>,
    pub last_price: Option<f64>,
    pub last_error: Option<String>,
}

impl CaptureStatus {
    pub fn new(exchange: Exchange) -> Self {
        Self {
            exchange,
            running: false,
            symbol: "BTCUSDT".to_string(),
            started_at: None,
            messages: 0,
            last_event_at: None,
            last_price: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prediction {
    pub id: String,
    pub exchange: Exchange,
    pub symbol: String,
    pub created_at: i64,
    pub horizon_secs: i64,
    pub direction: String,
    pub entry_price: f64,
    pub target_price: f64,
    pub stop_price: f64,
    pub confidence: f64,
    pub score: f64,
    pub expected_return: f64,
    pub strategy: String,
    pub status: String,
    pub resolved_at: Option<i64>,
    pub exit_price: Option<f64>,
    pub pnl_bps: Option<f64>,
    pub config_id: String,
    pub fee_bps: f64,
    pub slippage_bps: f64,
    pub notional: f64,
    pub gross_pnl_bps: Option<f64>,
    pub cursor_id: i64,
    pub last_quote_ts: i64,
    pub mark_price: Option<f64>,
    pub max_gap_ms: i64,
    pub entry_reason: String,
    /// Immutable inputs captured when the position was opened. None for older rows.
    #[serde(default)]
    pub entry_snapshot: Option<serde_json::Value>,
    #[serde(default)]
    pub execution_audit: Option<crate::audit::ExecutionAudit>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PredictionStats {
    pub total: i64,
    pub resolved: i64,
    pub wins: i64,
    pub losses: i64,
    pub timeouts: i64,
    pub win_rate: f64,
    pub avg_pnl_bps: f64,
    pub open: i64,
    pub unmarked_open: i64,
    pub invalid: i64,
    pub breakeven: i64,
    pub target_hits: i64,
    pub strict_win_rate: f64,
    pub win_rate_low: f64,
    pub win_rate_high: f64,
    pub net_pnl: f64,
    pub unrealized_pnl: f64,
    pub profit_factor: Option<f64>,
    pub gross_profit: f64,
    pub gross_loss: f64,
    pub closed_drawdown_pct: f64,
    pub initial_capital: f64,
    pub return_pct: f64,
    pub sample_ready: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Dashboard {
    pub capture: CaptureStatus,
    pub analysis_running: bool,
    pub stored_events: i64,
    pub stats: PredictionStats,
    pub predictions: Vec<Prediction>,
    pub strategies: Vec<crate::paper::StrategyReport>,
    pub diagnostics: crate::paper::Diagnostics,
    pub config: crate::paper::LabConfig,
    pub config_id: String,
    pub archived_predictions: i64,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
