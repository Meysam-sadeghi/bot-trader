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
    pub price: Option<f64>,
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
    pub status: String,
    pub resolved_at: Option<i64>,
    pub exit_price: Option<f64>,
    pub pnl_bps: Option<f64>,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct Dashboard {
    pub capture: CaptureStatus,
    pub analysis_running: bool,
    pub stored_events: i64,
    pub stats: PredictionStats,
    pub predictions: Vec<Prediction>,
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
