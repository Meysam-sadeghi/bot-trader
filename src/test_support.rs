use crate::{
    db::Quote,
    model::{Exchange, MarketEvent, Prediction},
    paper::LabConfig,
};
pub fn prediction() -> Prediction {
    Prediction {
        id: uuid::Uuid::new_v4().to_string(),
        exchange: Exchange::Binance,
        symbol: "BTCUSDT".into(),
        created_at: 1000,
        horizon_secs: 60,
        direction: "LONG".into(),
        entry_price: 100.,
        target_price: 103.,
        stop_price: 99.,
        confidence: 0.,
        score: 0.5,
        expected_return: 0.,
        strategy: "flow_follow_v3".into(),
        status: "OPEN".into(),
        resolved_at: None,
        exit_price: None,
        pnl_bps: None,
        config_id: LabConfig::default().id(),
        fee_bps: 10.,
        slippage_bps: 1.,
        notional: 1000.,
        gross_pnl_bps: None,
        cursor_id: 0,
        last_quote_ts: 1000,
        mark_price: Some(100.),
        max_gap_ms: 15000,
        entry_reason: "Test fixture, not market evidence".into(),
        entry_snapshot: None,
        execution_audit: None,
    }
}
pub fn quote(id: i64, ts: i64, bid: f64, ask: f64) -> Quote {
    Quote {
        id,
        ts,
        event_ts: ts,
        bid,
        ask,
        bid_qty: 100.,
        ask_qty: 100.,
    }
}
pub fn event(exchange: Exchange, kind: &str, ts: i64, price: f64) -> MarketEvent {
    MarketEvent {
        exchange,
        symbol: "BTCUSDT".into(),
        kind: kind.into(),
        event_ts: ts,
        received_ts: ts,
        price: Some(price),
        qty: Some(3.),
        side: Some("BUY".into()),
        bid_price: Some(price - 0.005),
        ask_price: Some(price + 0.005),
        bid_qty: Some(200.),
        ask_qty: Some(40.),
        raw_json: "{}".into(),
    }
}
