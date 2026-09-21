use super::CaptureContext;
use crate::model::{Exchange, MarketEvent, now_ms};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub async fn run(context: CaptureContext) -> anyhow::Result<()> {
    let mut backoff = 1_u64;
    loop {
        match connect_once(context.clone()).await {
            Ok(()) => {
                backoff = 1;
            }
            Err(error) => {
                context.set_error(&error).await;
                tracing::warn!(exchange = "binance", %error, backoff, "reconnecting");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
            }
        }
    }
}

async fn connect_once(context: CaptureContext) -> anyhow::Result<()> {
    let base = std::env::var("BINANCE_WS_BASE")
        .unwrap_or_else(|_| "wss://stream.binance.com:9443".to_string());
    let symbol = context.symbol.to_ascii_lowercase();
    let streams = [
        format!("{symbol}@trade"),
        format!("{symbol}@aggTrade"),
        format!("{symbol}@bookTicker"),
        format!("{symbol}@depth@100ms"),
        format!("{symbol}@kline_1s"),
    ]
    .join("/");
    let url = format!("{base}/stream?streams={streams}");

    let (mut socket, _) = connect_async(url.as_str())
        .await
        .with_context(|| format!("connect Binance websocket: {url}"))?;

    // The WS connection is opened first so depth deltas can queue while the REST
    // snapshot is fetched. The persisted snapshot lastUpdateId and each delta U/u
    // are sufficient to replay Binance's documented synchronization procedure.
    capture_snapshot(&context).await?;

    tracing::info!(
        exchange = "binance",
        symbol = %context.symbol,
        depth = "full-diff+5000-snapshot",
        "websocket connected"
    );

    while let Some(message) = socket.next().await {
        match message? {
            Message::Text(text) => {
                let root: Value = serde_json::from_str(text.as_str())?;
                let stream = root
                    .get("stream")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let data = root.get("data").cloned().unwrap_or(Value::Null);

                if let Some(event) = normalize(stream, &data, &context.symbol) {
                    context.emit(event).await?;
                }
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    anyhow::bail!("Binance websocket disconnected")
}

async fn capture_snapshot(context: &CaptureContext) -> anyhow::Result<()> {
    let rest_base = std::env::var("BINANCE_REST_BASE")
        .unwrap_or_else(|_| "https://api.binance.com".to_string());
    let url = format!(
        "{rest_base}/api/v3/depth?symbol={}&limit=5000",
        context.symbol
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let response = client
        .get(&url)
        .send()
        .await
        .context("fetch Binance depth snapshot")?
        .error_for_status()
        .context("Binance depth snapshot HTTP error")?;
    let data: Value = response.json().await?;
    let ts = now_ms();

    let event = MarketEvent {
        exchange: Exchange::Binance,
        symbol: context.symbol.clone(),
        kind: "depth_snapshot".to_string(),
        event_ts: ts,
        received_ts: ts,
        price: None,
        qty: None,
        side: None,
        bid_price: nested_number(data.get("bids"), 0, 0),
        bid_qty: nested_number(data.get("bids"), 0, 1),
        ask_price: nested_number(data.get("asks"), 0, 0),
        ask_qty: nested_number(data.get("asks"), 0, 1),
        raw_json: data.to_string(),
    };
    context.emit(event).await
}

fn normalize(stream: &str, data: &Value, symbol: &str) -> Option<MarketEvent> {
    let received_ts = now_ms();
    let mut event = MarketEvent {
        exchange: Exchange::Binance,
        symbol: symbol.to_string(),
        kind: "unknown".to_string(),
        event_ts: data
            .get("E")
            .and_then(Value::as_i64)
            .or_else(|| data.get("T").and_then(Value::as_i64))
            .unwrap_or(received_ts),
        received_ts,
        price: None,
        qty: None,
        side: None,
        bid_price: None,
        bid_qty: None,
        ask_price: None,
        ask_qty: None,
        raw_json: data.to_string(),
    };

    if stream.ends_with("@trade") {
        event.kind = "trade".to_string();
        event.price = number(data.get("p"));
        event.qty = number(data.get("q"));
        event.side = Some(if data.get("m").and_then(Value::as_bool).unwrap_or(false) {
            "SELL".to_string()
        } else {
            "BUY".to_string()
        });
    } else if stream.ends_with("@aggTrade") {
        event.kind = "agg_trade".to_string();
        event.price = number(data.get("p"));
        event.qty = number(data.get("q"));
        event.side = Some(if data.get("m").and_then(Value::as_bool).unwrap_or(false) {
            "SELL".to_string()
        } else {
            "BUY".to_string()
        });
    } else if stream.ends_with("@bookTicker") {
        event.kind = "book_ticker".to_string();
        event.bid_price = number(data.get("b"));
        event.bid_qty = number(data.get("B"));
        event.ask_price = number(data.get("a"));
        event.ask_qty = number(data.get("A"));
        if let (Some(bid), Some(ask)) = (event.bid_price, event.ask_price) {
            event.price = Some((bid + ask) / 2.0);
        }
    } else if stream.contains("@depth") {
        event.kind = "depth".to_string();
        event.bid_price = nested_number(data.get("bids"), 0, 0)
            .or_else(|| nested_number(data.get("b"), 0, 0));
        event.bid_qty = nested_number(data.get("bids"), 0, 1)
            .or_else(|| nested_number(data.get("b"), 0, 1));
        event.ask_price = nested_number(data.get("asks"), 0, 0)
            .or_else(|| nested_number(data.get("a"), 0, 0));
        event.ask_qty = nested_number(data.get("asks"), 0, 1)
            .or_else(|| nested_number(data.get("a"), 0, 1));
    } else if stream.contains("@kline_") {
        event.kind = "kline".to_string();
        if let Some(kline) = data.get("k") {
            event.price = number(kline.get("c"));
            event.qty = number(kline.get("v"));
            event.event_ts = kline
                .get("T")
                .and_then(Value::as_i64)
                .unwrap_or(event.event_ts);
        }
    } else {
        return None;
    }

    Some(event)
}

fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| {
        value
            .as_str()
            .and_then(|text| text.parse::<f64>().ok())
            .or_else(|| value.as_f64())
    })
}

fn nested_number(value: Option<&Value>, row: usize, column: usize) -> Option<f64> {
    value
        .and_then(Value::as_array)
        .and_then(|rows| rows.get(row))
        .and_then(Value::as_array)
        .and_then(|columns| columns.get(column))
        .and_then(|value| value.as_str())
        .and_then(|text| text.parse::<f64>().ok())
}
