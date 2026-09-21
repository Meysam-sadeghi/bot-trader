use super::CaptureContext;
use crate::model::{Exchange, MarketEvent, now_ms};
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
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
                tracing::warn!(exchange = "bybit", %error, backoff, "reconnecting");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
            }
        }
    }
}

async fn connect_once(context: CaptureContext) -> anyhow::Result<()> {
    let url = std::env::var("BYBIT_WS_URL")
        .unwrap_or_else(|_| "wss://stream.bybit.com/v5/public/spot".to_string());
    let (mut socket, _) = connect_async(&url)
        .await
        .with_context(|| format!("connect Bybit websocket: {url}"))?;

    let subscribe = json!({
        "op": "subscribe",
        "args": [
            format!("publicTrade.{}", context.symbol),
            format!("tickers.{}", context.symbol),
            format!("orderbook.200.{}", context.symbol),
            format!("kline.1.{}", context.symbol)
        ]
    });
    socket
        .send(Message::Text(subscribe.to_string().into()))
        .await?;

    let (mut write, mut read) = socket.split();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(exchange = "bybit", symbol = %context.symbol, "websocket connected");

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                write
                    .send(Message::Text(r#"{"op":"ping"}"#.into()))
                    .await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    anyhow::bail!("Bybit websocket disconnected");
                };
                match message? {
                    Message::Text(text) => {
                        let root: Value = serde_json::from_str(text.as_str())?;
                        let topic = root.get("topic").and_then(Value::as_str).unwrap_or_default();
                        if topic.is_empty() {
                            continue;
                        }

                        for event in normalize(topic, &root, &context.symbol) {
                            context.emit(event).await?;
                        }
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                    }
                    Message::Close(_) => anyhow::bail!("Bybit websocket closed"),
                    _ => {}
                }
            }
        }
    }
}

fn normalize(topic: &str, root: &Value, symbol: &str) -> Vec<MarketEvent> {
    let received_ts = now_ms();
    let event_ts = root.get("ts").and_then(Value::as_i64).unwrap_or(received_ts);
    let data = root.get("data").cloned().unwrap_or(Value::Null);

    if topic.starts_with("publicTrade.") {
        return data
            .as_array()
            .into_iter()
            .flatten()
            .map(|trade| MarketEvent {
                exchange: Exchange::Bybit,
                symbol: symbol.to_string(),
                kind: "public_trade".to_string(),
                event_ts: trade.get("T").and_then(Value::as_i64).unwrap_or(event_ts),
                received_ts,
                price: number(trade.get("p")),
                qty: number(trade.get("v")),
                side: trade
                    .get("S")
                    .and_then(Value::as_str)
                    .map(|side| side.to_ascii_uppercase()),
                bid_price: None,
                bid_qty: None,
                ask_price: None,
                ask_qty: None,
                raw_json: trade.to_string(),
            })
            .collect();
    }

    let object = if let Some(array) = data.as_array() {
        array.first().unwrap_or(&Value::Null)
    } else {
        &data
    };

    let mut event = MarketEvent {
        exchange: Exchange::Bybit,
        symbol: symbol.to_string(),
        kind: "unknown".to_string(),
        event_ts,
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

    if topic.starts_with("tickers.") {
        event.kind = "book_ticker".to_string();
        event.price = number(object.get("lastPrice"));
        event.bid_price = number(object.get("bid1Price"));
        event.bid_qty = number(object.get("bid1Size"));
        event.ask_price = number(object.get("ask1Price"));
        event.ask_qty = number(object.get("ask1Size"));
    } else if topic.starts_with("orderbook.") {
        event.kind = "depth".to_string();
        event.bid_price = nested_number(object.get("b"), 0, 0);
        event.bid_qty = nested_number(object.get("b"), 0, 1);
        event.ask_price = nested_number(object.get("a"), 0, 0);
        event.ask_qty = nested_number(object.get("a"), 0, 1);
    } else if topic.starts_with("kline.") {
        event.kind = "kline".to_string();
        event.price = number(object.get("close"));
        event.qty = number(object.get("volume"));
        event.event_ts = object
            .get("timestamp")
            .and_then(Value::as_i64)
            .unwrap_or(event_ts);
    } else {
        return Vec::new();
    }

    vec![event]
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
