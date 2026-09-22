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
    let (mut socket, _) =
        tokio::time::timeout(Duration::from_secs(15), connect_async(url.as_str()))
            .await
            .context("Bybit websocket connection timed out")?
            .with_context(|| format!("connect Bybit websocket: {url}"))?;

    let subscribe = json!({
        "op": "subscribe",
        "args": [
            format!("publicTrade.{}", context.symbol),
            format!("tickers.{}", context.symbol),
            format!("orderbook.1.{}", context.symbol),
            format!("orderbook.full.{}", context.symbol),
            format!("kline.1.{}", context.symbol)
        ]
    });
    socket
        .send(Message::Text(subscribe.to_string().into()))
        .await?;

    // Subscribe before taking the REST snapshot so full-depth deltas can queue in
    // the socket. Persisted snapshot u/seq plus subsequent deltas allow exact
    // replay according to Bybit's documented synchronization procedure.
    if let Err(error) = capture_snapshot(&context).await {
        // Level-1 snapshots remain sufficient for the paper execution model.
        tracing::warn!(%error, "Bybit full-depth archive snapshot unavailable; continuing independent level-1 feed");
    }

    let (mut write, mut read) = socket.split();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(
        exchange = "bybit",
        symbol = %context.symbol,
        depth = "full+10000-snapshot",
        "websocket connected"
    );

    let mut last_message = tokio::time::Instant::now();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                anyhow::ensure!(last_message.elapsed() < Duration::from_secs(60), "Bybit websocket received no messages for 60 seconds");
                write
                    .send(Message::Text(r#"{"op":"ping"}"#.into()))
                    .await?;
            }
            message = read.next() => {
                let Some(message) = message else {
                    anyhow::bail!("Bybit websocket disconnected");
                };
                last_message = tokio::time::Instant::now();
                match message? {
                    Message::Text(text) => {
                        let root: Value = serde_json::from_str(text.as_str())?;
                        let topic = root.get("topic").and_then(Value::as_str).unwrap_or_default();
                        if topic.is_empty() {
                            if root.get("success").and_then(Value::as_bool) == Some(false) {
                                anyhow::bail!("Bybit subscription rejected: {}", root.get("ret_msg").unwrap_or(&Value::Null));
                            }
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

async fn capture_snapshot(context: &CaptureContext) -> anyhow::Result<()> {
    let rest_base =
        std::env::var("BYBIT_REST_BASE").unwrap_or_else(|_| "https://api.bybit.com".to_string());
    let url = format!(
        "{rest_base}/v5/market/full_orderbook?category=spot&symbol={}",
        context.symbol
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let response = client
        .get(&url)
        .send()
        .await
        .context("fetch Bybit full depth snapshot")?
        .error_for_status()
        .context("Bybit full depth snapshot HTTP error")?;
    let root: Value = response.json().await?;

    let ret_code = root.get("retCode").and_then(integer).unwrap_or(-1);
    if ret_code != 0 {
        anyhow::bail!(
            "Bybit full depth snapshot rejected: {}",
            root.get("retMsg")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        );
    }

    let data = root.get("result").cloned().unwrap_or(Value::Null);
    let ts = data.get("ts").and_then(integer).unwrap_or_else(now_ms);
    let event = MarketEvent {
        exchange: Exchange::Bybit,
        symbol: context.symbol.clone(),
        kind: "depth_snapshot".to_string(),
        event_ts: ts,
        received_ts: now_ms(),
        price: None,
        qty: None,
        side: None,
        bid_price: nested_number(data.get("b"), 0, 0),
        bid_qty: nested_number(data.get("b"), 0, 1),
        ask_price: nested_number(data.get("a"), 0, 0),
        ask_qty: nested_number(data.get("a"), 0, 1),
        raw_json: data.to_string(),
    };
    context.emit(event).await
}

fn normalize(topic: &str, root: &Value, symbol: &str) -> Vec<MarketEvent> {
    let received_ts = now_ms();
    let event_ts = root.get("ts").and_then(integer).unwrap_or(received_ts);
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
                event_ts: trade.get("T").and_then(integer).unwrap_or(event_ts),
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
        event.kind = "price_ticker".to_string();
        event.price = number(object.get("lastPrice"));
        event.bid_price = number(object.get("bid1Price"));
        event.bid_qty = number(object.get("bid1Size"));
        event.ask_price = number(object.get("ask1Price"));
        event.ask_qty = number(object.get("ask1Size"));
    } else if topic.starts_with("orderbook.1.") {
        // Bybit spot ticker does not supply reliable bid1/ask1 fields. Level 1 is snapshot-only.
        event.kind = "book_ticker".to_string();
        event.bid_price = nested_number(object.get("b"), 0, 0);
        event.bid_qty = nested_number(object.get("b"), 0, 1);
        event.ask_price = nested_number(object.get("a"), 0, 0);
        event.ask_qty = nested_number(object.get("a"), 0, 1);
        if let (Some(bid), Some(ask)) = (event.bid_price, event.ask_price) {
            event.price = Some((bid + ask) / 2.0);
        }
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
            .and_then(integer)
            .unwrap_or(event_ts);
    } else {
        return Vec::new();
    }

    vec![event]
}

fn integer(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn spot_level_one_is_an_executable_snapshot_but_full_delta_is_not() {
        let root = json!({"ts": "1000", "type": "snapshot", "data": {"b": [["100", "2"]], "a": [["100.01", "3"]]}});
        let best = normalize("orderbook.1.BTCUSDT", &root, "BTCUSDT").remove(0);
        assert_eq!(best.kind, "book_ticker");
        assert_eq!(best.event_ts, 1000);
        assert_eq!(best.bid_price, Some(100.));
        assert_eq!(best.ask_qty, Some(3.));
        let delta = normalize("orderbook.full.BTCUSDT", &root, "BTCUSDT").remove(0);
        assert_eq!(delta.kind, "depth");
        assert_eq!(delta.price, None);
    }
    #[test]
    fn spot_ticker_is_not_misrepresented_as_a_zero_spread_quote() {
        let root = json!({"ts": 1000, "data": {"lastPrice": "100"}});
        let ticker = normalize("tickers.BTCUSDT", &root, "BTCUSDT").remove(0);
        assert_eq!(ticker.kind, "price_ticker");
        assert_eq!(ticker.bid_price, None);
    }
}
