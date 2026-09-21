use crate::{
    analysis::AnalysisManager,
    capture::CaptureManager,
    db::Database,
    event_bus::EventBus,
    model::{Dashboard, Exchange},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, str::FromStr};
use tower_http::{cors::CorsLayer, services::ServeDir};

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub capture: CaptureManager,
    pub analysis: AnalysisManager,
    pub bus: EventBus,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui))
        .route("/binance", get(ui))
        .route("/bybit", get(ui))
        .route("/health", get(health))
        .route("/api/capture/{exchange}/start", post(start_capture))
        .route("/api/capture/{exchange}/stop", post(stop_capture))
        .route("/api/analysis/{exchange}/start", post(start_analysis))
        .route("/api/analysis/{exchange}/stop", post(stop_analysis))
        .route("/api/dashboard/{exchange}", get(dashboard))
        .route("/api/predictions/{exchange}", get(predictions))
        .route("/ws/{exchange}", get(ws_upgrade))
        .nest_service("/assets", ServeDir::new("static"))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn ui() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"ok": true, "service": "market-lab"}))
}

#[derive(Debug, Deserialize)]
struct StartParams {
    symbol: Option<String>,
}

async fn start_capture(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
    Query(params): Query<StartParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let symbol = normalize_symbol(params.symbol.as_deref().unwrap_or("BTCUSDT"))?;
    let started = state.capture.start(exchange, symbol.clone()).await;

    Ok(Json(json!({
        "started": started,
        "exchange": exchange,
        "symbol": symbol
    })))
}

async fn stop_capture(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let stopped = state.capture.stop(exchange).await;
    Ok(Json(json!({"stopped": stopped, "exchange": exchange})))
}

async fn start_analysis(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
    Query(params): Query<StartParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let current = state.capture.status(exchange).await;
    let requested = params.symbol.as_deref().unwrap_or(&current.symbol);
    let symbol = normalize_symbol(requested)?;
    let started = state.analysis.start(exchange, symbol.clone()).await;

    Ok(Json(json!({
        "started": started,
        "exchange": exchange,
        "symbol": symbol
    })))
}

async fn stop_analysis(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let stopped = state.analysis.stop(exchange).await;
    Ok(Json(json!({"stopped": stopped, "exchange": exchange})))
}

async fn dashboard(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Json<Dashboard>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let capture = state.capture.status(exchange).await;
    let symbol = capture.symbol.clone();

    let stored_events = state
        .db
        .count_events(exchange, &symbol)
        .await
        .map_err(ApiError::internal)?;
    let stats = state
        .db
        .prediction_stats(exchange, &symbol)
        .await
        .map_err(ApiError::internal)?;
    let predictions = state
        .db
        .recent_predictions(exchange, &symbol, 30)
        .await
        .map_err(ApiError::internal)?;
    let analysis_running = state.analysis.is_running(exchange).await;

    Ok(Json(Dashboard {
        capture,
        analysis_running,
        stored_events,
        stats,
        predictions,
    }))
}

async fn predictions(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let capture = state.capture.status(exchange).await;
    let symbol = query
        .get("symbol")
        .cloned()
        .unwrap_or(capture.symbol);
    let symbol = normalize_symbol(&symbol)?;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 500);

    let items = state
        .db
        .recent_predictions(exchange, &symbol, limit)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"predictions": items})))
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Response, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    Ok(ws
        .on_upgrade(move |socket| ws_session(socket, state.bus, exchange))
        .into_response())
}

async fn ws_session(mut socket: WebSocket, bus: EventBus, exchange: Exchange) {
    let mut receiver = bus.subscribe(exchange);

    loop {
        match receiver.recv().await {
            Ok(event) => {
                let Ok(text) = serde_json::to_string(&event) else {
                    continue;
                };
                if socket.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
}

fn parse_exchange(value: &str) -> Result<Exchange, ApiError> {
    Exchange::from_str(value).map_err(|_| ApiError {
        status: StatusCode::BAD_REQUEST,
        message: "exchange must be binance or bybit".to_string(),
    })
}

fn normalize_symbol(value: &str) -> Result<String, ApiError> {
    let symbol = value.trim().to_ascii_uppercase();
    if symbol.len() < 5
        || symbol.len() > 24
        || !symbol.chars().all(|character| character.is_ascii_alphanumeric())
    {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "invalid symbol".to_string(),
        });
    }
    Ok(symbol)
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(%error, "api internal error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": self.message}))).into_response()
    }
}
