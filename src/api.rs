use crate::{
    analysis::AnalysisManager,
    capture::CaptureManager,
    db::Database,
    event_bus::EventBus,
    export::{self, ExportFilters},
    model::{Dashboard, Exchange},
    paper::{self, HORIZONS, STRATEGIES},
};
use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
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
    pub admin_token: String,
    pub update_request_path: String,
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
        .route("/api/data/{exchange}/clear", post(clear_data))
        .route("/api/system/update", post(update_system))
        .route("/api/lab/start", post(start_both))
        .route("/api/lab/stop", post(stop_both))
        .route("/api/dashboard/{exchange}", get(dashboard))
        .route("/api/predictions/{exchange}", get(predictions))
        .route("/api/positions/{id}", get(position_details))
        .route("/api/exports/positions", get(export_positions))
        .route("/api/exports/summary", get(export_summary))
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
    let requested_symbol = normalize_symbol(params.symbol.as_deref().unwrap_or("BTCUSDT"))?;
    let current = state.capture.status(exchange).await;
    if current.running && current.symbol != requested_symbol {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "Stop this exchange before changing its symbol".into(),
        });
    }
    let capture_started = state
        .capture
        .start(exchange, requested_symbol.clone())
        .await;
    if capture_started {
        state.analysis.stop(exchange).await;
    }

    // Starting capture also starts the analysis/paper-trading loop automatically.
    let active_symbol = state.capture.status(exchange).await.symbol;
    let analysis_started = state.analysis.start(exchange, active_symbol.clone()).await;

    Ok(Json(json!({
        "started": capture_started,
        "analysis_started": analysis_started,
        "exchange": exchange,
        "symbol": active_symbol,
        "automatic": true
    })))
}

async fn start_both(
    State(state): State<AppState>,
    Query(params): Query<StartParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let symbol = normalize_symbol(params.symbol.as_deref().unwrap_or("BTCUSDT"))?;
    // Validate both before starting either, so a symbol conflict is never hidden.
    for exchange in [Exchange::Binance, Exchange::Bybit] {
        let current = state.capture.status(exchange).await;
        if current.running && current.symbol != symbol {
            return Err(ApiError {
                status: StatusCode::CONFLICT,
                message: format!("Stop {exchange} before changing its symbol"),
            });
        }
    }
    for exchange in [Exchange::Binance, Exchange::Bybit] {
        if state.capture.start(exchange, symbol.clone()).await {
            state.analysis.stop(exchange).await;
        }
        state.analysis.start(exchange, symbol.clone()).await;
    }
    Ok(Json(
        json!({"ok": true, "symbol": symbol, "exchanges": ["binance", "bybit"], "strategies_per_exchange": STRATEGIES.len()}),
    ))
}
async fn stop_both(State(state): State<AppState>) -> Json<serde_json::Value> {
    for exchange in [Exchange::Binance, Exchange::Bybit] {
        state.analysis.stop(exchange).await;
        state.capture.stop(exchange).await;
    }
    Json(json!({"ok": true}))
}

async fn stop_capture(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let analysis_stopped = state.analysis.stop(exchange).await;
    let capture_stopped = state.capture.stop(exchange).await;
    Ok(Json(json!({
        "stopped": capture_stopped,
        "analysis_stopped": analysis_stopped,
        "exchange": exchange
    })))
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
    if !current.running || current.symbol != symbol {
        return Err(ApiError {
            status: StatusCode::CONFLICT,
            message: "Start capture for this symbol first".into(),
        });
    }
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

async fn clear_data(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&state, &headers)?;
    let exchange = parse_exchange(&exchange)?;

    // Stop producers first, then flush the database queue before deleting rows.
    state.analysis.stop(exchange).await;
    state.capture.stop(exchange).await;

    let (events_deleted, predictions_deleted) = state
        .db
        .clear_exchange(exchange)
        .await
        .map_err(ApiError::internal)?;
    state.capture.reset_status(exchange).await;

    Ok(Json(json!({
        "ok": true,
        "exchange": exchange,
        "events_deleted": events_deleted,
        "predictions_deleted": predictions_deleted
    })))
}

async fn update_system(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    require_admin(&state, &headers)?;

    let binance = state.capture.status(Exchange::Binance).await;
    let bybit = state.capture.status(Exchange::Bybit).await;
    let mut resume = String::new();
    if binance.running {
        resume.push_str(&format!("binance {}\n", binance.symbol));
    }
    if bybit.running {
        resume.push_str(&format!("bybit {}\n", bybit.symbol));
    }

    tokio::fs::write(&state.update_request_path, resume)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(json!({
        "ok": true,
        "message": "update queued; GitHub will be rebuilt and the service will restart automatically"
    })))
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
    let config_id = state.analysis.config.id();
    let mut predictions = state
        .db
        .experiment_predictions(exchange, &symbol, &config_id)
        .await
        .map_err(ApiError::internal)?;
    let strategies = paper::reports(&predictions, &state.analysis.config);
    let refs: Vec<_> = predictions.iter().collect();
    let stats = paper::stats(
        &refs,
        state.analysis.config.initial_capital * HORIZONS.len() as f64 * STRATEGIES.len() as f64,
        &state.analysis.config,
    );
    let archived_predictions = state
        .db
        .archived_count(exchange, &symbol, &config_id)
        .await
        .map_err(ApiError::internal)?;
    predictions.truncate(120);
    let analysis_running = state.analysis.is_running(exchange).await;

    Ok(Json(Dashboard {
        capture,
        analysis_running,
        stored_events,
        stats,
        predictions,
        strategies,
        diagnostics: state.analysis.diagnostics(exchange).await,
        config: state.analysis.config.clone(),
        config_id,
        archived_predictions,
    }))
}

async fn predictions(
    State(state): State<AppState>,
    Path(exchange): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let exchange = parse_exchange(&exchange)?;
    let capture = state.capture.status(exchange).await;
    let symbol = query.get("symbol").cloned().unwrap_or(capture.symbol);
    let symbol = normalize_symbol(&symbol)?;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 500);

    let config_id = state.analysis.config.id();
    let requested_config = query
        .get("config")
        .map(String::as_str)
        .unwrap_or(&config_id);
    let filter = if requested_config == "all" {
        None
    } else {
        Some(requested_config)
    };
    let items = state
        .db
        .recent_predictions(exchange, &symbol, filter, limit)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(json!({"predictions": items})))
}

async fn position_details(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<export::ExportPosition>, ApiError> {
    let position = state
        .db
        .prediction_by_id(&id)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError {
            status: StatusCode::NOT_FOUND,
            message: "Position not found".into(),
        })?;
    Ok(Json(position.into()))
}

async fn build_export(
    state: &AppState,
    query: &HashMap<String, String>,
) -> Result<export::ExportReport, ApiError> {
    let filters =
        ExportFilters::parse(query, &state.analysis.config.id()).map_err(|message| ApiError {
            status: StatusCode::BAD_REQUEST,
            message,
        })?;
    let snapshot = state.db.export_positions(&filters).await.map_err(|error| {
        if error.downcast_ref::<export::ExportTooLarge>().is_some() {
            ApiError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: error.to_string(),
            }
        } else {
            ApiError::internal(error)
        }
    })?;
    tokio::task::spawn_blocking(move || export::report(snapshot, filters))
        .await
        .map_err(ApiError::internal)
}

async fn export_summary(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let report = build_export(&state, &query).await?;
    Ok((
        [("cache-control", "no-store")],
        Json(export::summary_json(&report)),
    )
        .into_response())
}

async fn export_positions(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let format = query.get("format").map(String::as_str).unwrap_or("json");
    if !matches!(format, "json" | "csv") {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            message: "Export format must be json or csv".into(),
        });
    }
    let report = build_export(&state, &query).await?;
    let count = report.position_count;
    let filename = format!(
        "market-lab-positions-{}-{}.{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        count,
        format
    );
    let is_csv = format == "csv";
    let bytes = tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<u8>> {
        if is_csv {
            Ok(export::csv(&report)?.into_bytes())
        } else {
            Ok(serde_json::to_vec(&report)?)
        }
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::internal)?;
    Ok((
        [
            (
                "content-type",
                if is_csv {
                    "text/csv; charset=utf-8".into()
                } else {
                    "application/json; charset=utf-8".into()
                },
            ),
            (
                "content-disposition",
                format!("attachment; filename=\"{filename}\""),
            ),
            ("cache-control", "no-store".into()),
            ("x-position-count", count.to_string()),
            ("x-content-type-options", "nosniff".into()),
        ],
        bytes,
    )
        .into_response())
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

fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.admin_token.is_empty() {
        return Err(ApiError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: "admin actions are not configured on this server".to_string(),
        });
    }

    let supplied = headers
        .get("x-admin-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if supplied != state.admin_token {
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid admin token".to_string(),
        });
    }

    Ok(())
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
        || !symbol
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
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
