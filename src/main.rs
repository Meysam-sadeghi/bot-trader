mod analysis;
mod api;
mod capture;
mod db;
mod event_bus;
mod model;

use analysis::AnalysisManager;
use api::AppState;
use capture::CaptureManager;
use db::Database;
use event_bus::EventBus;
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("market_lab=info,tower_http=info")),
        )
        .init();

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
    std::fs::create_dir_all(&data_dir)?;
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| format!("sqlite://{data_dir}/market.db"));

    let db = Database::connect(&database_url).await?;
    let bus = EventBus::new();
    let capture = CaptureManager::new(db.clone(), bus.clone());
    let analysis = AnalysisManager::new(db.clone());
    analysis.spawn_resolver();

    let admin_token = std::env::var("ADMIN_TOKEN").unwrap_or_default();
    let update_request_path = format!("{data_dir}/update.request");

    let state = AppState {
        db,
        capture,
        analysis,
        bus,
        admin_token,
        update_request_path,
    };

    let app = api::router(state).layer(TraceLayer::new_for_http());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(address).await?;

    tracing::info!(%address, "Market Lab listening");
    axum::serve(listener, app).await?;
    Ok(())
}
