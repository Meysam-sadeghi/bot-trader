pub mod binance;
pub mod bybit;

use crate::{
    db::Database,
    event_bus::EventBus,
    model::{BusEvent, CaptureStatus, Exchange, MarketEvent, now_ms},
};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::RwLock,
    task::{AbortHandle, JoinHandle},
};

#[derive(Clone)]
pub struct CaptureContext {
    pub exchange: Exchange,
    pub symbol: String,
    db: Database,
    bus: EventBus,
    statuses: Arc<RwLock<HashMap<Exchange, CaptureStatus>>>,
}

impl CaptureContext {
    pub async fn emit(&self, event: MarketEvent) -> anyhow::Result<()> {
        {
            let mut statuses = self.statuses.write().await;
            let status = statuses
                .entry(self.exchange)
                .or_insert_with(|| CaptureStatus::new(self.exchange));
            status.messages = status.messages.saturating_add(1);
            status.last_event_at = Some(event.received_ts);
            if let Some(price) = event.price {
                status.last_price = Some(price);
            } else if event.kind == "book_ticker"
                && let (Some(bid), Some(ask)) = (event.bid_price, event.ask_price)
            {
                status.last_price = Some((bid + ask) / 2.0);
            }
            status.last_error = None;
        }

        self.bus.send(BusEvent::from(&event));
        self.db.insert_event(event).await
    }

    pub async fn set_error(&self, error: impl ToString) {
        let mut statuses = self.statuses.write().await;
        let status = statuses
            .entry(self.exchange)
            .or_insert_with(|| CaptureStatus::new(self.exchange));
        status.last_error = Some(error.to_string());
    }
}

#[derive(Clone)]
pub struct CaptureManager {
    db: Database,
    bus: EventBus,
    statuses: Arc<RwLock<HashMap<Exchange, CaptureStatus>>>,
    tasks: Arc<RwLock<HashMap<Exchange, AbortHandle>>>,
}

impl CaptureManager {
    pub fn new(db: Database, bus: EventBus) -> Self {
        let mut statuses = HashMap::new();
        statuses.insert(Exchange::Binance, CaptureStatus::new(Exchange::Binance));
        statuses.insert(Exchange::Bybit, CaptureStatus::new(Exchange::Bybit));
        Self {
            db,
            bus,
            statuses: Arc::new(RwLock::new(statuses)),
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&self, exchange: Exchange, symbol: String) -> bool {
        let mut tasks = self.tasks.write().await;
        if tasks.contains_key(&exchange) {
            return false;
        }

        {
            let mut statuses = self.statuses.write().await;
            let status = statuses
                .entry(exchange)
                .or_insert_with(|| CaptureStatus::new(exchange));
            status.running = true;
            status.symbol = symbol.clone();
            status.started_at = Some(now_ms());
            status.messages = 0;
            status.last_error = None;
        }

        let context = CaptureContext {
            exchange,
            symbol,
            db: self.db.clone(),
            bus: self.bus.clone(),
            statuses: self.statuses.clone(),
        };
        let manager = self.clone();

        let task: JoinHandle<()> = tokio::spawn(async move {
            let result = match exchange {
                Exchange::Binance => binance::run(context).await,
                Exchange::Bybit => bybit::run(context).await,
            };

            if let Err(error) = result {
                tracing::error!(exchange = %exchange, %error, "capture task ended");
                let mut statuses = manager.statuses.write().await;
                let status = statuses
                    .entry(exchange)
                    .or_insert_with(|| CaptureStatus::new(exchange));
                status.last_error = Some(error.to_string());
                status.running = false;
            }

            manager.tasks.write().await.remove(&exchange);
        });

        tasks.insert(exchange, task.abort_handle());
        true
    }

    pub async fn stop(&self, exchange: Exchange) -> bool {
        let handle = self.tasks.write().await.remove(&exchange);
        if let Some(handle) = handle {
            handle.abort();
            let mut statuses = self.statuses.write().await;
            if let Some(status) = statuses.get_mut(&exchange) {
                status.running = false;
            }
            true
        } else {
            false
        }
    }

    pub async fn reset_status(&self, exchange: Exchange) {
        let mut statuses = self.statuses.write().await;
        statuses.insert(exchange, CaptureStatus::new(exchange));
    }

    pub async fn status(&self, exchange: Exchange) -> CaptureStatus {
        self.statuses
            .read()
            .await
            .get(&exchange)
            .cloned()
            .unwrap_or_else(|| CaptureStatus::new(exchange))
    }
}
