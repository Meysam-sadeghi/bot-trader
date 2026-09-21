use crate::model::{BusEvent, Exchange};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct EventBus {
    binance: broadcast::Sender<BusEvent>,
    bybit: broadcast::Sender<BusEvent>,
}

impl EventBus {
    pub fn new() -> Self {
        let (binance, _) = broadcast::channel(4096);
        let (bybit, _) = broadcast::channel(4096);
        Self { binance, bybit }
    }

    pub fn send(&self, event: BusEvent) {
        let _ = match event.exchange {
            Exchange::Binance => self.binance.send(event),
            Exchange::Bybit => self.bybit.send(event),
        };
    }

    pub fn subscribe(&self, exchange: Exchange) -> broadcast::Receiver<BusEvent> {
        match exchange {
            Exchange::Binance => self.binance.subscribe(),
            Exchange::Bybit => self.bybit.subscribe(),
        }
    }
}
