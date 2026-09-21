use crate::model::{Exchange, MarketEvent, MarketPoint, Prediction, PredictionStats};
use anyhow::Context;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{str::FromStr, time::Duration};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    writer: mpsc::Sender<MarketEvent>,
}

impl Database {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .context("connect sqlite")?;

        Self::init_schema(&pool).await?;

        let (writer, rx) = mpsc::channel(50_000);
        Self::spawn_writer(pool.clone(), rx);

        Ok(Self { pool, writer })
    }

    async fn init_schema(pool: &SqlitePool) -> anyhow::Result<()> {
        let statements = [
            r#"CREATE TABLE IF NOT EXISTS market_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL,
                event_ts INTEGER NOT NULL,
                received_ts INTEGER NOT NULL,
                price REAL,
                qty REAL,
                side TEXT,
                bid_price REAL,
                bid_qty REAL,
                ask_price REAL,
                ask_qty REAL,
                raw_json TEXT NOT NULL
            )"#,
            r#"CREATE INDEX IF NOT EXISTS idx_market_lookup
               ON market_events(exchange, symbol, received_ts)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_market_kind
               ON market_events(exchange, symbol, kind, received_ts)"#,
            r#"CREATE TABLE IF NOT EXISTS predictions (
                id TEXT PRIMARY KEY,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                horizon_secs INTEGER NOT NULL,
                direction TEXT NOT NULL,
                entry_price REAL NOT NULL,
                target_price REAL NOT NULL,
                stop_price REAL NOT NULL,
                confidence REAL NOT NULL,
                score REAL NOT NULL,
                expected_return REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                resolved_at INTEGER,
                exit_price REAL,
                pnl_bps REAL
            )"#,
            r#"CREATE INDEX IF NOT EXISTS idx_predictions_lookup
               ON predictions(exchange, symbol, created_at)"#,
            r#"CREATE INDEX IF NOT EXISTS idx_predictions_status
               ON predictions(status, created_at)"#,
        ];

        for statement in statements {
            sqlx::query(statement).execute(pool).await?;
        }
        Ok(())
    }

    fn spawn_writer(pool: SqlitePool, mut rx: mpsc::Receiver<MarketEvent>) {
        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = Vec::with_capacity(256);
                batch.push(first);
                let deadline = tokio::time::Instant::now() + Duration::from_millis(40);

                while batch.len() < 256 {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(event)) => batch.push(event),
                        _ => break,
                    }
                }

                let mut tx = match pool.begin().await {
                    Ok(tx) => tx,
                    Err(error) => {
                        tracing::error!(%error, "database begin failed");
                        continue;
                    }
                };

                let mut failed = false;
                for event in batch {
                    let result = sqlx::query(
                        r#"INSERT INTO market_events (
                            exchange, symbol, kind, event_ts, received_ts,
                            price, qty, side, bid_price, bid_qty, ask_price, ask_qty, raw_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                    )
                    .bind(event.exchange.to_string())
                    .bind(event.symbol)
                    .bind(event.kind)
                    .bind(event.event_ts)
                    .bind(event.received_ts)
                    .bind(event.price)
                    .bind(event.qty)
                    .bind(event.side)
                    .bind(event.bid_price)
                    .bind(event.bid_qty)
                    .bind(event.ask_price)
                    .bind(event.ask_qty)
                    .bind(event.raw_json)
                    .execute(&mut *tx)
                    .await;

                    if let Err(error) = result {
                        tracing::error!(%error, "database insert failed");
                        failed = true;
                        break;
                    }
                }

                if failed {
                    let _ = tx.rollback().await;
                } else if let Err(error) = tx.commit().await {
                    tracing::error!(%error, "database commit failed");
                }
            }
        });
    }

    pub async fn insert_event(&self, event: MarketEvent) -> anyhow::Result<()> {
        self.writer
            .send(event)
            .await
            .context("market event writer closed")
    }

    pub async fn count_events(&self, exchange: Exchange, symbol: &str) -> anyhow::Result<i64> {
        let row = sqlx::query(
            "SELECT COUNT(*) AS n FROM market_events WHERE exchange = ? AND symbol = ?",
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.try_get::<i64, _>("n")?)
    }

    pub async fn load_points(
        &self,
        exchange: Exchange,
        symbol: &str,
        since_ms: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<MarketPoint>> {
        let rows = sqlx::query(
            r#"SELECT kind, received_ts, price, qty, side,
                      bid_price, bid_qty, ask_price, ask_qty
               FROM market_events
               WHERE exchange = ? AND symbol = ? AND received_ts >= ?
               ORDER BY received_ts ASC
               LIMIT ?"#,
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .bind(since_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(MarketPoint {
                    kind: row.try_get("kind")?,
                    ts: row.try_get("received_ts")?,
                    price: row.try_get("price")?,
                    qty: row.try_get("qty")?,
                    side: row.try_get("side")?,
                    bid_price: row.try_get("bid_price")?,
                    bid_qty: row.try_get("bid_qty")?,
                    ask_price: row.try_get("ask_price")?,
                    ask_qty: row.try_get("ask_qty")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(Into::into)
    }

    pub async fn insert_prediction(&self, prediction: &Prediction) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO predictions (
                id, exchange, symbol, created_at, horizon_secs, direction,
                entry_price, target_price, stop_price, confidence, score,
                expected_return, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&prediction.id)
        .bind(prediction.exchange.to_string())
        .bind(&prediction.symbol)
        .bind(prediction.created_at)
        .bind(prediction.horizon_secs)
        .bind(&prediction.direction)
        .bind(prediction.entry_price)
        .bind(prediction.target_price)
        .bind(prediction.stop_price)
        .bind(prediction.confidence)
        .bind(prediction.score)
        .bind(prediction.expected_return)
        .bind(&prediction.status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn open_predictions(&self) -> anyhow::Result<Vec<Prediction>> {
        let rows = sqlx::query(
            "SELECT * FROM predictions WHERE status = 'OPEN' ORDER BY created_at ASC LIMIT 500",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_prediction).collect()
    }

    pub async fn recent_predictions(
        &self,
        exchange: Exchange,
        symbol: &str,
        limit: i64,
    ) -> anyhow::Result<Vec<Prediction>> {
        let rows = sqlx::query(
            r#"SELECT * FROM predictions
               WHERE exchange = ? AND symbol = ?
               ORDER BY created_at DESC LIMIT ?"#,
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_prediction).collect()
    }

    pub async fn prediction_stats(
        &self,
        exchange: Exchange,
        symbol: &str,
    ) -> anyhow::Result<PredictionStats> {
        let row = sqlx::query(
            r#"SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status != 'OPEN' THEN 1 ELSE 0 END) AS resolved,
                SUM(CASE WHEN status = 'WIN' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN status = 'TIMEOUT' THEN 1 ELSE 0 END) AS timeouts,
                AVG(CASE WHEN status != 'OPEN' THEN pnl_bps END) AS avg_pnl_bps
               FROM predictions
               WHERE exchange = ? AND symbol = ?"#,
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .fetch_one(&self.pool)
        .await?;

        let total = row.try_get::<i64, _>("total")?;
        let resolved = row.try_get::<Option<i64>, _>("resolved")?.unwrap_or(0);
        let wins = row.try_get::<Option<i64>, _>("wins")?.unwrap_or(0);
        let losses = row.try_get::<Option<i64>, _>("losses")?.unwrap_or(0);
        let timeouts = row.try_get::<Option<i64>, _>("timeouts")?.unwrap_or(0);
        let avg_pnl_bps = row
            .try_get::<Option<f64>, _>("avg_pnl_bps")?
            .unwrap_or(0.0);
        let win_rate = if resolved > 0 {
            wins as f64 / resolved as f64
        } else {
            0.0
        };

        Ok(PredictionStats {
            total,
            resolved,
            wins,
            losses,
            timeouts,
            win_rate,
            avg_pnl_bps,
        })
    }

    pub async fn first_crossing(
        &self,
        prediction: &Prediction,
        end_ts: i64,
    ) -> anyhow::Result<Option<(i64, f64, bool)>> {
        let (upper, lower) = if prediction.direction == "LONG" {
            (prediction.target_price, prediction.stop_price)
        } else {
            (prediction.stop_price, prediction.target_price)
        };

        let row = sqlx::query(
            r#"SELECT received_ts, price
               FROM market_events
               WHERE exchange = ? AND symbol = ?
                 AND kind IN ('trade', 'agg_trade', 'public_trade')
                 AND received_ts > ? AND received_ts <= ?
                 AND price IS NOT NULL
                 AND (price >= ? OR price <= ?)
               ORDER BY received_ts ASC, id ASC
               LIMIT 1"#,
        )
        .bind(prediction.exchange.to_string())
        .bind(&prediction.symbol)
        .bind(prediction.created_at)
        .bind(end_ts)
        .bind(upper)
        .bind(lower)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let ts: i64 = row.try_get("received_ts")?;
            let price: f64 = row.try_get("price")?;
            let won = if prediction.direction == "LONG" {
                price >= prediction.target_price
            } else {
                price <= prediction.target_price
            };
            Ok(Some((ts, price, won)))
        } else {
            Ok(None)
        }
    }

    pub async fn price_at_or_before(
        &self,
        exchange: Exchange,
        symbol: &str,
        ts: i64,
    ) -> anyhow::Result<Option<f64>> {
        let row = sqlx::query(
            r#"SELECT price FROM market_events
               WHERE exchange = ? AND symbol = ? AND received_ts <= ?
                 AND price IS NOT NULL
               ORDER BY received_ts DESC, id DESC LIMIT 1"#,
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .bind(ts)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|row| row.try_get::<Option<f64>, _>("price").ok().flatten()))
    }

    pub async fn resolve_prediction(
        &self,
        id: &str,
        status: &str,
        resolved_at: i64,
        exit_price: f64,
        pnl_bps: f64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"UPDATE predictions
               SET status = ?, resolved_at = ?, exit_price = ?, pnl_bps = ?
               WHERE id = ? AND status = 'OPEN'"#,
        )
        .bind(status)
        .bind(resolved_at)
        .bind(exit_price)
        .bind(pnl_bps)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn row_to_prediction(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<Prediction> {
    let exchange_text: String = row.try_get("exchange")?;
    Ok(Prediction {
        id: row.try_get("id")?,
        exchange: exchange_text.parse()?,
        symbol: row.try_get("symbol")?,
        created_at: row.try_get("created_at")?,
        horizon_secs: row.try_get("horizon_secs")?,
        direction: row.try_get("direction")?,
        entry_price: row.try_get("entry_price")?,
        target_price: row.try_get("target_price")?,
        stop_price: row.try_get("stop_price")?,
        confidence: row.try_get("confidence")?,
        score: row.try_get("score")?,
        expected_return: row.try_get("expected_return")?,
        status: row.try_get("status")?,
        resolved_at: row.try_get("resolved_at")?,
        exit_price: row.try_get("exit_price")?,
        pnl_bps: row.try_get("pnl_bps")?,
    })
}
