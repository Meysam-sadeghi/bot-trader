use crate::{
    model::{Exchange, MarketEvent, MarketPoint, Prediction},
    paper::LabConfig,
};
use anyhow::Context;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::{str::FromStr, time::Duration};
use tokio::sync::{mpsc, oneshot};

enum WriterCommand {
    Event(Box<MarketEvent>),
    Flush(oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    writer: mpsc::Sender<WriterCommand>,
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
                strategy TEXT NOT NULL DEFAULT 'normal',
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

        // Existing databases predate strategy tracking. Preserve all historical
        // rows as "normal" and separate new Binance contrarian statistics.
        let columns = sqlx::query("PRAGMA table_info(predictions)")
            .fetch_all(pool)
            .await?;
        let has_strategy = columns.iter().any(|row| {
            row.try_get::<String, _>("name")
                .map(|name| name == "strategy")
                .unwrap_or(false)
        });
        if !has_strategy {
            sqlx::query(
                "ALTER TABLE predictions ADD COLUMN strategy TEXT NOT NULL DEFAULT 'normal'",
            )
            .execute(pool)
            .await?;
        }
        // Additive migration: legacy outcomes and their original assumptions stay intact.
        let existing: Vec<String> = columns
            .iter()
            .filter_map(|r| r.try_get("name").ok())
            .collect();
        for (name, definition) in [
            ("config_id", "TEXT NOT NULL DEFAULT 'legacy'"),
            ("fee_bps", "REAL NOT NULL DEFAULT 0"),
            ("slippage_bps", "REAL NOT NULL DEFAULT 0"),
            ("notional", "REAL NOT NULL DEFAULT 1000"),
            ("gross_pnl_bps", "REAL"),
            ("cursor_id", "INTEGER NOT NULL DEFAULT 0"),
            ("last_quote_ts", "INTEGER NOT NULL DEFAULT 0"),
            ("mark_price", "REAL"),
            ("max_gap_ms", "INTEGER NOT NULL DEFAULT 15000"),
            (
                "entry_reason",
                "TEXT NOT NULL DEFAULT 'Legacy gross-price simulation'",
            ),
            ("entry_snapshot", "TEXT"),
            ("execution_audit", "TEXT"),
        ] {
            if !existing.iter().any(|c| c == name) {
                sqlx::query(&format!(
                    "ALTER TABLE predictions ADD COLUMN {name} {definition}"
                ))
                .execute(pool)
                .await?;
            }
        }
        for statement in [
            "CREATE TABLE IF NOT EXISTS lab_configs (id TEXT PRIMARY KEY, json TEXT NOT NULL, created_at INTEGER NOT NULL)",
            "CREATE INDEX IF NOT EXISTS idx_predictions_config ON predictions(exchange, symbol, config_id, created_at)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_open_lane_v3 ON predictions(exchange, symbol, strategy, horizon_secs, config_id) WHERE status = 'OPEN' AND config_id != 'legacy'",
            "CREATE INDEX IF NOT EXISTS idx_quotes_cursor ON market_events(exchange, symbol, id) WHERE kind = 'book_ticker'",
        ] {
            sqlx::query(statement).execute(pool).await?;
        }

        Ok(())
    }

    fn spawn_writer(pool: SqlitePool, mut rx: mpsc::Receiver<WriterCommand>) {
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                let first = match command {
                    WriterCommand::Event(event) => event,
                    WriterCommand::Flush(done) => {
                        let _ = done.send(());
                        continue;
                    }
                };

                let mut batch = Vec::with_capacity(256);
                batch.push(first);
                let deadline = tokio::time::Instant::now() + Duration::from_millis(40);
                let mut barrier = None;

                while batch.len() < 256 {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(WriterCommand::Event(event))) => batch.push(event),
                        Ok(Some(WriterCommand::Flush(done))) => {
                            barrier = Some(done);
                            break;
                        }
                        _ => break,
                    }
                }

                // Retain and retry the entire batch. A disk error must never silently
                // drop market ticks and then acknowledge a successful flush.
                loop {
                    let result = async {
                        let mut tx = pool.begin().await?;
                        for event in &batch {
                            sqlx::query(
                                r#"INSERT INTO market_events (
                                exchange, symbol, kind, event_ts, received_ts,
                                price, qty, side, bid_price, bid_qty, ask_price, ask_qty, raw_json
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
                            )
                            .bind(event.exchange.to_string())
                            .bind(&event.symbol)
                            .bind(&event.kind)
                            .bind(event.event_ts)
                            .bind(event.received_ts)
                            .bind(event.price)
                            .bind(event.qty)
                            .bind(&event.side)
                            .bind(event.bid_price)
                            .bind(event.bid_qty)
                            .bind(event.ask_price)
                            .bind(event.ask_qty)
                            .bind(&event.raw_json)
                            .execute(&mut *tx)
                            .await?;
                        }
                        tx.commit().await
                    }
                    .await;
                    match result {
                        Ok(()) => break,
                        Err(error) => {
                            tracing::error!(%error, "market batch retained; retrying database write");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }

                if let Some(done) = barrier {
                    let _ = done.send(());
                }
            }
        });
    }

    pub async fn insert_event(&self, event: MarketEvent) -> anyhow::Result<()> {
        self.writer
            .send(WriterCommand::Event(Box::new(event)))
            .await
            .context("market event writer closed")
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.writer
            .send(WriterCommand::Flush(done_tx))
            .await
            .context("market event writer closed")?;
        done_rx.await.context("market event flush cancelled")?;
        Ok(())
    }

    pub async fn clear_exchange(&self, exchange: Exchange) -> anyhow::Result<(u64, u64)> {
        self.flush().await?;

        let mut tx = self.pool.begin().await?;
        let predictions = sqlx::query("DELETE FROM predictions WHERE exchange = ?")
            .bind(exchange.to_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let events = sqlx::query("DELETE FROM market_events WHERE exchange = ?")
            .bind(exchange.to_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tx.commit().await?;

        let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await;

        Ok((events, predictions))
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
        until_ms: i64,
    ) -> anyhow::Result<Vec<MarketPoint>> {
        let rows = sqlx::query(
            r#"SELECT id, kind, received_ts, price, qty, side,
                      bid_price, bid_qty, ask_price, ask_qty
               FROM (
                   SELECT id, kind, received_ts, price, qty, side,
                          bid_price, bid_qty, ask_price, ask_qty
                   FROM market_events
                   WHERE exchange = ? AND symbol = ? AND received_ts >= ?
                     AND received_ts <= ?
                     AND kind IN ('trade', 'public_trade', 'book_ticker')
                     AND event_ts >= received_ts - 5000 AND event_ts <= received_ts + 1000
                   ORDER BY received_ts DESC, id DESC
                   LIMIT ?
               ) recent
               ORDER BY received_ts ASC, id ASC"#,
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .bind(since_ms)
        .bind(until_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(MarketPoint {
                    kind: row.try_get("kind")?,
                    ts: row.try_get("received_ts")?,
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

    pub async fn register_config(&self, config: &LabConfig) -> anyhow::Result<()> {
        let json = serde_json::to_string(config)?;
        sqlx::query("INSERT OR IGNORE INTO lab_configs(id, json, created_at) VALUES (?, ?, ?)")
            .bind(config.id())
            .bind(&json)
            .bind(crate::model::now_ms())
            .execute(&self.pool)
            .await?;
        let stored: String = sqlx::query_scalar("SELECT json FROM lab_configs WHERE id = ?")
            .bind(config.id())
            .fetch_one(&self.pool)
            .await?;
        anyhow::ensure!(stored == json, "configuration identifier collision");
        Ok(())
    }

    pub async fn insert_prediction(&self, p: &Prediction) -> anyhow::Result<()> {
        sqlx::query(
            r#"INSERT INTO predictions (
            id, exchange, symbol, created_at, horizon_secs, direction,
            entry_price, target_price, stop_price, confidence, score, expected_return,
            strategy, status, config_id, fee_bps, slippage_bps, notional,
            cursor_id, last_quote_ts, mark_price, max_gap_ms, entry_reason,
            entry_snapshot, execution_audit
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&p.id)
        .bind(p.exchange.to_string())
        .bind(&p.symbol)
        .bind(p.created_at)
        .bind(p.horizon_secs)
        .bind(&p.direction)
        .bind(p.entry_price)
        .bind(p.target_price)
        .bind(p.stop_price)
        .bind(p.confidence)
        .bind(p.score)
        .bind(p.expected_return)
        .bind(&p.strategy)
        .bind(&p.status)
        .bind(&p.config_id)
        .bind(p.fee_bps)
        .bind(p.slippage_bps)
        .bind(p.notional)
        .bind(p.cursor_id)
        .bind(p.last_quote_ts)
        .bind(p.mark_price)
        .bind(p.max_gap_ms)
        .bind(&p.entry_reason)
        .bind(
            p.entry_snapshot
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        )
        .bind(
            p.execution_audit
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn open_predictions(&self) -> anyhow::Result<Vec<Prediction>> {
        let rows =
            sqlx::query("SELECT * FROM predictions WHERE status = 'OPEN' ORDER BY created_at, id")
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter().map(row_to_prediction).collect()
    }

    pub async fn experiment_predictions(
        &self,
        exchange: Exchange,
        symbol: &str,
        config_id: &str,
    ) -> anyhow::Result<Vec<Prediction>> {
        let rows = sqlx::query("SELECT * FROM predictions WHERE exchange = ? AND symbol = ? AND config_id = ? ORDER BY created_at DESC, id DESC")
            .bind(exchange.to_string()).bind(symbol).bind(config_id).fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_prediction).collect()
    }

    pub async fn recent_predictions(
        &self,
        exchange: Exchange,
        symbol: &str,
        config_id: Option<&str>,
        limit: i64,
    ) -> anyhow::Result<Vec<Prediction>> {
        let rows = sqlx::query("SELECT * FROM predictions WHERE exchange = ? AND symbol = ? AND (? IS NULL OR config_id = ?) ORDER BY created_at DESC, id DESC LIMIT ?")
            .bind(exchange.to_string()).bind(symbol).bind(config_id).bind(config_id).bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter().map(row_to_prediction).collect()
    }

    pub async fn archived_count(
        &self,
        exchange: Exchange,
        symbol: &str,
        config_id: &str,
    ) -> anyhow::Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COUNT(*) FROM predictions WHERE exchange = ? AND symbol = ? AND config_id != ?",
        )
        .bind(exchange.to_string())
        .bind(symbol)
        .bind(config_id)
        .fetch_one(&self.pool)
        .await?)
    }

    /// A single SQLite read snapshot includes both positions and their original configs.
    /// Never reuse the dashboard's 120/500-row limits for an export.
    pub async fn export_positions(
        &self,
        filters: &crate::export::ExportFilters,
    ) -> anyhow::Result<crate::export::ExportSnapshot> {
        let mut tx = self.pool.begin().await?;
        let mut query =
            sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT * FROM predictions WHERE 1=1");
        if let Some(exchange) = filters.exchange {
            query
                .push(" AND exchange = ")
                .push_bind(exchange.to_string());
        }
        for (column, value) in [
            ("symbol", &filters.symbol),
            ("strategy", &filters.strategy),
            ("config_id", &filters.config_id),
            ("status", &filters.status),
        ] {
            if let Some(value) = value {
                query.push(format!(" AND {column} = ")).push_bind(value);
            }
        }
        if let Some(horizon) = filters.horizon_secs {
            query.push(" AND horizon_secs = ").push_bind(horizon);
        }
        if let Some(from) = filters.from_ms {
            query.push(" AND created_at >= ").push_bind(from);
        }
        if let Some(to) = filters.to_ms {
            query.push(" AND created_at < ").push_bind(to);
        }
        query
            .push(" ORDER BY created_at, id LIMIT ")
            .push_bind(crate::export::MAX_EXPORT_POSITIONS + 1);
        let rows = query.build().fetch_all(&mut *tx).await?;
        if rows.len() as i64 > crate::export::MAX_EXPORT_POSITIONS {
            return Err(crate::export::ExportTooLarge.into());
        }
        let positions = rows
            .into_iter()
            .map(row_to_prediction)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let config_ids: std::collections::HashSet<_> =
            positions.iter().map(|p| p.config_id.as_str()).collect();
        let rows =
            sqlx::query("SELECT id, json, created_at FROM lab_configs ORDER BY created_at, id")
                .fetch_all(&mut *tx)
                .await?;
        let mut configurations = Vec::new();
        for row in rows {
            let id: String = row.try_get("id")?;
            if config_ids.contains(id.as_str()) {
                configurations.push(serde_json::json!({
                    "id": id,
                    "registered_at": row.try_get::<i64, _>("created_at")?,
                    "parameters": serde_json::from_str::<serde_json::Value>(&row.try_get::<String, _>("json")?)?
                }));
            }
        }
        tx.commit().await?;
        Ok(crate::export::ExportSnapshot {
            positions,
            configurations,
        })
    }

    pub async fn prediction_by_id(&self, id: &str) -> anyhow::Result<Option<Prediction>> {
        sqlx::query("SELECT * FROM predictions WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(row_to_prediction)
            .transpose()
    }

    pub async fn latest_quote(
        &self,
        exchange: Exchange,
        symbol: &str,
        at: i64,
    ) -> anyhow::Result<Option<Quote>> {
        let row = sqlx::query("SELECT id, received_ts, event_ts, bid_price, ask_price, bid_qty, ask_qty FROM market_events WHERE exchange = ? AND symbol = ? AND kind = 'book_ticker' AND received_ts <= ? ORDER BY received_ts DESC, id DESC LIMIT 1")
            .bind(exchange.to_string()).bind(symbol).bind(at).fetch_optional(&self.pool).await?;
        row.map(quote_from_row).transpose()
    }

    pub async fn subsequent_quotes(
        &self,
        p: &Prediction,
        until: i64,
    ) -> anyhow::Result<Vec<Quote>> {
        // Cursor prevents rescanning every tick since entry on every resolver iteration.
        let rows = sqlx::query("SELECT id, received_ts, event_ts, bid_price, ask_price, bid_qty, ask_qty FROM market_events WHERE exchange = ? AND symbol = ? AND kind = 'book_ticker' AND id > ? AND received_ts > ? AND received_ts <= ? ORDER BY id ASC LIMIT 5000")
            .bind(p.exchange.to_string()).bind(&p.symbol).bind(p.cursor_id).bind(p.created_at).bind(until).fetch_all(&self.pool).await?;
        rows.into_iter().map(quote_from_row).collect()
    }

    pub async fn checkpoint(&self, p: &Prediction) -> anyhow::Result<()> {
        sqlx::query("UPDATE predictions SET cursor_id = ?, last_quote_ts = ?, mark_price = ?, execution_audit = ? WHERE id = ? AND status = 'OPEN'")
            .bind(p.cursor_id).bind(p.last_quote_ts).bind(p.mark_price)
            .bind(p.execution_audit.as_ref().map(serde_json::to_string).transpose()?)
            .bind(&p.id).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn finish(&self, p: &Prediction) -> anyhow::Result<()> {
        sqlx::query("UPDATE predictions SET status = ?, resolved_at = ?, exit_price = ?, pnl_bps = ?, gross_pnl_bps = ?, cursor_id = ?, last_quote_ts = ?, mark_price = ?, execution_audit = ? WHERE id = ? AND status = 'OPEN'")
            .bind(&p.status).bind(p.resolved_at).bind(p.exit_price).bind(p.pnl_bps).bind(p.gross_pnl_bps)
            .bind(p.cursor_id).bind(p.last_quote_ts).bind(p.mark_price)
            .bind(p.execution_audit.as_ref().map(serde_json::to_string).transpose()?)
            .bind(&p.id).execute(&self.pool).await?;
        Ok(())
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
        strategy: row.try_get("strategy")?,
        status: row.try_get("status")?,
        resolved_at: row.try_get("resolved_at")?,
        exit_price: row.try_get("exit_price")?,
        pnl_bps: row.try_get("pnl_bps")?,
        config_id: row.try_get("config_id")?,
        fee_bps: row.try_get("fee_bps")?,
        slippage_bps: row.try_get("slippage_bps")?,
        notional: row.try_get("notional")?,
        gross_pnl_bps: row.try_get("gross_pnl_bps")?,
        cursor_id: row.try_get("cursor_id")?,
        last_quote_ts: row.try_get("last_quote_ts")?,
        mark_price: row.try_get("mark_price")?,
        max_gap_ms: row.try_get("max_gap_ms")?,
        entry_reason: row.try_get("entry_reason")?,
        entry_snapshot: row
            .try_get::<Option<String>, _>("entry_snapshot")?
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
        execution_audit: row
            .try_get::<Option<String>, _>("execution_audit")?
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Quote {
    pub id: i64,
    pub ts: i64,
    pub event_ts: i64,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
}
impl Quote {
    pub fn valid(&self) -> bool {
        [self.bid, self.ask, self.bid_qty, self.ask_qty]
            .iter()
            .all(|v| v.is_finite() && *v > 0.)
            && self.ask >= self.bid
            && self.event_ts >= self.ts - 5000
            && self.event_ts <= self.ts + 1000
    }
    pub fn exit_price(&self, direction: &str) -> f64 {
        if direction == "LONG" {
            self.bid
        } else {
            self.ask
        }
    }
    pub fn entry_qty(&self, direction: &str) -> f64 {
        if direction == "LONG" {
            self.ask_qty
        } else {
            self.bid_qty
        }
    }
    pub fn exit_qty(&self, direction: &str) -> f64 {
        if direction == "LONG" {
            self.bid_qty
        } else {
            self.ask_qty
        }
    }
}
fn quote_from_row(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<Quote> {
    Ok(Quote {
        id: row.try_get("id")?,
        ts: row.try_get("received_ts")?,
        event_ts: row.try_get("event_ts")?,
        bid: row.try_get::<Option<f64>, _>("bid_price")?.unwrap_or(0.),
        ask: row.try_get::<Option<f64>, _>("ask_price")?.unwrap_or(0.),
        bid_qty: row.try_get::<Option<f64>, _>("bid_qty")?.unwrap_or(0.),
        ask_qty: row.try_get::<Option<f64>, _>("ask_qty")?.unwrap_or(0.),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        paper::{HORIZONS, STRATEGIES},
        test_support::{event, prediction},
    };

    #[tokio::test]
    async fn forty_eight_lanes_are_independent_and_duplicate_open_lane_is_rejected() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let mut original = None;
        for exchange in [Exchange::Binance, Exchange::Bybit] {
            for (strategy, _, _) in STRATEGIES {
                for horizon in HORIZONS {
                    let mut p = prediction();
                    p.exchange = exchange;
                    p.strategy = strategy.into();
                    p.horizon_secs = horizon;
                    db.insert_prediction(&p).await.unwrap();
                    original = Some(p);
                }
            }
        }
        assert_eq!(db.open_predictions().await.unwrap().len(), 48);
        let mut duplicate = original.unwrap();
        duplicate.id = uuid::Uuid::new_v4().to_string();
        assert!(db.insert_prediction(&duplicate).await.is_err());
        duplicate.config_id = "changed-costs".into();
        db.insert_prediction(&duplicate).await.unwrap();
    }

    #[tokio::test]
    async fn legacy_migration_is_additive_and_idempotent() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        sqlx::query("INSERT INTO predictions(id,exchange,symbol,created_at,horizon_secs,direction,entry_price,target_price,stop_price,confidence,score,expected_return,strategy,status,pnl_bps) VALUES ('old','binance','BTCUSDT',1,60,'LONG',100,103,99,0.8,0.5,0.02,'normal','WIN',300)")
            .execute(&db.pool).await.unwrap();
        // Simulate the actual pre-strategy schema, not merely a fresh empty database.
        for name in ["idx_open_lane_v3", "idx_predictions_config"] {
            sqlx::query(&format!("DROP INDEX {name}"))
                .execute(&db.pool)
                .await
                .unwrap();
        }
        for name in [
            "strategy",
            "config_id",
            "fee_bps",
            "slippage_bps",
            "notional",
            "gross_pnl_bps",
            "cursor_id",
            "last_quote_ts",
            "mark_price",
            "max_gap_ms",
            "entry_reason",
            "entry_snapshot",
            "execution_audit",
        ] {
            sqlx::query(&format!("ALTER TABLE predictions DROP COLUMN {name}"))
                .execute(&db.pool)
                .await
                .unwrap();
        }
        Database::init_schema(&db.pool).await.unwrap();
        Database::init_schema(&db.pool).await.unwrap();
        let rows = db
            .recent_predictions(Exchange::Binance, "BTCUSDT", None, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pnl_bps, Some(300.));
        assert_eq!(rows[0].config_id, "legacy");
        assert_eq!(rows[0].fee_bps, 0.);
        assert_eq!(rows[0].strategy, "normal");
        assert!(rows[0].entry_snapshot.is_none());
        assert!(rows[0].execution_audit.is_none());
        assert!(
            db.experiment_predictions(Exchange::Binance, "BTCUSDT", &LabConfig::default().id())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn oversized_export_fails_explicitly_instead_of_returning_a_partial_file() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        sqlx::query("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < ?) INSERT INTO predictions(id,exchange,symbol,created_at,horizon_secs,direction,entry_price,target_price,stop_price,confidence,score,expected_return,status) SELECT 'limit-' || i,'binance','BTCUSDT',i,60,'LONG',100,103,99,0,0,0,'LOSS' FROM n")
            .bind(crate::export::MAX_EXPORT_POSITIONS + 1).execute(&db.pool).await.unwrap();
        let result = db
            .export_positions(&crate::export::ExportFilters::default())
            .await;
        assert!(
            result
                .err()
                .unwrap()
                .downcast_ref::<crate::export::ExportTooLarge>()
                .is_some()
        );
        let filters = crate::export::ExportFilters {
            to_ms: Some(100),
            ..Default::default()
        };
        assert_eq!(
            db.export_positions(&filters).await.unwrap().positions.len(),
            99
        );
    }

    #[tokio::test]
    async fn query_uses_quote_order_and_cursor_instead_of_trade_prices() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let mut p = prediction();
        db.insert_event(event(Exchange::Binance, "trade", 2000, 104.))
            .await
            .unwrap();
        db.insert_event(event(Exchange::Binance, "book_ticker", 2000, 100.))
            .await
            .unwrap();
        db.insert_event(event(Exchange::Binance, "book_ticker", 2000, 98.))
            .await
            .unwrap();
        db.insert_event(event(Exchange::Bybit, "book_ticker", 2000, 110.))
            .await
            .unwrap();
        db.insert_event(event(Exchange::Binance, "book_ticker", 70000, 110.))
            .await
            .unwrap();
        db.flush().await.unwrap();
        let q = db.subsequent_quotes(&p, 61000).await.unwrap();
        assert_eq!(q.len(), 2);
        assert!(q[0].bid > q[1].bid);
        p.cursor_id = q[0].id;
        let later = db.subsequent_quotes(&p, 61000).await.unwrap();
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].id, q[1].id);
    }

    #[tokio::test]
    async fn point_loader_honors_as_of_and_ignores_duplicated_or_stale_sources() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        for (kind, ts) in [
            ("trade", 1000),
            ("agg_trade", 1000),
            ("kline", 1000),
            ("book_ticker", 2000),
            ("trade", 4000),
        ] {
            db.insert_event(event(Exchange::Binance, kind, ts, 100.))
                .await
                .unwrap();
        }
        let mut stale = event(Exchange::Binance, "book_ticker", 3000, 100.);
        stale.event_ts = -5000;
        db.insert_event(stale).await.unwrap();
        db.flush().await.unwrap();
        let points = db
            .load_points(Exchange::Binance, "BTCUSDT", 0, 100, 3000)
            .await
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].kind, "trade");
        assert_eq!(points[1].kind, "book_ticker");
    }

    #[tokio::test]
    async fn fee_snapshot_and_resolver_checkpoint_survive_database_round_trip() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        let mut p = prediction();
        p.fee_bps = 7.5;
        p.entry_snapshot = Some(serde_json::json!({"features": {"flow_60s": 0.65}}));
        p.execution_audit = Some(crate::audit::ExecutionAudit::start(&p, true, 1000));
        crate::audit::observe(
            &mut p,
            &crate::test_support::quote(1, 1000, 99.99, 100.01),
            true,
        );
        db.insert_prediction(&p).await.unwrap();
        // Checkpoint/finish must never rewrite the original entry evidence, even if
        // an in-memory caller mistakenly mutates it during resolution.
        p.entry_snapshot = Some(serde_json::json!({"features": {"flow_60s": -1}}));
        crate::audit::observe(
            &mut p,
            &crate::test_support::quote(42, 2300, 101., 101.01),
            true,
        );
        p.cursor_id = 42;
        p.last_quote_ts = 2300;
        p.mark_price = Some(101.);
        db.checkpoint(&p).await.unwrap();
        let saved = db.open_predictions().await.unwrap().remove(0);
        assert_eq!(saved.fee_bps, 7.5);
        assert_eq!(saved.cursor_id, 42);
        assert_eq!(saved.mark_price, Some(101.));
        assert_eq!(
            saved.entry_snapshot.as_ref().unwrap()["features"]["flow_60s"],
            0.65
        );
        assert_eq!(
            saved
                .execution_audit
                .as_ref()
                .unwrap()
                .best
                .as_ref()
                .unwrap()
                .quote
                .id,
            42
        );
        p.status = "WIN".into();
        p.pnl_bps = Some(15.);
        p.gross_pnl_bps = Some(30.);
        p.resolved_at = Some(2500);
        p.exit_price = Some(103.);
        crate::audit::record_exit(&mut p, "take_profit", 2500, None);
        db.finish(&p).await.unwrap();
        assert!(db.open_predictions().await.unwrap().is_empty());
        let closed = db.prediction_by_id(&p.id).await.unwrap().unwrap();
        assert_eq!(
            closed.entry_snapshot.as_ref().unwrap()["features"]["flow_60s"],
            0.65
        );
        assert_eq!(
            closed
                .execution_audit
                .as_ref()
                .unwrap()
                .exit
                .as_ref()
                .unwrap()
                .code,
            "take_profit"
        );
        // A delayed resolver checkpoint cannot overwrite a closed position's audit.
        let mut stale = saved;
        stale.execution_audit = None;
        db.checkpoint(&stale).await.unwrap();
        assert!(
            db.prediction_by_id(&p.id)
                .await
                .unwrap()
                .unwrap()
                .execution_audit
                .unwrap()
                .exit
                .is_some()
        );
    }
}
