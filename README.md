# Market Lab — Realtime Crypto Market Research & Paper Trading

A low-latency Rust service that captures live Spot market data from **Binance** and **Bybit**, persists normalized + raw events, analyzes market microstructure, finds similar historical patterns, produces forward paper-trade predictions, and measures its own strict win rate.

> This build is intentionally **paper trading only**. It does not submit real orders and does not require exchange API keys for public market data.

## What it captures

### Binance Spot

The collector uses a combined WebSocket connection for the selected symbol:

- `trade` — individual trades
- `aggTrade` — aggregate trades
- `bookTicker` — best bid/ask and quantities
- `depth20@100ms` — 20-level partial order-book snapshots
- `kline_1s` — 1-second candlestick stream

Default public endpoint:

`wss://stream.binance.com:9443`

### Bybit Spot

The collector subscribes to:

- `publicTrade.{symbol}` — realtime public trades
- `tickers.{symbol}` — last price + best bid/ask
- `orderbook.200.{symbol}` — 200-level order book, 100ms
- `kline.1.{symbol}` — 1-minute kline updates

Default public endpoint:

`wss://stream.bybit.com/v5/public/spot`

Each exchange has an independent capture task, reconnect/backoff logic and status.

## UI

Two separate pages are available:

- `/binance`
- `/bybit`

Each page provides:

- Symbol selection
- Start Capture / Stop Capture
- Start Analysis / Stop Analysis
- Live price chart
- Live normalized event tape
- Persisted event count
- Paper-trade prediction table
- 1 / 3 / 5 minute prediction horizons
- Confidence score
- Entry / target / stop
- WIN / LOSS / TIMEOUT resolution
- Strict win rate
- Average paper PnL in basis points

## Prediction engine

This first research model deliberately avoids pretending that an LLM can predict price with certainty. It uses measurable microstructure features and validates every signal after it is issued.

Market events are grouped into 5-second feature windows. Current features include:

1. 15-second return
2. 60-second return
3. 15-second taker buy/sell flow imbalance
4. 60-second taker buy/sell flow imbalance
5. Realized short-term volatility
6. Best-book bid/ask quantity imbalance
7. Bid/ask spread in basis points

The engine computes a microstructure score and then performs **historical analog matching**:

1. Build the current feature vector.
2. Search prior feature windows for the closest historical patterns.
3. Select up to 30 nearest analogs.
4. Measure what price did after 1, 3 and 5 minutes for those historical analogs.
5. Weight closer analogs more heavily.
6. Blend historical forward returns with the current microstructure score.
7. Generate a LONG or SHORT paper prediction with confidence, target and stop.

This is a strong baseline for later ML models because the generated dataset and evaluation loop remain usable when the prediction model changes.

## Honest evaluation

Predictions are immutable once created.

For every prediction the system records:

- creation timestamp
- exchange
- symbol
- horizon
- direction
- entry
- target
- stop
- confidence
- model score
- expected return

A background resolver then checks subsequent real trade prices:

- **WIN**: target is reached first
- **LOSS**: stop is reached first
- **TIMEOUT**: neither target nor stop is reached before the prediction horizon

Win rate is therefore based on actual forward data, not back-filled predictions.

## Persistence

SQLite is configured in **WAL mode**.

Incoming events are sent through a bounded asynchronous channel and committed in batches instead of issuing one database transaction per WebSocket message.

Stored columns include normalized fields for fast analysis plus the original raw JSON payload for later research/replay.

Important tables:

- `market_events`
- `predictions`

For a much larger multi-symbol production deployment, the storage layer can later be replaced or supplemented with ClickHouse/Parquet while keeping the collector and model interfaces.

## Run locally with Rust

Requirements:

- Current stable Rust toolchain
- Internet access to Binance/Bybit public WebSocket endpoints

```bash
cp .env.example .env
cargo run --release
```

Open:

- http://localhost:8080/binance
- http://localhost:8080/bybit

## Run with Docker

```bash
docker compose up --build
```

Market data is persisted under:

```
./data
```

## Configuration

```env
PORT=8080
DATA_DIR=data
DATABASE_URL=sqlite://data/market.db

BINANCE_WS_BASE=wss://stream.binance.com:9443
BYBIT_WS_URL=wss://stream.bybit.com/v5/public/spot

ANALYSIS_INTERVAL_SECS=30
RISK_REWARD=3.0
```

To use public testnet streams:

```env
BINANCE_WS_BASE=wss://stream.testnet.binance.vision:9443
BYBIT_WS_URL=wss://stream-testnet.bybit.com/v5/public/spot
```

## API

### Capture

```
POST /api/capture/binance/start?symbol=BTCUSDT
POST /api/capture/binance/stop

POST /api/capture/bybit/start?symbol=BTCUSDT
POST /api/capture/bybit/stop
```

### Analysis

```
POST /api/analysis/binance/start?symbol=BTCUSDT
POST /api/analysis/binance/stop

POST /api/analysis/bybit/start?symbol=BTCUSDT
POST /api/analysis/bybit/stop
```

### Dashboard

```
GET /api/dashboard/binance
GET /api/dashboard/bybit
GET /api/predictions/binance
GET /api/predictions/bybit
```

### Live browser stream

```
GET /ws/binance
GET /ws/bybit
```

## Architecture

```
Binance WS ─┐
            ├──> Rust async collectors ──> normalized event bus ──> browser WebSocket
Bybit WS ───┘              │
                           └──> batched SQLite WAL writer
                                      │
                                      v
                              feature aggregation
                                      │
                                      v
                         historical analog matcher
                                      │
                                      v
                            paper predictions
                                      │
                                      v
                          forward outcome resolver
                                      │
                                      v
                         win rate + paper PnL
```

## Current scope and next upgrades

This branch establishes the full capture → storage → analysis → prediction → forward-validation loop.

High-value next upgrades:

- Sequence-validated full local order book reconstruction
- Multi-symbol capture
- Cross-exchange lead/lag features
- Order-book slope and depth imbalance at multiple distances
- Trade intensity / inter-arrival time
- CVD and volume-profile features
- Regime detection
- Walk-forward train/validation separation
- XGBoost/LightGBM or neural sequence model trained from exported feature windows
- ClickHouse or Parquet archival for long retention
- Prometheus metrics and capture-gap alarms
- Replay/backtest service using the exact same event model
- Optional authenticated testnet execution only after paper results justify it

## CI

GitHub Actions runs:

```bash
cargo check --all-targets
cargo test --all-targets
```

on the feature branch and pull requests.
