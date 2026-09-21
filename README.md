# Market Lab — Realtime Crypto Market Research & Paper Trading

A low-latency Rust service that captures live Spot market data from **Binance** and **Bybit**, persists normalized + raw events, analyzes market microstructure, finds similar historical patterns, produces forward paper-trade predictions, and measures its own strict win rate.

> This build is intentionally **paper trading only**. It does not submit real orders and does not require exchange API keys for public market data.


## Ubuntu — one-command installation

The repository is public. On a fresh Ubuntu server, copy and paste this **single command**:

```bash
curl -fsSL https://raw.githubusercontent.com/Meysam-sadeghi/bot-trader/mobile/scripts/install-ubuntu.sh | sudo bash
```

If you are already logged in as `root`, this also works:

```bash
curl -fsSL https://raw.githubusercontent.com/Meysam-sadeghi/bot-trader/mobile/scripts/install-ubuntu.sh | bash
```

The installer automatically:

- updates Ubuntu packages and installs the required build dependencies
- installs the current stable Rust toolchain
- clones the public `mobile` branch
- builds the optimized `--release` binary
- installs the application under `/opt/market-lab`
- creates the persistent data directory at `/var/lib/market-lab`
- creates a dedicated unprivileged `marketlab` system user
- creates and enables the `market-lab.service` systemd service
- starts the service automatically and enables it after reboot
- opens TCP port `8080` when UFW is already enabled
- verifies `/health` before reporting a successful installation

After installation:

```
http://SERVER_IP:8080/binance
http://SERVER_IP:8080/bybit
http://SERVER_IP:8080/health
```

Useful service commands:

```bash
systemctl status market-lab
journalctl -u market-lab -f
systemctl restart market-lab
```

Market data and the SQLite database remain under:

```
/var/lib/market-lab
```

## Market data capture

Each exchange has its own independent capture task, Start/Stop control, reconnect loop and status.

### Binance Spot

The collector opens a combined public WebSocket for:

- `trade` — individual trades
- `aggTrade` — aggregate trades
- `bookTicker` — best bid/ask and quantities
- `depth@100ms` — full incremental depth updates
- `kline_1s` — 1-second kline stream

Immediately after the WebSocket is open it also persists a REST depth snapshot with up to **5,000 levels per side**.

The raw snapshot includes `lastUpdateId`; subsequent diff-depth messages retain `U/u`. That means stored data can be replayed using Binance's official local-order-book synchronization algorithm rather than treating deltas as standalone snapshots.

Default endpoints:

```
wss://stream.binance.com:9443
https://api.binance.com
```

### Bybit Spot

The collector subscribes to:

- `publicTrade.{symbol}` — realtime public trades
- `tickers.{symbol}` — last price + best bid/ask
- `orderbook.full.{symbol}` — full-depth delta stream
- `kline.1.{symbol}` — kline updates

After subscribing it persists Bybit's REST **full depth snapshot**, which can contain up to **10,000 levels per side**.

Both the snapshot and full-depth deltas retain Bybit's `u` and `seq` fields in raw JSON so the local order book can be reconstructed with continuity validation.

Default endpoints:

```
wss://stream.bybit.com/v5/public/spot
https://api.bybit.com
```

### Storage warning

Full-depth order books generate substantial data. Long-running capture, multiple symbols, and volatile markets can grow SQLite quickly. SQLite WAL is appropriate for this research build; large-scale archival should move raw depth events to ClickHouse or compressed Parquet while retaining the same normalized model.

## UI

Separate pages:

- `/binance`
- `/bybit`

Each page includes:

- Symbol selection
- Start Capture / Stop Capture
- Start Analysis / Stop Analysis
- Live price chart
- Live normalized event tape
- Persisted event count
- Paper prediction table
- 1 / 3 / 5 minute horizons
- Direction, confidence, entry, target and stop
- WIN / LOSS / TIMEOUT resolution
- Strict win rate
- Average paper PnL in basis points

## Prediction engine

The current model is a measurable baseline, not a claim of guaranteed prediction accuracy.

Events are aggregated into 5-second research windows. Features currently include:

1. 15-second return
2. 60-second return
3. 15-second taker buy/sell flow imbalance
4. 60-second taker buy/sell flow imbalance
5. Realized short-term volatility
6. Best-book bid/ask quantity imbalance
7. Bid/ask spread in basis points

For every analysis iteration the engine:

1. Builds the current microstructure feature vector.
2. Scores immediate order-flow / momentum pressure.
3. Searches historical windows for the closest feature patterns.
4. Selects up to 30 nearest analogs.
5. Measures actual forward returns after 1, 3 and 5 minutes for those analogs.
6. Weights more similar analogs more heavily.
7. Blends historical forward behavior with current microstructure.
8. Creates immutable LONG/SHORT paper predictions.

## Forward-only evaluation

Predictions are written before their outcome is known.

Each prediction stores:

- creation time
- exchange and symbol
- horizon
- LONG / SHORT
- entry
- target
- stop
- confidence
- model score
- expected return

A separate resolver looks only at **subsequent** real trade events:

- **WIN** — target is reached first
- **LOSS** — stop is reached first
- **TIMEOUT** — horizon expires before either level is reached

This prevents back-filled wins from contaminating the displayed win rate.

## Persistence

SQLite runs in **WAL mode**.

A bounded async channel decouples market ingestion from disk I/O. Events are committed in batches rather than opening one transaction per WebSocket message.

Important tables:

- `market_events`
- `predictions`

Every market row contains normalized analysis fields plus the original raw JSON payload.

## Run with Rust

```bash
cp .env.example .env
cargo run --release
```

Open:

```
http://localhost:8080/binance
http://localhost:8080/bybit
```

## Docker

```bash
docker compose up --build
```

Data is persisted under:

```
./data
```

## Configuration

```env
PORT=8080
DATA_DIR=data
DATABASE_URL=sqlite://data/market.db

BINANCE_WS_BASE=wss://stream.binance.com:9443
BINANCE_REST_BASE=https://api.binance.com

BYBIT_WS_URL=wss://stream.bybit.com/v5/public/spot
BYBIT_REST_BASE=https://api.bybit.com

ANALYSIS_INTERVAL_SECS=30
RISK_REWARD=3.0
```

Public testnet alternatives:

```env
BINANCE_WS_BASE=wss://stream.testnet.binance.vision:9443
BINANCE_REST_BASE=https://testnet.binance.vision

BYBIT_WS_URL=wss://stream-testnet.bybit.com/v5/public/spot
BYBIT_REST_BASE=https://api-testnet.bybit.com
```

## API

Capture:

```
POST /api/capture/binance/start?symbol=BTCUSDT
POST /api/capture/binance/stop
POST /api/capture/bybit/start?symbol=BTCUSDT
POST /api/capture/bybit/stop
```

Analysis:

```
POST /api/analysis/binance/start?symbol=BTCUSDT
POST /api/analysis/binance/stop
POST /api/analysis/bybit/start?symbol=BTCUSDT
POST /api/analysis/bybit/stop
```

Dashboard:

```
GET /api/dashboard/binance
GET /api/dashboard/bybit
GET /api/predictions/binance
GET /api/predictions/bybit
```

Browser realtime stream:

```
GET /ws/binance
GET /ws/bybit
```

## Architecture

```
Binance WS + REST snapshot ─┐
                           ├──> Rust async collectors ──> browser live event bus
Bybit WS + REST snapshot ──┘             │
                                         └──> batched SQLite WAL
                                                    │
                                                    v
                                            5s feature windows
                                                    │
                                                    v
                                      microstructure + analog model
                                                    │
                                                    v
                                          1m / 3m / 5m signals
                                                    │
                                                    v
                                         forward outcome resolver
                                                    │
                                                    v
                                         win rate + paper PnL
```

## Next research upgrades

The current code establishes the complete capture → storage → analysis → prediction → validation loop. High-value next work:

- In-memory sequence-validated local order book state for both exchanges
- Cross-exchange Binance/Bybit lead-lag features
- Multi-level depth imbalance (1/5/10/25/50 bps)
- Order-book slope, replenishment and cancellation pressure
- CVD, trade intensity and inter-arrival time
- Spoof-resistance / fleeting-liquidity features
- Market regime detection
- Walk-forward training and validation partitions
- Replay/backtest using the exact same event structures
- XGBoost/LightGBM or sequence model trained from exported features
- ClickHouse / Parquet archival
- Prometheus capture-gap and latency metrics
- Optional authenticated **testnet** execution only after paper results demonstrate stable out-of-sample performance

## CI

GitHub Actions runs:

```bash
cargo check --all-targets
cargo test --all-targets
```

on feature branches and pull requests.
