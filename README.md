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
- creates a protected root updater watched by `market-lab-update.path`
- generates a persistent Admin Token for **Clear Data** and **Update System**
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


## Update an existing Ubuntu installation

Run this one command as root to pull the latest `mobile` branch, build it in release mode, install it, restart Market Lab and verify the health endpoint:

```bash
curl -fsSL https://raw.githubusercontent.com/Meysam-sadeghi/bot-trader/mobile/scripts/update-ubuntu.sh | bash
```

The updater keeps the existing SQLite data, keeps the existing Admin Token, creates a rollback copy before replacing the running build, and restores the previous binary/UI if the new service fails its health check. When possible it also resumes Binance/Bybit Auto Lab sessions that were running before the update.

After this upgrade, the **Update System** button performs the same update through a root-owned systemd path/service pair. The web process itself never receives sudo privileges.

To display the Admin Token again on the server:

```bash
sudo sed -n 's/^ADMIN_TOKEN=//p' /etc/market-lab.env
```

Updater logs:

```bash
journalctl -u market-lab-update -f
```

## Market data capture

Each exchange has its own independent capture task, Start/Stop control, reconnect loop and status. Pressing **Start Auto Lab** starts capture **and** the analysis/paper-trading engine together. Pressing Stop stops both.

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
- Start Auto Lab / Stop
- Automatic capture → analysis → simulated paper trading
- Clear Data for the current exchange (Admin Token protected)
- Update System from GitHub (Admin Token protected)
- Live price chart
- Live normalized event tape
- Persisted event count
- Paper-position table
- 1 / 5 / 15 / 60 minute horizons
- Maximum one open paper position per horizon
- Fixed 1:3 risk/reward on every simulated position
- Hard maximum holding time of 60 minutes
- Direction, confidence, entry, target and stop
- WIN / LOSS / TIMEOUT resolution
- Strict win rate
- Average paper PnL in basis points

## Strategy modes

- **Binance: CONTRARIAN Selective V2** — every accepted model direction is still inverted, but weak/noisy setups are rejected before a paper trade is created.
- **Bybit: NORMAL Selective V2** — follows the accepted model direction without inversion.
- Take-profit / stop-loss remains fixed at **3:1 reward:risk** after the direction is inverted.
- The maximum paper-position holding time remains **60 minutes**.
- Predictions are tagged with their strategy mode. Existing historical Binance predictions are preserved as `normal`; the Binance dashboard and Strict Win Rate now report the current `contrarian` strategy separately, so the experiment starts with clean statistics without deleting captured market history.

This is an experiment on paper trades only. Inverting a historical win rate does not mathematically imply that the new win rate will equal `1 - old_win_rate`, because target/stop distances are asymmetric (3:1), timeouts exist, and path ordering determines whether TP or SL is reached first.

## Prediction engine — Selective Signal V2

V2 is designed to improve **forward selectivity**, not to manufacture a backtest win rate. The default research gate asks for an 80% weighted strict win rate among similar historical analogs, but **80% is a target threshold, not a guarantee of future performance**. If the evidence is weak, the correct action is NO TRADE.

Events are aggregated into continuous 5-second research windows. Missing windows are filled so 1/5/15/60-minute horizons remain clock-time accurate. Features now include:

1. 15-second, 60-second and 5-minute log returns
2. Momentum acceleration
3. 15-second, 60-second and 5-minute taker-flow imbalance
4. 60-second and 5-minute realized volatility
5. Current and persistent top-book imbalance
6. Bid/ask spread in basis points
7. Relative traded volume
8. Relative trade intensity
9. 5-minute trend efficiency
10. Position inside the recent 5-minute range
11. Optional cross-exchange Binance/Bybit microstructure confirmation

For every analysis iteration the engine:

1. Builds the enriched microstructure feature vector.
2. Searches a configurable longer history for similar states.
3. De-correlates neighboring analogs in time so adjacent 5-second samples are not treated as independent evidence.
4. Estimates forward return and direction probability from the nearest analogs.
5. Builds the same fixed 1:3 TP/SL geometry used by the paper trade.
6. Replays each historical analog forward and measures **which barrier was hit first**: TP, SL, or timeout.
7. Treats an ambiguous bucket that touched both TP and SL as a loss, avoiding optimistic backtest bias.
8. Computes weighted strict win probability plus a Wilson lower confidence bound.
9. Applies spread, edge, confidence, minimum-sample and re-entry cooldown gates.
10. Opens a paper position only when all gates pass; otherwise it records no forced trade.

Binance still uses the requested CONTRARIAN direction rule, but V2 only permits the inverted trade when historical barrier evidence supports it. Bybit remains NORMAL.

A useful statistical reference: with a symmetric random walk and a take-profit three times farther away than the stop, the theoretical TP-before-SL probability is about 25%. Therefore a 25–30% win rate with 3:1 reward:risk is not automatically a losing system; expectancy, timeouts, fees and slippage matter as much as raw win rate.

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
- **TIMEOUT** — the horizon expires before either level is reached; no position can remain open beyond 60 minutes

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
MAX_POSITION_SECS=3600

# Selective Signal Engine V2.
# TARGET_STRICT_WIN_RATE is a historical analog admission threshold,
# not a promise that forward results will equal this percentage.
ANALYSIS_LOOKBACK_HOURS=24
ANALYSIS_MAX_POINTS=750000
TARGET_STRICT_WIN_RATE=0.80
MIN_SIGNAL_CONFIDENCE=0.72
MIN_WIN_LOWER_BOUND=0.55
MIN_SIGNAL_EDGE_BPS=2.0
MAX_SPREAD_BPS=3.0
MIN_ANALOG_SAMPLES=24
ANALOG_NEIGHBORS=80
REENTRY_COOLDOWN_SECS=180

# Admin actions are disabled when this is empty.
# Ubuntu installer generates this automatically in /etc/market-lab.env.
# ADMIN_TOKEN=replace-with-a-long-random-secret
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

Analysis is automatically started/stopped by the Capture endpoints. Manual analysis endpoints remain available for compatibility:

```
POST /api/analysis/binance/start?symbol=BTCUSDT
POST /api/analysis/binance/stop
POST /api/analysis/bybit/start?symbol=BTCUSDT
POST /api/analysis/bybit/stop
```

Protected administration:

```
POST /api/data/binance/clear
POST /api/data/bybit/clear
POST /api/system/update
```

The protected routes require the `X-Admin-Token` header.

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
                                      1m / 5m / 15m / 60m positions
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

GitHub Actions validates both Ubuntu shell scripts and runs:

```bash
bash -n scripts/install-ubuntu.sh
bash -n scripts/update-ubuntu.sh
cargo check --all-targets
cargo test --all-targets
```

on feature branches and pull requests.
