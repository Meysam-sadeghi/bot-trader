# Market Lab — Realtime Crypto Market Research & Paper Trading

A Rust research service that captures live spot market data from **Binance** and **Bybit**, runs six simultaneous paper strategies on each exchange, and measures forward net profitability under explicit execution assumptions.

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

Each exchange page now shows **both exchanges together**, with six comparison cards per exchange and a common 1 / 5 / 15 / 60 minute horizon selector. Use **Start BOTH exchanges** to run all 48 strategy/horizon lanes. The existing per-exchange Start/Stop controls remain available.

## Position diagnostics and export

برای بررسی وین‌ریت پایین، پس از **Update System** در بخش **Position diagnostics** روی **Download diagnostics JSON** بزنید و فایل را برای تحلیل ارسال کنید. پیش‌فرض، هر دو صرافی، همهٔ استراتژی‌ها، همهٔ بازه‌ها و تنظیمات قدیمی را شامل می‌شود. **Clear Data** اطلاعات مورد نیاز تحلیل را حذف می‌کند؛ برای خروجی گرفتن نیازی به آن نیست.

- **Download diagnostics JSON**: full positions, immutable entry inputs, measured rule checks/thresholds, peer features, analog evidence where used, original configuration parameters, execution evidence and grouped outcome counts.
- **Download CSV**: a flat Excel-compatible comparison, including numeric features, costs, outcome categories and embedded evidence JSON. Text cells beginning with spreadsheet formula characters are escaped; numeric losses remain numeric.
- **Preview loss breakdown**: stop losses, losing timeouts, unverifiable paths, fee-related losses and trades that gave back an observed net profit. Groups separate exchange, symbol, configuration, strategy, horizon and direction. Fee-related and giveback counts are overlapping subsets, not extra losses.
- **Details** beside a position: inspect recorded entry conditions, thresholds, costs, quote extrema and the specific exit trigger.

Filters support exchange, symbol, strategy, horizon, exit status, configuration and entry-time range. Browser date controls use your local time, converted to UTC milliseconds. The end is exclusive. The download uses one SQLite read snapshot and is independent of the recent-position table's 120/500-row limits. It includes all matching positions up to **25,000**; a larger selection returns an explicit error asking for a narrower range, never a silently truncated file. JSON states the filters, record count and evidence coverage.

New positions save the closed-window features, quote timestamps, exact accepted rule values, entry bid/ask and available size before the outcome exists. Execution audits preserve first/last/best/worst observed quotes and modeled net marks, quote counts, maximum gap and exit details. These are compact observations, **not a full tick-path archive**. Quote extrema use uncapped hypothetical executable fills; an actual favorable target fill remains capped at the target. Fees are gross minus net; spread and slippage are already in fills and must not be deducted twice.

Existing trades and cohort IDs are preserved. Old positions cannot recover entry features that were never saved. They export `not_recorded` evidence and status-inferred exits. A V3 position already open during the update receives a `partial_after_upgrade` audit. Legacy outcomes retain the original gross-price accounting; legacy notional/config defaults are not recovered account facts. This update instruments the existing rules; it does not claim to improve or establish any live-market win rate.

Recorded exit codes:

| Code | Recorded event |
| --- | --- |
| `take_profit` / `stop_loss` | First executable target/stop crossing |
| `horizon_timeout` | Time limit, using a sufficiently fresh executable mark |
| `quote_gap` / `feed_stale` | Missing continuous quotes / no timely quote while holding |
| `invalid_or_delayed_quote` / `quote_out_of_order` | Invalid price/size/event time / backward receipt timestamp |
| `insufficient_exit_liquidity` | A barrier was crossed without enough visible size to fill |
| `timeout_quote_stale` / `timeout_insufficient_liquidity` | Time-limit exit cannot be verified from recent liquid quotes |

The JSON distinguishes profitable/losing outcomes from missing data and reports both profitable/all-closed and profitable/priced-closed rates. The latter excludes unknown outcomes and must not be substituted silently for the dashboard metric. Observations can identify execution, cost and signal patterns; they do not by themselves prove the market cause of a loss.

## Parallel Strategy Lab V3

Every exchange uses the same strategy definitions. Binance is no longer exclusively inverted: the old reversal idea is retained as a separate control on **both** exchanges.

| Strategy | Entry condition | Purpose |
| --- | --- | --- |
| Order-flow follow | Aligned taker flow, persistent best-book imbalance and short momentum | Directional reference |
| Order-flow reverse | Opposite direction at the same qualifying flow setup | Tests the contrarian hypothesis independently |
| Trend pullback | Efficient five-minute trend, short pullback and a turn back toward the trend | Selective trend continuation |
| Confirmed breakout | Breakout of the previous five-minute range plus volume, activity and flow confirmation | Momentum expansion |
| Range reversion | Low trend efficiency, a range extreme and inward flow/acceleration | Range-bound conditions |
| Selective consensus | Fresh agreement between venues plus cost-aware, purged historical analog evidence | High-selectivity research gate |

These are deterministic research rules, **not a trained AI model or calibrated probabilities**. No strategy is known to achieve 80% forward wins. No forced trades are created when conditions are absent. In particular, selective consensus may wait a long time for enough independent data.

Each `(exchange, symbol, strategy, horizon, configuration)` is an independent paper account:

- Initial capital defaults to **10,000 quote-asset units**, fixed entry notional **1,000 quote-asset units** (USDT for BTCUSDT; no conversion to USD).
- One open position per account; cooldown starts at the previous **exit**.
- Gross target/stop distance remains **3:1** for every strategy. Net reward/risk is lower after costs.
- Horizons are exactly 60 / 300 / 900 / 3,600 seconds. The UI's all-horizons view combines **four independent accounts**, not one shared leveraged account.
- New entries require enough account equity and visible best-quote size for the notional.
- All material configuration values form a stable experiment ID; the full configuration is stored in `lab_configs`. Changing costs or gates creates a new cohort. Each position also persists its own execution assumptions.
- Existing V1/V2 data remains untouched under the `legacy` cohort. Already-open legacy positions finish using the previous gross-price model. Their results are never pooled with V3.

## Market data and execution assumptions

Research features use **closed five-second windows**, mid prices from complete best quotes, and deduplicated-by-stream taker flow (raw trade/publicTrade, never aggTrade plus trade). Kline closes and arbitrary full-depth delta levels cannot overwrite the feature price. Long quote outages remain missing rather than being filled indefinitely. Stale peers cannot confirm a signal.

Bybit subscribes to `orderbook.1.SYMBOL` in addition to the raw full-depth archive: its spot ticker is a last-price feed, not the executable best-book source. A full-depth delta's first updated row is not necessarily the best price. Archive snapshot failures are logged without blocking the independent best-quote stream. References: [Bybit level-1 orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook), [Bybit ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker), [Binance bookTicker](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#individual-symbol-book-ticker-streams).

V3 simulation:

1. Compute the signal from history available at that time; fetch a fresh quote after computation. Never backdate an entry.
2. LONG enters at ask plus adverse slippage; SHORT enters at bid minus adverse slippage.
3. LONG exits against bid; SHORT exits against ask, with adverse exit slippage. Evaluate subsequent quote events in persisted arrival order, including a deterministic ID tiebreaker.
4. Cap favorable target gaps at the target price. Preserve the worse actual fill on adverse stop gaps.
5. Deduct entry and exit fees on their respective notionals. Defaults are **10 bps per side fee and 1 bp per side slippage**, research assumptions that must be adjusted to the actual account tier. Spread is already in fills; it is not charged twice.
6. At timeout, use the last sufficiently recent executable quote. If the path contains an excessive quote gap, stale/crossed quote, insufficient visible exit liquidity or no usable deadline quote, mark **DATA_GAP**, leave PnL unknown and retain it in the win-rate denominator.
7. Persist resolver cursors and marks, so restarts resume ordered evaluation without rescanning every historical tick. Market-write failures retain and retry batches instead of silently losing observations.

This is a **top-book paper approximation**, not an execution guarantee. SHORT is synthetic spot-price research; margin borrowing, funding, queue priority, execution latency, full-depth market impact and account order restrictions are not modeled. The collectors send no real orders and require no exchange API keys.

## Historical evidence versus forward performance

Selective consensus uses past analogs with disjoint **feature + outcome windows** and a full feature-window embargo before the current state. Labels must be complete. Historical barrier replay uses executable bid/ask sides and the same fee/slippage arithmetic; an ambiguous five-second bar is scored conservatively as a loss. Gapped paths are not labeled. Expected net return, weighted and unweighted net target-hit rates, minimum samples and a Wilson lower bound must all pass.

Historical analog evidence is a **selection heuristic**, not out-of-sample performance. With a one-hour horizon and a 24-hour lookback, 24 disjoint analog paths generally cannot fit; the lane correctly waits. Raw event caps can shorten effective history further, which the UI explicitly reports. Increase history/capacity only when the server can support it.

Dashboard cards show **forward-only** positions recorded before their outcomes:

- **Net win rate** = positive-net closed positions / **all** closed positions. Profitable timeouts count as net wins; losses, flat exits, losing timeouts and DATA_GAP remain in the denominator.
- **Net TP hit rate** = target hits with positive net PnL / all closed positions. It does not relabel a profitable timeout as a target hit.
- Sample size and descriptive 95% Wilson interval. A tiny 100%-winning sample is not evidence for an 80% sustainable strategy.
- Realized net PnL, unrealized PnL at the last quote, net expectancy per priced trade, net profit factor, return on allocated capital and **closed-trade** drawdown.
- Closed-trade drawdown excludes intratrade adverse movement. Unknown DATA_GAP PnL is not included in monetary totals; any account with such gaps is incomplete.
- The 80% lower-bound marker requires at least 100 closed positions, no data gaps, positive net PnL and an individual horizon's Wilson lower bound at/above the configured target. It **does not authorize real trading** or establish future performance. Serial correlation and strategy selection can make statistical intervals overconfident.
- A per-lane reason explains waiting, filters, missing evidence, open positions or cooldown. Zero trades are shown as **no result**, not 0% performance.

Run forward collection across multiple regimes, compare fixed configurations on fresh data, and inspect net profitability and drawdown alongside hit rate. The repository has no production database or historic performance report attached, so the software tests cannot establish an achieved live-market win rate.

## Persistence

SQLite runs in **WAL mode**.

A bounded async channel decouples market ingestion from disk I/O. Events are committed in batches rather than opening one transaction per WebSocket message.

Important tables:

- `market_events`
- `predictions`
- `lab_configs`

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

# Parallel Strategy Lab V3.
# TARGET_STRICT_WIN_RATE is a historical analog admission threshold,
# not a promise that forward results will equal this percentage.
ANALYSIS_LOOKBACK_HOURS=24
ANALYSIS_MAX_POINTS=750000
TARGET_STRICT_WIN_RATE=0.80
MIN_WIN_LOWER_BOUND=0.55
MIN_SIGNAL_EDGE_BPS=2.0
MAX_SPREAD_BPS=3.0
MIN_ANALOG_SAMPLES=24
ANALOG_NEIGHBORS=80
REENTRY_COOLDOWN_SECS=180
MAX_QUOTE_AGE_MS=5000
MAX_DATA_GAP_MS=15000
BINANCE_TAKER_FEE_BPS=10
BYBIT_TAKER_FEE_BPS=10
PAPER_SLIPPAGE_BPS=1
PAPER_NOTIONAL=1000
PAPER_INITIAL_CAPITAL=10000

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
POST /api/lab/start?symbol=BTCUSDT
POST /api/lab/stop
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
GET /api/predictions/binance?config=all&limit=500
GET /api/positions/{position-id}
GET /api/exports/positions?format=json
GET /api/exports/positions?format=csv&exchange=bybit&config=current
GET /api/exports/positions?format=json&exchange=binance&strategy=flow_follow_v3&horizon=300&from_ms=1700000000000&to_ms=1700086400000
GET /api/exports/summary
```

Export filters default to `all`. `config` accepts `all`, `current`, `legacy` or an exact cohort ID. `status` filters the exit label (`OPEN`, `WIN`, `LOSS`, `TIMEOUT`, `DATA_GAP`), not net profitability; a timeout can be profitable or losing. No credentials or raw market archives are included.

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

## Validation and remaining research

`cargo test --locked --all-targets` exercises execution costs, quote ordering, stop gaps, timeouts, data outages, legacy migrations, restart checkpoints, cohort separation, independent lanes, closed-window/no-future signals and snapshots, purged analogs, both exchange flows, immutable entry evidence, exit/quote audit persistence, export filtering beyond the dashboard limit, explicit oversized-export rejection, loss categories and CSV escaping. Synthetic fixtures prove software behavior only; they are not profitable-strategy evidence.

Further research before any separate live-execution project includes chronological replay on real captured datasets, held-out regimes, parameter selection bias, full-depth fills/latency/borrowing and intratrade portfolio drawdown. No real-trading switch is included.

## CI

GitHub Actions validates both Ubuntu shell scripts and runs:

```bash
bash -n scripts/install-ubuntu.sh
bash -n scripts/update-ubuntu.sh
cargo check --locked --all-targets
cargo test --locked --all-targets
```

on feature branches and pull requests.
