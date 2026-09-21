#!/usr/bin/env bash
set -Eeuo pipefail

REPO="Meysam-sadeghi/bot-trader"
BRANCH="${MARKET_LAB_BRANCH:-mobile}"
INSTALL_DIR="/opt/market-lab"
DATA_DIR="/var/lib/market-lab"
SERVICE_NAME="market-lab"
SERVICE_USER="marketlab"
PORT="${MARKET_LAB_PORT:-8080}"
REQUEST_FILE="$DATA_DIR/update.request"
ENV_FILE="/etc/market-lab.env"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: updater must run as root."
  exit 1
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  echo "ERROR: $SERVICE_USER does not exist. Run the full installer first."
  exit 1
fi

install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 "$DATA_DIR"

RESUME_LINES=""
if [[ -f "$REQUEST_FILE" ]]; then
  RESUME_LINES="$(cat "$REQUEST_FILE" 2>/dev/null || true)"
  rm -f "$REQUEST_FILE"
fi

# When invoked manually, preserve currently running capture sessions when possible.
if [[ -z "$RESUME_LINES" ]]; then
  for exchange in binance bybit; do
    dashboard="$(curl -fsS "http://127.0.0.1:$PORT/api/dashboard/$exchange" 2>/dev/null || true)"
    if [[ "$dashboard" == *'"running":true'* ]]; then
      symbol="$(printf '%s' "$dashboard" | sed -n 's/.*"capture":{[^}]*"symbol":"\([^"]*\)".*/\1/p' | head -n1)"
      if [[ "$symbol" =~ ^[A-Z0-9]{5,24}$ ]]; then
        RESUME_LINES+="$exchange $symbol"$'\n'
      fi
    fi
  done
fi

echo "[1/7] Preparing Rust toolchain..."
if [[ -f /root/.cargo/env ]]; then
  source /root/.cargo/env
fi
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
  source /root/.cargo/env
fi
rustup toolchain install stable --profile minimal
rustup default stable

echo "[2/7] Downloading latest $BRANCH from GitHub..."
git clone --depth 1 --single-branch --branch "$BRANCH" \
  "https://github.com/$REPO.git" "$TMP_DIR/source"

echo "[3/7] Building optimized release..."
cd "$TMP_DIR/source"
cargo build --release
NEW_VERSION="$(git rev-parse HEAD)"

echo "[4/7] Preparing rollback copy..."
if [[ -x "$INSTALL_DIR/market-lab" ]]; then
  cp -a "$INSTALL_DIR/market-lab" "$TMP_DIR/market-lab.previous"
fi
if [[ -d "$INSTALL_DIR/static" ]]; then
  cp -a "$INSTALL_DIR/static" "$TMP_DIR/static.previous"
fi

echo "[5/7] Installing application and management units..."
install -d -m 0755 "$INSTALL_DIR"
install -m 0755 target/release/market-lab "$INSTALL_DIR/market-lab"
rm -rf "$INSTALL_DIR/static"
cp -a static "$INSTALL_DIR/static"
printf '%s\n' "$NEW_VERSION" > "$INSTALL_DIR/VERSION"
chown -R root:root "$INSTALL_DIR"

install -m 0755 scripts/update-ubuntu.sh /usr/local/sbin/market-lab-update

if [[ ! -s "$ENV_FILE" ]] || ! grep -q '^ADMIN_TOKEN=' "$ENV_FILE"; then
  ADMIN_TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
  printf 'ADMIN_TOKEN=%s\n' "$ADMIN_TOKEN" > "$ENV_FILE"
fi
chown root:"$SERVICE_USER" "$ENV_FILE"
chmod 0640 "$ENV_FILE"

cat >"/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Market Lab Realtime Crypto Market Research Engine
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=-$ENV_FILE
Environment=PORT=$PORT
Environment=DATA_DIR=$DATA_DIR
Environment=DATABASE_URL=sqlite://$DATA_DIR/market.db
Environment=BINANCE_WS_BASE=wss://stream.binance.com:9443
Environment=BINANCE_REST_BASE=https://api.binance.com
Environment=BYBIT_WS_URL=wss://stream.bybit.com/v5/public/spot
Environment=BYBIT_REST_BASE=https://api.bybit.com
Environment=ANALYSIS_INTERVAL_SECS=30
Environment=MAX_POSITION_SECS=3600
Environment=RUST_LOG=market_lab=info,tower_http=info
ExecStart=$INSTALL_DIR/market-lab
Restart=always
RestartSec=3
TimeoutStopSec=20
LimitNOFILE=1048576
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF

cat >"/etc/systemd/system/market-lab-update.service" <<EOF
[Unit]
Description=Update Market Lab from GitHub
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
Environment=MARKET_LAB_BRANCH=$BRANCH
Environment=MARKET_LAB_PORT=$PORT
ExecStart=/usr/local/sbin/market-lab-update
EOF

cat >"/etc/systemd/system/market-lab-update.path" <<EOF
[Unit]
Description=Watch for Market Lab update requests

[Path]
PathExists=$REQUEST_FILE
Unit=market-lab-update.service

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now market-lab-update.path

echo "[6/7] Restarting Market Lab..."
systemctl enable "$SERVICE_NAME" >/dev/null
systemctl restart "$SERVICE_NAME"

HEALTH_OK=0
for _ in $(seq 1 45); do
  if curl -fsS "http://127.0.0.1:$PORT/health" | grep -q '"ok":true'; then
    HEALTH_OK=1
    break
  fi
  sleep 1
done

if [[ "$HEALTH_OK" -ne 1 ]]; then
  echo "ERROR: updated service failed health check; rolling back."
  systemctl stop "$SERVICE_NAME" || true
  if [[ -f "$TMP_DIR/market-lab.previous" ]]; then
    install -m 0755 "$TMP_DIR/market-lab.previous" "$INSTALL_DIR/market-lab"
  fi
  if [[ -d "$TMP_DIR/static.previous" ]]; then
    rm -rf "$INSTALL_DIR/static"
    cp -a "$TMP_DIR/static.previous" "$INSTALL_DIR/static"
  fi
  systemctl restart "$SERVICE_NAME" || true
  journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
  exit 1
fi

echo "[7/7] Restoring Auto Lab sessions..."
while read -r exchange symbol; do
  [[ -z "${exchange:-}" || -z "${symbol:-}" ]] && continue
  if [[ "$exchange" =~ ^(binance|bybit)$ && "$symbol" =~ ^[A-Z0-9]{5,24}$ ]]; then
    curl -fsS -X POST "http://127.0.0.1:$PORT/api/capture/$exchange/start?symbol=$symbol" >/dev/null || true
  fi
done <<< "$RESUME_LINES"

echo
echo "Market Lab update completed successfully."
echo "Version: $NEW_VERSION"
echo "Admin token: $(sed -n 's/^ADMIN_TOKEN=//p' "$ENV_FILE")"
