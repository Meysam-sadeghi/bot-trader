#!/usr/bin/env bash
set -Eeuo pipefail

REPO="Meysam-sadeghi/bot-trader"
BRANCH="${MARKET_LAB_BRANCH:-mobile}"
INSTALL_DIR="/opt/market-lab"
DATA_DIR="/var/lib/market-lab"
SERVICE_NAME="market-lab"
SERVICE_USER="marketlab"
PORT="${MARKET_LAB_PORT:-8080}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  unset GH_TOKEN AUTH_HEADER || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

if [[ "$(id -u)" -ne 0 ]]; then
  echo "ERROR: installer must run as root. Use the README one-command installer."
  exit 1
fi

if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "ERROR: GH_TOKEN is required because the repository is private."
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "[1/8] Installing Ubuntu dependencies..."
apt-get update
apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  curl \
  git \
  pkg-config \
  libsqlite3-dev

echo "[2/8] Installing/updating stable Rust..."
if [[ ! -x /root/.cargo/bin/rustup ]]; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
source /root/.cargo/env
rustup toolchain install stable --profile minimal
rustup default stable
rustc --version
cargo --version

echo "[3/8] Downloading private repository..."
AUTH_HEADER="$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)"
git -c http.extraHeader="Authorization: Basic $AUTH_HEADER" \
  clone --depth 1 --single-branch --branch "$BRANCH" \
  "https://github.com/$REPO.git" "$TMP_DIR/source"
unset AUTH_HEADER GH_TOKEN

echo "[4/8] Building optimized release binary..."
cd "$TMP_DIR/source"
cargo build --release

echo "[5/8] Installing application files..."
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home-dir "$DATA_DIR" --create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

install -d -m 0755 "$INSTALL_DIR"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0750 "$DATA_DIR"
install -m 0755 target/release/market-lab "$INSTALL_DIR/market-lab"

rm -rf "$INSTALL_DIR/static"
cp -a static "$INSTALL_DIR/static"
chown -R root:root "$INSTALL_DIR"

echo "[6/8] Creating hardened systemd service..."
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

Environment=PORT=$PORT
Environment=DATA_DIR=$DATA_DIR
Environment=DATABASE_URL=sqlite://$DATA_DIR/market.db
Environment=BINANCE_WS_BASE=wss://stream.binance.com:9443
Environment=BINANCE_REST_BASE=https://api.binance.com
Environment=BYBIT_WS_URL=wss://stream.bybit.com/v5/public/spot
Environment=BYBIT_REST_BASE=https://api.bybit.com
Environment=ANALYSIS_INTERVAL_SECS=30
Environment=RISK_REWARD=3.0
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

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"

echo "[7/8] Waiting for health check..."
HEALTH_OK=0
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:$PORT/health" | grep -q '"ok":true'; then
    HEALTH_OK=1
    break
  fi
  sleep 1
done

if [[ "$HEALTH_OK" -ne 1 ]]; then
  echo
  echo "ERROR: service did not pass the health check."
  echo "----- systemd status -----"
  systemctl --no-pager --full status "$SERVICE_NAME" || true
  echo "----- recent logs -----"
  journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
  exit 1
fi

echo "[8/8] Finalizing..."
if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q '^Status: active'; then
  ufw allow "$PORT/tcp" >/dev/null
  echo "UFW is active: opened TCP port $PORT."
fi

PRIMARY_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
echo
echo "============================================================"
echo " Market Lab installed successfully"
echo "============================================================"
echo " Service:   systemctl status $SERVICE_NAME"
echo " Logs:      journalctl -u $SERVICE_NAME -f"
echo " Data:      $DATA_DIR"
echo " Binance:   http://${PRIMARY_IP:-SERVER_IP}:$PORT/binance"
echo " Bybit:     http://${PRIMARY_IP:-SERVER_IP}:$PORT/bybit"
echo " Health:    http://${PRIMARY_IP:-SERVER_IP}:$PORT/health"
echo "============================================================"
