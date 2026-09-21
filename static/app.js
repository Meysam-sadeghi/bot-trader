(() => {
  const pathExchange = location.pathname.toLowerCase().includes("bybit") ? "bybit" : "binance";
  const exchange = pathExchange;
  const displayExchange = exchange === "binance" ? "Binance" : "Bybit";

  const $ = (id) => document.getElementById(id);
  const symbolInput = $("symbol");
  const pricePoints = [];
  const liveEvents = [];
  let socket;
  let perSecond = 0;
  let lastRateAt = Date.now();

  $("exchange-title").textContent = displayExchange;
  document.title = displayExchange + " Market Lab";
  document.querySelectorAll(".nav-link").forEach((link) => {
    link.classList.toggle("active", link.dataset.exchange === exchange);
  });

  function symbol() {
    return symbolInput.value.trim().toUpperCase() || "BTCUSDT";
  }

  async function request(path, options = {}) {
    const response = await fetch(path, options);
    let body = {};
    try { body = await response.json(); } catch (_) {}
    if (!response.ok) {
      throw new Error(body.error || response.statusText || "Request failed");
    }
    return body;
  }

  function toast(message) {
    const node = $("toast");
    node.textContent = message;
    node.hidden = false;
    clearTimeout(node._timer);
    node._timer = setTimeout(() => { node.hidden = true; }, 3500);
  }

  async function action(name) {
    const encoded = encodeURIComponent(symbol());
    try {
      if (name === "capture-start") {
        await request("/api/capture/" + exchange + "/start?symbol=" + encoded, { method: "POST" });
        toast(displayExchange + " capture started");
      } else if (name === "capture-stop") {
        await request("/api/capture/" + exchange + "/stop", { method: "POST" });
        toast(displayExchange + " capture stopped");
      } else if (name === "analysis-start") {
        await request("/api/analysis/" + exchange + "/start?symbol=" + encoded, { method: "POST" });
        toast("Analysis engine started");
      } else if (name === "analysis-stop") {
        await request("/api/analysis/" + exchange + "/stop", { method: "POST" });
        toast("Analysis engine stopped");
      }
      await refresh();
    } catch (error) {
      toast(error.message);
    }
  }

  $("start-capture").addEventListener("click", () => action("capture-start"));
  $("stop-capture").addEventListener("click", () => action("capture-stop"));
  $("start-analysis").addEventListener("click", () => action("analysis-start"));
  $("stop-analysis").addEventListener("click", () => action("analysis-stop"));

  function formatPrice(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
    const number = Number(value);
    const decimals = number >= 1000 ? 2 : number >= 1 ? 5 : 8;
    return number.toLocaleString(undefined, { maximumFractionDigits: decimals });
  }

  function formatTime(timestamp) {
    if (!timestamp) return "—";
    return new Date(timestamp).toLocaleTimeString([], {
      hour: "2-digit", minute: "2-digit", second: "2-digit"
    });
  }

  function formatRelative(timestamp) {
    if (!timestamp) return "No events yet";
    const seconds = Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
    if (seconds < 2) return "Updated now";
    if (seconds < 60) return "Updated " + seconds + "s ago";
    return "Updated " + Math.floor(seconds / 60) + "m ago";
  }

  function renderPredictions(items) {
    $("prediction-total").textContent = items.length + " signals";
    const body = $("prediction-body");
    if (!items.length) {
      body.innerHTML = '<tr><td colspan="9" class="empty">No predictions yet. Capture enough market history, then start analysis.</td></tr>';
      return;
    }

    body.innerHTML = items.map((item) => {
      const sideClass = item.direction === "LONG" ? "side-long" : "side-short";
      const statusClass = "status-" + item.status.toLowerCase();
      const pnl = item.pnl_bps === null || item.pnl_bps === undefined
        ? "—"
        : (item.pnl_bps >= 0 ? "+" : "") + Number(item.pnl_bps).toFixed(2);
      return "<tr>" +
        "<td>" + formatTime(item.created_at) + "</td>" +
        "<td>" + Math.round(item.horizon_secs / 60) + "m</td>" +
        '<td class="' + sideClass + '">' + item.direction + "</td>" +
        "<td>" + formatPrice(item.entry_price) + "</td>" +
        "<td>" + formatPrice(item.target_price) + "</td>" +
        "<td>" + formatPrice(item.stop_price) + "</td>" +
        "<td>" + (Number(item.confidence) * 100).toFixed(1) + "%</td>" +
        '<td class="' + statusClass + '">' + item.status + "</td>" +
        "<td>" + pnl + "</td>" +
        "</tr>";
    }).join("");
  }

  function renderChart() {
    if (pricePoints.length < 2) {
      $("price-line").setAttribute("points", "");
      return;
    }
    const values = pricePoints.map((point) => point.price);
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (max === min) {
      max += 1;
      min -= 1;
    }
    const width = 1000;
    const height = 250;
    const points = pricePoints.map((point, index) => {
      const x = (index / Math.max(1, pricePoints.length - 1)) * width;
      const y = height - ((point.price - min) / (max - min)) * (height - 20) - 10;
      return x.toFixed(2) + "," + y.toFixed(2);
    }).join(" ");
    $("price-line").setAttribute("points", points);
  }

  function pushLiveEvent(event) {
    perSecond += 1;
    const price = event.price ?? (
      event.bid_price && event.ask_price
        ? (Number(event.bid_price) + Number(event.ask_price)) / 2
        : null
    );
    if (price && Number.isFinite(Number(price))) {
      pricePoints.push({ ts: event.ts, price: Number(price) });
      if (pricePoints.length > 240) pricePoints.shift();
      $("last-price").textContent = formatPrice(price);
      renderChart();
    }

    liveEvents.unshift(event);
    if (liveEvents.length > 42) liveEvents.pop();
    $("event-tape").innerHTML = liveEvents.map((item) => {
      const side = item.side || "";
      const sideClass = side === "BUY" ? "buy" : side === "SELL" ? "sell" : "";
      return '<div class="event-row">' +
        "<span>" + formatTime(item.ts) + "</span>" +
        '<span class="kind">' + item.kind + "</span>" +
        "<span>" + formatPrice(item.price ?? item.bid_price) + "</span>" +
        '<span class="' + sideClass + '">' + side + "</span>" +
        "</div>";
    }).join("");
  }

  async function refresh() {
    try {
      const data = await request("/api/dashboard/" + exchange);
      const capture = data.capture;
      if (capture.symbol && document.activeElement !== symbolInput) {
        symbolInput.value = capture.symbol;
      }
      $("symbol-title").textContent = capture.symbol || symbol();

      $("status-dot").classList.toggle("running", !!capture.running);
      $("status-text").textContent = capture.running ? "Capturing" : "Stopped";
      $("event-count").textContent = Number(data.stored_events || 0).toLocaleString();
      $("last-event").textContent = formatRelative(capture.last_event_at);
      if (capture.last_price) $("last-price").textContent = formatPrice(capture.last_price);

      const stats = data.stats || {};
      $("win-rate").textContent = ((Number(stats.win_rate) || 0) * 100).toFixed(2) + "%";
      $("resolved-count").textContent = (stats.resolved || 0) + " resolved predictions";
      const avg = Number(stats.avg_pnl_bps) || 0;
      $("avg-pnl").textContent = (avg >= 0 ? "+" : "") + avg.toFixed(2) + " bps";

      $("analysis-badge").textContent = data.analysis_running ? "Analysis running" : "Analysis stopped";
      $("analysis-badge").classList.toggle("active", !!data.analysis_running);

      renderPredictions(data.predictions || []);
      if (capture.last_error) {
        $("status-text").textContent = capture.running ? "Reconnecting" : "Stopped";
      }
    } catch (error) {
      $("status-text").textContent = "API offline";
    }
  }

  function connectSocket() {
    if (socket) socket.close();
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(protocol + "//" + location.host + "/ws/" + exchange);
    socket.onmessage = (message) => {
      try { pushLiveEvent(JSON.parse(message.data)); } catch (_) {}
    };
    socket.onclose = () => setTimeout(connectSocket, 1500);
  }

  setInterval(() => {
    const now = Date.now();
    const elapsed = Math.max(1, (now - lastRateAt) / 1000);
    $("live-rate").textContent = (perSecond / elapsed).toFixed(1) + " msg/s";
    perSecond = 0;
    lastRateAt = now;
  }, 1000);

  refresh();
  connectSocket();
  setInterval(refresh, 1500);
})();
