(() => {
  const pathExchange = location.pathname.toLowerCase().includes("bybit") ? "bybit" : "binance";
  const exchange = pathExchange;
  const displayExchange = exchange === "binance" ? "Binance" : "Bybit";

  const $ = (id) => document.getElementById(id);
  const symbolInput = $("symbol");
  const pricePoints = [];
  const liveEvents = [];
  let socket;
  let dashboards = {};
  let refreshing = false;
  let perSecond = 0;
  let lastRateAt = Date.now();

  $("exchange-title").textContent = displayExchange;
  document.title = displayExchange + " Market Lab";
  $("strategy-mode").textContent = "6 strategies × 4 horizons per exchange";
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
        toast(displayExchange + " Auto Lab started: capture + analysis + paper trading");
      } else if (name === "capture-stop") {
        await request("/api/capture/" + exchange + "/stop", { method: "POST" });
        toast(displayExchange + " Auto Lab stopped");
      }
      await refresh();
    } catch (error) {
      toast(error.message);
    }
  }

  $("start-capture").addEventListener("click", () => action("capture-start"));
  $("stop-capture").addEventListener("click", () => action("capture-stop"));
  for (const operation of ["start", "stop"]) {
    $(operation + "-both").addEventListener("click", async () => {
      try {
        await request("/api/lab/" + operation + "?symbol=" + encodeURIComponent(symbol()), { method: "POST" });
        toast(operation === "start" ? "Both exchanges started · all 24 strategy/horizon lanes per exchange" : "Both exchanges stopped");
        await refresh();
      } catch (error) { toast(error.message); }
    });
  }
  $("comparison-horizon").addEventListener("change", renderComparisons);
  $("strategy-filter").addEventListener("change", () => renderPredictions(dashboards[exchange]?.predictions || []));
  $("export-json").addEventListener("click", () => runExport("json"));
  $("export-csv").addEventListener("click", () => runExport("csv"));
  $("export-preview").addEventListener("click", () => runExport("summary"));
  document.querySelectorAll(".export-filters input, .export-filters select").forEach(node => node.addEventListener("change", () => {
    $("export-summary").hidden = true;
    $("export-message").textContent = "Filters changed. Preview or download to get all matching positions.";
  }));
  $("position-close").addEventListener("click", () => $("position-dialog").close());
  $("prediction-body").addEventListener("click", event => {
    const button = event.target.closest("button[data-position-id]");
    if (button) showPosition(button.dataset.positionId);
  });

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
  }
  function pct(value) { return (Number(value || 0) * 100).toFixed(1) + "%"; }
  function money(value) { return Number(value || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2}); }
  function signed(value) { return (Number(value) >= 0 ? "+" : "") + Number(value || 0).toFixed(2); }

  function exportParams() {
    const params = new URLSearchParams();
    for (const name of ["exchange", "config", "strategy", "horizon", "status", "symbol"]) {
      const value = $("export-" + name).value.trim();
      if (value && value !== "all") params.set(name, value);
    }
    for (const [id, key] of [["export-from", "from_ms"], ["export-to", "to_ms"]]) {
      if ($(id).value) {
        const time = new Date($(id).value).getTime();
        if (!Number.isFinite(time)) throw new Error("Enter a valid date and time.");
        params.set(key, String(time));
      }
    }
    return params;
  }

  async function runExport(format) {
    const controls = document.querySelectorAll(".export-actions button, .export-filters input, .export-filters select");
    controls.forEach(node => { node.disabled = true; });
    $("export-message").textContent = "Preparing all matching positions…";
    try {
      const params = exportParams();
      if (format === "summary") {
        const result = await request("/api/exports/summary?" + params);
        renderExportSummary(result);
        $("export-message").textContent = result.position_count + " matching positions · snapshot " + new Date(result.generated_at).toLocaleString();
      } else {
        params.set("format", format);
        const response = await fetch("/api/exports/positions?" + params);
        if (!response.ok) {
          const error = await response.json().catch(() => ({}));
          throw new Error(error.error || "Download failed (" + response.status + ")");
        }
        const blob = await response.blob();
        const filename = response.headers.get("content-disposition")?.match(/filename="([^"]+)"/)?.[1] || "market-lab-positions." + format;
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url; link.download = filename;
        document.body.appendChild(link); link.click(); link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 10000);
        const count = response.headers.get("x-position-count") || "All matching";
        $("export-message").textContent = count + " positions exported. " + (format === "json" ? "Send this JSON file for analysis of entries, exits and losses." : "CSV includes costs, outcome categories and entry/exit evidence.");
      }
    } catch (error) {
      $("export-message").textContent = error.message;
    } finally { controls.forEach(node => { node.disabled = false; }); }
  }

  function renderExportSummary(data) {
    const c = data.counts;
    const metrics = [["Closed positions", c.closed], ["Profitable", c.profitable], ["Stop losses", c.stop_losses],
      ["Timeout losses", c.timeout_losses], ["Unverifiable data / fill", c.unverifiable], ["Fees changed profit to loss", c.fees_changed_profit_to_loss]];
    const rows = (data.groups || []).map(g => {
      const s = g.counts;
      return '<tr>' + [g.exchange, g.symbol, g.config_id, g.strategy, g.horizon_secs / 60 + 'm', g.direction,
        s.closed, s.profitable, s.losing, s.unverifiable,
        g.net_win_rate_all_closed === null ? '—' : pct(g.net_win_rate_all_closed),
        g.net_win_rate_priced_closed === null ? '—' : pct(g.net_win_rate_priced_closed),
        money(g.known_net_pnl_quote)].map(value => '<td>' + escapeHtml(value) + '</td>').join('') + '</tr>';
    }).join('');
    $("export-summary").innerHTML = '<div class="audit-counts">' + metrics.map(([label, value]) => '<div><span>' + label + '</span><strong>' + Number(value || 0).toLocaleString() + '</strong></div>').join('') + '</div>' +
      '<p class="research-note">' + c.open + ' open · ' + c.breakeven + ' flat · ' + c.other_losses + ' other losses. Fee-related losses overlap stop/timeout losses. ' + c.gave_back_observed_profit + ' losing trades were previously profitable at an observed executable quote.</p>' +
      '<p class="research-note">Entry evidence missing: ' + c.missing_entry_snapshot + ' · Execution evidence missing: ' + c.missing_execution_audit + ' · Partial after upgrade: ' + c.partial_execution_audit + '. Unknown paths are shown separately from priced losses. Old records cannot reveal unrecorded entry features.</p>' +
      '<div class="table-wrap"><table><thead><tr><th>Exchange</th><th>Symbol</th><th>Config</th><th>Strategy</th><th>Horizon</th><th>Side</th><th>Closed</th><th>Profit</th><th>Loss</th><th>Unknown</th><th>Win / all closed</th><th>Win / priced</th><th>Known PnL</th></tr></thead><tbody>' +
      (rows || '<tr><td colspan="13" class="empty">No positions match these filters.</td></tr>') + '</tbody></table></div>' +
      '<p class="research-note">Legacy rows use the original gross-price assumptions. Each configuration stays separate. Counts describe observed outcomes; they do not prove why the market moved.</p>';
    $("export-summary").hidden = false;
    const strategySelect = $("export-strategy");
    for (const g of data.groups || []) {
      if (!Array.from(strategySelect.options).some(option => option.value === g.strategy)) strategySelect.add(new Option(g.strategy, g.strategy));
    }
  }

  async function showPosition(id) {
    const dialog = $("position-dialog");
    $("position-detail").textContent = "Loading recorded evidence…";
    if (!dialog.open) dialog.showModal();
    try {
      const p = await request("/api/positions/" + encodeURIComponent(id));
      const d = p.diagnosis;
      const bps = value => value === null || value === undefined ? 'Not recorded' : signed(value) + ' bps';
      const facts = [["Position", p.exchange + ' · ' + p.symbol + ' · ' + p.strategy + ' · ' + p.direction],
        ["Entry time", new Date(p.created_at).toLocaleString()], ["Entry reason", p.entry_reason],
        ["Exit", d.exit_reason + ' (' + d.exit_reason_source + ')'], ["Net result", bps(p.pnl_bps)],
        ["Modeled fees paid", bps(d.fees_paid_bps)], ["Best observed net mark", bps(d.best_observed_net_bps)],
        ["Worst observed net mark", bps(d.worst_observed_net_bps)], ["Entry evidence", d.entry_evidence], ["Path coverage", d.path_evidence]];
      $("position-detail").innerHTML = '<div class="evidence-facts">' + facts.map(([label,value]) => '<p><span>' + label + '</span>' + escapeHtml(value) + '</p>').join('') + '</div>' +
        '<p class="research-note">' + (p.entry_snapshot ? 'Entry values were stored when the signal was accepted. Scores are not win probabilities.' : 'This position predates detailed entry recording. Its missing conditions cannot be recovered from its final result.') + '</p>' +
        '<details open><summary>Recorded entry conditions and thresholds</summary><pre>' + escapeHtml(JSON.stringify(p.entry_snapshot, null, 2)) + '</pre></details>' +
        '<details><summary>Observed execution and exact exit reason</summary><pre>' + escapeHtml(JSON.stringify(p.execution_audit, null, 2)) + '</pre></details>';
    } catch (error) { $("position-detail").textContent = error.message; }
  }

  function renderComparisons() {
    const horizon = $("comparison-horizon").value;
    $("comparison-exchanges").innerHTML = ["binance", "bybit"].map(key => {
      const data = dashboards[key];
      if (!data) return '<div class="panel feed-diagnostic">' + escapeHtml(key) + ': comparison data unavailable</div>';
      const diag = data.diagnostics || {};
      const config = data.config || {};
      const active = data.capture.running && data.analysis_running;
      const stale = !data.capture.last_event_at || Date.now() - data.capture.last_event_at > 15000;
      const fee = key === "binance" ? config.binance_fee_bps : config.bybit_fee_bps;
      const header = '<div class="exchange-comparison-header"><h3>' + (key === "binance" ? "Binance" : "Bybit") + ' <small>' + escapeHtml(data.capture.symbol) + '</small></h3>' +
        '<span class="badge ' + (active && !stale ? 'active' : '') + '">' + (active ? (stale ? "Waiting for fresh feed" : "Lab running") : "Stopped") + '</span></div>';
      const notice = '<div class="feed-diagnostic">' + escapeHtml(data.capture.last_error || diag.message) +
        '<small>History: ' + Number(diag.history_minutes || 0).toFixed(1) + ' min' + (diag.history_truncated ? ' · History capped by event limit' : '') +
        ' · Fee assumption: ' + Number(fee).toFixed(1) + ' bps / side · Slippage: ' + Number(config.slippage_bps).toFixed(1) +
        ' bps / side · Notional: ' + money(config.notional) + ' per trade' +
        ' · Archive: ' + (data.archived_predictions || 0) + ' older/config-changed positions' +
        ' · <a href="/api/predictions/' + key + '?config=all&amp;limit=500" target="_blank" rel="noopener">Inspect archive JSON</a></small></div>';
      const cards = (data.strategies || []).map(strategy => {
        const lane = (strategy.horizons || []).find(h => String(h.horizon_secs) === horizon);
        const stats = lane ? lane.stats : strategy.stats;
        const reason = (diag.lanes || []).find(d => d.strategy === strategy.id && String(d.horizon_secs) === horizon)?.reason || (horizon === "all" ? "Four separate accounts combined; inspect each horizon before comparing." : diag.message);
        const n = Number(stats.resolved || 0);
        const pf = stats.profit_factor === null ? (stats.gross_profit > 0 ? "No losses yet" : "—") : Number(stats.profit_factor).toFixed(2);
        const assessment = n < config.min_forward_trades ? "Early sample · " + n + "/" + config.min_forward_trades + " closed" : (stats.invalid ? "Incomplete paths · inspect data gaps" : (stats.sample_ready && lane ? "80% lower bound reached · keep validating" : "Forward evidence · target not established"));
        return '<article class="strategy-card panel"><div class="strategy-heading"><h4>' + escapeHtml(strategy.name) + '</h4><span class="badge">' + stats.open + ' open</span></div>' +
          '<p class="strategy-description">' + escapeHtml(strategy.description) + '</p>' +
          '<div class="strategy-primary"><div><span>Net win rate</span><strong>' + (n ? pct(stats.win_rate) : '—') + '</strong><small>' + (n ? stats.wins + ' profitable / ' + n + ' closed' : 'Awaiting closed positions') + '</small></div>' +
          '<div><span>Known realized net PnL</span><strong class="' + (stats.net_pnl < 0 ? 'negative' : 'positive') + '">' + money(stats.net_pnl) + '</strong><small>Account: ' + money(stats.initial_capital) + '</small></div></div>' +
          '<dl class="strategy-details"><div><dt>95% interval</dt><dd>' + (n ? pct(stats.win_rate_low) + '–' + pct(stats.win_rate_high) : '—') + '</dd></div>' +
          '<div><dt>Net TP hit rate</dt><dd>' + (n ? pct(stats.strict_win_rate) : '—') + '</dd></div>' +
          '<div><dt>Profit factor</dt><dd>' + pf + '</dd></div><div><dt>Avg net / trade</dt><dd>' + signed(stats.avg_pnl_bps) + ' bps</dd></div>' +
          '<div><dt>Closed drawdown</dt><dd>' + Number(stats.closed_drawdown_pct).toFixed(2) + '%</dd></div>' +
          '<div><dt>Open PnL · last quote</dt><dd>' + (stats.unmarked_open ? 'Incomplete mark' : money(stats.unrealized_pnl)) + '</dd></div>' +
          '<div><dt>Return incl. open</dt><dd>' + (stats.invalid || stats.unmarked_open ? 'Incomplete' : signed(stats.return_pct) + '%') + '</dd></div>' +
          '<div><dt>Loss / flat / timeout</dt><dd>' + stats.losses + ' / ' + stats.breakeven + ' / ' + stats.timeouts + '</dd></div>' +
          '<div><dt>Data / fill gaps</dt><dd class="' + (stats.invalid ? 'negative' : '') + '">' + stats.invalid + '</dd></div></dl>' +
          '<p class="sample-state">' + escapeHtml(assessment) + '</p><p class="lane-reason">' + escapeHtml(reason) + '</p></article>';
      }).join('');
      return '<section class="exchange-comparison">' + header + notice + '<div class="strategy-grid">' + cards + '</div></section>';
    }).join('');
  }

  function getAdminToken() {
    let token = sessionStorage.getItem("marketLabAdminToken") || "";
    if (!token) {
      token = window.prompt("Admin token (shown by the Ubuntu installer):") || "";
      if (token) sessionStorage.setItem("marketLabAdminToken", token);
    }
    return token;
  }

  async function adminRequest(path) {
    const token = getAdminToken();
    if (!token) throw new Error("Admin token is required");
    try {
      return await request(path, {
        method: "POST",
        headers: { "X-Admin-Token": token }
      });
    } catch (error) {
      if (error.message.toLowerCase().includes("admin token")) {
        sessionStorage.removeItem("marketLabAdminToken");
      }
      throw error;
    }
  }

  $("clear-data").addEventListener("click", async () => {
    if (!window.confirm("Stop Auto Lab and permanently clear all captured data and paper positions for " + displayExchange + "?")) {
      return;
    }
    try {
      const result = await adminRequest("/api/data/" + exchange + "/clear");
      pricePoints.length = 0;
      liveEvents.length = 0;
      $("event-tape").innerHTML = "";
      renderChart();
      toast("Cleared " + Number(result.events_deleted || 0).toLocaleString() + " events and " +
        Number(result.predictions_deleted || 0).toLocaleString() + " paper positions");
      await refresh();
    } catch (error) {
      toast(error.message);
    }
  });

  $("update-system").addEventListener("click", async () => {
    if (!window.confirm("Update Market Lab from the latest mobile branch on GitHub? The service will restart automatically.")) {
      return;
    }
    try {
      await adminRequest("/api/system/update");
      toast("Update started. Market Lab will rebuild and restart automatically.");
    } catch (error) {
      toast(error.message);
    }
  });

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
    const strategy = $("strategy-filter").value;
    items = items.filter(item => strategy === "all" || item.strategy === strategy);
    $("prediction-total").textContent = items.length + " signals";
    const body = $("prediction-body");
    if (!items.length) {
      body.innerHTML = '<tr><td colspan="11" class="empty">No paper positions yet. Auto analysis starts with capture and opens positions after enough history is available.</td></tr>';
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
        "<td title=\"" + escapeHtml(item.entry_reason) + "\">" + escapeHtml(item.strategy.replace("_v3", "")) + "</td>" +
        "<td>" + Math.round(item.horizon_secs / 60) + "m</td>" +
        '<td class="' + sideClass + '">' + item.direction + "</td>" +
        "<td>" + formatPrice(item.entry_price) + "</td>" +
        "<td>" + formatPrice(item.target_price) + "</td>" +
        "<td>" + formatPrice(item.stop_price) + "</td>" +
        "<td>" + Number(item.score).toFixed(3) + "</td>" +
        '<td class="' + statusClass + '">' + item.status + "</td>" +
        "<td>" + pnl + "</td>" +
        '<td><button class="evidence-button" data-position-id="' + escapeHtml(item.id) + '">Details</button></td>' +
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
      event.kind === "book_ticker" && event.bid_price && event.ask_price
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
        '<span class="kind">' + escapeHtml(item.kind) + "</span>" +
        "<span>" + formatPrice(item.price ?? item.bid_price) + "</span>" +
        '<span class="' + sideClass + '">' + escapeHtml(side) + "</span>" +
        "</div>";
    }).join("");
  }

  async function refresh() {
    if (refreshing) return;
    refreshing = true;
    try {
      const results = await Promise.allSettled(["binance", "bybit"].map(async key => [key, await request("/api/dashboard/" + key)]));
      results.forEach((result, i) => {
        const key = ["binance", "bybit"][i];
        if (result.status === "fulfilled") dashboards[key] = result.value[1];
        else delete dashboards[key];
      });
      renderComparisons();
      const data = dashboards[exchange];
      if (!data) throw new Error("Dashboard offline");
      if ($("strategy-filter").options.length === 1) {
        (data.strategies || []).forEach(s => $("strategy-filter").add(new Option(s.name, s.id)));
      }
      for (const s of data.strategies || []) {
        if (!Array.from($("export-strategy").options).some(option => option.value === s.id)) $("export-strategy").add(new Option(s.name, s.id));
      }
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
      $("win-rate").textContent = stats.resolved ? pct(stats.win_rate) : "—";
      $("resolved-count").textContent = (stats.resolved || 0) + " resolved paper positions";
      const avg = Number(stats.avg_pnl_bps) || 0;
      $("avg-pnl").textContent = (avg >= 0 ? "+" : "") + avg.toFixed(2) + " bps";

      $("analysis-badge").textContent = data.analysis_running ? "Auto analysis running" : "Auto analysis stopped";
      $("analysis-badge").classList.toggle("active", !!data.analysis_running);

      renderPredictions(data.predictions || []);
      if (capture.last_error) {
        $("status-text").textContent = capture.running ? "Reconnecting" : "Stopped";
      }
    } catch (error) {
      $("status-text").textContent = "API offline";
    } finally { refreshing = false; }
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
  setInterval(refresh, 5000);
})();
