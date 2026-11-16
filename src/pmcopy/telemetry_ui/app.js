(function () {
  "use strict";

  const MAX_EVENTS = 150;
  const SNAPSHOT_POLL_MS = 5000;

  const state = {
    paused: false,
    filter: "all",
    events: [],
    seenEventIds: new Set(),
    eventSource: null,
    pollTimer: null,
  };

  function el(id) {
    return document.getElementById(id);
  }

  function formatBytes(bytes) {
    const n = Number(bytes || 0);
    if (n < 1024) return n.toFixed(0) + " B";
    if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
    return (n / (1024 * 1024)).toFixed(2) + " MB";
  }

  function formatInt(val) {
    return Math.round(Number(val || 0)).toLocaleString("en-US");
  }

  function formatDuration(sec) {
    const s = Math.max(0, Math.floor(Number(sec || 0)));
    const h = String(Math.floor(s / 3600)).padStart(2, "0");
    const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
    const r = String(s % 60).padStart(2, "0");
    return h + ":" + m + ":" + r;
  }

  function setPill(node, text, tone) {
    if (!node) return;
    node.textContent = text;
    node.className = "pill pill-" + tone;
  }

  function renderHeaderAndReadiness(readiness, detection) {
    const rState = (readiness && readiness.state) || {};
    const mode = String(rState.mode || "configured").toUpperCase();
    setPill(
      el("badge-trading-mode"),
      "MODE: " + mode,
      mode === "LIVE" ? "warn" : "neutral"
    );

    const effMode = (detection && detection.effective_mode) || "unwired";
    const tripped = Boolean(detection && detection.breaker_tripped);
    setPill(
      el("badge-detection-mode"),
      "DETECTION: " + effMode + (tripped ? " [AUTO-DOWNGRADED]" : ""),
      tripped ? "warn" : "ok"
    );

    const strictOk = Boolean(readiness && readiness.strict_ok);
    const basicOk = Boolean(readiness && readiness.ok);
    if (strictOk) {
      setPill(el("badge-readiness"), "READINESS: STRICT OK", "ok");
    } else if (basicOk) {
      setPill(el("badge-readiness"), "READINESS: DEGRADED", "warn");
    } else {
      setPill(el("badge-readiness"), "READINESS: NOT READY", "err");
    }

    el("label-uptime").textContent =
      "UPTIME " + formatDuration(readiness && readiness.uptime_seconds);

    const failed = (readiness && readiness.failed) || [];
    el("failed-gates-count").textContent =
      failed.length === 0 ? "All gates green" : failed.length + " failed: " + failed.join(", ");

    const gates = [
      ["Database", Boolean(rState.db_ok)],
      ["Polymarket API", Boolean(rState.api_ok)],
      ["CCXT Gateway", Boolean(rState.gateway_ok)],
      ["Market WSS", Boolean(rState.wss_market_ok)],
      ["User WSS", Boolean(rState.wss_user_ok)],
      ["Market Data", Boolean(rState.market_data_ready)],
      ["Reconciler", Boolean(rState.reconciler_ok)],
      ["Position Drift", Boolean(rState.drift_ok)],
      ["Supervisor", Boolean(rState.supervisor_ok !== false)],
      ["Circuit Breaker", rState.circuit_breaker_state === "closed"],
    ];
    const grid = el("subsystem-grid");
    grid.replaceChildren();
    for (const [label, ok] of gates) {
      const tile = document.createElement("div");
      tile.className = "status-tile";
      const span = document.createElement("span");
      span.textContent = label;
      const dot = document.createElement("span");
      dot.className = "dot " + (ok ? "dot-ok" : "dot-err");
      tile.append(span, dot);
      grid.appendChild(tile);
    }
  }

  function renderAlchemyMeter(detection) {
    if (!detection || !detection.available) {
      setPill(el("alchemy-breaker-pill"), "DETECTION IDLE", "neutral");
      return;
    }
    const tripped = Boolean(detection.breaker_tripped);
    setPill(
      el("alchemy-breaker-pill"),
      tripped ? "TRIPPED → LOGS ONLY" : "BREAKER ARMED",
      tripped ? "err" : "ok"
    );

    const rollingBytes = Number(detection.rolling_hour_bytes || 0);
    const maxBytes = Math.max(1, Number(detection.max_hourly_bytes || 1));
    const rollingMb = rollingBytes / (1024 * 1024);
    const maxMb = maxBytes / (1024 * 1024);
    const pct = Math.min(100, (rollingBytes / maxBytes) * 100);

    el("alchemy-mb-label").textContent =
      rollingMb.toFixed(2) + " / " + maxMb.toFixed(2) + " MB (" + pct.toFixed(1) + "%)";
    const bar = el("alchemy-mb-bar");
    bar.style.width = pct.toFixed(2) + "%";
    bar.style.backgroundColor =
      pct >= 90 ? "var(--tone-err)" : pct >= 70 ? "var(--tone-warn)" : "var(--tone-ok)";

    el("kpi-cu-min").textContent = Number(detection.estimated_cu_min || 0).toFixed(2);
    el("kpi-cu-day").textContent = formatInt(detection.estimated_cu_day_projection || 0);
    el("kpi-frames-bytes").textContent =
      formatInt(detection.ws_frames_in) + " / " + formatBytes(detection.ws_bytes_in);
    el("kpi-active-subs").textContent = formatInt(detection.active_subscriptions);
    el("kpi-matched-events").textContent = formatInt(detection.matched_user_events);
    el("kpi-ws-reconnects").textContent = formatInt(detection.reconnect_count);

    el("stat-tracked-users").textContent =
      formatInt(detection.tracked_leaders) + " / " + formatInt(detection.tracked_addresses);
    el("stat-mempool-active").textContent = formatInt(detection.active_reservations);
    el("stat-mempool-promoted").textContent = formatInt(detection.promoted_confirmed);
    el("stat-mempool-direct").textContent = formatInt(detection.direct_confirmed);
    el("stat-mempool-ghost").textContent = formatInt(detection.expired_ghost_fills);
  }

  function renderEntitiesAndGauges(readiness, metrics) {
    const rState = (readiness && readiness.state) || {};
    const gauges = (metrics && metrics.gauges) || {};
    const pairs = [
      ["Monitored Leaders", rState.monitored_leader_count || 0],
      ["Runtime Copiers", rState.runtime_user_count || 0],
      ["Pending Intents", rState.pending_intents_count || 0],
      ["Active Reservations", rState.active_reservations_count || 0],
      ["Stale Locks", rState.stale_locks_count || 0],
    ];
    for (const [k, v] of Object.entries(gauges)) {
      if (
        k.startsWith("telemetry_active_entities") ||
        k.startsWith("telemetry_queue_depth") ||
        k.startsWith("telemetry_queue_utilization_ratio") ||
        k.startsWith("telemetry_subscriptions")
      ) {
        const shortKey = k
          .replace("telemetry_active_entities", "entity")
          .replace("telemetry_queue_depth", "queue_depth")
          .replace("telemetry_queue_utilization_ratio", "queue_util");
        pairs.push([shortKey, typeof v === "number" && v < 1 && v > 0 ? (v * 100).toFixed(1) + "%" : v]);
      }
    }
    const container = el("entities-grid");
    container.replaceChildren();
    for (const [k, v] of pairs) {
      const row = document.createElement("div");
      row.className = "kv-item";
      const keySpan = document.createElement("span");
      keySpan.className = "kv-key";
      keySpan.textContent = k;
      const valSpan = document.createElement("span");
      valSpan.className = "kv-val";
      valSpan.textContent = String(v);
      row.append(keySpan, valSpan);
      container.appendChild(row);
    }
  }

  function toMs(histName, secondsOrMs) {
    if (secondsOrMs === null || secondsOrMs === undefined) return null;
    const n = Number(secondsOrMs);
    if (!Number.isFinite(n)) return null;
    return histName.endsWith("_seconds") ? n * 1000.0 : n;
  }

  function renderLatencyTable(metrics) {
    const histograms = ((metrics && metrics.histograms) || []).filter(
      (h) => h && Number(h.count || 0) > 0
    );
    el("latency-phase-count").textContent = histograms.length + " active stages";
    const tbody = el("latency-tbody");
    tbody.replaceChildren();
    if (histograms.length === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 6;
      td.className = "empty-cell";
      td.textContent = "No latency observations recorded yet.";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    let maxP99 = 1.0;
    const rows = histograms.map((h) => {
      const phase = (h.labels && (h.labels.phase || h.labels.stage)) || h.name;
      const p50 = toMs(h.name, h.p50);
      const p95 = toMs(h.name, h.p95);
      const p99 = toMs(h.name, h.p99);
      if (p99 !== null && p99 > maxP99) maxP99 = p99;
      return { phase, count: h.count, p50, p95, p99 };
    });

    for (const r of rows) {
      const tr = document.createElement("tr");
      const tdStage = document.createElement("td");
      tdStage.className = "mono";
      tdStage.textContent = r.phase;

      const tdCount = document.createElement("td");
      tdCount.className = "num";
      tdCount.textContent = formatInt(r.count);

      const fmt = (v) => (v === null ? "--" : v.toFixed(2));
      const td50 = document.createElement("td");
      td50.className = "num";
      td50.textContent = fmt(r.p50);

      const td95 = document.createElement("td");
      td95.className = "num";
      td95.textContent = fmt(r.p95);

      const td99 = document.createElement("td");
      td99.className = "num";
      td99.textContent = fmt(r.p99);

      const tdBar = document.createElement("td");
      tdBar.className = "bar-cell";
      const bg = document.createElement("div");
      bg.className = "lat-bar-bg";
      const fill = document.createElement("div");
      fill.className = "lat-bar-fill";
      const ratio = r.p99 !== null ? Math.min(100, (r.p99 / maxP99) * 100) : 0;
      fill.style.width = ratio.toFixed(1) + "%";
      bg.appendChild(fill);
      tdBar.appendChild(bg);

      tr.append(tdStage, tdCount, td50, td95, td99, tdBar);
      tbody.appendChild(tr);
    }
  }

  function renderCounters(metrics, generatedAt) {
    if (generatedAt) {
      const dt = new Date(generatedAt);
      el("counters-updated-at").textContent = Number.isNaN(dt.getTime())
        ? String(generatedAt)
        : dt.toLocaleTimeString();
    }
    const counters = (metrics && metrics.counters) || {};
    const entries = Object.entries(counters);
    const list = el("counters-list");
    list.replaceChildren();
    if (entries.length === 0) {
      const empty = document.createElement("div");
      empty.className = "empty-cell";
      empty.textContent = "No non-zero counters recorded yet.";
      list.appendChild(empty);
      return;
    }
    for (const [name, value] of entries) {
      const row = document.createElement("div");
      row.className = "counter-row";
      const nSpan = document.createElement("span");
      nSpan.className = "counter-name";
      nSpan.textContent = name;
      const vSpan = document.createElement("span");
      vSpan.className = "counter-val";
      vSpan.textContent = Number.isInteger(value) ? formatInt(value) : Number(value).toFixed(2);
      row.append(nSpan, vSpan);
      list.appendChild(row);
    }
  }

  function matchesFilter(ev, filter) {
    if (filter === "all") return true;
    const cat = String((ev.payload && ev.payload.category) || "").toLowerCase();
    const sev = String(ev.severity || "").toLowerCase();
    if (filter === "trades") return cat === "trade" || cat === "fill" || cat === "ambiguous";
    if (filter === "leaders") return cat === "leader" || cat === "roster" || cat === "task" || cat === "discovery";
    if (filter === "skips") return cat === "skip";
    if (filter === "risk") return cat === "risk" || cat === "protective_exit" || cat === "lock" || cat === "reconciliation";
    if (filter === "alerts") return sev === "warning" || sev === "error" || sev === "critical" || cat === "failure";
    return true;
  }

  function summarizeEvent(ev) {
    const p = ev.payload || {};
    const parts = [ev.event_type || "event"];
    for (const key of ["outcome", "side", "reason", "component", "code", "source", "leader_ref", "order_ref", "task_ref"]) {
      if (p[key] !== undefined && p[key] !== null && p[key] !== "") {
        parts.push(key + "=" + String(p[key]));
      }
    }
    return parts.join(" · ");
  }

  function renderEvents() {
    const feed = el("events-feed");
    feed.replaceChildren();
    const filtered = state.events.filter((ev) => matchesFilter(ev, state.filter));
    if (filtered.length === 0) {
      const empty = document.createElement("div");
      empty.className = "empty-cell";
      empty.textContent = "No matching telemetry events in buffer.";
      feed.appendChild(empty);
      return;
    }
    for (const ev of filtered) {
      const row = document.createElement("div");
      const sev = String(ev.severity || "info").toLowerCase();
      row.className = "event-item event-sev-" + sev;

      const tSpan = document.createElement("span");
      tSpan.className = "event-time";
      const dt = ev.timestamp ? new Date(ev.timestamp) : null;
      tSpan.textContent = dt && !Number.isNaN(dt.getTime()) ? dt.toLocaleTimeString() : "--:--:--";

      const cSpan = document.createElement("span");
      cSpan.className = "event-cat";
      cSpan.textContent = String((ev.payload && ev.payload.category) || ev.producer || "sys");

      const sSpan = document.createElement("span");
      sSpan.className = "event-summary";
      sSpan.textContent = summarizeEvent(ev);

      row.append(tSpan, cSpan, sSpan);
      feed.appendChild(row);
    }
  }

  function ingestEvent(ev, prepend) {
    if (!ev || !ev.event_id || state.seenEventIds.has(ev.event_id)) return;
    state.seenEventIds.add(ev.event_id);
    if (prepend) {
      state.events.unshift(ev);
    } else {
      state.events.push(ev);
    }
    if (state.events.length > MAX_EVENTS) {
      const removed = state.events.pop();
      if (removed && removed.event_id) state.seenEventIds.delete(removed.event_id);
    }
  }

  async function fetchSnapshot() {
    try {
      const resp = await fetch("/api/telemetry/snapshot?limit=100", { cache: "no-store" });
      if (!resp.ok) return;
      const snap = await resp.json();
      renderHeaderAndReadiness(snap.readiness, snap.detection);
      renderAlchemyMeter(snap.detection);
      renderEntitiesAndGauges(snap.readiness, snap.metrics);
      renderLatencyTable(snap.metrics);
      renderCounters(snap.metrics, snap.generated_at);
      if (!state.paused && Array.isArray(snap.events)) {
        const ordered = snap.events.slice().reverse();
        for (const ev of ordered) {
          ingestEvent(ev, true);
        }
        renderEvents();
      }
    } catch (_err) {
      setPill(el("badge-readiness"), "SNAPSHOT OFFLINE", "err");
    }
  }

  function connectStream() {
    if (state.eventSource) {
      state.eventSource.close();
    }
    const es = new EventSource("/api/telemetry/stream");
    state.eventSource = es;

    es.addEventListener("connected", function () {
      setPill(el("badge-sse"), "SSE: LIVE", "ok");
    });

    es.addEventListener("telemetry", function (msg) {
      if (state.paused) return;
      try {
        const ev = JSON.parse(msg.data);
        ingestEvent(ev, true);
        renderEvents();
      } catch (_e) {
        // Ignore malformed frame
      }
    });

    es.addEventListener("resync", function () {
      if (!state.paused) fetchSnapshot();
    });

    es.onerror = function () {
      setPill(el("badge-sse"), "SSE: RECONNECTING", "warn");
    };
  }

  function bindControls() {
    el("btn-refresh").addEventListener("click", function () {
      fetchSnapshot();
    });

    el("btn-pause").addEventListener("click", function () {
      state.paused = !state.paused;
      el("btn-pause").textContent = state.paused ? "Resume Stream" : "Pause Stream";
      setPill(
        el("badge-sse"),
        state.paused ? "SSE: PAUSED" : "SSE: LIVE",
        state.paused ? "warn" : "ok"
      );
      if (!state.paused) fetchSnapshot();
    });

    for (const btn of document.querySelectorAll(".chip[data-filter]")) {
      btn.addEventListener("click", function () {
        for (const b of document.querySelectorAll(".chip[data-filter]")) {
          b.classList.remove("chip-active");
        }
        btn.classList.add("chip-active");
        state.filter = btn.getAttribute("data-filter") || "all";
        renderEvents();
      });
    }
  }

  bindControls();
  fetchSnapshot();
  connectStream();
  state.pollTimer = window.setInterval(fetchSnapshot, SNAPSHOT_POLL_MS);
})();
