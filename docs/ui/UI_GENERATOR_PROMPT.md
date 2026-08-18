# Build an operator console for a Polymarket copy-trading bot

You are generating a **frontend only**. The backend already exists, is frozen, and
is described completely below and in the machine-readable companion file
`api_contract.json`. Do not design endpoints, do not invent fields, and do not
ask the backend to change. Where this document and `api_contract.json` disagree,
`api_contract.json` wins — it is generated from the running code and kept in
sync by a test.

The product is a single-operator control room for a bot that mirrors ("copies")
the trades of chosen leader wallets on Polymarket prediction markets. The
operator needs to see, at a glance, whether detection is alive, what the bot
just did and why, what it is holding, what it made or lost, and how close it is
to its risk limits — and needs a small number of heavily-guarded controls to
stop it.

---

## 1. Non-negotiables

These are correctness requirements, not style preferences. A console that
breaks one of them is worse than no console.

1. **Never parse money, prices, sizes, share counts, or ratios as a
   floating-point number.** Every such value crosses the wire as a *decimal
   string* (`"352.02"`, `"0.0025"`). Keep it as a string, and do arithmetic with
   an arbitrary-precision decimal library (`decimal.js`, `big.js`, `BigDecimal`,
   Python `Decimal`, …). `parseFloat`, `Number()`, `JSON.parse` into a number,
   or `toFixed` on a coerced float are all defects. Every field marked
   **decimal string** in the schemas below, and every entry of `decimal_fields`
   in `api_contract.json`, is subject to this rule. Round only at the moment of
   display, never before storing or comparing.
2. **Render only what `GET /api/v1/system/capabilities` reports.** The backend
   registers a route only when the process actually composed the service behind
   it, so an absent capability means the endpoint returns `404`, not that it is
   temporarily broken. Fetch capabilities first, and hide or disable every
   control, tab, and panel whose capability is missing. Never show an error
   because a capability is absent.
3. **The WebSocket stream is a notification channel; REST is the authority.**
   Never build state solely by accumulating stream events. Load a REST snapshot
   first, use events to mark data stale and to drive a live activity feed, and
   re-read REST when the server says to.
4. **Deduplicate detection events by `correlation_id`.** REST polling keeps
   running as a parallel fallback even when an on-chain detection mode is
   selected, so with `detection.mode = on_chain_pending` (or
   `on_chain_confirmed`) a single leader fill normally produces **two**
   `leader_signal` events with `payload.outcome: "received"` — one with
   `payload.source: "on_chain_pending"` and one with `payload.source:
   "polling"` — plus **one** `leader_signal` with `payload.outcome: "dedup"`
   from whichever source lost the race. In plain `polling` mode there is one
   `received` and no `dedup`. The two `received` events share a
   `correlation_id` (the leader transaction hash), so collapse them into one
   row that lists both sources. Compare case-insensitively: the id is the hash
   exactly as the detecting source spelled it. The `dedup` event carries
   `correlation_id: null` and therefore cannot be joined to the transaction —
   render it as a counter, not as a row.
5. **Realized PnL is session-scoped.** `GET /pnl` → `realized` comes from an
   in-process ledger that starts empty at every process start and is lost on
   restart. Label it "this session" everywhere, show `realized.session_started_at`
   next to it, and never present it as lifetime performance.
6. **`leader_ws_connected: null` is not "disconnected".** `null` means no
   detection-transport probe is attached yet (the control plane starts before
   the detection runtime). Render three states: connected, disconnected,
   unknown. Never coerce `null` to `false`.
7. **Respect `unrealized_basis`.** The account-level `unrealized_pnl_usdc` on
   `GET /pnl` is published only when every open position could be marked from a
   fresh two-sided cached quote. See §9.2 for the four values and exactly what
   each one licenses you to display. Never sum the per-position marks yourself
   to fill in a `null` account total — the missing positions are precisely the
   ones you cannot value.
8. **Settings writes never take effect immediately.** Every successful write
   returns `restart_required: true` and `live_applied: false`. Render "saved —
   takes effect after restart". Do **not** offer an "apply now" button; there is
   no such operation. Writes are revision-guarded (§10).
9. **A viewer credential has no controls.** If the console was given the viewer
   credential, every mutation returns `403 permission_denied`. Detect the role
   once (§3) and render read-only: no pause/resume/full-stop, no settings form,
   no discovery run, no lock reset, no task editing. Disabling is better than
   hiding here, with a tooltip explaining the credential is read-only.
10. **Never display or log a credential.** The bearer token belongs in exactly
    two places — the `Authorization` header, and the WebSocket subprotocol
    carrier the upgrade accepts instead of it (§2). Not in a URL, not in
    `localStorage` next to user-visible state, not in an error toast, not in a
    debug panel.
11. **Every destructive control needs an explicit confirmation dialog** that
    names the consequence, requires a typed or selected `reason_code`, and shows
    the `expected_revision` being asserted. Full-stop additionally cancels
    resting orders; say so in the dialog.
12. **Never invent a value the backend declined to give.** `null` means the
    process could not observe it. Show an em-dash, a "not available" chip, or a
    tooltip — never `0`, `0.00`, "N/A%" computed from nothing, or a
    last-known-good value presented as current.

---

## 2. Connecting

**Base URL:** `http://127.0.0.1:9103/api/v1`

The server binds to loopback by default and is disabled unless the deployment
enables it. In production, serve the console from behind a **same-origin reverse
proxy** that forwards `/api/v1` to the bot. Cross-origin browsers work only when
the operator has added the console origin to the server's CORS allowlist; there
is no wildcard mode and `Access-Control-Allow-Credentials` is never sent, so
cookie-based auth is impossible. Origin handling is uniform across every
`/api/v1` request, preflights and WebSocket upgrades included:

- **no `Origin` header** (same-origin, or a non-browser client) — the request
  proceeds and no CORS headers are added;
- **`Origin` on the allowlist** — the request proceeds and the response carries
  `Access-Control-Allow-Origin` and `Vary: Origin`; a preflight additionally
  carries `Access-Control-Allow-Methods: DELETE, GET, PATCH, POST, PUT` and
  `Access-Control-Allow-Headers: Authorization, Content-Type, X-Correlation-ID`;
- **`Origin` not on the allowlist** — `403 origin_not_allowed`, decided before
  authentication and before routing.

A preflight `OPTIONS` that passes the origin check always returns `204`. It is
short-circuited ahead of authentication, so it never needs a credential — but a
`204` with no CORS headers means the origin was simply absent, not allowed.

**Authentication:** send the credential on every request, including the
WebSocket upgrade:

```
Authorization: Bearer <credential>
```

Two principals exist. Which one the console holds is a deployment decision:

| Principal | Holds | Use |
| --- | --- | --- |
| `operator` | every permission | full control room |
| `viewer` | every `*.read` permission plus `telemetry.stream` | read-only dashboard |

Missing or unparseable credential → `401` with
`{"error":{"code":"authentication_required"...}}` or `"invalid_bearer_token"`,
and a `WWW-Authenticate: Bearer` header. Authenticated but not permitted →
`403 permission_denied`. Treat `401` as "re-prompt for the credential" and
`403` as "this build/credential cannot do that" — never retry a `403`.

**Browser WebSocket auth.** The browser `WebSocket` constructor cannot set
request headers, so the upgrade — and *only* the upgrade — also accepts the
credential as a subprotocol offer:

```js
const socket = new WebSocket(`${base}/stream`, ["bearer", credential]);
```

That sends `Sec-WebSocket-Protocol: bearer, <credential>`; the two tokens are
order-insensitive, and a successful `101` echoes `Sec-WebSocket-Protocol:
bearer`, which is what makes the browser accept the handshake. `socket.protocol`
is then `"bearer"`. The credential is checked by the same constant-time bearer
path as the header, so permissions, rate limits and every frame contract are
identical.

Four rules come with it:

- **Only the upgrade reads it.** Every REST route ignores
  `Sec-WebSocket-Protocol`; a REST call still needs the `Authorization` header.
- **Send exactly one carrier.** If you send both, they must present the same
  credential in the same `Bearer <credential>` form; anything else is
  `401 conflicting_bearer_credentials`. The header wins when they agree.
- **Any other subprotocol is refused** with `400 unsupported_subprotocol`
  before the handshake. Do not offer a second subprotocol alongside `bearer`.
- **The credential must be subprotocol-token safe** — only
  `A-Za-z0-9._~+/=-` (a `secrets.token_urlsafe` value qualifies). A deployment
  whose credential uses anything else logs one startup warning and disables
  this carrier entirely, with no fallback; such a console needs a reverse proxy
  that injects the header, or a local agent that holds the socket.

Design the live feed so that a stream that cannot connect degrades to REST
polling instead of breaking the app.

**Response headers** you can rely on: `X-Correlation-ID`, `Cache-Control:
no-store`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`.

**Request bodies** must be `Content-Type: application/json`, at most
`262144` bytes, with no duplicate object keys and no `NaN`/`Infinity` literals —
all three are rejected as `invalid_request`. You may supply your own
`X-Correlation-ID` request header; it is echoed back when it matches
`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`, otherwise the server generates one.

---

## 3. Startup sequence

Do this once, in order, before rendering anything but a splash state:

1. `GET /system/capabilities` → `capabilities: string[]`. Store it; gate all UI
   on it.
2. `GET /system/status` → process identity plus readiness. Drives the header
   badge.
3. `GET /accounts` → the follower accounts this process composed. **Most reads
   require a `follower_account` query parameter and there is no default**, so
   this list populates the account switcher. If `account_read` is absent from
   capabilities, ask the operator to enter the account identifier once and
   persist it locally.
4. Determine the role. There is no "who am I" endpoint and no way to probe a
   permission without exercising it, so do not try: render controls for the
   capabilities that exist, and on the first `403 permission_denied` from any
   mutation, latch `role = viewer`, switch the whole app to read-only, and show
   a persistent banner explaining that the configured credential is read-only.
   Offer a manual "read-only mode" switch so an operator who knows they hold the
   viewer credential can set it before clicking anything.
5. Open the WebSocket (§12) and, on `stream_ready`, refresh the snapshots you
   already loaded so nothing raced the connect.

---

## 4. Response envelope and errors

Every success:

```json
{ "data": <response>, "correlation_id": "9f2c…" }
```

Every failure:

```json
{ "error": { "code": "revision_mismatch",
             "message": "The resource revision no longer matches.",
             "correlation_id": "9f2c…" } }
```

There is no partial-success shape and no stack trace, ever. `message` is a
fixed, safe sentence — show it, but drive behaviour off `code`. Always surface
the `correlation_id` in error UI (a copy button is ideal); it is how the
operator joins a failed click to the server's activity log.

### Application error codes

| Code | HTTP | Message |
| --- | --- | --- |
| `audit_unavailable` | 503 | The audit store is unavailable. |
| `bulk_failed` | 409 | The atomic bulk command was not applied. |
| `bulk_limit_exceeded` | 400 | Bulk commands support between 1 and 100 items. |
| `conflict` | 409 | The requested change conflicts with current state. |
| `discovery_busy` | 409 | A discovery run is already pending. |
| `discovery_unavailable` | 503 | Discovery cannot accept work right now. |
| `duplicate_bulk_target` | 400 | A bulk command contains a duplicate target. |
| `full_stop_active` | 409 | Entry resume is blocked while full stop is active. |
| `internal_error` | 500 | The control plane could not complete the request. |
| `invalid_decimal_string` | 400 | A financial value must be a decimal string. |
| `invalid_page` | 400 | The requested page is invalid. |
| `invalid_request` | 400 | The request is invalid. |
| `not_found` | 404 | The requested resource was not found. |
| `portfolio_stale` | 503 | The cached portfolio snapshot is stale. |
| `portfolio_unavailable` | 503 | A cached portfolio snapshot is unavailable. |
| `revision_mismatch` | 409 | The resource revision no longer matches. |
| `runtime_unavailable` | 503 | Runtime control is unavailable. |
| `settings_invalid` | 400 | The settings document is invalid. |
| `settings_path_forbidden` | 400 | The settings path is not operator-editable. |
| `settings_secret_forbidden` | 400 | Secrets cannot cross the settings boundary. |
| `settings_symlink_forbidden` | 400 | Symbolic links are forbidden for settings. |
| `settings_unavailable` | 503 | Settings storage is unavailable. |
| `task_deleted` | 409 | A deleted task cannot be changed. |
| `task_runtime_unavailable` | 503 | The task cannot be activated by the current runtime. |

### Transport error codes

| Code | HTTP | Meaning |
| --- | --- | --- |
| `authentication_required` | 401 | No Authorization header on an authenticated deployment. |
| `conflicting_bearer_credentials` | 401 | Upgrade sent both carriers without the same credential in the same bearer form (§2). |
| `http_error` | varies | Generic transport failure; the HTTP status carries the meaning. |
| `invalid_bearer_token` | 401 | Malformed or unknown bearer credential. |
| `method_not_allowed` | 405 | Endpoint exists but not for this method. |
| `not_found` | 404 | No such endpoint, or that capability is not composed. |
| `origin_not_allowed` | 403 | Origin header outside the configured CORS allowlist. |
| `payload_too_large` | 413 | Request body exceeds request_max_bytes. |
| `permission_denied` | 403 | Authenticated principal lacks this route's permission. |
| `rate_limited` | 429 | Per subject:action command budget exhausted; see Retry-After. |
| `telemetry_unavailable` | 503 | Telemetry stream unavailable or shutting down. |
| `unsupported_subprotocol` | 400 | Upgrade offered a `Sec-WebSocket-Protocol` value the endpoint will not echo (§2). |

Handling rules: `409` means someone or something else changed the resource —
re-read it, show the new state, and make the operator re-confirm; never retry
automatically. `429` carries `Retry-After` in seconds; back off and disable the
control until it elapses. `503` codes ending in `_unavailable`/`_stale` are
transient composition or freshness problems — show a "not available right now"
state and retry on the next poll, not in a tight loop.

---

## 5. Pagination

Collections return:

```json
{ "items": [...], "next_cursor": "opaque-string-or-null" }
```

- `limit` is an integer in **1..100**, default `50`. Anything else →
  `400 invalid_page`.
- `cursor` is **opaque** (max 512 characters). Never parse it, never construct
  one, never reuse a cursor from a different endpoint — cursors are typed per
  collection and a foreign cursor is rejected.
- `next_cursor: null` means the end of the collection.
- Only the endpoints listing `cursor` in §7 are paginated. `GET /accounts` and
  `GET /leaders` return plain arrays. Three other ceilings exist and none of
  them is pagination:
  - `GET /leaders` returns at most **500** rows — a query limit, with no flag.
    Disclose the cap rather than implying the list is complete.
  - `GET /pnl` renders at most **200** positions and sets `truncated: true`
    when it did, while the complete-book counters stay exact (§9.2). This is
    the only genuine truncation in the API.
  - `GET /risk` returns at most **200** `active_blocks` and the **25** most
    recent `recent_triggers`. The 200 is a query limit *and* a contract
    ceiling: the response schema refuses more than 200, so the read fails
    closed rather than silently trimming. Neither list carries a flag, so
    label them "active" and "most recent", never "all".

Use cursor pagination for infinite scroll or a "load more" button. Do not build
page-number navigation; there is no total count and no offset.

---

## 6. Rate limits

Mutating routes are throttled at **30 actions per 60 seconds per
`subject:action` bucket** — the subject being the credential's principal, and
the action being the per-route bucket named in `api_contract.json` (`task.create`,
`task.update`, `task.pause`, `task.resume`, `task.delete`, `task.duplicate`,
`task.bulk`, `discovery.run`, `lock.reset`, `settings.write`, `runtime.pause`,
`runtime.resume`, `runtime.full_stop`). Reads are not throttled.

WebSocket **client** messages share the same limiter under
`telemetry.stream.message`. Exceeding it sends `{"type":"error","code":
"rate_limited"}` **and then closes the socket**. This matters: a naive
client-side heartbeat that pings more than 30 times a minute will disconnect
itself. Do not ping more than once per 30 seconds; the server already sends
protocol-level pings on a 30-second heartbeat, so an application-level `ping`
is only needed as a liveness probe.

Client-side guidance: debounce buttons, disable a control while its request is
in flight, and never poll a mutation.

Every number in this section — and every value under `limits` in
`api_contract.json` — is a **deployment default**, not a protocol constant. The
operator can change the rate limit and window, request size, telemetry snapshot
limit, and WebSocket queue capacity in `config/control_plane.yml`. Treat the
limits as the expected shape, keep them in one configuration module, and always
prefer the `Retry-After` header over your own assumption.

---

## 7. Endpoint reference

Paths below are relative to `/api/v1`. `*` marks a required query parameter.
"Capability" is the entry that must appear in `GET /system/capabilities` for the
route to exist at all; `*always*` means it is present whenever the control plane
is enabled. Several handlers are published under two paths — they are the same
handler, so pick one spelling and stay consistent.

| Method | Path | Permission | Capability | Query | Body | Response |
| --- | --- | --- | --- | --- | --- | --- |
| GET | `/accounts` | `system.read` | `account_read` | — | — | `AccountView[]` |
| GET | `/activity` | `activity.read` | `activity_read` | `follower_account`, `limit`, `cursor` | — | `Page<ActivityView>` |
| GET | `/capabilities` | `system.read` | *always* | — | — | `CapabilitiesView` |
| GET | `/detection` | `runtime.read` | `detection_read` | — | — | `DetectionStatusView` |
| GET | `/discovery/candidates` | `discovery.read` | `discovery_read` | `limit`, `cursor` | — | `Page<DiscoveryCandidateView>` |
| GET | `/discovery/cycles` | `discovery.read` | `discovery_read` | `limit`, `cursor` | — | `Page<DiscoveryCycleView>` |
| GET | `/discovery/roster` | `discovery.read` | `discovery_read` | `limit`, `cursor` | — | `Page<DiscoveryRosterView>` |
| POST | `/discovery/run` | `discovery.run` | `discovery_manual_run` | — | `force` | `DiscoveryRunReceipt` · 202 |
| GET | `/discovery/status` | `discovery.read` | `discovery_read` | — | — | `DiscoveryStatusView` |
| GET | `/fills` | `fill.read` | `fill_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<FillView>` |
| GET | `/leaders` | `leader.read` | `leader_read` | — | — | `LeaderView[]` |
| GET | `/locks` | `lock.read` | `reentry_lock_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<ReentryLockView>` |
| POST | `/locks/reset` | `lock.reset` | `reentry_lock_reset` | — | `follower_account`, `task_id`, `leader_wallet`, `market_id`, `expected_revision`, `reason_code` | `LockResetResult` |
| GET | `/metrics/snapshot` | `telemetry.read` | `metrics_snapshot` | — | — | `MetricsSnapshot` |
| GET | `/orders` | `order.read` | `order_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<OrderView>` |
| GET | `/orders/{order_id}` | `order.read` | `order_read` | `follower_account`\* | — | `OrderView` |
| GET | `/pnl` | `portfolio.read` | `pnl_read` | `follower_account`\* | — | `PnlView` |
| GET | `/portfolio` | `portfolio.read` | `portfolio_read` | `follower_account`\* | — | `PortfolioView` |
| GET | `/positions` | `position.read` | `position_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<PositionView>` |
| GET | `/positions/{position_id}` | `position.read` | `position_read` | `follower_account`\* | — | `PositionView` |
| GET | `/readiness` | `system.read` | *always* | — | — | `ReadinessView` |
| GET | `/reentry-locks` | `lock.read` | `reentry_lock_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<ReentryLockView>` |
| POST | `/reentry-locks/{lock_id}/reset` | `lock.reset` | `reentry_lock_reset` | — | `follower_account`, `task_id`, `leader_wallet`, `market_id`, `expected_revision`, `reason_code` | `LockResetResult` |
| GET | `/reservations` | `reservation.read` | `reservation_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<ReservationView>` |
| GET | `/risk` | `risk.read` | `risk_read` | `follower_account`\* | — | `RiskStateView` |
| GET | `/runtime` | `runtime.read` | `runtime_read` | — | — | `RuntimeView` |
| POST | `/runtime/full-stop` | `runtime.write` | `runtime_full_stop` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| POST | `/runtime/pause` | `runtime.write` | `runtime_pause_entries` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| POST | `/runtime/resume` | `runtime.write` | `runtime_resume_entries` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| GET | `/runtime/safety` | `runtime.read` | `runtime_read` | — | — | `RuntimeView` |
| POST | `/runtime/safety/full-stop` | `runtime.write` | `runtime_full_stop` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| POST | `/runtime/safety/pause-entries` | `runtime.write` | `runtime_pause_entries` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| POST | `/runtime/safety/resume-entries` | `runtime.write` | `runtime_resume_entries` | — | `expected_revision`, `reason_code` | `EntryControlState` |
| GET | `/settings` | `settings.read` | `settings_read` | — | — | `SettingsDocument[]` |
| GET | `/settings/{environment}/{section}` | `settings.read` | `settings_read` | — | — | `SettingsDocument` |
| PUT | `/settings/{environment}/{section}` | `settings.write` | `settings_write` | — | `values`, `expected_revision` | `SettingsWriteResult` |
| GET | `/settings/{section}` | `settings.read` | `settings_read` | — | — | `SettingsDocument` |
| PUT | `/settings/{section}` | `settings.write` | `settings_write` | — | `values`, `expected_revision` | `SettingsWriteResult` |
| GET | `/status` | `system.read` | *always* | — | — | `SystemStatusView` |
| GET | `/stream` | `telemetry.stream` | `live_stream` | — | — | `WebSocket` |
| GET | `/system` | `system.read` | *always* | — | — | `SystemView` |
| GET | `/system/capabilities` | `system.read` | *always* | — | — | `CapabilitiesView` |
| GET | `/system/readiness` | `system.read` | *always* | — | — | `ReadinessView` |
| GET | `/system/status` | `system.read` | *always* | — | — | `SystemStatusView` |
| GET | `/tasks` | `task.read` | `task_read` | `follower_account`\*, `limit`, `cursor` | — | `Page<TaskView>` |
| POST | `/tasks` | `task.write` | `task_create` | — | `task` | `TaskView` · 201 |
| POST | `/tasks/bulk` | `task.write` | `task_bulk` | — | `items` | `BulkTaskResult` |
| DELETE | `/tasks/{task_id}` | `task.write` | `task_delete` | — | `expected_version` | `TaskView` |
| GET | `/tasks/{task_id}` | `task.read` | `task_read` | — | — | `TaskDetailView` |
| PATCH | `/tasks/{task_id}` | `task.write` | `task_update` | — | `task`, `expected_version` | `TaskView` |
| PUT | `/tasks/{task_id}` | `task.write` | `task_update` | — | `task`, `expected_version` | `TaskView` |
| POST | `/tasks/{task_id}/duplicate` | `task.write` | `task_duplicate` | — | `new_task_id`, `new_name` | `TaskView` · 201 |
| POST | `/tasks/{task_id}/pause` | `task.write` | `task_pause` | — | `expected_version` | `TaskView` |
| POST | `/tasks/{task_id}/resume` | `task.write` | `task_resume` | — | `expected_version` | `TaskView` |
| GET | `/telemetry` | `telemetry.read` | `telemetry_snapshot` | `limit` | — | `TelemetrySnapshot` |
| GET | `/telemetry/snapshot` | `telemetry.read` | `telemetry_snapshot` | `limit` | — | `TelemetrySnapshot` |

Notes:

- `follower_account` is required, not optional, wherever marked. Omitting it is
  `400 invalid_request`, not "all accounts".
- `/fills`, `/pnl`, and `/risk` return `404 not_found` for a follower account
  this process did not compose. Only offer accounts from `GET /accounts`.
- `GET /telemetry` takes `limit` in **0..1000** (default 100) — a different
  range from collection pagination, and no cursor.
- `GET /portfolio` reads a background cache and never calls the venue. Before
  the first refresh succeeds, or once the snapshot is stale, it returns
  `503 portfolio_unavailable` / `503 portfolio_stale` rather than inventing
  balances. Render that as an explicit "waiting for first snapshot" state.

---

## 8. Request bodies

All mutating routes take a JSON object. Concurrency guards come in two flavours
and are **not** interchangeable:

- `expected_version` — a **positive integer**, used by task routes; comes from
  `TaskView.version`.
- `expected_revision` — a **positive integer** for runtime and lock routes
  (from `EntryControlState.revision` / `ReentryLockView.revision`), and a
  **64-character lowercase SHA-256 hex string** for settings (from
  `SettingsDocument.revision`).

| Route | Body |
| --- | --- |
| `POST /tasks` | `{"task": {…task configuration…}}` (the object may also be sent unwrapped) |
| `PUT`/`PATCH` `/tasks/{task_id}` | `{"task": {…complete configuration…}, "expected_version": 7}` — `task.task_id` must equal the path id; `PATCH` is **not** a partial update |
| `DELETE /tasks/{task_id}` | `{"expected_version": 7}` — yes, `DELETE` requires a body |
| `POST /tasks/{task_id}/pause` `…/resume` | `{"expected_version": 7}` |
| `POST /tasks/{task_id}/duplicate` | `{"new_task_id": "…", "new_name": "…"}` → `201`, created paused |
| `POST /tasks/bulk` | `{"items": [{"task_id": "…", "action": "pause"\|"resume"\|"delete", "expected_version": 7}, …]}` — 1..100 items, all-or-none |
| `POST /discovery/run` | `{"force": false}` (body optional) → `202` with a receipt; at most one cycle is queued |
| `POST /locks/reset`, `POST /reentry-locks/{lock_id}/reset` | `{"follower_account","task_id","leader_wallet","market_id","expected_revision":N,"reason_code":"…"}` — on the `{lock_id}` spelling the path segment must equal `ReentryLockView.lock_id` for that identity |
| `PUT /settings/{section}`, `PUT /settings/{environment}/{section}` | `{"values": {…}, "expected_revision": "<64 hex chars>"}` — replaces the whole section |
| `POST /runtime/pause`, `…/resume`, `…/full-stop` | `{"expected_revision": N, "reason_code": "…"}` |

`reason_code` is required free text (1..512 characters, trimmed). Offer a short
select of operator-meaningful codes plus a free-text option; it is written to
the audit trail and surfaces in `GET /activity`.

There is deliberately **no** endpoint to place, modify, or cancel an individual
order, and none to move funds. Do not build one, and do not fake one.

---

## 9. Response schemas

Types: `string`, `integer`, `number`, `boolean`, `timestamp` (ISO-8601 UTC,
§13), `object` (free-form JSON), `array<X>`, or a named schema below.
`Page<X>` is `{items: array<X>, next_cursor: string|null}`. `X[]` is a bare JSON
array. A field marked **decimal string** is a `string` that holds an exact
decimal — see non-negotiable 1.

### 9.1 Semantics you must encode in the UI

- `SystemView.mode` is `"paper"` or `"live"`. Paper means simulated execution.
  Badge it prominently and permanently; an operator must never be unsure which
  one they are watching.
- `ReadinessView.ready` is the single "is the bot working" boolean;
  `failed_checks` names what is wrong. `live` is process liveness only — a
  process can be `live: true, ready: false`.
- Several fields are typed `string` on the wire but carry a **closed
  vocabulary**, listed in the Vocabulary column of the §9.3 schema tables and
  published by `api_contract.json` both per field and under `enums`:
  `TaskView.status` (`active` | `paused` | `deleted`),
  `TaskView.monitoring_mode`, `TaskView.sizing_mode`, `TaskSizingView.mode`,
  `TaskPriceView.offset_mode`, `TaskPriceView.offset_direction`,
  `TaskReactionView.copy_side`, and `PnlView.unrealized_basis`. Generate a
  union type from those lists rather than a bare `string`, but still fail soft
  on an unrecognised value.
- `OrderView.status` and `DiscoveryCycleView.status` are open vocabularies —
  render them verbatim, do not switch on a guessed list, and do not colour-code
  an unknown value as an error.
- `OrderView.amount` is USDC **spend** for a BUY and outcome **shares** for a
  SELL. Never label the column "size" without the side, and never add BUY and
  SELL amounts together.
- `OrderView.price_limit` may be `null`; `matched_total` is always present.
  An unknown average fill price is reported as unknown — the backend
  deliberately never substitutes the limit price, so neither may you.
- `PortfolioView.valuation_basis` is `cost_basis` or `market_mark`; it is
  validated server-side but has no named vocabulary in the contract.
  `cost_basis` means no market mark was available and `unrealized_pnl_usdc` is
  `null`.
- `DetectionStatusView.pending_confirmations` is `null` unless the selected
  detection mode composed a provisional-copy confirmation tracker; that is a
  configuration fact, not an outage.
- `RiskHeadroomView` is an **upper bound**. It applies only the account cap,
  reserve, and daily-drawdown gates; per-market, per-category and per-leader
  exposure are unknown to the cached snapshot the projection reads. Label it as
  an estimate, never as "you may spend exactly this".
- `RiskStateView.headroom` is `null` when no fresh cached portfolio snapshot
  exists; `day_start_equity_usdc` is `null` until a baseline is recorded, and
  reading it never records one.
- `LeaderView.wallet` is lower-cased; `configured` means it is in the operator's
  roster, `monitored` means the running process is currently watching it. Both
  false with non-zero counters means a leader that used to be watched.
- `FillView.filled_at` is nullable (the durable row's timestamp is optional);
  `created_at` never is. Sort by `created_at` when `filled_at` is missing.
- `ReentryLockView.lock_id` is a server-derived opaque id; never compute it.
- `LockResetResult.outcome` is `reset` | `not_found` | `revision_mismatch`, and
  `released_at` is `null` for the two failure outcomes. A `not_found` here is a
  `200` body, not an HTTP `404` — read the outcome, do not rely on the status.
- `BulkTaskItem.action` in a bulk request is `pause` | `resume` | `delete`.

### 9.2 `unrealized_basis` on `GET /pnl`

| Value | Meaning | What you may display |
| --- | --- | --- |
| `wss_mid` | every open position was marked from a fresh two-sided cached quote | the account `unrealized_pnl_usdc`, and every per-position mark |
| `partial_wss_mid` | only some positions could be marked | per-position marks only; the account total is `null` — show "partial" and the marked/total counts |
| `truncated` | the open book is larger than the rendered ceiling (`truncated: true`) | per-position marks for the rendered rows only; the account total is `null` because you cannot reconcile it against what is on screen |
| `unavailable` | nothing could be marked | cost basis only; say so |

`position_count`, `cost_basis_usdc`, and `marked_position_count` always describe
the **complete** open book even when `positions` is truncated. Use them for
headline counts; use `positions` only for the rows you render.

### 9.3 Schemas

#### `AccountView`

One composed follower account and the leaders bound to it.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `wallet_address` | `string` | no | — | — |
| `leader_count` | `integer` | no | — | — |

#### `ActivityView`

Append-only operator activity projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `activity_id` | `string` | no | — | — |
| `actor` | `string` | no | — | — |
| `action` | `string` | no | — | — |
| `target_type` | `string` | no | — | — |
| `target_id` | `string` | no | — | — |
| `outcome` | `string` | no | — | — |
| `reason_code` | `string` | no | — | — |
| `occurred_at` | `timestamp` | no | — | — |

#### `BulkTaskResult`

Successful atomic task mutation result.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `tasks` | `array<TaskView>` | no | — | — |
| `atomic` | `boolean` | no | — | — |

#### `CapabilitiesView`

Explicitly composed control-plane capabilities.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `capabilities` | `array<string>` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |

#### `DetectionSourceStatusView`

Local signal counters for exactly one detection source.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `source` | `string` | no | — | — |
| `signals_total` | `integer` | no | — | — |
| `deduplicated_total` | `integer` | no | — | — |
| `last_signal_at` | `timestamp` | yes | — | — |

#### `DetectionStatusView`

Detection-source health assembled from local counters and readiness.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `mode` | `string` | no | — | — |
| `sources` | `array<DetectionSourceStatusView>` | no | — | — |
| `leader_ws_connected` | `boolean` | yes | — | — |
| `market_ws_ok` | `boolean` | no | — | — |
| `user_ws_ok` | `boolean` | no | — | — |
| `market_ws_reconnects` | `integer` | no | — | — |
| `user_ws_reconnects` | `integer` | no | — | — |
| `pending_confirmations` | `PendingConfirmationStatsView` | yes | — | — |
| `watched_leader_count` | `integer` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |

#### `DiscoveryCandidateView`

Provider-safe candidate score projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `wallet` | `string` | no | — | — |
| `score` | `string` | no | **yes** | — |
| `accepted` | `boolean` | no | — | — |
| `reason_code` | `string` | no | — | — |
| `cycle_id` | `string` | no | — | — |

#### `DiscoveryCycleView`

Discovery-cycle state without provider payloads or raw errors.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `cycle_id` | `string` | no | — | — |
| `status` | `string` | no | — | — |
| `started_at` | `timestamp` | no | — | — |
| `completed_at` | `timestamp` | yes | — | — |
| `candidate_count` | `integer` | no | — | — |
| `kept_count` | `integer` | no | — | — |
| `config_revision` | `string` | no | — | — |

#### `DiscoveryRosterView`

Active or inactive discovery roster projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `wallet` | `string` | no | — | — |
| `tier` | `string` | no | — | — |
| `active` | `boolean` | no | — | — |
| `score` | `string` | no | **yes** | — |
| `updated_at` | `timestamp` | no | — | — |

#### `DiscoveryRunReceipt`

Immediate queue acknowledgement for a discovery request.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `run_id` | `string` | no | — | — |
| `accepted` | `boolean` | no | — | — |
| `reason_code` | `string` | no | — | — |

#### `DiscoveryStatusView`

Current discovery scheduler state without provider error material.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `state` | `string` | no | — | — |
| `current_run_id` | `string` | yes | — | — |
| `last_cycle_id` | `string` | yes | — | — |
| `observed_at` | `timestamp` | no | — | — |

#### `EntryControlState`

Durable global entry-pause and full-stop state.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `entries_paused` | `boolean` | no | — | — |
| `full_stop` | `boolean` | no | — | — |
| `revision` | `integer` | no | — | — |
| `reason_code` | `string` | no | — | — |
| `updated_at` | `timestamp` | no | — | — |
| `updated_by` | `string` | no | — | — |

#### `FillView`

One durable fill projection using exact decimal strings.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `fill_id` | `string` | no | — | — |
| `follower_account` | `string` | no | — | — |
| `leader_wallet` | `string` | yes | — | — |
| `order_id` | `string` | yes | — | — |
| `condition_id` | `string` | yes | — | — |
| `token_id` | `string` | yes | — | — |
| `side` | `string` | yes | — | — |
| `size` | `string` | no | **yes** | — |
| `price` | `string` | no | **yes** | — |
| `fee` | `string` | no | **yes** | — |
| `filled_at` | `timestamp` | yes | — | — |
| `created_at` | `timestamp` | no | — | — |

#### `LeaderView`

One leader identity merged from configuration, roster, and counters.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `wallet` | `string` | no | — | — |
| `configured` | `boolean` | no | — | — |
| `monitored` | `boolean` | no | — | — |
| `tier` | `string` | yes | — | — |
| `score` | `string` | yes | **yes** | — |
| `signals_total` | `integer` | no | — | — |
| `deduplicated_total` | `integer` | no | — | — |
| `last_signal_at` | `timestamp` | yes | — | — |
| `trade_count` | `integer` | no | — | — |
| `open_position_count` | `integer` | no | — | — |

#### `LockResetResult`

Result of a lock reset attempt.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `outcome` | `LockResetOutcome` | no | — | — |
| `released_at` | `timestamp` | yes | — | — |

#### `OrderView`

Sanitized order projection without raw request or response payloads.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `idempotency_key` | `string` | no | — | — |
| `follower_account` | `string` | no | — | — |
| `leader_wallet` | `string` | yes | — | — |
| `order_id` | `string` | yes | — | — |
| `status` | `string` | no | — | — |
| `side` | `string` | yes | — | — |
| `amount` | `string` | yes | **yes** | — |
| `price_limit` | `string` | yes | **yes** | — |
| `matched_total` | `string` | no | **yes** | — |
| `created_at` | `timestamp` | no | — | — |
| `updated_at` | `timestamp` | no | — | — |

#### `Page`

Immutable query page with an optional opaque continuation cursor.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `items` | `array<T>` | no | — | — |
| `next_cursor` | `string` | yes | — | — |

#### `PendingConfirmationStatsView`

Bounded state of the provisional-copy confirmation tracker.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `tracked` | `integer` | no | — | — |
| `running` | `boolean` | no | — | — |
| `deadline_seconds` | `number` | no | — | — |
| `max_tracked` | `integer` | no | — | — |

#### `PnlPositionView`

One open position valued only from a fresh cached WSS mid quote.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `condition_id` | `string` | no | — | — |
| `token_id` | `string` | no | — | — |
| `leader_wallet` | `string` | yes | — | — |
| `size` | `string` | no | **yes** | — |
| `avg_entry_price` | `string` | no | **yes** | — |
| `cost_basis_usdc` | `string` | no | **yes** | — |
| `mark_price` | `string` | yes | **yes** | — |
| `market_value_usdc` | `string` | yes | **yes** | — |
| `unrealized_pnl_usdc` | `string` | yes | **yes** | — |

#### `PnlSummaryView`

Session-scoped realized ledger metrics for one follower account.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `realized_pnl_usdc` | `string` | no | **yes** | — |
| `total_invested_usdc` | `string` | no | **yes** | — |
| `total_returned_usdc` | `string` | no | **yes** | — |
| `max_drawdown_usdc` | `string` | no | **yes** | — |
| `trade_count` | `integer` | no | — | — |
| `buy_count` | `integer` | no | — | — |
| `sell_count` | `integer` | no | — | — |
| `win_count` | `integer` | no | — | — |
| `loss_count` | `integer` | no | — | — |
| `session_started_at` | `timestamp` | no | — | — |

#### `PnlView`

Realized session PnL paired with honestly marked open positions.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `realized` | `PnlSummaryView` | no | — | — |
| `positions` | `array<PnlPositionView>` | no | — | — |
| `position_count` | `integer` | no | — | — |
| `truncated` | `boolean` | no | — | — |
| `cost_basis_usdc` | `string` | no | **yes** | — |
| `unrealized_pnl_usdc` | `string` | yes | **yes** | — |
| `unrealized_basis` | `string` | no | — | `wss_mid`, `partial_wss_mid`, `truncated`, `unavailable` |
| `marked_position_count` | `integer` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |

#### `PortfolioView`

Cached portfolio snapshot using exact decimal strings.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `cash_usdc` | `string` | no | **yes** | — |
| `reserved_usdc` | `string` | no | **yes** | — |
| `positions_value_usdc` | `string` | no | **yes** | — |
| `equity_usdc` | `string` | no | **yes** | — |
| `observed_at` | `timestamp` | no | — | — |
| `stale` | `boolean` | no | — | — |
| `copyable_balance_usdc` | `string` | no | **yes** | — |
| `position_count` | `integer` | no | — | — |
| `valuation_basis` | `string` | no | — | — |
| `unrealized_pnl_usdc` | `string` | yes | **yes** | — |

#### `PositionView`

Open-position projection with decimal strings.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `condition_id` | `string` | no | — | — |
| `token_id` | `string` | no | — | — |
| `leader_wallet` | `string` | yes | — | — |
| `size` | `string` | no | **yes** | — |
| `avg_entry_price` | `string` | no | **yes** | — |
| `updated_at` | `timestamp` | no | — | — |

#### `ReadinessView`

Authoritative liveness/readiness projection from the runtime.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `live` | `boolean` | no | — | — |
| `ready` | `boolean` | no | — | — |
| `failed_checks` | `array<string>` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |
| `circuit_breaker_state` | `string` | no | — | — |
| `wss_market_ok` | `boolean` | no | — | — |
| `wss_user_ok` | `boolean` | no | — | — |
| `reconciliation_ok` | `boolean` | no | — | — |
| `detection_ok` | `boolean` | no | — | — |
| `discovery_ok` | `boolean` | no | — | — |
| `copy_workers_ok` | `boolean` | no | — | — |
| `control_plane_ok` | `boolean` | no | — | — |
| `telemetry_ok` | `boolean` | no | — | — |
| `active_leader_count` | `integer` | no | — | — |
| `active_task_count` | `integer` | no | — | — |
| `pending_intents` | `integer` | no | — | — |
| `reservations` | `integer` | no | — | — |
| `stale_locks` | `integer` | no | — | — |
| `last_reconciliation` | `timestamp` | yes | — | — |
| `last_source_event` | `timestamp` | yes | — | — |

#### `ReentryLockView`

Protective-exit lock projection carrying a CAS revision.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `task_id` | `string` | no | — | — |
| `leader_wallet` | `string` | no | — | — |
| `market_id` | `string` | no | — | — |
| `reason` | `string` | no | — | — |
| `fill_id` | `string` | no | — | — |
| `locked_at` | `timestamp` | no | — | — |
| `revision` | `integer` | no | — | — |
| `lock_id` | `string` | no | — | — |

#### `ReservationView`

Task-budget reservation projection with exact notional text.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `reservation_id` | `string` | no | — | — |
| `follower_account` | `string` | no | — | — |
| `task_id` | `string` | no | — | — |
| `market_id` | `string` | no | — | — |
| `amount` | `string` | no | **yes** | — |
| `status` | `string` | no | — | — |
| `reserved_at` | `timestamp` | no | — | — |
| `expires_at` | `timestamp` | no | — | — |

#### `RiskBlockView`

One active durable risk block on a follower position.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `condition_id` | `string` | no | — | — |
| `token_id` | `string` | no | — | — |
| `reason` | `string` | no | — | — |
| `source` | `string` | no | — | — |
| `leader_wallet` | `string` | yes | — | — |
| `triggered_at` | `timestamp` | no | — | — |
| `expires_at` | `timestamp` | yes | — | — |

#### `RiskHeadroomView`

Account-level BUY headroom recomputed from one cached snapshot.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `allowed` | `boolean` | no | — | — |
| `reason` | `string` | no | — | — |
| `policy_name` | `string` | no | — | — |
| `cap_notional_usdc` | `string` | no | **yes** | — |
| `equity_usdc` | `string` | no | **yes** | — |
| `free_cash_usdc` | `string` | no | **yes** | — |
| `daily_loss_usdc` | `string` | no | **yes** | — |
| `open_position_count` | `integer` | no | — | — |

#### `RiskPolicyView`

Effective account-level risk limits as configured for this process.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `name` | `string` | no | — | — |
| `risk_management_enabled` | `boolean` | no | — | — |
| `tiered_policy_enabled` | `boolean` | no | — | — |
| `kill_switch` | `boolean` | no | — | — |
| `max_open_positions` | `integer` | no | — | — |
| `max_market_exposure_usdc` | `string` | no | **yes** | — |
| `max_category_exposure_usdc` | `string` | no | **yes** | — |
| `max_leader_exposure_usdc` | `string` | no | **yes** | — |
| `max_daily_drawdown_pct` | `string` | no | **yes** | — |
| `reserve_pct` | `string` | no | **yes** | — |
| `reserve_floor_usdc` | `string` | no | **yes** | — |
| `hard_cap_pct` | `string` | no | **yes** | — |
| `hard_cap_ceiling_usdc` | `string` | no | **yes** | — |
| `allow_partial_exits` | `boolean` | no | — | — |
| `allow_pyramiding` | `boolean` | no | — | — |

#### `RiskStateView`

Configured policy, active blocks, recent triggers, and headroom.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `follower_account` | `string` | no | — | — |
| `policy` | `RiskPolicyView` | no | — | — |
| `active_blocks` | `array<RiskBlockView>` | no | — | — |
| `recent_triggers` | `array<RiskTriggerView>` | no | — | — |
| `headroom` | `RiskHeadroomView` | yes | — | — |
| `day_start_equity_usdc` | `string` | yes | **yes** | — |
| `observed_at` | `timestamp` | no | — | — |

#### `RiskTriggerView`

One recorded protective-exit trigger and its fill status.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `trigger_id` | `string` | no | — | — |
| `follower_account` | `string` | no | — | — |
| `condition_id` | `string` | no | — | — |
| `token_id` | `string` | no | — | — |
| `reason` | `string` | no | — | — |
| `source` | `string` | no | — | — |
| `fill_status` | `string` | no | — | — |
| `fill_order_id` | `string` | yes | — | — |
| `triggered_at` | `timestamp` | no | — | — |

#### `RuntimeView`

Bounded local runtime state.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `entry_control` | `EntryControlState` | no | — | — |
| `worker_count` | `integer` | no | — | — |
| `queue_depth` | `integer` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |

#### `SettingsApplyMetadata`

Truthful write/apply metadata for restart-required settings.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `previous_revision` | `string` | no | — | — |
| `revision` | `string` | no | — | — |
| `applied_at` | `timestamp` | no | — | — |
| `applied_by` | `string` | no | — | — |
| `restart_required` | `boolean` | no | — | — |
| `live_applied` | `boolean` | no | — | — |

#### `SettingsDocument`

Secret-free immutable settings document and content revision.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `path` | `SettingsPath` | no | — | — |
| `values` | `object` | no | — | — |
| `revision` | `string` | no | — | — |

#### `SettingsPath`

Allowlisted environment and service-section identity.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `environment` | `string` | no | — | — |
| `section` | `string` | no | — | — |

#### `SettingsWriteResult`

Secret-free written document and truthful apply metadata.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `document` | `SettingsDocument` | no | — | — |
| `metadata` | `SettingsApplyMetadata` | no | — | — |

#### `SystemStatusView`

System identity paired with authoritative readiness state.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `system` | `SystemView` | no | — | — |
| `readiness` | `ReadinessView` | no | — | — |

#### `SystemView`

Process identity and environment snapshot.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `service` | `string` | no | — | — |
| `version` | `string` | no | — | — |
| `environment` | `string` | no | — | — |
| `observed_at` | `timestamp` | no | — | — |
| `mode` | `string` | no | — | — |
| `backend` | `string` | no | — | — |
| `uptime_seconds` | `number` | no | — | — |

#### `TaskConfigurationView`

Complete typed configuration projection for one copy task.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `trigger` | `TaskTriggerView` | no | — | — |
| `spend` | `TaskSpendView` | no | — | — |
| `price` | `TaskPriceView` | no | — | — |
| `reaction` | `TaskReactionView` | no | — | — |
| `market_filter` | `TaskMarketFilterView` | no | — | — |
| `schedule` | `TaskScheduleView` | no | — | — |
| `sizing` | `TaskSizingView` | no | — | — |

#### `TaskDetailView`

Task summary and complete typed configuration projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `task` | `TaskView` | no | — | — |
| `configuration` | `TaskConfigurationView` | no | — | — |

#### `TaskMarketFilterView`

Task metadata-filter projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `include_keywords` | `array<string>` | no | — | — |
| `exclude_keywords` | `array<string>` | no | — | — |
| `categories` | `array<string>` | no | — | — |
| `tokens` | `array<string>` | no | — | — |
| `timeframes` | `array<string>` | no | — | — |
| `min_liquidity_usdc` | `string` | no | **yes** | — |
| `min_volume_24h_usdc` | `string` | no | **yes** | — |
| `max_days_until_close` | `string` | no | **yes** | — |
| `allow_expired_unresolved` | `boolean` | no | — | — |

#### `TaskPriceView`

Exact task source-price and execution-protection projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `min_price` | `string` | no | **yes** | — |
| `max_price` | `string` | no | **yes** | — |
| `offset_mode` | `string` | no | — | `disabled`, `fixed`, `percentage` |
| `offset_direction` | `string` | no | — | `above`, `below` |
| `offset` | `string` | no | **yes** | — |
| `max_slippage_percent` | `string` | no | **yes** | — |

#### `TaskReactionView`

Task source-action reaction policy projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `follow_buys` | `boolean` | no | — | — |
| `copy_sells` | `boolean` | no | — | — |
| `buy_on_sells` | `boolean` | no | — | — |
| `counter_trade` | `boolean` | no | — | — |
| `copy_side` | `string` | no | — | `both`, `yes_only`, `no_only` |
| `copy_merges` | `boolean` | no | — | — |
| `copy_splits` | `boolean` | no | — | — |
| `copy_conversions` | `boolean` | no | — | — |

#### `TaskScheduleView`

Task schedule projection using IANA timezone and ISO local times.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `timezone_name` | `string` | no | — | — |
| `start` | `string` | no | — | — |
| `end` | `string` | no | — | — |

#### `TaskSizingView`

Exact per-task sizing policy projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `mode` | `string` | no | — | `fixed`, `exact_mirror`, `proportional`, `balance_weighted`, `portfolio_weighted`, `adaptive`, `tiered` |
| `percentage` | `string` | no | **yes** | — |
| `fixed_amount` | `string` | no | **yes** | — |
| `trade_multiplier` | `string` | no | **yes** | — |
| `weighted_multiplier` | `string` | no | **yes** | — |
| `buy_min_amount` | `string` | no | **yes** | — |
| `buy_max_amount` | `string` | no | **yes** | — |
| `copyable_balance_percent` | `string` | no | **yes** | — |
| `proportional_balance_cap_enabled` | `boolean` | no | — | — |
| `require_leader_balance` | `boolean` | no | — | — |
| `strict_fixed_amount` | `boolean` | no | — | — |
| `strict_exact_mirror` | `boolean` | no | — | — |

#### `TaskSpendView`

Exact task spend and duplicate-entry policy projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `max_total` | `string` | no | **yes** | — |
| `max_daily` | `string` | no | **yes** | — |
| `max_per_market` | `string` | no | **yes** | — |
| `one_entry_per_market` | `boolean` | no | — | — |
| `global_one_entry_per_market` | `boolean` | no | — | — |
| `one_entry_per_sports_type` | `boolean` | no | — | — |

#### `TaskTriggerView`

Exact task source-notional eligibility projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `enabled` | `boolean` | no | — | — |
| `min_notional` | `string` | no | **yes** | — |
| `max_notional` | `string` | no | **yes** | — |

#### `TaskView`

Safe task identity, lifecycle, and revision projection.

| Field | Type | Nullable | Decimal string | Vocabulary |
| --- | --- | --- | --- | --- |
| `task_id` | `string` | no | — | — |
| `name` | `string` | no | — | — |
| `leader_wallet` | `string` | no | — | — |
| `follower_account` | `string` | no | — | — |
| `status` | `string` | no | — | `active`, `paused`, `deleted` |
| `monitoring_mode` | `string` | no | — | `normal`, `priority` |
| `sizing_mode` | `string` | no | — | `fixed`, `exact_mirror`, `proportional`, `balance_weighted`, `portfolio_weighted`, `adaptive`, `tiered` |
| `version` | `integer` | no | — | — |
| `notifications_muted` | `boolean` | no | — | — |

### 9.4 Telemetry and metrics snapshots

`GET /telemetry` and `GET /telemetry/snapshot` return a plain object, not a
typed view:

```json
{ "generated_at": "2026-08-18T14:00:00+00:00",
  "events": [ <TelemetryEvent>, … ],
  "metrics": { "counters": {…}, "gauges": {…}, "histograms": […] } }
```

`GET /metrics/snapshot` returns the same numbers without `events`:

```json
{ "generated_at": "…", "counters": {…}, "gauges": {…}, "histograms": […] }
```

`counters` and `gauges` are `{"<metric_key>": number}` where the key is a
Prometheus-style string that may embed labels, e.g.
`wss_reconnect_total{stream="market"}`. Treat keys as opaque, group by the part
before `{`, and never assume a key exists. `histograms` is an array of
`{name, labels, count, sum, buckets:[{le, count}], p50, p95, p99}`.

Metric *values* are JSON numbers, not decimal strings — they are counts and
latencies, never money. Everything monetary is a decimal string on a typed view.

---

## 10. Settings

`GET /settings` lists every operator-editable section with its deterministic
`revision`. `PUT` replaces one whole section:

1. `GET` the section, keep `revision`.
2. Send `{"values": {…the complete section…}, "expected_revision": revision}`.
3. `200` → the response carries the new `SettingsDocument` plus
   `SettingsApplyMetadata` with `restart_required: true` and
   `live_applied: false`. Show "saved — takes effect after restart" and keep a
   persistent banner listing pending-restart sections until the operator
   dismisses it or the process restarts.
4. `409 revision_mismatch` → someone else wrote first. Re-`GET`, show a diff of
   what changed, and make the operator re-confirm. Never force the write.

Wallet keys, bearer credentials, database passwords, RPC credentials, and
provider secrets are excluded from the settings surface entirely — including the
whole `control_plane.auth` object. If a key is not in the `GET` response, it
cannot be written; do not add free-form key entry to the form.

---

## 11. Runtime safety controls

These exist only in a live composition (capabilities `runtime_pause_entries`,
`runtime_resume_entries`, `runtime_full_stop`). In paper mode the routes are not
registered and the capabilities are absent — hide the whole control group.

| Control | Effect | Confirmation copy must say |
| --- | --- | --- |
| Pause entries | blocks new entries; **protective exits keep running** | "No new positions will be opened. Existing stop-loss and take-profit exits continue." |
| Resume entries | re-enables entries through the durable live gate | "Entries resume immediately." Cannot override a full stop — a resume attempt while full-stop is active returns `409 full_stop_active`. |
| Full stop | halts entries **and** protective exits, then makes a best-effort pass to cancel resting orders | "Everything stops, including protective exits, and resting orders are cancelled. Positions are NOT closed." |

Each takes `expected_revision` from `RuntimeView.entry_control.revision` and a
`reason_code`. After a `409 revision_mismatch`, re-read `GET /runtime` and show
the current state before letting the operator try again. Full stop deserves a
type-to-confirm dialog, not a plain OK/Cancel.

`EntryControlState` invariant: `full_stop: true` always implies
`entries_paused: true`. Render one tri-state (running / entries paused /
full stop), not two independent toggles.

---

## 12. Live stream

Upgrade `GET /api/v1/stream` with the same bearer credential, carried either by
the `Authorization` header or by the `["bearer", credential]` subprotocol a
browser must use (§2). Requires the `telemetry.stream` permission, which the
viewer credential holds. Text frames only — a binary frame closes the socket. The server
heartbeats every **30 seconds**.

**Message size is capped in one direction only.** A **client-to-server** frame
must stay under **4096 bytes**: that is the inbound reader's limit, it rejects a
frame whose projected size reaches it, and the socket then closes. Both legal
client frames are a handful of bytes, so this is only a hazard if you invent
one. **Server-to-client frames are not bounded by that number** — a
`telemetry_snapshot` carrying up to 100 events is routinely far larger. Do not
size a receive buffer, a validator, or a "message too large" check on 4096.

### Server → client frames

| `type` | Payload | Do |
| --- | --- | --- |
| `stream_ready` | — | connection accepted; refresh REST snapshots |
| `telemetry_event` | `data`: a `TelemetryEvent` (§13) | append to the feed, mark affected screens stale |
| `telemetry_snapshot` | `data`: a telemetry snapshot object, or `null` | replace the feed contents |
| `resync_required` | `dropped_count`: integer | **you missed events** — a `telemetry_snapshot` follows; also re-read every REST screen you are showing |
| `pong` | — | answer to your `ping` |
| `error` | `code`: `invalid_stream_message` \| `unsupported_stream_message` \| `rate_limited`; the `rate_limited` variant also carries `retry_after_seconds` (integer) | the first two are recoverable and the socket stays open; `rate_limited` is immediately followed by a close, so wait `retry_after_seconds` before reconnecting |

### Client → server frames

Only two, both plain JSON text: `{"type":"ping"}` and `{"type":"resync"}`.
Anything else returns `{"type":"error","code":"unsupported_stream_message"}`.
Remember the 30-per-60-seconds message budget (§6).

### Overflow semantics

Each subscriber owns a bounded queue (128 deliveries). Producers never wait for
a browser: when a slow consumer overflows, the missed increments are **gone**
and the server sends `resync_required` with how many were dropped. Old
incremental state is never reconstructed. So: treat `resync_required` as a
full-refresh trigger for the whole app, surface the drop count in a subtle
"reconnected, refreshed" toast, and never try to interpolate.

### Reconnection

Reconnect with exponential backoff and jitter (e.g. 0.5 s → 30 s cap), refresh
all REST snapshots on every successful reconnect, and show a clear
"live / reconnecting / offline" indicator. While offline, fall back to polling
the REST screens the operator is looking at (a 5–15 s interval is plenty; the
reads are cheap and never hit the venue).

---

## 13. Event taxonomy

Every `telemetry_event` frame carries a `TelemetryEvent` envelope:

| Field | Type | Nullable |
| --- | --- | --- |
| `event_id` | `string` | no |
| `event_type` | `TelemetryEventType` | no |
| `timestamp` | `timestamp` | no |
| `severity` | `TelemetrySeverity` | no |
| `correlation_id` | `string` | yes |
| `payload` | `TelemetryPayload` | no |
| `producer` | `TelemetryProducer` | no |
| `sequence` | `integer` | no |

`sequence` is monotonic **per producer**, not globally — order the feed by
`timestamp`, and use `(producer, sequence)` only to detect a gap within one
producer.

**Discriminate on `payload.category`, not on `event_type`.** They are not
one-to-one: `leader_promoted` and `leader_demoted` are emitted for both
`LeaderEvent` (category `leader`) and `RosterEvent` (category `roster`), which
have different fields. `payload.category` is present on every payload and maps
1:1 to the payload shape.

| `payload.category` | Payload | `event_type` | Fields besides `category` |
| --- | --- | --- | --- |
| `ambiguous` | `AmbiguousEvent` | `ambiguous_order` | `operation`: `AmbiguousOperation`, `order_ref`: `string` or null, `reconciliation_required`: `boolean` |
| `discovery` | `DiscoveryEvent` | `discovery_cycle` | `outcome`: `DiscoveryOutcome`, `candidate_count`: `integer` |
| `failure` | `FailureEvent` | `trade_failure` | `component`: `FailureComponent`, `code`: `string`, `retryable`: `boolean`, `order_outcome`: `OrderOutcome` or null |
| `fill` | `FillEvent` | `trade_fill` | `outcome`: `FillOutcome`, `order_ref`: `string`, `fill_ref`: `string` |
| `leader` | `LeaderEvent` | `dedup` → `leader_signal`; `demoted` → `leader_demoted`; `pending` → `leader_signal`; `promoted` → `leader_promoted`; `received` → `leader_signal`; `retracted` → `leader_signal` | `outcome`: `LeaderOutcome`, `leader_ref`: `string`, `source`: `string` |
| `lock` | `LockEvent` | `reentry_lock` | `outcome`: `LockOutcome`, `lock_ref`: `string` |
| `protective` | `ProtectiveExitEvent` | `protective_exit` | `outcome`: `ProtectiveOutcome`, `task_ref`: `string` |
| `reconciliation` | `ReconciliationEvent` | `reconciliation` | `outcome`: `ReconciliationOutcome`, `affected_count`: `integer` |
| `risk` | `RiskEvent` | `risk_state` | `outcome`: `RiskOutcome`, `task_ref`: `string` or null |
| `roster` | `RosterEvent` | `empty` → `leader_demoted`; `unchanged` → `leader_promoted`; `updated` → `leader_promoted` | `outcome`: `RosterOutcome`, `active_count`: `integer` |
| `runtime` | `RuntimeEvent` | `runtime_state` | `outcome`: `RuntimeOutcome` |
| `skip` | `SkipEvent` | `trade_skip` | `reason`: `SkipReason`, `task_ref`: `string` or null |
| `task` | `TaskEvent` | `task_decision` | `outcome`: `TaskOutcome`, `task_ref`: `string`, `leader_ref`: `string` or null |
| `trade` | `TradeEvent` | `trade_submitted` | `outcome`: `TradeOutcome`, `task_ref`: `string`, `side`: `TradeSide`, `order_ref`: `string` or null, `order_outcome`: `OrderOutcome` or null |
| `wss` | `WssEvent` | `wss_state` | `outcome`: `WssOutcome`, `stream`: `string` |

### Enum vocabularies

- `AmbiguousOperation` — `order_submission`, `order_cancel`, `fill_reconciliation`
- `DiscoveryOutcome` — `request`, `failure`, `schema`, `completed`
- `FailureComponent` — `detection`, `normalization`, `worker`, `copy_decision`, `sizing`, `risk`, `execution`, `reconciliation`, `protective_exit`, `discovery`, `wss`, `runtime`, `telemetry`
- `FillOutcome` — `observed`, `duplicate`
- `LeaderOutcome` — `received`, `dedup`, `pending`, `promoted`, `demoted`, `retracted`
- `LockOutcome` — `created`, `blocked`, `reset`
- `OrderOutcome` — `submitted`, `canceled`, `rejected`, `not_fillable`, `ambiguous`
- `ProtectiveOutcome` — `triggered`, `filled`
- `ReconciliationOutcome` — `run`, `drift`
- `RiskOutcome` — `approved`, `rejected`, `limit_reached`, `exit_triggered`
- `RosterOutcome` — `updated`, `empty`, `unchanged`
- `RuntimeOutcome` — `started`, `ready`, `degraded`, `stopping`, `stopped`
- `SkipReason` — `duplicate`, `risk_rejected`, `task_inactive`, `schedule_closed`, `market_not_allowed`, `reentry_locked`, `budget_exhausted`, `insufficient_balance`, `below_minimum_size`, `stale_market_data`, `price_limit`, `capacity`, `finality_retracted`, `unsupported`, `paper_only`, `other`
- `TaskOutcome` — `enqueued`, `admitted`, `started`, `completed`, `canceled`, `failed`
- `TelemetryCategory` — `leader`, `task`, `trade`, `fill`, `skip`, `failure`, `ambiguous`, `protective`, `lock`, `reconciliation`, `discovery`, `roster`, `wss`, `runtime`, `risk`
- `TelemetryEventType` — `leader_signal`, `task_decision`, `trade_submitted`, `trade_fill`, `trade_skip`, `trade_failure`, `ambiguous_order`, `protective_exit`, `reentry_lock`, `reconciliation`, `discovery_cycle`, `leader_promoted`, `leader_demoted`, `wss_state`, `runtime_state`, `risk_state`
- `TelemetryProducer` — `detection`, `normalization`, `worker`, `copy_decision`, `sizing`, `risk`, `execution`, `reconciliation`, `protective_exit`, `discovery`, `wss`, `runtime`, `control_plane`
- `TelemetrySeverity` — `debug`, `info`, `notice`, `warning`, `error`, `critical`
- `TradeOutcome` — `candidate_evaluated`, `submitted`, `executed`, `partial`, `skipped`, `failed`, `ambiguous`
- `TradeSide` — `BUY`, `SELL`
- `WssOutcome` — `connected`, `disconnected`, `reconnected`, `stalled`

Two payload fields are bounded strings rather than enums:

- `WssEvent.stream` — the contract accepts `leader`, `market`, `user`.
- `LeaderEvent.source` — the detection source code, in practice `polling`,
  `on_chain_pending`, or `on_chain_confirmed`. Treat it as an open vocabulary
  (lowercase, `[a-z0-9_.-]`, at most 64 characters) and render an unknown value
  verbatim rather than dropping the event.

### What this build actually emits

The contract vocabulary is wider than what the current backend produces. Build
for the whole vocabulary — never crash or drop an event whose enum value you do
not recognise — but design the default screens around what really arrives:

| Producer | Emits |
| --- | --- |
| `detection` | `LeaderEvent` (`received`, `dedup`), `RosterEvent` (`updated`, `empty`), `FailureEvent` (component `detection`, codes prefixed `pending_copy_`) |
| `worker` | `SkipEvent` (post-submission projection of the copy pipeline, including `paper_only`), `FailureEvent` (component `worker`), `ReconciliationEvent` (`run`) |
| `execution` | `TradeEvent`, `FailureEvent` (component `execution`), `AmbiguousEvent`, `FillEvent` (`observed`), `ReconciliationEvent` (`drift`) |
| `risk` | `ProtectiveExitEvent` (`triggered`, `filled`), `FailureEvent` (component `protective_exit`), `RiskEvent` (`rejected`) |
| `wss` | `WssEvent` (`connected`, `reconnected`, `disconnected`) for the `market` and `user` streams only |
| `runtime` | `RuntimeEvent` (`started`, `stopping` from the telemetry runtime; `ready`, `degraded` from a runtime-safety change) |
| `discovery` | `DiscoveryEvent` (`request`, `completed`, `failure`) |

Not emitted by this build: `TaskEvent` / `task_decision`, `LockEvent` /
`reentry_lock`, `FillOutcome.duplicate`, the `leader` WSS stream name, and the
`candidate_evaluated`, `approved`, `limit_reached`, `exit_triggered`,
`unchanged`, `schema`, `stalled`, `order_cancel` and `fill_reconciliation`
outcomes. Do not build a screen whose only data source is one of those. In
particular, leader-transport health comes from `GET /detection` →
`leader_ws_connected`, not from a `wss_state` event.

### Correlation ids on events

`correlation_id` is **not** uniformly populated, so do not build the feed around
it as a universal join key:

| Event | `correlation_id` |
| --- | --- |
| `LeaderEvent(received)` | the leader transaction hash — the copy correlation id |
| `LeaderEvent(dedup)` | `null` |
| `SkipEvent` | the leader transaction hash when the signal is still known, else `null` |
| `FailureEvent` (worker) | an opaque leader/account reference |
| `FailureEvent` (execution, exception path) | the task reference |
| `TradeEvent`, `FillEvent`, `AmbiguousEvent` | `null` — join by `payload.order_ref` and `payload.task_ref` instead |
| `ReconciliationEvent(drift)` | an opaque order reference |
| `WssEvent`, `RuntimeEvent`, `RosterEvent`, `DiscoveryEvent` | `null` |

References (`leader_ref`, `task_ref`, `order_ref`, `fill_ref`, `lock_ref`) are
opaque and privacy-safe: a value that is already a bounded token appears
verbatim, anything else appears as `"h:"` plus a hex digest. Use them for
grouping and equality only; never parse them, and never display a raw `h:` value
as if it were a wallet or an order id.

### Severity

`debug` | `info` | `notice` | `warning` | `error` | `critical`. Map to visual
weight, default the feed filter to `notice` and above, and let the operator
widen it.

---

## 14. Timestamps

Everything is ISO-8601 in UTC, in **two renderings**, and a client must accept
both:

- typed REST bodies and the WS `telemetry_event` frame end in `Z` —
  `2026-08-18T14:00:00Z`;
- the telemetry and metrics snapshots (`generated_at`, the per-event `timestamp`
  inside a snapshot, and the WS `telemetry_snapshot` frame) carry an explicit
  offset — `2026-08-18T14:00:00+00:00`.

They are the same instant; only the suffix differs. Parse both, store UTC,
render in the operator's local timezone with the UTC offset visible, and use
relative times ("12s ago") only alongside the absolute value, never instead of
it. Never send a timestamp back to the server — no endpoint takes one.

---

## 15. Screens

Build these. Each lists the endpoints and events it needs; hide a screen
entirely when its capabilities are absent.

**1. Overview / Health** (default landing)
Reads `GET /system/status`, `GET /readiness`, `GET /detection`, `GET /runtime`,
`GET /portfolio`, `GET /pnl`. Events: `runtime_state`, `wss_state`,
`trade_failure`.
Show: paper/live badge, ready/degraded badge with `failed_checks` listed,
uptime, entry-control state, detection mode and per-source signal counters,
three WSS lamps — `leader` from `leader_ws_connected` (tri-state: the `null`
case is "unknown", not "down"), `market` from `market_ws_ok` and `user` from
`user_ws_ok`, each with its reconnect count where one exists — equity / free
cash / reserved, session realized PnL with its
"this session" label, and the count of open positions. This screen must answer
"is it working and is it making money" in under two seconds.

**2. Detection**
Reads `GET /detection`, `GET /leaders`. Events: `leader_signal`, `wss_state`,
`trade_failure` from `detection`.
Show: mode, per-source `signals_total` / `deduplicated_total` / `last_signal_at`,
watched leader count, pending-confirmation tracker stats (or "not composed"),
and a live signal feed deduplicated per non-negotiable 4 — one row per
transaction, with a badge listing which sources saw it and which lost the race.

**3. Positions**
Reads `GET /positions` (paged), `GET /positions/{position_id}`,
`GET /portfolio`. Events: `trade_fill`, `protective_exit`.
Show: market, token, side-agnostic size, average entry, leader attribution,
last update. Link each row to the market's fills.

**4. Orders**
Reads `GET /orders` (paged), `GET /orders/{order_id}`. Events:
`trade_submitted`, `trade_failure`, `ambiguous_order`.
Show: status, side, amount **labelled by side semantics**, price limit (or
"none"), matched total, timestamps. Give `ambiguous_order` its own visual
treatment: the submission reached the venue but the outcome is unknown and
reconciliation owns it — explicitly tell the operator not to resubmit.

**5. Fills**
Reads `GET /fills` (paged, newest first). Events: `trade_fill`.
Show: size, price, fee (all decimal strings), leader and order attribution,
`filled_at` with a "time not recorded" fallback.

**6. PnL**
Reads `GET /pnl`. Events: `trade_fill`.
Show: the realized session block with `session_started_at`; the marked-position
table; and a prominent basis chip driven by §9.2. When the account total is
`null`, show the basis explanation instead of a number.

**7. Risk**
Reads `GET /risk`, `GET /reservations` (paged). Events: `risk_state`,
`protective_exit`.
Show: the effective policy (kill switch, caps, reserve, drawdown), active blocks
with reasons and expiry, recent protective-exit triggers with fill status,
headroom as a clearly-labelled estimate, day-start equity (or "no baseline
yet"), and open collateral reservations.

**8. Leaders**
Reads `GET /leaders`, `GET /discovery/status`, `GET /discovery/roster`,
`GET /discovery/candidates`, `GET /discovery/cycles`. Events: `leader_promoted`,
`leader_demoted`, `discovery_cycle`.
Show: merged wallet list with `configured`/`monitored` chips, tier, score,
signal counters, trade and open-position counts; the discovery scheduler state;
and — only with `discovery_manual_run` — a "run discovery now" button
(confirm, `force` checkbox, `202` receipt toast, disabled until the cycle
completes because a second request returns `409 discovery_busy`).

**9. Tasks** — only when `task_read` is present, and note this build may not
compose task commands at all.
Reads `GET /tasks` (paged), `GET /tasks/{task_id}`. Writes: create, update,
pause, resume, duplicate, delete, bulk. Show the full typed configuration from
`TaskDetailView` (trigger, spend, price, reaction, market filter, schedule,
sizing) with every money field as a decimal input, and carry `version` through
every mutation.

**10. Settings**
Reads `GET /settings`, `GET /settings/{section}`. Writes `PUT`. Follow §10
exactly, including the persistent restart-required banner and the `409` diff
flow. Never render a free-form key/value editor.

**11. Runtime controls**
Reads `GET /runtime`. Writes pause/resume/full-stop per §11. Place these behind
a deliberate affordance — a "Safety" panel, not a toolbar button next to
"Refresh".

**12. Event feed** (a rail available from every screen)
Reads `GET /telemetry?limit=…` for backfill; live from the WS. Filters by
category, producer, and severity; groups by `correlation_id` where present;
links a row to the screen it concerns. Show a "reconnected — N events missed"
marker wherever a `resync_required` landed.

**13. Activity / audit** — only with `activity_read`.
Reads `GET /activity` (paged, optional `follower_account`). Show actor, action,
target, outcome, `reason_code`, timestamp. This is where the operator finds the
consequence of their own clicks; join to error toasts by correlation id.

**14. Reentry locks** — only with `reentry_lock_read`.
Reads `GET /locks`. With `reentry_lock_reset`, offer a guarded reset per §8 that
explains a lock exists to prevent immediately re-entering a position the bot
just protectively exited.

---

## 16. Data layer

Tech-agnostic, but do all of this:

- **Snapshot then stream.** One typed client per endpoint returning the
  unwrapped `data`. A single fetch wrapper injects the bearer header, applies
  the envelope, throws a typed error carrying `code` + `correlation_id`, and
  maps `401`/`403`/`409`/`429`/`503` to the behaviours in §4.
- **One capability gate.** A single `has(capability)` helper used by routing,
  navigation, and every control. No component decides for itself.
- **Decimal everywhere.** Wrap money at the boundary: parse into your decimal
  type immediately on receipt, keep the original string for round-tripping, and
  make it a type error to pass a `number` into a money slot. A shared
  `<Money value={…}/>` component that refuses numbers is the cheapest way to
  enforce non-negotiable 1.
- **Cache with explicit staleness.** A query cache (TanStack Query or
  equivalent) keyed by endpoint + `follower_account` + cursor. Stream events
  invalidate keys; they do not patch cached values. `resync_required`
  invalidates everything.
- **Backoff.** Exponential with jitter for both WS reconnects and REST retries;
  retry only idempotent reads; never retry a mutation automatically — a
  duplicate command is a real-money risk.
- **Bounded feed.** Cap the in-memory event list (a few thousand) and drop the
  oldest; the server ring is bounded too, so a longer client history is a lie.
- **No polling of mutations.** After a successful command, re-read the affected
  resource once and rely on the stream thereafter.
- **Timezone once.** One formatter module; no ad-hoc `toLocaleString` calls.
- **Accessible controls.** Confirmation dialogs must be keyboard-navigable and
  focus-trapped; the destructive action must never be the default focus.

---

## 17. Machine-readable contract

`api_contract.json` accompanies this prompt and is the source of truth. It is
generated from the running backend and holds:

| Key | Contents |
| --- | --- |
| `api_prefix` | `/api/v1` |
| `authentication` | header form, principals, 401/403 statuses |
| `capabilities` | the complete capability vocabulary |
| `decimal_fields` | every view field that is a decimal string |
| `enums` | REST enum vocabularies, including the domain vocabularies behind string-typed view fields |
| `envelope` | success and error envelope shapes |
| `errors.application`, `errors.transport` | every error code with its HTTP status |
| `limits` | page, bulk, rate-limit, request-size and WebSocket limits — the deployment's `ControlPlaneHttpConfig` defaults, overridable in `config/control_plane.yml`; `_note` restates that caveat and the inbound-only WebSocket cap |
| `permissions.all`, `permissions.viewer` | permission vocabulary and the read-only set |
| `routes` | every method/path with permission, capability, query, body, response, status, rate-limit action |
| `telemetry` | the event envelope, every payload with its fields and category, every enum, the payload → `event_type` mapping, WSS stream names |
| `views` | every response schema: field name, type, nullability, decimal flag, and closed vocabulary where one exists |
| `websocket` | both credential carriers with the subprotocol form, frame types in both directions, and the client error codes |

Generate your types from it rather than transcribing the tables above by hand.
