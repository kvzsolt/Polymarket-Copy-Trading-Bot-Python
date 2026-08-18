# Frontend control-plane handoff

The backend exposes an optional operator API at `/api/v1`. It is an adapter
over application services; it never calls CCXT, runs SQL, edits arbitrary
files, or submits manual trades from an HTTP handler. The server binds to
`127.0.0.1:9103` by default and is disabled until `control_plane.enabled` is
set. The `live` environment sets it
(`config/environments/live/control_plane.yml`); `paper` leaves it off. A
non-loopback bind is rejected unless bearer authentication is enabled.

## Authentication and transport

When authentication is enabled, send `Authorization: Bearer <credential>`.
Credentials are loaded only from the deployment secret boundary and are never
readable or writable through the API:

| Variable | Principal | Permissions |
| --- | --- | --- |
| `PMCOPY__CONTROL_PLANE__AUTH__TOKEN` | `operator` | every permission |
| `PMCOPY__CONTROL_PLANE__AUTH__VIEWER_TOKEN` | `viewer` | every `*.read` plus `telemetry.stream` |

The operator credential is required whenever the control plane is enabled and
authenticated; startup fails closed without it. The viewer credential is
optional, must differ from the operator credential, and is composed only when
its own variable is set — an unset variable means the `viewer` principal does
not exist. Give a dashboard the viewer credential: every mutation route answers
`403 permission_denied` for it, including task writes, settings writes,
discovery runs, lock resets, and every runtime safety command.

Both credentials are capped at 1024 characters and are compared in constant
time, with every configured credential compared even after a match so
credential order carries no timing signal.

Deploy browsers behind a same-origin reverse proxy. CORS is disabled unless an
explicit origin allowlist is configured, and wildcard authenticated CORS is not
supported.

All request and response JSON uses `snake_case`. Money, prices, shares, and PnL
use decimal strings; clients must not parse them through binary floating point.
Large collections use `limit` plus an opaque `cursor`; `limit` is restricted to
`1..100`.

Timestamps are ISO-8601 UTC in two renderings, and a client must accept both:

- values serialized from a typed DTO end in `Z`
  (`2026-08-18T14:00:00Z`) — this covers every REST resource body and the WS
  `telemetry_event` frame;
- the telemetry and metrics snapshots are pre-rendered by
  `TelemetryQueryService` with `datetime.isoformat()`, so they keep the explicit
  offset (`2026-08-18T14:00:00+00:00`) — this covers `generated_at` on
  `GET /telemetry`, `GET /telemetry/snapshot`, and `GET /metrics/snapshot`, the
  per-event `timestamp` inside a telemetry snapshot, and the WS
  `telemetry_snapshot` frame.

The two are the same instant; only the suffix differs.

Errors have a stable shape:

```json
{
  "error": {
    "code": "revision_mismatch",
    "message": "The resource revision no longer matches.",
    "correlation_id": "opaque-request-id"
  }
}
```

Provider exceptions and stack traces are never returned. Use the correlation
ID to join an API error with sanitized operator activity.

## Snapshot endpoints

Start a client by reading snapshots, then attach the stream. The current build
registers only routes backed by composed services; consult
`GET /api/v1/system/capabilities` instead of assuming a route is mutable.

| Area | Snapshot endpoints |
| --- | --- |
| System | `GET /system/status`, `/system/readiness`, `/system/capabilities` |
| Accounts | `GET /accounts` |
| Tasks | `GET /tasks`, `GET /tasks/{task_id}` |
| Portfolio | `GET /portfolio?follower_account=...` |
| Positions | `GET /positions`, `GET /positions/{position_id}` |
| Orders | `GET /orders`, `GET /orders/{order_id}` |
| Activity | `GET /activity` |
| Discovery | `GET /discovery/status`, `/discovery/candidates`, `/discovery/roster` |
| Locks | `GET /reentry-locks` |
| Reservations | `GET /reservations` |
| Settings | `GET /settings`, `GET /settings/{section}` |
| Runtime | `GET /runtime/safety` |
| Detection | `GET /detection` |
| Fills | `GET /fills?follower_account=...` |
| PnL | `GET /pnl?follower_account=...` |
| Risk | `GET /risk?follower_account=...` |
| Leaders | `GET /leaders` |
| Telemetry | `GET /telemetry/snapshot`, `GET /metrics/snapshot` |

Each read carries exactly one permission: `system.read` (`/accounts`),
`runtime.read` (`/detection`), `fill.read` (`/fills`), `portfolio.read`
(`/pnl`), `risk.read` (`/risk`), `leader.read` (`/leaders`), and
`telemetry.read` (`/telemetry/snapshot`, `/metrics/snapshot`). Every one of
them is in the read-only set the `viewer` credential holds, so a read-only
dashboard needs no per-route exceptions.

Follower-scoped collections require `follower_account`. Position and order
detail lookups remain follower-scoped. Portfolio reads use a background cache;
they never trigger venue calls. Until a first refresh succeeds, or once a
snapshot is stale, the endpoint returns a stable service error rather than
inventing balances, marks, or PnL. `valuation_basis: "cost_basis"` means no
market mark was available and `unrealized_pnl_usdc` remains `null`.

`GET /fills` pages newest-first on the durable fill identity with the same
opaque `cursor` contract as other collections. `GET /metrics/snapshot` returns
`generated_at`, `counters`, `gauges`, and `histograms` — the same numbers as
`/telemetry/snapshot` without the bounded event history.

`GET /detection` reports local counters per detection source plus the WSS
health this process can actually observe. `leader_ws_connected` is `null` when
no detection transport probe is attached (the control plane starts before the
detection runtime), which is different from a probed disconnected transport.
`pending_confirmations` is `null` unless the selected mode composed a
provisional-copy confirmation tracker.

`GET /pnl` is deliberately conservative. `realized` comes from the in-process
session ledger, so it covers this process run only and resets on restart;
`session_started_at` is the instant that ledger began recording. A position is marked only from
a cached WSS quote that is two-sided and fresher than
`market_data.executable_quote_max_age_seconds`; otherwise `mark_price`,
`market_value_usdc`, and `unrealized_pnl_usdc` are `null`. The account-level
`unrealized_pnl_usdc` is published only when every open position is marked
(`unrealized_basis: "wss_mid"`); a partially marked book reports `null` with
`"partial_wss_mid"`, and an unmarked book reports `"unavailable"`. A book
larger than the rendering ceiling sets `truncated: true` and
`unrealized_basis: "truncated"` and also withholds the account total, because
the client cannot reconcile it against the rendered rows; `position_count`,
`cost_basis_usdc`, and `marked_position_count` still describe the complete
book. `/fills`, `/pnl`, and `/risk` return `404` for a follower account this
process did not compose. No PnL read ever calls the venue.

`GET /risk` returns the configured policy, active durable risk blocks, recent
protective-exit triggers, and `headroom`. `headroom` is `null` unless a present
non-stale cached portfolio snapshot exists, and it applies only the account
cap, reserve, and daily-drawdown gates — per-market, per-category, and
per-leader exposure are unknown to a cached snapshot, so the published cap is
an upper bound on what the trade pipeline computes immediately before a real
submission. `day_start_equity_usdc` reflects the same day-start baseline the
risk engine uses and is `null` until one is recorded; reading it never records
one.

`GET /leaders` merges configured wallets, the discovery roster, the currently
monitored set, and local detection counters under one lower-cased wallet
identity, capped at 500 rows; the roster read itself is bounded by the same
ceiling. `GET /accounts` is projected per request, so `leader_count` follows
leader bindings that change while the process runs.

In `FillView`, `filled_at` is nullable because the durable fill timestamp is;
`size`, `price`, and `fee` are always exact decimal strings.

The immutable DTO definitions in `services/control_plane/contracts.py` and
`services/control_plane/contracts_reads.py` are the canonical response
contract. The narrow HTTP ports are defined in
`integrations/control_plane/contracts.py`.

## Commands

Task creation, update, pause, resume, duplication, and soft deletion use the
same task application service and optimistic task version as other adapters.
Created and duplicated tasks start paused. Resume fails closed unless the
running process has the exact follower/leader execution binding and is
currently monitoring that leader. Deletion never closes positions implicitly.

Discovery can be requested with `POST /discovery/run`. Admission is bounded to
one queued cycle and returns immediately; it evaluates discovery state but
does not execute trades or start a second trading path.

Reentry-lock reset uses `POST /reentry-locks/{lock_id}/reset` with the complete
lock identity and `expected_revision`. It delegates to the lock service and
does not delete rows directly.

Live runtime controls are registered only in live composition:

- `POST /runtime/safety/pause-entries` blocks new entries while preserving the
  existing protective-exit policy.
- `POST /runtime/safety/resume-entries` uses the canonical database-backed
  live gate and cannot override a full stop.
- `POST /runtime/safety/full-stop` persists the stop first, then makes a
  best-effort pass over every follower gateway to cancel resting orders.

There is no raw buy or sell endpoint. Every execution continues through risk,
reservation, execution, ambiguous-submission handling, and reconciliation.

## Settings revisions and apply semantics

Settings are service-owned YAML documents. A read returns a deterministic
revision and per-field safe metadata. A write supplies `values` plus
`expected_revision`; stale writes receive HTTP `409` and never overwrite the
newer document. The repository validates known typed keys, rejects secret-like
keys, writes only the selected environment override, fsyncs a temporary file,
and atomically replaces the target.

There is exactly one apply mode. `AppConfig` is immutable and loaded once, so
no setting is hot-reloadable and `SettingsApplyMetadata` rejects any write
result that claims otherwise (`restart_required` must be true and
`live_applied` must be false). A successful write persists the validated
desired state and returns `restart_required: true`; the HTTP handler does not
restart or shell out to the process. A client should render every successful
settings write as "saved, takes effect on restart" and must not offer an
apply-now affordance.

Wallet keys, bearer credentials, database passwords, RPC credentials, and
provider secrets are omitted entirely from settings values and schemas. The
whole `control_plane.auth` object — the operator credential and the viewer
credential alike — is excluded from the settings surface, so neither can be
read or rotated over the API.

## Live stream and resynchronization

Connect an authenticated WebSocket to `GET /api/v1/stream` after reading the
REST snapshots. It needs `telemetry.stream`, which the viewer credential holds.
The stream carries immutable safe telemetry events and state changes. It is
incremental notification, not the state authority.

Server frames are `{"type": ...}` objects: `stream_ready` on connect,
`telemetry_event` with a `data` event envelope, `telemetry_snapshot` with a
`data` snapshot (or `null` when no telemetry query is composed), `pong`,
`resync_required` with a `dropped_count`, and `error` with a `code`. A client
may send `{"type": "ping"}` or `{"type": "resync"}` as text; binary frames close
the socket, and client messages are rate limited per principal.

Every subscriber owns a bounded queue. Producers never await a browser. When a
slow subscriber overflows, old incremental state is not reconstructed; the
subscriber receives `resync_required` when possible and must reread the
relevant REST snapshots. Disconnecting a browser removes its subscription, and
server shutdown closes all stream tasks.

## Capability-driven frontend behavior

Hide or disable controls absent from the capability response. In particular,
paper mode intentionally omits live runtime mutations, and task mutations are
omitted if the process cannot validate runtime activation. Read capability
names describe the exact build (`ccxt_backend`, `portfolio_read`,
`telemetry_snapshot`, and related domain reads); they are not promises about a
future frontend or unavailable venue feature. The extended reads advertise
`account_read`, `detection_read`, `fill_read`, `leader_read`,
`metrics_snapshot`, `pnl_read`, and `risk_read`, and each appears only when
this process composed the local state that backs it.
