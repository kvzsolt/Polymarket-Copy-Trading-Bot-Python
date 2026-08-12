# Frontend control-plane handoff

The backend exposes an optional operator API at `/api/v1`. It is an adapter
over application services; it never calls CCXT, runs SQL, edits arbitrary
files, or submits manual trades from an HTTP handler. The server binds to
`127.0.0.1:9103` by default and is disabled until `control_plane.enabled` is
set. A non-loopback bind is rejected unless bearer authentication is enabled.

## Authentication and transport

When authentication is enabled, send `Authorization: Bearer <token>`. The
token is loaded only from the deployment secret boundary as
`PMCOPY__CONTROL_PLANE__AUTH__TOKEN`; it is not readable or writable through
the API. Deploy browsers behind a same-origin reverse proxy. CORS is disabled
unless an explicit origin allowlist is configured, and wildcard authenticated
CORS is not supported.

All request and response JSON uses `snake_case`. Timestamps are ISO-8601 with
an explicit UTC offset. Money, prices, shares, and PnL use decimal strings;
clients must not parse them through binary floating point. Large collections
use `limit` plus an opaque `cursor`; `limit` is restricted to `1..100`.

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
| Telemetry | `GET /telemetry/snapshot` |

Follower-scoped collections require `follower_account`. Position and order
detail lookups remain follower-scoped. Portfolio reads use a background cache;
they never trigger venue calls. Until a first refresh succeeds, or once a
snapshot is stale, the endpoint returns a stable service error rather than
inventing balances, marks, or PnL. `valuation_basis: "cost_basis"` means no
market mark was available and `unrealized_pnl_usdc` remains `null`.

The immutable DTO definitions in `services/control_plane/contracts.py` are the
canonical response contract. The narrow HTTP ports are defined in
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

Each setting reports one apply mode: `hot_reloadable`, `restart_required`, or
`immutable_at_runtime`. A successful restart-required write persists the
validated desired state and returns `restart_required: true`; the HTTP handler
does not restart or shell out to the process. Wallet keys, bearer tokens,
database passwords, RPC credentials, and provider secrets are omitted entirely
from settings values and schemas.

## Live stream and resynchronization

Connect an authenticated WebSocket to `GET /api/v1/stream` after reading the
REST snapshots. The stream carries immutable safe telemetry events and state
changes. It is incremental notification, not the state authority.

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
future frontend or unavailable venue feature.
