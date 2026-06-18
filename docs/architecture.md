# Architecture

This document describes the live runtime architecture of the Polymarket
copy-trading bot as it stands at the end of the `full-rework` remediation
(issues #66–#77). It is the single source of truth for "how the system is
wired today." When the architecture changes, update this file in the same
PR.

For the *why* behind specific safety decisions, see `audit.md` and `plan.md`
at repo root. For operator procedures during incidents, see
`docs/runbooks/`. For the user-facing operator manual, see the numbered
guides starting at `docs/00-read-this-first.md`.

---

## Layered view

```
                +-----------------------------+
                |   bot_cli  (operator CLI)   |
                +--------------+--------------+
                               |
                +--------------v--------------+
                |       app.py (entry)        |
                |   + bootstrap.build_app     |
                +--------------+--------------+
                               |
            +------------------+------------------+
            |                  |                  |
   +--------v------+  +--------v--------+  +-----v--------+
   | orchestrators |  |    services/    |  |  storage/    |
   |   (runtime    |  |  (domain logic, |  |   (sqlite    |
   |   loops)      |  |  side effects)  |  |   repos)     |
   +--------+------+  +--------+--------+  +--------------+
            |                  |
            +---------+--------+
                      |
         +------------v-------------+
         |    interfaces/           |
         |    (ClobGateway port)    |
         +------------+-------------+
                      |
         +------------v-------------+
         | integrations/polymarket/ |
         |  (CLOB V2 SDK adapter)   |
         +--------------------------+
```

---

## Composition root

The composition root is split in two for testability:

1. **`bootstrap.py::build_app`** — pure synchronous wiring of the shared
   graph. No network. Returns an `Application` dataclass holding `AppConfig`,
   `ReadinessTracker`, the SQLite path, `LeadersRepo`, the
   `TraderDiscoveryService`, and the `LeaderRegistry`. Has *no import-time
   side effects*; safe to call repeatedly in tests.
2. **`app.py::main`** — async startup. Calls `build_app`, then per-user
   wires the CLOB client, repos, and `ExecutionService` via
   `orchestrators.runtime_bootstrap`. Hosts the runtime loops
   (`runtime_monitor`, `runtime_discovery`, `reconciliation_loop`, the health
   server).

Configuration is loaded by `config.app_config.load_config()` from
`config/runtime.env` + `config/runtime.local.env` + `secrets.env`, with
strict typed validation. Production code paths must consume the typed
`AppConfig`; raw `os.environ` reads are restricted to the legacy
compatibility shim in `config/env.py` and the `config.runtime_env`
loader.

---

## Components

### CLOB gateway (`interfaces/`, `integrations/polymarket/`)

`interfaces.clob_gateway.ClobGateway` is the protocol all execution code
talks to. It owns:

- order signing
- order creation / submission
- market metadata (tick, min size, neg-risk flag)
- order-book snapshots
- price + fee lookups
- collateral / balance / allowance checks
- venue error normalisation

`integrations.polymarket.clob_v2_gateway.ClobV2Gateway` is the concrete
adapter against the `py_clob_client_v2` SDK. **All** SDK type imports are
walled inside this package and `utils.createClobClient`. No service or
orchestrator may import SDK order types directly.

### Execution subsystem (`services/execution/`)

Owned by `services.executionService.ExecutionService`. Sub-modules:

| File | Role |
|---|---|
| `coordinator.py` | Per-`PositionKey` action serialisation. |
| `state_machine.py` | Allowed intent transitions. |
| `submission.py` | Place / cancel / replace via `ClobGateway`. |
| `idempotency.py` | Effective idempotency keys (post-sizing, post-tick). |
| `reservation_service.py` | BUY-quote / SELL-share reservations. |
| `intent_recovery.py` | Restart-recoverable intent state. |
| `reconciliation_service.py` | Owns terminal settlement + drift handling. |
| `book_spread.py`, `quote_policy.py` | Market-quality gates. |
| `expiry_bridge.py` | Expiring intent → cancel/release. |
| `types.py` | Shared dataclasses (intent, request, ack, fill). |

The `IntentRegistry` (`services/intentRegistry.py`) is composed by
`PositionCoordinator` (`services/positionCoordinator.py`) which wraps the
per-scope lock from `PositionActionCoordinator`
(`services/positionActionCoordinator.py`). The snake-case shim
`services/position_action_coordinator.py` re-exports the camelCase
implementation so either import style works.

### Position identity

Canonical key lives in `domain/position_key.py`:

```
PositionKey(user_id, condition_id, token_id)
```

Every state mutation path (signal ingestion, copy decision, execution,
storage, risk, reconciliation, market data, reservations, audit logs)
preserves this triple. `outcome` may travel along for display but never
substitutes for `token_id`.

### Market data (`services/`, `clients/`)

- `clients/market_ws.py`, `clients/user_ws.py` — WSS clients with 5 s
  `PING` heartbeat, REST snapshot on reconnect before applying deltas.
  `price_change` with size 0 deletes a level. Subscriptions use
  `assets_ids`.
- `services/market_data_service.py` and `cache/market_state_store.py` —
  freshness-aware market data with per-component WSS/REST source split,
  invalidation on own fills, price changes, tick-size changes, and after
  reconciliation. (Legacy standalone price-cache and market-meta modules
  have been removed; this is the single authority.)
- Decisions fail closed when quote / book / tick / metadata is stale or
  missing.

### Reconciliation

`services.reconciliation.ReconciliationService` (composed with
`services.execution.reconciliation_service.ReconciliationService`) owns
terminal settlement:

- Prefers venue `order_id` lookup; idempotency-key fallback only for
  legacy rows.
- Startup reconciliation runs before risk and trade loops.
- Applies only positive incremental deltas from cumulative matched size.
- Releases reservations on terminal state.

### Risk + protective exits (`services/risk/`)

Risk triggers are *not* the durable re-entry block. The block activates
only after a confirmed protective SELL fully closes the scoped
`PositionKey`. Restart recovery uses the same logic.

### Drift (manual / off-bot)

`services/execution/reconciliation_service.py` plus the SELL-side
`insufficient_balance` / allowance error classifier treats those as
non-retryable state drift, fetches live holdings, shrinks or deletes the
local position, releases reservations, and emits
`manual_position_drift_reconciled`.

### Compliance + discovery

- `services/compliance.py` — fail-closed geoblock check.
- `services/discovery/`, `services/traderDiscoveryService.py` — leader
  discovery + ranking.

### Observability (`services/healthServer.py`, `services/metrics.py`, `services/observability/`)

- `ReadinessTracker` aggregates per-component readiness for the health
  server (`/health`, `/ready`).
- Prometheus-style counters in `services/metrics.py`.
- Structured JSONL audit logs via `utils/logger.py`.

### Operator CLI (`bot_cli/`)

`bot.py` shim → `bot_cli.main:main`. Commands include:

| Command | Purpose |
|---|---|
| `doctor` | Pre-flight readiness checks (paper / live). |
| `config` | Print redacted typed config. |
| `reservations` | Inspect / release reservations (see runbook). |
| `stale_locks` | Inspect / clear stale coordinator locks. |
| `drift` | Inspect or reconcile manual/off-bot drift. |

---

## Storage

SQLite, schema versioned by `storage/sqlite/migrations.py`. Repos:

- `LeadersRepo`, `FillsRepo`, `OrdersRepo`, `PositionsRepo` (per-user),
  `IntentsRepo`, `ReservationsRepo`.

All multi-step writes use explicit transactions. Idempotency keys are
unique-indexed.

---

## What is *not* in active use

- `legacy/tradeExecutor.py` — kept for two unit-test files only. The
  release-gate test `tests/release_gate/test_no_legacy_imports.py`
  enforces that no production module imports from `legacy.*`.
- The deleted root probe scripts (`test_probe*.py`, `test_doctor_*.py`)
  removed in issue #77.
- `utils/postOrder.py` — removed earlier in the rework. Replaced by
  `ExecutionService`.
- The pre-audit planning docs in `docs/archive/` (see
  `docs/archive/README.md`).

---

## Test suite organisation

- `tests/` — unit + integration suites.
- `tests/release_gate/` — invariants that must hold for any release:
  CLI commands, readiness aggregation, metrics, redacted config,
  legacy-import absence, etc.

Run gates locally:

```bash
.venv/bin/pytest tests/release_gate/ -v
.venv/bin/pytest -q
.venv/bin/python -m compileall -q .
```

---

## Boundaries to defend

1. SDK types stay inside `integrations/polymarket/` and
   `utils/createClobClient.py`.
2. Production modules never import `legacy.*` or `test_*` modules.
3. Production paths read typed `AppConfig` — not raw `os.environ`.
4. Position identity stays `(user, condition_id, token_id)` everywhere.
5. Reconciliation owns terminal settlement; execution records pending
   state only.
6. Decisions fail closed when market data, config, venue state, or
   account state is missing.
