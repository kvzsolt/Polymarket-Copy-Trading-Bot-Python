# Migration — `main` / `develop` → `full-rework`

The `full-rework` branch is the post-audit rewrite of the bot. Issues
#66–#77 closed every must-fix item from `audit.md`. The runtime
contract, persistence layout, and config surface have all changed; do
not roll a `main` or `develop` deployment into this branch without
reading this page.

This document only covers what *changed*. For the live architecture,
see [`ARCHITECTURE.md`](ARCHITECTURE.md). For runbooks, see
[`runbooks/`](runbooks/00-index.md).

## Headline changes

| Area | Before (`main` / `develop`) | After (`full-rework`) |
|---|---|---|
| Order placement | `utils/postOrder.py` called the SDK directly. | All execution flows through `interfaces.clob_gateway.ClobGateway`. SDK types are walled inside `integrations/polymarket/`. `utils/postOrder.py` is removed. |
| CLOB SDK | `py_clob_client` v1. | `py_clob_client_v2` (CLOB V2). Requires `CLOB_V2_READY=true`. |
| Position identity | `(condition_id, outcome)` in many places. | Canonical `domain.position_key.PositionKey(user_id, condition_id, token_id)` everywhere. |
| Coordination | Implicit; race-prone for same-position actions. | `PositionCoordinator` + `IntentRegistry` serialise per-`PositionKey` actions. |
| Reservations | Pre-submit balance check only. | `ReservationService` holds BUY-quote / SELL-share reservations through the intent lifecycle, with restart recovery. |
| Reconciliation | Spread across services; opportunistic. | `ReconciliationService` owns terminal settlement. Startup reconciliation runs before risk and trade loops. |
| Market data | Ad-hoc REST polling. | `market_ws` + `user_ws` (5 s `PING`, REST snapshot on reconnect, subscribe via `assets_ids`); TTL-based `price_cache` and `marketMetaService` with explicit invalidation. |
| Idempotency | Raw request hash. | Effective key built **after** sizing + tick rounding from `(user, condition, token, side, effective_size, effective_price, tick_at_decision, intent/source event id, policy version)`. |
| Sizing | Hard-coded constants. | Metadata-driven, fed by `marketMetaService`. |
| Config | Direct `os.environ` reads. | Typed `config.app_config.AppConfig`; production paths fail closed without it. `secrets.env` overrides only allowlisted sensitive keys. |
| Manual / off-bot drift | Retried until balance matched. | Classified as non-retryable; `services/execution/reconciliation_service.py` shrinks/deletes the local position from live holdings. |
| Protective re-entry block | Activated on risk trigger. | Activated only on confirmed protective SELL fully closing the scoped `PositionKey`. |
| Operator CLI | `bot.py` only ran the trading loop. | New subcommands: `doctor`, `config`, `reservations`, `stale-locks`, `drift`. |
| Health surface | None. | `services.healthServer` exposes `/health`, `/ready`, `/metrics`. |

## New required configuration

The minimum typed-config surface for live mode (see
[`02-configuration.md`](02-configuration.md) for the full list):

- `PRIVATE_KEY`, `FUNDER_ADDRESS`, `RPC_URL`, `CHAIN_ID`,
  `LEADER_WALLETS`.
- `CLOB_V2_READY=true` (otherwise `create_clob_client()` raises).
- `EXECUTION_*` budgets (submit concurrency, pending-submission TTL).
- `MARKET_WS_*` / `USER_WS_*` tuning if defaults need overriding.

`config/runtime.env` ships safe defaults; `config/runtime.local.env` is
the operator's machine-specific public override; `secrets.env` holds
sensitive credentials only.

## Breaking interface changes (in-process)

If you have hand-written extensions on top of `main` / `develop`:

- Imports of `utils.postOrder` will fail at import time — replace with
  `services.executionService.ExecutionService`.
- Any code keying positions by `(condition_id, outcome)` must migrate to
  `PositionKey`. The factory helpers in `domain/position_key.py` (with
  hex-norm) are the canonical constructors.
- Direct CLOB SDK imports outside `integrations/polymarket/` and
  `utils/createClobClient.py` will be flagged by code review and break
  the architectural boundary the audit established.
- Direct `os.environ` reads in services should migrate to typed
  `AppConfig` accessors. `config/env.py` retains a compatibility shim
  for legacy callers — do not extend it.
- `tests/test_execution_idempotency_extended.py` constructed
  `ExecutionService(...)` without typed config; production constructors
  now fail closed without config. Update tests via the typed-config
  fixture pattern.

## Database migration

`storage/sqlite/migrations.py` will apply schema upgrades on first
startup. New tables: `intents`, `reservations`. Existing tables gained
columns; older snapshots of `data/copy_bot.sqlite3` will migrate
forward, but **not** backward. Take a backup before first start.

## Operational migration steps

1. Backup the existing SQLite database.
2. Set `KILL_SWITCH=true`; let the old process drain.
3. Pull `full-rework`, install (`pip install -e ".[v2]"`), set
   `CLOB_V2_READY=true`.
4. Run `bot doctor --paper`. Fix every failure before continuing.
5. Run a full paper-trading soak (see
   [`03-paper-trading.md`](03-paper-trading.md)) for a meaningful window
   — at minimum until reconciliation + drift checks have run a full
   cycle clean.
6. Only then clear the kill switch and let live trading resume.

## Live-trading gate (still in force)

Live trading remains **blocked** unless every condition in the
"Live-trading gate" section of `CLAUDE.md` is satisfied. Issue #77 is
not a release-enabling change.
