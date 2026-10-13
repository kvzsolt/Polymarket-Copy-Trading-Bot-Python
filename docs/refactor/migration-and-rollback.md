# Migration and rollback — standalone refactor (2026-10)

How to move an existing deployment onto the refactored standalone system,
and what rollback does and does not promise. Companion documents:
`deletion-manifest.md` (what was removed), `handoff.md` (what changed and
why), `docs/02-configuration.md` (current configuration reference).

## 1. Database migrations

All schema changes are **additive** and applied automatically by the
standard migration runner at startup (`src/pmcopy/storage/postgres/migrations.py`
+ `src/pmcopy/storage/postgres/migrations/`). The full catalog, oldest
first:

| Migration | Purpose | Refactor-relevant |
|---|---|---|
| `20260808_01_copy_tasks_and_reentry_locks` … `20260917_11_evidence_transaction_outbox` | Pre-refactor baseline schema (copy tasks, lifecycle, recovery scope, evidence, positions, redemption, auto-trader, outbox) | unchanged |
| `20260811_03_control_plane` | Control-plane tables | **Kept on purpose.** The control-plane code is gone but its migration stays so existing databases keep applying cleanly; the tables are inert on the standalone system. |
| `20261011_12_order_submission_identity` | A02: additive `orders` columns — `token_id`, `amount_unit` (`quote`/`shares`), `order_kind`, `effective_price_limit`, `correlation_ids` — every statement `IF NOT EXISTS`; legacy rows keep NULL identity and the recovery reader treats missing unit/token identity as fail-closed (duplicate-blocking), never fabricated | **new in this refactor** |
| `20261011_13_account_execution_leases` | Cross-process lease: one `account_execution_leases` row per copier account (TTL 60s, heartbeat 15s); at most one active writer per account regardless of trading mode | **new in this refactor** |

No column is renamed or dropped anywhere in the catalog, and migrations are
never deleted.

## 2. Config migration

- **Legacy single-account env stays valid.** A deployment using
  `PMCOPY__WALLET__*` / `PMCOPY__DATABASE__DSN` / etc. keeps running as the
  single default copier account. The historical default still applies only
  while exactly one config-less account exists (it auto-follows every global
  leader); the moment a roster file or a second account appears, follows
  become an explicit whitelist.
- **Schema v2 is authoritative when set.** Pointing
  `app.copiers_config_file` at a schema-v2 roster (`schema_version: 2`,
  template `src/pmcopy/config/copiers.example.yml`) makes that file the sole
  account source: legacy `PMCOPY__WALLET__*` single-account values are
  ignored, and combining it with the legacy v1
  `app.multi_user_config_file` is a hard configuration error — exactly one
  roster may be configured.
- **Wallet fields in the roster are env-var references** (`funder_address_env`
  / `private_key_env`); move the actual values into `secrets.env` / the
  process environment. Copier `id` is the persisted ledger identity —
  renaming a copier starts a NEW empty ledger.
- **Removed surfaces reject fail-closed.** A stale `control_plane` section
  arriving via overrides, the legacy shim, `secrets.env`, or process env
  (`PMCOPY__CONTROL_PLANE__*`) is a startup error naming the removal; the
  same applies to the obsolete `venue_backend` /
  `allow_experimental_ccxt_live` / `shadow_ccxt` keys. Remove those keys
  before upgrading.
- **Docker**: `db.env` now holds ONLY `POSTGRES_PASSWORD` (postgres service
  reads nothing else); all bot secrets stay in `secrets.env` (bot service
  only). An empty `POSTGRES_PASSWORD` fails closed — there is no default.
- The legacy `<deployment root>/config/runtime.local.yml` shim still
  participates (deprecated); migrate it to the service/environment files.
  The deployment root is `PMCOPY_DEPLOYMENT_ROOT` (existing dir) or the
  CWD — parent directories are never walked.

## 3. Upgrading with in-flight state (pending orders / account state)

An upgrade is a restart; the restart-reconciliation contract is the
preservation mechanism (see `docs/recovery_playbook.md` §1):

1. **Before anything else**, the new process acquires the account lease. If
   the old process is still running (or crashed < 60s ago), startup fails
   closed — stop the old deployment first (graceful SIGTERM releases the
   lease before the pool closes).
2. `startup_reconcile_unconfirmed` re-hydrates outstanding reservations
   against the venue; unambiguous terminal states settle immediately.
3. `submitted_unconfirmed` rows WITHOUT a venue order id (lost POST) are
   never released by restart: the bounded no-ID evidence loop resolves them
   — `recovered_order_id` re-attaches an authoritative id,
   `confirmed_absent` (after `required_absence_scans` consecutive complete
   absence scans) is the only auto-release. Until then the row stays
   duplicate-blocking. Legacy rows lacking `token_id`/`condition_id` or
   `amount_unit` fail closed forever; resolve them with `pmcopy resolve`.
4. `startup_drift_check` then compares re-hydrated state to the venue
   snapshot before any monitor opens.
5. Existing open positions keep protective exits, reconciliation, and risk
   monitoring even for accounts whose follows become empty (risk-only
   contexts).

Reservation expiry never releases an unresolved no-ID submission (A14), so a
crash mid-upgrade cannot silently free budget that a venue order might still
claim.

## 4. Rollback limitations (safe-forward-only)

- **Schema is forward-only.** Both refactor migrations are additive and no
  migration down-script exists. Rolling the CODE back to a pre-refactor
  commit while keeping the refactored DATABASE is supported: old code
  ignores the added columns/tables (that is the point of additive-only).
  The reverse — running new code on a database the old code already
  downgraded — does not apply, because nothing downgrades the schema.
- **Rolling code back past the control-plane removal** reactivates surfaces
  this refactor deleted; if that pre-refactor code runs against a database
  whose `trading_control` row was last written by the new CLI (for example a
  durable full stop), the old `resume` path would clear it — the new
  `resume` refuses. Treat a code rollback to pre-refactor as a return to the
  old product, not an undo of this one.
- **Config is not auto-migrated back.** Once a schema-v2 roster is in use,
  pre-refactor code cannot read it; rolling back requires restoring the
  legacy env/JSON configuration alongside.
- **Rolling back the lease**: `account_execution_leases` rows are ignored by
  pre-lease code (additive table); leftover rows expire naturally after the
  60s TTL and are simply rewritten on the next forward upgrade.
- For code-level rollback procedure see `docs/runbooks/rollback.md`; for
  database snapshot practice see `scripts/rollback.sh` (snapshot tooling).
