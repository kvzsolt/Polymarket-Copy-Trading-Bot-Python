# Release gate

The release-gate test suite (introduced in issue #75) is the executable
contract behind the `live-trading` block in `CLAUDE.md`. Live trading is
**not** permitted unless the gate is green AND every operator-side check in
CLAUDE.md's "Live-trading gate" section is satisfied.

## Running locally

```bash
pytest -m release_gate
```

The default `pytest` invocation excludes the gate (it also excludes
`integration`-marked tests) so day-to-day TDD remains fast. The gate runs
in its own CI job, plus a dedicated `release-gate.yml` workflow on release
events.

## Matrix coverage

Each item in the issue-#75 matrix has its own gate module under
`tests/release_gate/`:

| # | Matrix item | Module |
| - | ----------- | ------ |
| 1 | PositionKey uniqueness | `test_position_key_uniqueness.py` |
| 2 | Coordinator lock serialization | `test_coordinator_serialization.py` |
| 3 | Cache TTL and invalidation | `test_cache_ttl_invalidation.py` |
| 4 | WSS heartbeat at 5 seconds | `test_wss_heartbeat.py` |
| 5 | WSS reconnect/resync | `test_wss_reconnect_resync.py` |
| 6 | Metadata-driven min sizing | `test_sizing_metadata_driven.py` |
| 7 | Effective idempotency | `test_effective_idempotency.py` |
| 8 | Reservation lifecycle | `test_reservation_lifecycle.py` |
| 9 | Reconciler terminal ownership | `test_reconciler_terminal_ownership.py` |
| 10 | Startup reconciliation before monitors | `test_startup_recovery.py` |
| 11 | Manual/off-bot drift reconciliation | `test_manual_drift_reconciliation.py` |
| 12 | Typed config fail-closed | `test_typed_config_fail_closed.py` |
| 13 | CLOB V2 gateway compatibility smoke | `test_clob_v2_gateway_smoke.py` |
| — | Same-position concurrency stress | `test_concurrency_stress.py` |
| — | End-to-end dry-run BUY | `test_e2e_dry_run_buy.py` |
| — | End-to-end dry-run SELL | `test_e2e_dry_run_sell.py` |
| — | Protective-exit re-entry block | `test_protective_exit_integration.py` |

The meta-test `test_matrix_coverage.py` imports every module above so an
accidental rename or deletion breaks the gate before it can silently shrink
coverage.

## Operator checklist (from CLAUDE.md, reproduced here)

Before enabling live trading, the operator must:

- Confirm CI is green on `main` / `full-rework` and the release-gate
  workflow passed on the target tag.
- Verify mock-CLOB integration tests pass (`pytest -m release_gate`).
- Run paper-trading soak for the configured window.
- Manually verify collateral, negative-risk behavior, CLOB gateway
  readiness, typed config, Docker/runtime, DB backup, and rollback plan.

## Why the gate exists

Per CLAUDE.md, the bot is safety-critical trading software. Unit tests
prove individual modules behave; the release-gate proves they behave
correctly **together** under the same wiring the production runtime uses.
Any change that breaks the gate is by definition a release blocker.
