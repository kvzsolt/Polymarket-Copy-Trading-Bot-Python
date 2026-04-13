# Phase 6 — Remaining Work: ENV Eradication

## Status

Phase 6 landed the safe-scope items:

1. `config/env.py` — deprecation docstring at module top, steering new code to `AppConfig`/`bootstrap.build_app`.
2. `services/reconciliation.py` — removed the last direct `os.environ.get` reads; now goes through `ENV` (single chokepoint).
3. `tests/test_reconciliation.py::test_env_defaults` — updated to patch `config.env.ENV` instead of `os.environ`.

Verification:

```bash
rg "os\.getenv|os\.environ\." services/ orchestrators/ clients/ storage/ domain/ app.py bootstrap.py
```

→ **0 hits** in services/orchestrators/clients/storage/domain. Only `utils/logger.py:323,410` remains (low-level logging boundary — intentional).

Full test suite: **1017 passed in ~17s**.

## What's NOT done

The plan's primary Phase 6 goal — eradicating `from config.env import ENV` from production code — requires ~130 individual `ENV.` reads across 20 files to be refactored to consume `AppConfig` via constructor DI.

## ENV read counts (top offenders)

| File | ENV reads |
|------|----------:|
| `services/riskEngine.py` | 15 |
| `services/riskMonitor.py` | 13 |
| `services/negRiskArb.py` | 13 |
| `services/traderDiscoveryService.py` | 11 |
| `orchestrators/leader_registry.py` | 11 |
| `orchestrators/runtime_bootstrap.py` | 10 |
| `services/positionManager.py` | 9 |
| `services/leaderScoringService.py` | 9 |
| `services/executionService.py` | 8 |
| `orchestrators/runtime_summary.py` | 8 |
| `services/multiUserConfig.py` | 7 |
| `services/copyDecisionService.py` | 7 |
| `orchestrators/trade_pipeline.py` | 7 |
| `services/tradeScoringService.py` | 5 |
| `services/autoRedeem.py` | 5 |
| `services/redemptionService.py` | 4 |
| `services/tradeMonitor.py` | 3 |
| `services/pnlTracker.py` | 1 |
| `services/feeService.py` | 1 |
| `orchestrators/runtime_discovery.py` | 1 |

## Per-file migration pattern

For each service, the refactor is mechanical:

1. **Add `config: AppConfig` (or the relevant sub-config, e.g. `RiskConfig`) to `__init__`.**
2. **Replace each `ENV.get("KEY", default)`** with `self._config.<section>.<field>`.
3. **Update every call site** in `bootstrap.py`, `orchestrators/runtime_bootstrap.py`, and `app.py` to pass the sub-config in.
4. **Update tests** — replace `patch.dict("config.env.ENV", {...})` with constructing a test `AppConfig` via a `conftest.py` helper (e.g. `make_test_config(risk={"stop_loss_pct": 10})`).
5. **Remove the `from config.env import ENV` import** from the service file.
6. **Run `pytest -v` after each file** — must stay green.

## Recommended ordering (lowest coupling → highest)

Each is a standalone PR (~50-150 lines of diff + test updates):

1. **`services/feeService.py`** (1 ENV read) — smallest; proves the pattern.
2. **`services/pnlTracker.py`** (1 ENV read) — also tiny.
3. **`services/tradeMonitor.py`** (3 ENV reads).
4. **`services/tradeScoringService.py`** (5 ENV reads).
5. **`services/redemptionService.py`** (4 ENV reads).
6. **`services/autoRedeem.py`** (5 ENV reads) — already has DI seams from Phase 4.
7. **`services/copyDecisionService.py`** (7 ENV reads).
8. **`services/multiUserConfig.py`** (7 ENV reads).
9. **`services/positionManager.py`** (9 ENV reads) — Phase 4 DI already established.
10. **`services/leaderScoringService.py`** (9 ENV reads).
11. **`services/executionService.py`** (8 ENV reads) — high-traffic; Phase 4 DI seams exist.
12. **`services/traderDiscoveryService.py`** (11 ENV reads).
13. **`services/riskMonitor.py`** (13 ENV reads).
14. **`services/riskEngine.py`** (15 ENV reads) — densest; do last since others may reuse its config surface.
15. **`services/negRiskArb.py`** (13 ENV reads).
16. **`orchestrators/runtime_discovery.py`** (1 ENV read).
17. **`orchestrators/runtime_summary.py`** (8 ENV reads).
18. **`orchestrators/trade_pipeline.py`** (7 ENV reads).
19. **`orchestrators/leader_registry.py`** (11 ENV reads).
20. **`orchestrators/runtime_bootstrap.py`** (10 ENV reads) — refactor last, as it's the per-user wiring helper called by `app.py`.

## Additional cleanup items

- **Delete legacy `risk_monitor_loop`** in `services/riskMonitor.py` (only `risk_monitor_loop_for_contexts` is used in production). Tests in `tests/test_new_modules.py` and `tests/test_services_di.py` reference it — migrate or delete those tests.
- **Delete `get_position_manager`** (`services/positionManager.py:413`) and **`get_pnl_tracker`** (`services/pnlTracker.py:152`) factory singletons. Their last callers are the DI fallback paths in `services/autoRedeem.py` and `services/riskMonitor.py` — make `position_manager=` / `pnl_tracker=` required and wire them from `bootstrap.build_app` via `UserRuntimeState`.
- **Delete the `_readiness_state`/`update_readiness`/`get_readiness` module shims** once all callers (tradeMonitor, riskMonitor, trade_pipeline, leader_registry) receive the shared `ReadinessTracker` from `Application` via DI.
- **Delete module-level `_CLOB_CIRCUIT`** in `services/executionService.py` once `ExecutionService.__init__` always receives a `circuit_breaker=` (no fallback).
- **Delete module-level `_basket_store`** in `services/negRiskArb.py` once every call site passes `store=`.

## Final verification (when Phase 6 is truly done)

```bash
# No ENV imports in production code
rg "from config.env import ENV" --glob '!config/**' --glob '!tests/**'
# No os.environ / os.getenv escape hatches outside config & runtime_env
rg "os\.getenv|os\.environ\." services/ orchestrators/ clients/ storage/ utils/ domain/ app.py bootstrap.py
# Baseline stays green
.venv/bin/pytest -v
```

All three must return zero hits / all-green.

## Effort estimate

~20 incremental PRs, 50-150 lines each. Total: several focused sessions. Each PR is independently reviewable and revertible.
