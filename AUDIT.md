# AUDIT.md

- **Current branch:** `develop`, commit `4b5d232`
- **Active execution path:** `services/executionService.py` and `orchestrators/trade_pipeline.py`. Too implicit, no reservations.
- **Active config path:** `ENV.get(...)` scattered across codebase. Needs `AppConfig` unification.
- **Active CLOB SDK usage:** `py_clob_client` imported in `clients/` and `services/executionService.py`. Needs `ClobV2Gateway`.
- **Active compliance path:** `services/compliance.py` exists but uses plain dicts/booleans. Needs strict `ComplianceDecision` fail-closed logic.
- **Active redemption path:** `services/redemptionService.py` and `services/autoRedeem.py`. Lacks `wallet_validator.py` and `redemption_reconciler.py`.
- **Unresolved duplicate-order risks:** No explicit `OrderLifecycleStatus` enum or reservation layer. Delayed venue responses can cause duplicate BUY/SELL.
- **Missing tests:** Execution idempotency, strict geoblock schema failure, state machine transitions.
- **Files to refactor:** `executionService.py`, `trade_pipeline.py`, `riskMonitor.py`, `redemptionService.py`, `env.py`, `app_config.py`.
- **Files to delete/quarantine:** `legacy/tradeExecutor.py`