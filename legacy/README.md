# Legacy Compatibility Paths

The active production runtime path is:

- `index.py` (orchestration only)
- `orchestrators/trade_pipeline.py`
- `domain/trading_context.py`
- `storage/sqlite/*`
- `services/riskMonitor.py::risk_monitor_loop_for_contexts`

The following modules are retained for backward compatibility and historical tests:

- `services/tradeExecutor.py`
- `services/riskMonitor.py::risk_monitor_loop` (singleton/in-memory variant)

Do not use these legacy paths for new production wiring.
