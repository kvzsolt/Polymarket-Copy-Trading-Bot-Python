# changes.md

## 2026-04-19

This repo is being parked / frozen for now.

### What changed today

#### 1. Risk system cleanup toward advanced-only exits
- Removed the active runtime wiring for primitive hard stop-loss and legacy single take-profit from the main config/runtime path.
- Kept the live exit direction focused on the advanced engines:
  - profit lock
  - near-max exit
  - trailing stop
  - staged take-profit
  - leader divergence exit
- Updated summaries, comments, and tests so the config surface matches the real behavior instead of pretending retired controls still matter.

#### 2. Risk polling now supports sub-second intervals
- `RISK_CHECK_INTERVAL` parsing was changed from integer-only to float-capable parsing.
- This allows intervals like `0.25` seconds in the active config/runtime path.

#### 3. Shared risk price cache introduced in the risk monitor
- Added a shared cache in `services/riskMonitor.py` for risk-path price reads.
- Added bounded concurrent refresh with a semaphore (default 8).
- The context risk loop refreshes cached prices before evaluating exits.
- `run_risk_check_for_context(...)` now reads from cache first, with lazy refill fallback on cache miss.
- This is the first step toward broader cache-based price reads instead of repeated direct venue calls.

#### 4. Legacy compatibility/docs/test cleanup
- Clarified that `config/settings.py` keeps primitive stop-loss / take-profit fields only for legacy compatibility.
- Updated tests to reflect the advanced-only live direction.
- Updated usage/docs comments to stop advertising retired runtime behavior as if it were live.

### Verification
- Targeted pytest run passed after the cache rollout and interval changes.
- Latest focused result during this session: `10 passed`.

### Current state at freeze time
- Repo branch: `develop`
- Intent: park the project, keep the working changes saved remotely, and come back later with fresh eyes.
