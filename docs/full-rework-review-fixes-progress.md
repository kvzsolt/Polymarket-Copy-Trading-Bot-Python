# `full-rework-review-fixes` — progress

Branch tracking the audit remediation defined in `audit.md` and `plan.md`. One issue per slice. Live trading remains disabled.

## Issue order

| Issue | Slice | Status |
|---|---|---|
| #66 | CLOB V2 gateway isolation | in progress |
| #67 | Canonical PositionKey + action coordinator | pending |
| #68 | Market-data cache + WSS lifecycle | pending |
| #69 | Metadata-driven sizing + effective idempotency | pending |
| #70 | Reservations + intent lifecycle ownership | pending |
| #71 | Reconciliation + startup recovery ownership | pending |
| #72 | Protective exits + re-entry blocking correctness | pending |
| #73 | Typed config fail-closed + stale tests | pending |
| #74 | Manual / off-bot position reconciliation | pending |
| #75 | Final integration tests + release gate | pending |
| #76 | Runtime ops, Docker readiness, observability | pending |
| #77 | Legacy cleanup + architecture documentation | pending |

## Pre-flight baseline

Captured before any code edits on `full-rework-review-fixes`.

- Base branch: `full-rework` at commit `fb9c142` (`docs: replace phased-rewrite docs with audit + remediation plan`).
- Working branch: `full-rework-review-fixes` (new, pushed `-u`).
- Baseline pytest collection: `1313 / 1317 tests collected (4 deselected)` — pytest 8.4.2.
- Baseline failures (pre-existing on `full-rework`):
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_same_signal_id_second_call_ignored`
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_different_signal_id_allowed`
  - `tests/test_execution_idempotency_extended.py::TestDuplicateSignalSuppression::test_same_idempotency_key_blocks`
  - `tests/test_execution_idempotency_extended.py::TestStaleLockExpiry::test_expired_submission_allows_new_execution`
  - All 4 are the audit-flagged stale tests — they pass `ExecutionService(...)` configless and the surrounding system now returns `rejected` instead of the legacy `duplicate_ignored`. Resolution: `#73 typed-config fail-closed` will rewrite the constructor + tests; `#66` only updates these tests' `clob` fixture to a gateway-shaped mock so they continue to fail in the same way (no new failures introduced).
- Baseline full-suite run: `pytest -q --ignore=test_probe.py` exceeded 600s wall clock without completing. Symptom localized to non-`#66` paths (likely socket / asyncio waits in unrelated suites). Not blocking issue #66; verification will run targeted subsets.
- `test_probe.py` (repo-root probe script that calls `polygon-rpc.example.com` at import time) is excluded from runs via `--ignore=test_probe.py`. Quarantine/removal is `#77`'s scope.

Targeted-subset baseline run for issue #66 (passes):

```text
tests/test_clob_v2_gateway_isolation.py ............................. 3 passed
tests/test_execution_idempotency_extended.py::TestIdempotencyKey..... 3 passed
tests/test_execution_idempotency_extended.py::TestDuplicate*.......... 3 failed (pre-existing)
tests/test_execution_idempotency_extended.py::TestStaleLock*.......... 1 failed (pre-existing)
```

## Issue #66 — CLOB V2 gateway isolation (in progress)

### Summary

_pending_

### Files changed

_pending_

### Tests run

_pending_

### Remaining risks

_pending_

### Commit SHA

_pending_

### Next issue

_#67 — canonical PositionKey + action coordinator_
