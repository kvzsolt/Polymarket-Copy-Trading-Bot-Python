# Concurrency & Engine-Conflict Audit — Response

Branch: `feature/concurrency-hardening` (cut from `develop` @ `004e7f0`).

This document is the result of reviewing the external "Concurrency & Engine
Conflict Audit" **against the actual code**. The audit was written as a generic
template ("the system *likely* uses…", "*if* dedupe is done using…") and
prescribes `threading.Lock`, a synchronous `ExecutionRegistry`, and an
event-queue rewrite. None of that fits this codebase: it is an **asyncio
single-loop** application, and **all eight audit categories are already
implemented** — several with inline comments that literally name the audit's own
IDs (`C2`, `C4`).

Per the agreed scope ("review current solutions against the audit; fix only what
is really needed; no threads"), this is a review-first pass. Each finding is
mapped to concrete evidence with a verdict. **One genuine, narrow residual gap
was found (C8-adjacent); see "Residual gap" below.**

## Verdict matrix

| ID | Audit concern | Status | Evidence |
|----|---------------|--------|----------|
| C1 | Duplicate-order race (ws + poll overlap) | **Already handled** | `storage/postgres/orders_repo.py` `try_claim_processed_trade` — atomic `INSERT … ON CONFLICT DO NOTHING RETURNING` (only one caller wins); `orchestrators/trade_pipeline.py:1395` claims before execute; copy trades route through `coordinator.submit_copy_trade` → `execute_intent`, serialized per scope. |
| C2 | Reservation leak on mid-flight failure | **Already handled** | `services/execution/coordinator.py:259-433` `_execute_intent_locked`: explicit `_release()` on every terminal path (create-failed, retryable, terminal, unknown), a `keep_reservation` flag for pending-reconciliation, and a `BaseException` safety net (420-433) that releases anything that escapes un-released. Reservations are DB-persisted (`storage/postgres/reservations_repo.py`), so a crash cannot lose them. |
| C3 | Manual / off-bot trade desync | **Already handled** | `services/manual_sync_service.py` (`ManualSyncService`, WSS-driven) + `coordinator._reconcile_drift_from_intent` (coordinator.py:481) emitting `manual_position_drift_reconciled`; insufficient-balance SELL drift is reconciled (coordinator.py:331-337). |
| C4 | Non-atomic balance/position/reservation update | **Already handled** | Settlement is reconciler-owned; order-state changes go through `services/execution/state_machine.py` `assert_transition_allowed`; reservation capacity checks + inserts are single advisory-lock transactions in `reservations_repo`. |
| C5 | Cancel/modify vs fill race | **Already handled** | Order lifecycle is a guarded state machine: `services/execution/state_machine.py:assert_transition_allowed` rejects illegal transitions; `services/intentRegistry.py` tracks intent state. |
| C6 | Lock scope (global vs none) | **Already handled** | `services/positionActionCoordinator.py:73` — per-`PositionKey` `asyncio.Lock` (`_locks: dict[(user,condition,token), asyncio.Lock]`), created on demand. Not global, not threaded; concurrency across distinct scopes is preserved. |
| C7 | ws/poll double-trigger | **Already handled** | Same atomic `try_claim_processed_trade` claim as C1 dedups duplicate detections regardless of source. |
| C8 | No reconciliation watchdog | **Mostly handled — one gap** | Startup reconcile (`orchestrators/runtime_bootstrap.py:startup_reconcile_unconfirmed`), WSS own-fill reconcile (`app.py` wake-reconciler → `reconcile_submitted_unconfirmed`), and an in-session expiry reaper (`services/risk/risk_monitor.py:_reap_expired_reservations_for_context` → `reservation_service.release_expired_active`). **See gap below.** |

The audit's recommended "single-writer event queue / threading" rearchitecture is
**explicitly rejected**: it would duplicate the coordinator/reservation/
idempotency/reconciliation services that already exist and introduce thread
primitives into an asyncio event loop. No code was changed for C1-C7.

## Residual gap (C8-adjacent): in-session reservation reaping is gated behind risk management

**Finding.** The only in-session caller of `release_expired_active` (the TTL
expiry reaper) is the risk-monitor loop
(`services/risk/risk_monitor.py:_reap_expired_reservations_for_context`). Its own
docstring states: *"Without an in-loop call a reservation stranded mid-session …
would never be reaped until the next restart."* The standalone periodic
`reconciliation_loop` (`services/reconciliation.py:856`) exists but is **never
started** as a task in non-test code.

**Impact.** When `enable_risk_mgmt = false`, a reservation whose linked order is
stuck in `submitted_unconfirmed` past its TTL — and for which no user-WSS
terminal event arrives to trigger `reconcile_submitted_unconfirmed` — is not
reaped until the process restarts. The capital (BUY) or shares (SELL) stay
reserved, blocking new intents on that scope. This is the audit's "phantom
reserved funds" / "drift not auto-corrected" concern, narrowed to the
risk-disabled configuration. It is fail-*closed* (it blocks rather than
double-trades), so it is a reliability/capital-availability gap, not a safety
breach.

**Why it isn't covered by the existing paths:**
- Startup reconcile runs once, not in-session.
- WSS own-fill reconcile only fires when the venue pushes a terminal event for
  *our* order; a silently-expired/never-acked order produces no such push.
- The expiry reaper is the catch-all for that case — but it only runs if the
  risk loop runs.

**Fix (implemented — asyncio-native, reuses existing code, no rearchitecture):**
`app.py` now starts a standalone reservation reaper **only when
`enable_risk_mgmt` is false** (so the risk-on path is byte-for-byte unchanged).
The reaper periodically (interval = `polling.reconcile_interval_seconds`, floored
at 30s) calls the new testable helper `app._reap_reservations_once(contexts)`,
which delegates to the existing, vetted
`services.risk.risk_monitor._reap_expired_reservations_for_context` →
`reservation_service.release_expired_active()`. That path only releases
reservations whose linked order reconciliation confirms is venue-terminal, so
duplicate-order and stale-lock protections are preserved. The task is cancelled
on shutdown via `_shutdown_runtime` (new `reservation_reaper_tasks` param).
Regression tests: `tests/test_reservation_reaper.py`.

## Conclusion
The concurrency audit's P0/P1 items are already implemented correctly for an
asyncio application; no threading rewrite is warranted or safe. The single
actionable item — the risk-gated reservation reaper (C8-adjacent) — has been
fixed with the minimal, reuse-existing change described above.
