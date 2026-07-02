# Recovery Playbook

This playbook documents how the Polymarket copy-trading bot recovers from
a restart (planned or unplanned) and how the operator should react when
reconciliation cannot release a reservation before the
`ambiguous_timeout` fires. The recovery sequence is the bot's own startup
contract; the operator's role is to verify each phase and to never
force-flip a flag that the bot itself orchestrates.

## 1. The startup reconciliation contract

On every startup (live or paper), the bot:

1. Loads `AppConfig` via the typed env loader (fail-closed on schema
   violations; see `tests/test_config_smoke.py`).
2. Runs `startup_reconcile_unconfirmed` — a coroutine that takes the list
   of in-flight user contexts (those flagged `restart_reconcile=true`),
   re-hydrates their outstanding reservations against the venue, and
   marks any unfillable reservation as `auto_released_ambiguous` if its
   probes have aged out past `ambiguous_timeout`.
3. Runs `startup_drift_check` — compares the persisted in-memory state
   (positions, reservations) against the venue's REST snapshot of the
   funder wallet; emits drift warnings and surfaces restart-critical
   divergences.
4. Opens the cycle of monitors (the per-market WSS, the strategy profiler
   background tasks, the per-tick reconciliation pass).

This ordering is enforced by `tests/release_gate/test_startup_recovery.py`
(source ordering: `startup_reconcile_unconfirmed` precedes
`startup_drift_check` precedes any `monitor_*` open). Do NOT reorder
outside of a deliberate PR that revises the release-gate tests.

## 2. When `auto_released_ambiguous` fires

A reservation that the bot flagged `ambiguous` (multiple probes contradict
each other) eventually releases itself once all probes return to a clean
state. The release logs as `auto_released_ambiguous`. The operator's
contract:

- Do not submit a replacement order for an `auto_released_ambiguous`
  reservation. The reservation's idempotency key already reached the
  venue; re-submitting risks EXACTLY the kind of double-copy trade that
  the recovery state machine is designed to prevent. See
  `docs/audit/reconcile_state_machine_notes.md` for the invariant proof.
- Read the audit log for the reservation's `ambiguous_timeout` value.
  If it fired because every probe timed out, that signals RPC failure
  — surface it to the operator kill-switch decision (see
  `docs/emergency_shutdown.md`). If it fired because probes were
  inconclusive, surface it to the strategy review queue (the next
  `doctor --live` pass will refuse the reservation and emit a
  `reconcile_non_terminal` log tag).
- The reconciliation state machine guarantees absence of TTL-only
  releases — `auto_released_ambiguous` requires *all probes* clean,
  regardless of the TTL; the TTL only triggers the probe check, never
  the release itself. This invariant is pinned by the test
  `tests/test_reconciliation_state_machine_phase9.py::
  test_auto_released_ambiguous_is_terminal`.

## 3. Operator recovery procedure

1. Snapshot the recent state — even before diagnosing, snapshot:
   ```bash
   scripts/rollback.sh --dry-run
   ```
   The dry-run completes phase-1/6 (kill switch flip — no mutation in
   dry-run mode) and phase-2/6 (snapshot), logging the new
   `backups/<ts>` directory used as evidence.
2. Diagnose the wedge:
   - Look for `auto_released_ambiguous` and `ambiguous_timeout` log tags
     in the snapshot.
   - If `auto_released_ambiguous` fired *while* the bot was paused for a
     planned restart, no replacement order was placed — the audit log's
     executing line count for the affected `condition_id` should be
     exactly equal to the un-paused baseline.
   - If the log shows a duplicate submitting (banner: `already_submitted`),
     do NOT recover by removing the reservation manually; force the
     `doctor --live` to re-verify.
3. Re-run doctor live (doctor-or-die):
   ```bash
   python -m bot_cli.main doctor --live
   ```
4. Re-run smoke checks:
   ```bash
   scripts/rc1_check.sh
   ```
5. Resume trading: flip `LIVE_TRADING_ENABLED=true` *only* after the
   doctor returns `status=ok`. Reference `docs/live_runbook.md` §3 for
   the resume sequence.

## 4. Invariants the operator must not break

- `startup_reconcile_unconfirmed` runs before any monitor subscription
  opens. Do not skip it even in dry-run mode — the reservation rehydration
  cascade populates state that the dry-run executor depends on.
- `startup_drift_check` runs strictly after the reconciler; do not
  reorder so that the drift check sees pre-reconcile state (drift
  detection must observe re-hydrated reservations to compare against the
  venue's authoritative snapshot).
- `auto_released_ambiguous` is terminal — once emitted, the reservation
  is gone; re-submitting it does NOT preserve the idempotency key and
  may duplicate an order. Treat the auto-release as authoritative.

## 5. Related docs and code

- `docs/live_runbook.md` — daily operator workflow.
- `docs/runbooks/rollback.md` — code-level rollback procedure.
- `docs/audit/reconcile_state_machine_notes.md` — the full reconciliation
  invariant proof set (post-PR1 audit notes).
- `orchestrators/runtime_bootstrap.py` — the startup orchestrator that
  wires `startup_reconcile_unconfirmed` and `startup_drift_check`.
- `tests/release_gate/test_startup_recovery.py` — order-of-calls pinning.