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
5. Resume trading with `pmcopy trading resume --confirm LIVE --reason "recovery complete"` *only* after the
   doctor returns `status=ok`. Reference `docs/live_runbook.md` §3 for
   the resume sequence.

## 4. Automatic no-order-id (lost POST) recovery

A POST whose response was lost leaves an `orders` row at
`submitted_unconfirmed` with `order_id IS NULL`. That row is
duplicate-blocking by design, and order-id reconciliation cannot help: the
venue cannot be asked for "the order for this market" without an id.

The runtime resolves such rows automatically on a bounded supervised
schedule (`ambiguous-recovery`; cadence
`reconciliation.ambiguous_recovery.interval_seconds`, default 300s; disable
with `reconciliation.ambiguous_recovery.enabled=false`). The composition
lives in `orchestrators/runtime_bootstrap.py`
(`_build_ambiguous_recovery_service` / `ambiguous_recovery_loop`); the
evidence adapter is `integrations/polymarket/ccxt_ambiguous_evidence.py`.

Each cycle takes at most `batch_size` (default 100) oldest-first candidates
and, once a row is past `grace_seconds` (default 120s), runs one strictly
read-only evidence scan per row:

- the paginated **private fill** history for the row's market;
- that market's **open order** history;
- the account's outcome-token **position** and collateral **balance**.

Identity guarantees (A02):

- **Outcome-token-exact.** Both the persisted row (from the submit request)
  and every venue order/fill observation carry the raw outcome token. A
  match requires token equality on both sides — YES and NO orders of one
  condition share price, size, and side, so only the token separates them.
  A venue row with no token identity is a non-match, never a guess.
- **Strict market scoping.** `list_open_orders(market=...)` rejects any
  row whose payload carries no market identity; an unscoped row can never
  prove which condition (let alone which outcome) it belongs to.
- **Unit-strict sizing.** The row records its amount unit at submission
  time: `quote` (BUY = USDC spend) or `shares` (SELL = outcome shares).
  Fill aggregates are compared in exactly that unit — BUY legs must
  account for the quote spend, SELL legs for the share quantity — and the
  two aggregates are never interchangeable.

Guarantees:

- **Read-only.** The evidence source holds no collaborator able to submit,
  cancel, or otherwise mutate a venue, and it never retries the original
  submission.
- **Fail-closed.** Any transport error, timeout, incomplete pagination,
  missing market identity, unreadable unit identity, or unreadable account
  state marks that part of the scan incomplete. An incomplete scan can
  support neither a recovered identity nor an absence proof, and it resets
  the consecutive-absence counter to zero.
- **No weakened blocks.** Until a row is resolved it stays exactly as
  duplicate-blocking as before; a scan failure changes nothing.
- **Thresholds are unchanged.** Recovery still requires the configured
  `required_absence_scans` (default 3) *consecutive* complete-absence scans,
  each exhausting both private collections and finding every account check
  consistent with absence.

### Legacy rows without unit identity (quarantine)

Rows written before the amount-unit column existed may carry an `amount`
but no `amount_unit`. Identity is never fabricated for them: the scan
fails closed before any private read (log tag
`ambiguous_evidence_missing_unit_identity`), so the row can neither attach
a recovered order id nor accumulate absence scans. Such a row is
*duplicate-blocking forever* — that is deliberate. A row whose unit cannot
be proven cannot safely distinguish "my fill" from "a numerically equal
fill of the opposite kind", so auto-resolution (in either direction) would
risk attaching a foreign identity or double-submitting.

The operator resolution path is the same as for any unresolvable row:
`python bot.py resolve`. If the row's outcome is known from external
evidence (the venue UI, the leader's trade), record it there; do not
back-fill `amount_unit` by guessing.

Outcomes are recorded in `orders.response_json.ambiguous_recovery`:

| Resolution | Effect |
| --- | --- |
| `recovered_order_id` | An authoritative venue order id was attached to the still-`submitted_unconfirmed` row; normal order-id reconciliation then determines fills and terminal state. |
| `confirmed_absent` | After the required consecutive complete-absence scans the row moves to terminal status `confirmed_absent`, which stops duplicate-blocking. |

The same JSON keeps a bounded tail of scan evidence (`scans[]`, default 8
entries) with the completeness flags and account-check results observed at
each attempt.

The manual override still exists: `python bot.py resolve` remains the
operator path for a row the automatic scan cannot resolve — for example a
legacy row with no `token_id`/`condition_id` or no persisted
`amount_unit`, both of which fail closed forever by design. Prefer it
over direct DB edits.

## 5. Invariants the operator must not break

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

## 6. Related docs and code

- `docs/live_runbook.md` — daily operator workflow.
- `docs/runbooks/rollback.md` — code-level rollback procedure.
- `docs/audit/reconcile_state_machine_notes.md` — the full reconciliation
  invariant proof set (post-PR1 audit notes).
- `orchestrators/runtime_bootstrap.py` — the startup orchestrator that
  wires `startup_reconcile_unconfirmed`, `startup_drift_check`, and the
  bounded `ambiguous-recovery` loop.
- `integrations/polymarket/ccxt_ambiguous_evidence.py` — the read-only
  no-ID evidence adapter (§4).
- `tests/release_gate/test_startup_recovery.py` — order-of-calls pinning.
