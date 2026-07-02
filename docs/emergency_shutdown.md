# Emergency Shutdown Procedure

This runbook defines the operator's emergency-shutdown procedure for the
Polymarket copy-trading bot. An emergency shutdown is the structured,
replayable action of pulling the bot out of live trading as fast as
deterministically possible while preserving enough audit state to recover
after the cause is fixed.

The mechanism is **two complementary switches**: `KILL_SWITCH` (the
orchestrator-level no-trade hard pause) and `LIVE_TRADING_ENABLED` (the
operator doctor-or-die contract that demands a passing doctor before
re-arming). See `docs/live_runbook.md` for the daily counterpart that
covers benign pausing.

## 1. When to invoke an emergency shutdown

Invoke this procedure when any of the following is observed:

- An order reserved for the leader wallet refers to a tx hash that fails
  the `ambiguous_timeout` probe more than twice in a minute (signals a
  reconciliation state machine wedge, not just a slow RPC).
- An `auto_released_ambiguous` log line fires after the operator had
  flagged the wallet for manual review (race between automated release
  and operator attention).
- The CLOB venue or the underlying CTF/Neg Risk exchange fails a
  collateral-readiness probe with `non_zero_allowance` returned zero
  without an operator-initiated revocation.
- Operator-side suspicion of incident (private key exposure, RPC provider
  outage, leader wallet impersonation) — even if the bot has not logged
  an alarm yet.

## 2. The two switches

### KILL_SWITCH

`KILL_SWITCH=true` is the orchestrator-level emergency pause. The
orchestrator reads it at the top of every loop tick and refuses to enter
the downstream `execute_trade` path; in-flight fills are reviewed by the
post-loop reconciliation. `KILL_SWITCH` does NOT require doctor to flip
back on; it is the soft but immediate switch.

```bash
# Immediate kill-switch (kills trade submission for the next tick and
# backward-forward through in-flight reservations)
sed -i 's/^KILL_SWITCH=.*/KILL_SWITCH=true/' secrets.env
```

The bot's logging surface emits `KILL_SWITCH=on` as a red-magenta banner
(see `utils/logger.py`'s event-tag table) so the operator can confirm by
text-searching the audit log.

### LIVE_TRADING_ENABLED

`LIVE_TRADING_ENABLED=false` flips the bot into a fail-closed state that
will not re-arm without a passing `doctor --live` invocation (the
doctor-or-die contract). This is the structured kill: it survives
operator distraction by demanding a fresh doctor pass before resuming.

```bash
sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=false/' secrets.env
```

Both switches cooperate: `KILL_SWITCH=true` stops new orders; flipping
`LIVE_TRADING_ENABLED=false` next forces a doctor pass on resume.

## 3. Step-by-step emergency shutdown

1. Emit the kill switch and the live-trading false flag, in this order:

   ```bash
   sed -i 's/^KILL_SWITCH=.*/KILL_SWITCH=true/' secrets.env
   sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=false/' secrets.env
   ```

2. Wait for the orchestrator's next loop tick — the loop's `kill_switch`
   read completes within `tick_granularity_seconds` (typed config). The
   audit log should print the kill-switch banner within that window.
3. Confirm no new `execute_intent` log lines appear. The downstream
   `services/execution/coordinator.py` will refuse new requests because
   the upstream orchestrator stopped emitting them.
4. Snapshot the operator DB and audit log into `backups/<timestamp>/`
   (this is what `scripts/rollback.sh --dry-run` does in phase 2/6 —
   reuse that command for the snapshot if possible):
   ```bash
   scripts/rollback.sh --dry-run
   ```
   This produces phase-1/6 and phase-2/6 log lines without mutating the
   working tree.
5. Pull the relevant audit slice (the last minute before the kill-
   switch for provisioning evidence; the `ambiguous_timeout` and
   `auto_released_ambiguous` log tags in particular).
6. Investigate the root cause **before** resuming; the resume procedure is
   in §4.

## 4. Resuming live trading after an emergency

1. Verify the root cause is fixed (operator RPC approval refreshed, the
   wallet identity re-confirmed, the reconciliation wedges cleared).
2. Re-run the doctor live gate:
   ```bash
   python -m bot_cli.main doctor --live
   ```
   The doctor's structured result must print `status=ok` before you
   flip `LIVE_TRADING_ENABLED=true`.
3. Only after the doctor passes:
   ```bash
   sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=true/' secrets.env
   sed -i 's/^KILL_SWITCH=.*/KILL_SWITCH=false/' secrets.env
   ```
   The order matters: first re-arm live-trading (which is gated by the
   doctor), then unset the kill switch (so the next tick actually places
   orders).
4. Watch the audit log for the next minute: the live-trading banner must
   reappear, the `startup_reconcile_unconfirmed` phase must complete
   without raising, and `auto_released_ambiguous` log lines must NOT
   fire for reservations older than the time of the kill-switch event.

## 5. Related docs and code

- `docs/live_runbook.md` — daily operations, including benign pause
  that does not require the doctor.
- `docs/runbooks/rollback.md` — code-level rollback that uses
  `scripts/rollback.sh` and gates on `scripts/rc1_check.sh`.
- `utils/logger.py` — event-tag table including `ambiguous_timeout` and
  `auto_released_ambiguous`.
- `config/app_config.py` — typed `KILL_SWITCH` flag driven from the env
  file; the orchestrator's tick reads this typed config field, not the
  raw `ENV` pragma, so a dangling copy of `KILL_SWITCH` at the env level
  cannot bypass the typed contract.