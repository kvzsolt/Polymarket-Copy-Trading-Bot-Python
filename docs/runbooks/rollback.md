# Rollback Runbook (Audit #103)

This runbook is the operator-facing companion to `scripts/rollback.sh`.
The script is the audit-#103 single-command rollback; this runbook
documents what each phase does, when to invoke it, how to read its
output, and how to resume live trading after a successful rollback. The
flow assumes the operator is following the conditions for a planned
rollback (deterministic cause known) — see `docs/emergency_shutdown.md`
for the emergency counterpart.

## 1. When to run `scripts/rollback.sh`

Run `scripts/rollback.sh` when any of:

- A live-trading session has tripped a regression the operator has
  confirmed exists after the current HEAD commit (regression-of-the-day
  condition), AND the operator has identified a previously-known-good
  commit to revert to.
- The doctor live gate fails in a way that indicates a code-side
  regression rather than a wallet / RPC configuration change.
- The reconciliation state machine wedges to an unrecoverable state on
  HEAD that the previous commit did not exhibit.

If the cause is operator-config (wrong env, wrong leader wallet, wrong
approvals) and not a code change, prefer `docs/emergency_shutdown.md`
instead — a rollback does not fix env issues and may mask them.

## 2. The six phases

`scripts/rollback.sh` runs six phases, each idempotent and re-runnable:

| Phase | Action | Mutating? |
|---|---|---|
| 1/6 | Pause trading via the kill switch (`LIVE_TRADING_ENABLED=false`) | Mutates `secrets.env` only if the file already exists |
| 2/6 | Snapshot SQLite + audit/log JSONLs to `backups/<timestamp>/` | Mutates `backups/` (new directory) |
| 3/6 | `git checkout` the previous-known-good SHA | Mutates the working tree |
| 4/6 | Run `bot_cli.doctor --live --no-trade` | Read-only, but exercises RPC + reconciliation |
| 5/6 | Run `scripts/rc1_check.sh` | Read-only + bytecode compile + pytest |
| 6/6 | Optionally enter "canary mode" caps (`--canary <USDC>`) | Mutates `secrets.env` (set_or_append `BUY_MAX_AMOUNT`, `MAX_DAILY_SPEND_USDC`) |

Dry-run invocation (`--dry-run`) emits each phase's log line and snapshot
labels without mutating anything; canary invocation (`--canary N`) sets
both `BUY_MAX_AMOUNT` and `MAX_DAILY_SPEND_USDC` to `N` USDC, and
appends them when absent (PR-#105 review fix: prior `sed -i || true`
mis-silenced the missing-key case, leaving the operator believing caps
were active when they were not).

## 3. How to invoke

```bash
# Full rollback with a known-good SHA:
scripts/rollback.sh <previous-good-commit-sha>

# Snapshot-only dry-run (no mutation, including no kill-switch flip):
scripts/rollback.sh --dry-run

# Dry-run with explicit SHA (no mutation, but log line shows what would
# happen with the given SHA):
scripts/rollback.sh --dry-run <sha>

# Canary entry after a rollback:
scripts/rollback.sh --canary 50
```

## 4. How to read the output

The script emits `[rollback] phase N/6 — …` log lines. Watch them in
order:

- `phase 1/6 — pause trading (kill switch off)`: confirms the kill switch
  was applied. If `secrets.env` did not exist, the script does NOT create
  it (it's gitignored and operator-owned); the script logs the would-be
  mutation.
- `phase 2/6 — snapshot SQLite + logs to backups/<ts>`: confirms the new
  backup directory. Use that path for any post-mortem evidence request.
- `phase 3/6 — revert working tree to <sha>`: confirms the git checkout
  completed. If no SHA was supplied, the script keeps the current tree
  and logs that.
- `phase 4/6 — startup reconciliation (bot doctor live)`: confirms the
  doctor ran. The doctor's structured result determines whether phase
  5 proceeds; doctor fail stops the script here (exit code
  propagates).
- `phase 5/6 — smoke checks`: confirms rc1_check.sh ran. rc1_check.sh
  exit code propagates; if forbid scans find anything this is the
  hard stop.
- `phase 6/6 — entering canary mode` (only if `--canary` was supplied)
  confirms the caps applied. Otherwise the script logs `no canary
  requested; live trading still paused`.

The script finishes with `[rollback] DONE. Resume is operator-gated: flip
LIVE_TRADING_ENABLED=true manually` — the resume decision is **manually**
owned by the operator; the script does not auto-flip back on.

## 5. Resuming live trading after a rollback

1. Confirm phase 4 (doctor) returned `status=ok`.
2. Confirm phase 5 (rc1_check.sh) returned 0.
3. If canary mode was requested, confirm both `BUY_MAX_AMOUNT` and
   `MAX_DAILY_SPEND_USDC` are set to the canary notional in
   `secrets.env`.
4. Flip the kill switch:
   ```bash
   sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=true/' secrets.env
   ```
5. Watch the audit log per `docs/live_runbook.md` §3 for the first hour.
6. Once the canary window passes without alarms, the operator may lift
   the daily-spend cap. Removing the cap is the inverse `sed -i` —
   leave `BUY_MAX_AMOUNT` and `MAX_DAILY_SPEND_USDC` at the operator-
   comfortable baseline (probably the original values, not unlimited).

## 6. Operator gotchas

- `scripts/rollback.sh` will NOT delete the kill-switch flip after the
  canary window — that's intentional. The kill-switch flip-back is
  manual because "lift the pause" is the human responsibility.
- The script's `run()` helper does not `eval` strings (PR-#105 review
  fix: prior `eval` would break on snapshot filenames containing
  spaces); use positional arguments directly.
- The canary block appends `BUY_MAX_AMOUNT` / `MAX_DAILY_SPEND_USDC`
  when the keys are absent (PR-#105 follow-up `set_or_append_cap`
  helper); operators who have configured neither cap will see both
  added for the first canary invocation.
- The git fetch in phase 3/6 uses `--quiet`; if your working tree has
  unrelated untracked files the checkout may refuse — stash or discard
  first.

## 7. Related docs and code

- `docs/live_runbook.md` — daily operator workflow (resume from this
  rollback completes per the live runbook).
- `docs/emergency_shutdown.md` — operator response to *emergency* pull
  (use instead of this runbook for unknown-cause incidents).
- `docs/recovery_playbook.md` — recovery procedure for
  `auto_released_ambiguous` aftermath (rollback may seed the recovery
  pass through here via phase 4 + 5).
- `scripts/rollback.sh` — the executable referenced above.
- `tests/test_issue103_rollback_script.py` — the test tier that pins
  this runbook presence + the script's basic contract (executable,
  `--dry-run` produces `phase 1/6` + `phase 2/6` log lines and
  `DRY-RUN` banner; canary block appends missing caps).