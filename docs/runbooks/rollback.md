# Rollback runbook (audit #103)

One-command rollback for live trading. Backs the `scripts/rollback.sh`
script with prose context so an on-call operator does not have to think.

## When to invoke

- duplicate-submission skips spike (gateway error metrics, audit log)
- ambiguous-submission backlog grows past the configured window
- WSS staleness alarms persist after a normal reconnect
- any suspected execution regression in a freshly deployed build

## Sequence (each phase is idempotent / re-runnable)

1. **Pause trading** — script sets `LIVE_TRADING_ENABLED=false` in
   `secrets.env`. The runtime kill switch is the canonical gate; flipping
   it back on is a deliberate human step.
2. **Snapshot** — SQLite DBs + log/audit artifacts copied to
   `backups/<UTC-timestamp>/` so a post-mortem can compare pre/post state
   without re-fetching from the venue.
3. **Revert** — `git checkout <previous-good-sha>` puts the binary back to
   a known state. Optional: omit the SHA to keep the current tree and use
   the script as a snapshot-only tool.
4. **Reconcile** — `python -m bot_cli.doctor --live --no-trade` runs the
   startup reconciliation so the bot's in-memory view matches the venue
   before any new orders flow.
5. **Smoke** — `scripts/rc1_check.sh` runs `compileall` + pytest + forbid
   scans + doctor live gate. Must finish green.
6. **Canary** — optionally re-arm trading with capped notional via
   `scripts/rollback.sh --canary <max_usdc>`. This narrows `BUY_MAX_AMOUNT`
   and `MAX_DAILY_SPEND_USDC` for the first live window. Operator widens
   them again only after the canary window passes without alarms.

## Usage

```bash
# Full rollback to a known-good commit, no canary
scripts/rollback.sh 1a2b3c4d

# Snapshot-only (no checkout) — useful before a risky deploy
scripts/rollback.sh

# Dry-run — log every action without mutating disk / git
scripts/rollback.sh --dry-run 1a2b3c4d

# Rollback + enter canary mode at $50 cap
scripts/rollback.sh 1a2b3c4d --canary 50
```

## Resume gate

The script **does not auto-flip the kill switch back on**. After
rollback completes:

1. confirm `backups/<timestamp>/` contains the expected artifacts
2. confirm `doctor --live` exits 0
3. confirm `rc1_check.sh` exits 0
4. (if canary mode) wait the canary window (operator policy; suggest 1
   trading hour) and check metrics for the symptom that triggered
   rollback
5. flip `LIVE_TRADING_ENABLED=true` manually in `secrets.env`
6. restart the bot process
