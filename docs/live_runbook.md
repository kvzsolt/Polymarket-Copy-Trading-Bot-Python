# Live Operations Runbook (RC1)

This runbook is the operator-facing entry point for the Polymarket
copy-trading bot. It documents how to start live trading, what the
pre-flight gate (`doctor`) checks, and how to read the live-trading
indicators so that an operator on call has a single page to consult when
they need to bring the bot up after a maintenance window or a rollback.

This document is part of the RC1 release artifact bundle shipped with the
bot (`scripts/rc1_check.sh` validates its presence). Ownership: the live
operator. Update cadence: refresh whenever the startup contract changes
(LIVE_TRADING_ENABLED,KILL_SWITCH, doctor model, or
reconciliation phases).

## 1. Roles of the live operator

The live operator is the human on the keyboard who:

- owns the secrets (`secrets.env` / `config/runtime.local.env`) for the
  funder wallet;
- decides when the bot leaves dry-run / paper mode and enters live;
- gates the kill switch (`LIVE_TRADING_ENABLED`, `KILL_SWITCH`);
- runs the pre-flight doctor and rc1_check before flipping the switch;
- watches the audit log and reconciliation output for the first hour
  of live trading.

The bot itself is fail-closed: doctor-or-die. Live trading cannot start
unless the doctor passes; the kill switch must be explicitly flipped back
on by the operator after a rebound or rollback (see `docs/runbooks/rollback.md`).

## 2. Pre-flight contract

Before flipping `LIVE_TRADING_ENABLED=true`, the operator runs the
pre-flight gate. Conceptually there are two layers:

1. **`doctor`** — a structured readiness probe that the bot runs before
   it ever sends a live order. The doctor checks wallet config, R/W RPC,
   CLOB V2 collateral readiness (both the CTF and Neg Risk exchanges),
   recovery state on startup, and book-quality gates. Run:

   ```bash
   python -m bot_cli.main doctor --live
   ```

   The doctor's exit code is non-zero if it blocks (status `BLOCKED`) so a
   CI pipeline or operator shell can gate the next step on it. The bot's
   startup (`app.py`) awaits the doctor before `build_app` — see the
   tests in `tests/test_doctor_app_startup.py` for exact ordering.

2. **`scripts/rc1_check.sh`** — runs `compileall` over the production
   Python tree, runs `pytest -q --tb=line` against the full check tier,
   runs the forbid scans for legacy code (`services.price_cache`,
   `PositionPriceCache`, `MarketMetaService`) and the unguarded
   `execution_service.execute` alias scan, then optionally invokes
   `doctor --live` again if the wallet secrets are exported. Exits non-zero
   on the first blocker.

Both steps are re-runnable; the operator is expected to run them after
code changes, secrets rotation, or a rollback.

## 3. Starting live trading

After the pre-flight passes:

1. Confirm `LIVE_TRADING_ENABLED=true` is present in `secrets.env` (and
   `KILL_SWITCH` is *not* set — both gates are fail-closed).
2. Start the bot:

   ```bash
   python -m bot_cli.main trade --live
   ```

   `--live` is the only way to invoke the live trading mode; in paper
   mode the doctor still runs but the execution path routes to the
   simulator and the risk envelope never connects to the CLOB venue.
3. Watch the audit log. The expected first-hour log lines include a
   `do_operator_or_die` pass, the doctor's structured result, the
   `startup_reconcile_unconfirmed` phase completion, and the
   `startup_drift_check` phase. None of those lines should be missing
   in the first minute of a healthy live start.

## 4. Common alarms during live operation

| Log signature | Meaning | Operator action |
|---|---|---|
| `KILL_SWITCH=on` | Operator kill switch is set — bot refuses to trade | Unset the kill switch only after confirming operator intent (see `docs/emergency_shutdown.md`). |
| `ORDER AMBIGUOUS — TIMEOUT` | A reservation's `ambiguous_timeout` fired before all probes landed; reservation may be auto-released | Read `docs/recovery_playbook.md` for the `auto_released_ambiguous` semantics before re-submitting. |
| `reconcile_ambiguous_block` | Tail-end reconciliation could not release a reservation | Review the pending fill ID; do not manually force-release until `doctor` has re-checked. |
| `collateral_readiness.fail` | pUSD balance or allowance against the CTF / Neg Risk exchange dropped | Top up the funder wallet or refresh the ERC-20 approval; the bot fail-closes. |

## 5. Pausing live trading

Pause the bot without killing the process (graceful, fine-grained):

```bash
# Soft pause via the kill switch
sed -i 's/^KILL_SWITCH=.*/KILL_SWITCH=true/' secrets.env
# Hard pause via LIVE_TRADING_ENABLED
sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=false/' secrets.env
```

Either flag flips the bot to no-trade mode without restarting the process
(the orchestrator watches both flags at the top of each loop tick).

## 6. Related docs

- `docs/emergency_shutdown.md` — when to pull the kill switch and what to
  inspect on resume.
- `docs/recovery_playbook.md` — how the bot resumes after a restart,
  including the `auto_released_ambiguous` follow-up.
- `docs/runbooks/rollback.md` — operator-facing rollback procedure that
  calls `scripts/rollback.sh`.
- `docs/rc1_validation.md` — RC1 release-criteria checklist
  (`scripts/rc1_check.sh`).
- `docs/known_limitations.md` — known limitations: http polling,
  the external execution path, and the venue contract surface.