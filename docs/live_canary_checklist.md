# Live Canary Checklist

When to use: walking through every gate before the FIRST real live order.

Print this page. Tick each box. Don't skip.

## Pre-flight

- [ ] Branch is `live-trading`, tree clean.
- [ ] `pytest` passes (`1679 passed` or higher, 0 failed).
- [ ] `bash scripts/rc1_check.sh` exits 0 with all green ticks.
- [ ] `python -m bot_cli.main doctor --live` returns `status=ok` or `status=warning`
      with every warning reviewed and understood.
- [ ] Wallet balance large enough for the canary order PLUS gas reserve.
- [ ] Operator pager / phone ready.

## Canary configuration

- [ ] `LIVE_CANARY_MODE=true`
- [ ] `LIVE_CANARY_MAX_NOTIONAL_USDC=1.00` (or another small number you can lose)
- [ ] `LIVE_CANARY_MAX_ORDERS=1`
- [ ] `LIVE_CANARY_REQUIRE_OPERATOR_ACK=true`
- [ ] `LIVE_CANARY_ACK_TEXT=I_UNDERSTAND_LIVE_TRADING_RISK`

## Safety env

- [ ] `PAPER_TRADE_ONLY=false`
- [ ] `CLOB_V2_READY=true`
- [ ] `KILL_SWITCH=false`
- [ ] `POSITION_DRIFT_CHECK_ON_STARTUP=true`
- [ ] `POSITION_DRIFT_FAIL_CLOSED=true`

## Start

- [ ] Run `python -m bot_cli.main run`.
- [ ] Boot sequence: market data runtimes start, user data runtimes start,
      `startup_reconcile_unconfirmed` summary printed, `startup_drift_check`
      returns clean, monitors start.

## Observe one full lifecycle

When the first canary signal arrives:

- [ ] `📤 ORDER SUBMITTED — BUY` or `⏳ ORDER UNCONFIRMED` panel.
- [ ] `📘 ORDER LIVE` panel (or `⏳ ORDER DELAYED` → `📘 ORDER LIVE`).
- [ ] `🔄 ORDER MATCHED` panel (may be immediate for marketable orders).
- [ ] `✅ TRADE CONFIRMED` panel — terminal.

OR one of:

- [ ] `❌ ORDER FAILED` / `❌ ORDER CANCELLED` — read `reason` carefully.
- [ ] `⚠️ ORDER AMBIGUOUS — TIMEOUT` — investigate before second run.

## Verify state

- [ ] `drift_ok` remains `true` on `/health/ready`.
- [ ] No `ambiguous_timeout` rows in `data/bot_state.db.orders`.
- [ ] `position_drift` JSONL shows `clean` for the canary's scope.
- [ ] Wallet history matches the panel size + price.
- [ ] `LIVE CANARY LIMIT REACHED submitted=1/1` log appears after the cap.
- [ ] Subsequent signals on the same scope return `skip_code="live_canary_limit"`.

## Wind down

- [ ] Stop the bot (`Ctrl+C`).
- [ ] If everything matches, flip `LIVE_CANARY_MODE=false` and restart per
      `live_runbook.md` step 4.
- [ ] If anything looks wrong, follow `emergency_shutdown.md`.

## Common gotchas

- Ack text typo blocks doctor — must be exactly `I_UNDERSTAND_LIVE_TRADING_RISK`.
- `LIVE_CANARY_MAX_ORDERS=0` blocks every submit. That's the "dry live" mode.
- The canary counter is per-process. Don't expect it to survive restarts.
- Polymarket sometimes returns `delayed` for several seconds. Read the
  recovery playbook before assuming an order is stuck.
