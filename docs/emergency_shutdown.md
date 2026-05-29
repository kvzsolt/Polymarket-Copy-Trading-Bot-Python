# Emergency Shutdown

When to use: anything looks wrong — runaway orders, drift cascade, unexpected
fills, network split, manual venue activity, panic.

## Order of operations

1. **Set the kill switch.** This blocks every new submission immediately.
   ```bash
   export KILL_SWITCH=true
   ```
   The next `doctor --live` run will report `kill_switch_off` as **blocked**.

2. **Stop the bot process.**
   ```bash
   kill -TERM <pid>       # preferred — graceful
   # or
   pkill -TERM -f bot_cli.main
   ```
   Wait for the runtime to print `market_data_runtime_stopped` and
   `user_data_runtime_stopped`.

3. **Check open orders on the venue.** The bot's CLI:
   ```bash
   python -m bot_cli.main orders --status open
   ```
   Cross-check with Polymarket UI. Any order that the bot did NOT just print
   `✅ TRADE CONFIRMED` for is suspect.

4. **Cancel open orders if needed.**
   ```bash
   python -m bot_cli.main orders --cancel <order_id>
   ```
   Repeat per suspect order. Polymarket also allows manual cancellation in
   their web UI.

5. **Inspect ambiguous lifecycle rows.** Open `data/bot_state.db`:
   ```bash
   sqlite3 data/bot_state.db <<'SQL'
   .headers on
   SELECT order_id, status, request_json, response_json, updated_at
   FROM orders
   WHERE status IN (
       'submitted_unconfirmed',
       'submitted_delayed',
       'order_live',
       'matched_pending_confirmation',
       'ambiguous_timeout',
       'auto_released_ambiguous'
   )
   ORDER BY updated_at DESC LIMIT 50;
   SQL
   ```
   Anything in `ambiguous_timeout` needs operator decision before restart —
   poll the venue manually or wait for the auto-release-grace path described
   in `recovery_playbook.md`.

6. **Inspect active reservations.**
   ```bash
   python -m bot_cli.main reservations --active
   ```
   A reservation with `status='active'` but no linked `order_id` should be
   released manually:
   ```bash
   python -m bot_cli.main reservations --release <reservation_id> --reason manual_shutdown
   ```

7. **Restart in paper mode first.**
   ```bash
   PAPER_TRADE_ONLY=true python -m bot_cli.main run
   ```
   Confirm `startup_reconcile_unconfirmed` + `startup_drift_check` complete
   cleanly. Only flip back to live after `KILL_SWITCH=false` and a fresh
   `doctor --live` run.

## What to write down

- Timestamp of the emergency stop (UTC).
- Open orders that were live at stop time.
- Reservations that needed manual release and their reasons.
- Any `ambiguous_timeout` rows and their `order_id`s.
- Wallet activity in the 10 minutes before + after stop.

Keep that note alongside the JSONL log file. The recovery playbook uses it
to decide whether re-entering live is safe.

## Do not

- Do not delete rows from `orders` or `order_reservations`. The reconciler
  will lose track.
- Do not flip `KILL_SWITCH=false` and restart in one motion. Always paper
  first and read the drift/reconcile output.
- Do not bypass `doctor --live`. The doctor is the operator gate.
