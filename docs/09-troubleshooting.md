# Troubleshooting

> Common problems, diagnostic techniques, and solutions for the Polymarket Copy Trading Bot.

---

## Bot Won't Start

### Symptom: `EnvValidationError` on Startup

The bot validates all configuration at startup and will refuse to boot if any validation rule is violated.

**Diagnostic steps:**

1. Run `bot doctor --paper` to get a structured list of failing checks:
   ```bash
   python bot_cli/main.py doctor --paper
   ```

2. Check for common issues:
   - Missing required variables (`FUNDER_ADDRESS`, `RPC_URL`)
   - Invalid `PRIVATE_KEY` format (must be 66-char hex)
   - Invalid `FUNDER_ADDRESS` (must be 42-char hex Ethereum address)
   - `CHAIN_ID` not 137 or 80002
   - Python version not 3.11.x or 3.12.x
   - Inline comments in `.env` files (not allowed)

3. Review the error message — the bot aggregates all errors and prints them together.

**Common fix:**
```bash
# Verify Python version
python3 --version  # Must be 3.11.x or 3.12.x

# Verify environment is loaded
python -c "from config.runtime_env import load_runtime_env; load_runtime_env(); from config.env import rehydrate_env; rehydrate_env(); from config.app_config import load_config; load_config(); print('Config OK')"
```

### Symptom: `ModuleNotFoundError: py_clob_client_v2`

The V2 CLOB SDK is not installed.

**Fix:**
```bash
.venv/bin/pip install py-clob-client-v2
```

### Symptom: `CLOB V2 migration not finalized` Warning

The `CLOB_V2_READY` flag is not set.

**Fix:**
```env
CLOB_V2_READY=true
```

---

## Geoblock Failures

### Symptom: Geoblock Check Fails with `geoblock_failed_closed`

The bot queries `https://polymarket.com/api/geoblock` and fails closed if:
- The API is unreachable (network error)
- The response is not a valid JSON object
- The `blocked` field is missing from the response
- The `blocked` field is `true`

**Diagnostic steps:**

1. Test the geoblock endpoint manually:
   ```bash
   curl -s https://polymarket.com/api/geoblock | python -m json.tool
   ```

2. Expected response for allowed regions:
   ```json
   {"blocked": false}
   ```

3. If you get an HTTP error or timeout, check your network connectivity and DNS resolution.

**If you are in a restricted region:** You cannot use the bot for trading on Polymarket. Do not disable `ENFORCE_GEOBLOCK`.

**If the API is temporarily down:** The bot will fail closed and retry on the next cycle. This is expected behavior.

---

## Pending Orders Stuck

### Symptom: Orders in `submitted_unconfirmed` State for Extended Period

Orders can remain in `submitted_unconfirmed` state when:
- The CLOB is slow to confirm fills
- The network connection dropped mid-submission
- The venue returned an unrecognized status

**Diagnostic steps:**

1. Check the orders table:
   ```bash
   sqlite3 data/bot_state.db "SELECT id, status, venue_order_id, created_at FROM orders WHERE status = 'submitted_unconfirmed'"
   ```

2. Check for stale pending orders:
   ```bash
   python bot_cli/main.py doctor --live
   ```
   The doctor checks for orders older than `PENDING_SUBMISSION_TTL_SECONDS`.

3. Review the audit log for the order:
   ```bash
   rg "order_id.*<your_order_id>" log/PolymarketCopyTradingBot.jsonl
   ```

**Solutions:**

- **Wait for reconciliation** — The reconciliation service runs every `RECONCILE_INTERVAL_SECONDS` (default: 5 minutes) and will resolve unconfirmed orders.
- **Enable auto-cancel** — Set `RECONCILE_AUTO_CANCEL=true` to automatically cancel orders older than `RECONCILE_STALE_SECONDS`.
- **Manual resolution** — Check the order on Polymarket's UI and cancel it manually if needed.

### Symptom: Reservation Not Released

If an order reaches a terminal state but the reservation is still active, capital is locked unnecessarily.

**Diagnostic steps:**
```bash
sqlite3 data/bot_state.db "SELECT * FROM reservations WHERE status = 'active'"
```

**Fix:** The reconciliation service should release stale reservations. If it doesn't, restart the bot — expired reservations are cleaned up on startup.

---

## Position Mismatches

### Symptom: Bot Shows Different Positions Than Polymarket UI

This can happen when:
- A fill was missed or not properly persisted
- A redemption occurred outside the bot
- The positions table is out of sync with reality

**Diagnostic steps:**

1. Compare bot positions with the Polymarket Data API:
   ```bash
   curl -s "https://data-api.polymarket.com/positions?user=YOUR_FUNDER_ADDRESS" | python -m json.tool
   ```

2. Check the bot's positions table:
   ```bash
   sqlite3 data/bot_state.db "SELECT * FROM positions WHERE user_id = 'YOUR_FUNDER_ADDRESS'"
   ```

3. Check for fills that weren't applied:
   ```bash
   sqlite3 data/bot_state.db "SELECT * FROM fills ORDER BY created_at DESC LIMIT 20"
   ```

**Solutions:**
- If fills are missing, the reconciliation service should pick them up on the next cycle
- If positions were traded outside the bot (via Polymarket UI), the bot's database will be out of sync — this is expected
- For a fresh start, you can delete the database and let it rebuild: `rm data/bot_state.db`

---

## Reconciliation Issues

### Symptom: Order Stuck in `reconciliation_required`

This state means the reconciliation service could not determine the order's final status.

**Diagnostic steps:**

1. Check the order details:
   ```bash
   sqlite3 data/bot_state.db "SELECT id, status, venue_order_id FROM orders WHERE status = 'reconciliation_required'"
   ```

2. If `venue_order_id` is NULL, the order was never successfully submitted to the CLOB — it's safe to treat as terminal_failed

3. If `venue_order_id` is set, query the CLOB directly:
   ```bash
   curl -s "https://clob.polymarket.com/order/<venue_order_id>" | python -m json.tool
   ```

**Solutions:**
- If the CLOB shows the order as filled, the fill was missed — manually update the database
- If the CLOB shows the order as cancelled/expired, update the order status accordingly
- If the CLOB has no record of the order, it was never submitted — mark as terminal_failed

---

## Database Errors

### Symptom: `sqlite3.OperationalError: database is locked`

SQLite does not support concurrent writes. This error occurs when multiple processes try to write to the same database file simultaneously.

**Fix:**
- Ensure only one bot instance is running against the same database
- Check for zombie processes: `ps aux | grep app.py`
- If using Docker, ensure only one container is accessing the volume

### Symptom: `sqlite3.OperationalError: unable to open database file`

The database directory doesn't exist or is not writable.

**Fix:**
```bash
mkdir -p data
chmod 755 data
```

### Symptom: Schema Mismatch After Upgrade

The bot's migrations may add new tables or columns.

**Fix:**
- The bot applies migrations automatically on startup
- If migrations fail, check the error message and the `migrations.py` file
- As a last resort, backup and recreate: `cp data/bot_state.db data/bot_state.db.bak && rm data/bot_state.db`

---

## Performance Issues

### Symptom: High CPU Usage

The polling loop may be running too aggressively.

**Fix:**
- Increase `FETCH_INTERVAL` (default: 0.25 seconds) to reduce API call frequency
- Increase `LEADER_POSITIONS_MIN_FETCH_SPACING_MS` and `LEADER_ACTIVITY_MIN_FETCH_SPACING_MS`
- Reduce the number of monitored leaders

### Symptom: Slow Reconciliation

A large number of unconfirmed orders can slow down the reconciliation process.

**Fix:**
- Increase `RECONCILE_INTERVAL_SECONDS` to reduce reconciliation frequency
- Enable `RECONCILE_AUTO_CANCEL=true` to clear stale orders automatically
- Manually review and cancel old unconfirmed orders

### Symptom: Memory Growth Over Time

The bot accumulates in-memory state (price caches, position snapshots) that may grow unbounded.

**Fix:**
- Restart the bot periodically (the database persists all state)
- Reduce `META_CACHE_TTL_GOOD` and `META_CACHE_TTL_THIN` to limit cache size
- Monitor memory usage with the metrics endpoint

---

## Log Analysis Tips

### Finding Specific Events

```bash
# All execution events for a specific order
rg '"order_id".*"ord_abc123"' log/PolymarketCopyTradingBot.jsonl

# All risk rejections
rg '"risk_rejected"' log/PolymarketCopyTradingBot.jsonl

# All copy decision results
rg '"copy_decision"' log/PolymarketCopyTradingBot.jsonl

# All fills in the last hour
rg '"execution_filled"' log/PolymarketCopyTradingBot.jsonl | \
  python -c "import sys, json; [print(json.dumps(json.loads(l))) for l in sys.stdin if json.loads(json.loads(l).get('payload', '{}').get('timestamp', 0)) > time.time() - 3600]"
```

### Checking PnL

```bash
# Review PnL log entries
python -c "
import json
for line in open('log/pnl.jsonl'):
    entry = json.loads(line)
    print(f'{entry.get(\"timestamp\", \"?\")}: {entry.get(\"market\", \"?\")} PnL={entry.get(\"pnl_usdc\", 0)}')
"
```

### Database Queries

```bash
# Count orders by status
sqlite3 data/bot_state.db "SELECT status, COUNT(*) FROM orders GROUP BY status"

# Recent fills
sqlite3 data/bot_state.db "SELECT * FROM fills ORDER BY created_at DESC LIMIT 10"

# Active reservations
sqlite3 data/bot_state.db "SELECT * FROM reservations WHERE status = 'active'"

# Risk blocks
sqlite3 data/bot_state.db "SELECT * FROM risk_blocks ORDER BY created_at DESC LIMIT 10"
```

---

## Next Steps

- See [07-execution-lifecycle.md](07-execution-lifecycle.md) for understanding order states.
- See [10-security.md](10-security.md) for security-related issues.
