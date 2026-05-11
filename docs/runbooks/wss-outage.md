# Runbook — WSS outage

Applies to both `clients/market_ws.py` (market stream) and
`clients/user_ws.py` (user stream).

## Symptoms

- `wss_disconnected`, `wss_heartbeat_stalled`, or `wss_reconnect_failed`
  audit lines.
- Metrics: `wss_disconnects_total` rising, `wss_last_message_age_seconds`
  exceeds the 5 s heartbeat budget.
- Decisions blocked with `quote_stale` / `book_stale` because no fresh
  ticks arrived.
- `/health` shows `market_ws` or `user_ws` as not-ready.

## Diagnosis

1. Distinguish heartbeat stall from full disconnect:
   - Heartbeat stall: socket open, no `PING/PONG` for >5 s — usually a
     proxy or middlebox issue.
   - Full disconnect: `wss_disconnected` followed by reconnect attempts.
2. Check whether the *other* stream (market vs user) is also affected. If
   only one stream is dead, it's a subscription/auth problem; if both are
   dead, it's a network or venue-wide outage.
3. Confirm the bot is performing a REST snapshot on reconnect — search
   logs for `wss_resync_snapshot`. If it isn't, the WS layer is not
   following the reconnect contract.

## Resolution

1. The clients reconnect automatically with exponential backoff. No
   manual action needed for transient drops.
2. If reconnect fails for more than `WSS_MAX_RECONNECT_SECONDS`, the
   `CircuitBreaker` for that stream opens. Follow
   [circuit-breaker-open](circuit-breaker-open.md).
3. **Pause trading** (`KILL_SWITCH=true`) if `wss_last_message_age_seconds`
   stays above the staleness threshold — decisions are already failing
   closed, but the kill switch makes the operator intent explicit.
4. After reconnect, the next REST snapshot fully refreshes `price_cache`
   and `marketMetaService`; do not manually invalidate caches.

## Verification

```bash
curl -s localhost:${HEALTH_PORT:-8080}/health | jq '.checks.market_ws, .checks.user_ws'
grep wss_resync_snapshot logs/audit.jsonl | tail
.venv/bin/pytest tests/release_gate/test_readiness_aggregator.py -v
```

Both `market_ws` and `user_ws` should be `ready: true` and the latest
snapshot timestamp should be within the last reconnect window.

## Post-mortem notes

- Confirm `price_change` size-zero deletes were applied correctly (no
  stale levels remain).
- Confirm the user-stream resync did not double-apply fills — the
  reconciler must reject fills it has already settled.
- If the outage was caused by `assets_ids` subscription drift, audit the
  subscribed token set for stale ids.
