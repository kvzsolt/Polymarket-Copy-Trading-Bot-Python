# Runbook — CLOB outage

## Symptoms

- Repeated `clob_*` errors in logs (`clob_http_5xx`, `clob_timeout`,
  `clob_signing_failed`, `clob_unreachable`).
- `metrics`: `clob_request_errors_total` rising, `clob_request_latency`
  spiking.
- `bot doctor --paper` (or `--live`) fails the `clob_reachable` check.
- `/health` returns 503 with `clob` listed as not-ready.
- New BUY/SELL intents stuck in `pending_lock` or rejected with
  `venue_unreachable`.

## Diagnosis

1. Confirm scope: is it the CLOB REST API, the signing endpoint, or both?
   - REST: `curl -s -o /dev/null -w '%{http_code}\n' https://clob.polymarket.com/`
   - Signing: check `clob_signing_failed` audit lines.
2. Check the configured RPC: a Polygon RPC outage will surface as
   `clob_signing_failed` even when the CLOB itself is healthy.
   - `curl -s -X POST -H 'Content-Type: application/json' -d '{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}' "$RPC_URL"`
3. Look at `metrics`: is the `CircuitBreaker` for `clob` already `OPEN`? If
   so, this turns into the
   [circuit-breaker-open](circuit-breaker-open.md) runbook.

## Resolution

1. **Pause new trading** — set `KILL_SWITCH=true` in `secrets.env`.
   The bot continues to reconcile open orders but stops creating new
   intents.
2. **Do not** SIGKILL. Let in-flight intents reach a terminal state through
   the reconciler. Reservations release on terminal status.
3. If the outage is RPC-only, switch `RPC_URL` to a healthy provider and
   restart with `python bot.py` once trading is paused. Confirm with
   `bot doctor --live`.
4. If the outage is the CLOB itself, leave `KILL_SWITCH=true` until the
   public status page reports recovery.

## Verification

```bash
.venv/bin/python bot.py doctor --paper
curl -s localhost:${HEALTH_PORT:-8080}/health | jq .
.venv/bin/pytest tests/release_gate/test_readiness_aggregator.py -v
```

All three should be green before clearing the kill switch.

## Post-mortem notes

- Capture the outage window from the audit log (`logs/audit.jsonl`).
- Confirm no orders were submitted with stale book data — search the audit
  log for `book_stale` skips during the window.
- File issues for any reservation that did not release within the
  `EXECUTION_PENDING_SUBMISSION_TTL_SECONDS` window after CLOB recovered;
  inspect with `bot stale-locks list`.
