# Runbook — Circuit breaker open

`services/circuitBreaker.py` wraps external dependencies (CLOB REST,
WSS reconnects, RPC). When repeated failures cross the threshold, the
breaker transitions `CLOSED → OPEN` and rejects requests until the
cooldown elapses, then transitions `OPEN → HALF_OPEN` and admits a single
probe request.

## Symptoms

- Audit lines `circuit_open` or transition logs
  `Circuit '<name>' transitioning OPEN → HALF_OPEN after Ns cooldown`.
- Metrics: `circuit_breaker_state{name=...}` reads as `open` (`1`) or
  `half_open` (`2`).
- Trading paused for the affected dependency; intents fail fast with
  `circuit_open`.

## Diagnosis

1. Identify which breaker is open:
   ```bash
   curl -s localhost:${HEALTH_PORT:-8080}/metrics | grep circuit_breaker_state
   ```
2. Find the recent failure cluster that tripped it:
   ```bash
   grep -E "circuit_open|circuit_failure_recorded" logs/audit.jsonl | tail
   ```
3. Decide whether the underlying dependency is *actually* healthy now. The
   probe in `HALF_OPEN` does that automatically — do not race it manually.

## Resolution

1. **Do nothing first.** The breaker recovers itself: cooldown elapses →
   single probe → success closes the circuit.
2. If the dependency is known-recovered but the cooldown is long, you can
   restart the bot to reset the breaker state to `CLOSED`. Pause trading
   first (`KILL_SWITCH=true`) and let the reconciler drain.
3. If the underlying dependency is genuinely down, follow the matching
   runbook ([clob-outage](clob-outage.md), [wss-outage](wss-outage.md))
   and leave the breaker alone. It will reopen on the next probe failure.

Do **not** edit breaker thresholds at runtime. They live in typed
`AppConfig` and changes require a restart.

## Verification

```bash
curl -s localhost:${HEALTH_PORT:-8080}/metrics | grep circuit_breaker_state
grep "Circuit .* transitioning .* CLOSED" logs/audit.jsonl | tail -5
```

Expect `0` (`CLOSED`) for every breaker after recovery.

## Post-mortem notes

- Note the dependency, the failure rate, and whether the threshold should
  change. Any threshold change must land via a typed-config PR — never as
  an env-only override.
- If the breaker tripped because of stale-data fail-closed behaviour
  rather than a true upstream outage, that is correct behaviour, not a
  bug.
