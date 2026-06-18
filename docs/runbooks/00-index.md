# Runbook index

Operator runbooks for incident response. Each runbook follows the same
structure: **Symptoms → Diagnosis → Resolution → Verification → Post-mortem
notes**. Skim symptoms first; jump straight to resolution if the diagnosis
is obvious.

| Runbook | When to use |
|---|---|
| [`clob-outage.md`](clob-outage.md) | CLOB REST or signing endpoints failing or returning 5xx for sustained period. |
| [`wss-outage.md`](wss-outage.md) | Market or user WSS disconnects, heartbeat stalls, or stuck subscriptions. |
| [`circuit-breaker-open.md`](circuit-breaker-open.md) | A `CircuitBreaker` for an external dependency is `OPEN` and trading is paused. |
| [`stale-locks.md`](stale-locks.md) | Orders are stuck in a `pending_lock` / `submitting` state past the submission TTL. |
| [`reconciliation-drift.md`](reconciliation-drift.md) | Persisted positions / fills disagree with venue state. |
| [`manual-off-bot-changes.md`](manual-off-bot-changes.md) | A SELL hits `insufficient_balance`, or you trade manually while the bot is running. |

## Conventions

- Commands assume repo root and an active project venv (`.venv/`).
- All CLI commands listed are read-only by default; mutating subcommands are
  explicitly noted.
- "Pause trading" means setting the kill switch (`KILL_SWITCH=true` in
  `secrets.env`, see `../05-risk-controls.md`) and waiting for in-flight
  intents to settle. Do **not** SIGKILL the process — the reconciler must
  terminate cleanly to release reservations.
- For the architectural background that informs each procedure, see
  [`../architecture.md`](../architecture.md).
