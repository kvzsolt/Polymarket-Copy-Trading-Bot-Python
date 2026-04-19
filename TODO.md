# TODO

Project status: frozen / on ice as of 2026-04-19.

## Highest-value follow-ups when this repo wakes back up

1. Prevent duplicate SELLs on pending/unconfirmed exits.
   - Add explicit order-intent / order-state tracking.
   - Reserve size while a SELL is submitted but not yet reconciled.
   - Block follow-up SELLs against reserved size.

2. Make reconciliation the source of truth.
   - Do not treat `success: true` or `status: delayed` as a confirmed exit.
   - Drive final position state from venue reconciliation.
   - Add clear terminal handling for filled / cancelled / rejected / expired.

3. Finish the full shared price-cache rollout.
   - Current work centralizes the risk path.
   - Later, audit remaining direct `get_price(...)` calls and unify them behind one cache layer where appropriate.

4. Harden API-only trading assumptions.
   - Treat UI state as advisory, not truth.
   - Add AFK-safe guardrails for pending exits.
   - Improve logs/metrics around submitted-vs-confirmed order state.

5. Complete execution lifecycle hardening.
   - Per-position lock.
   - Reserved-aware BUY/SELL handling.
   - Explicit lifecycle state machine.
   - Venue gateway abstraction if/when the refactor resumes.

6. Optional later work
   - Run the benchmark plan in `benchmark.md`.
   - Revisit the execution-lifecycle strategy document when there is energy for a real hardening pass.

## Notes

- Advanced exits are the intended live direction.
- Primitive hard stop-loss / single take-profit should stay retired from the active runtime path.
- Do not wake this project back up with a giant speculative rewrite.
