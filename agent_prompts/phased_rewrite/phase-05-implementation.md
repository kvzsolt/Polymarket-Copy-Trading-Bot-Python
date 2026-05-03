# Phase 5 Implementation Prompt

Repo: `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
Base branch: latest `develop` after Phase 4 merge
Working branch: `phase/05-tests-hardening-release-gate`

Implement **Phase 5 only**.

## Mission
1. Close remaining high-value test gaps for phases 1-4.
2. Harden any tiny non-architectural issues found in prior reviews.
3. Prepare PR for final review and merge.

## Required test matrix
- PositionKey uniqueness
- coordinator lock serialization
- cache TTL + invalidation
- WSS heartbeat at 5s
- reconnect/resync
- issue #38 min sizing behaviors
- effective idempotency
- reservation lifecycle
- reconciler terminal ownership

## Constraints
- No new architecture detours.
- No broad cleanup spree.
- This phase is about proof and hardening.

## Required output
- final test suite updates
- tests passing
- short implementation summary
- git commit for the phase
