# Phase 3 Review Prompt B — Idempotency / Regression Review

Review the PR for branch `phase/03-issue-38-min-order-idempotency`.

Focus only on:
- whether idempotency is computed from effective adjusted request
- whether retries / duplicates / stale raw-signal logic can still break execution
- missing tests for below-min paths

Return:
1. duplication/regression risks
2. test gaps
3. merge recommendation
