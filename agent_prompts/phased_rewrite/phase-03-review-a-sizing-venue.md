# Phase 3 Review Prompt A — Sizing / Venue Semantics Review

Review the PR for branch `phase/03-issue-38-min-order-idempotency`.

Focus only on:
- whether venue metadata drives min sizing correctly
- whether BUY/SELL behavior is safe and policy-gated
- whether any hardcoded `5` venue-truth assumptions remain in the active path
- tick/precision/Decimal correctness

Return:
1. critical sizing issues
2. venue-semantic concerns
3. merge recommendation
