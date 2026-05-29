# Phase 2 Review Prompt A — WSS / Runtime Review

Review the PR for branch `phase/02-price-cache-wss-heartbeat`.

Focus only on:
- correctness of 5-second heartbeat behavior
- reconnect/resync correctness
- websocket payload correctness (`assets_ids`, user/market handling)
- stale cache risks
- event invalidation correctness

Return:
1. critical runtime issues
2. edge cases
3. merge recommendation
