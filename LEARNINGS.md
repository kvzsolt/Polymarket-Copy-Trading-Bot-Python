
**[2026-04-17] — Polymarket copy-bot silent sell rejection + 5-share min**
- Observation: Silent `bad_request` for sells was hiding the real CLOB error ("Size (X) lower than the minimum: 5"). `redact_error_detail()` was stripping the CLOB body.
- Observation: Polymarket CLOB enforces hard 5-share minimum on tick_size=0.01 markets. Lowering `COPY_MIN_SHARES` below 5 causes silent reject loops.
- Observation: Executor was retrying 3x on guaranteed-to-fail size-below-min errors — wasting ~1.5s per order.
- Action: Keep `max(5.0, COPY_MIN_SHARES)` hard floor in `CopyDecisionService`. Default env to 5.
- Action: In `createClobClient.post_signed_order`, surface raw CLOB body as `friendly_error` (tagged `size_below_min`, `tick_size_violation`, etc.) and set `non_retryable=True` for guaranteed rejects.
- Action: In `executionService`, honor `non_retryable` to break retry loop immediately.
- Action: In `trade_pipeline` SELL path, use `max(market_min_size, 5.0, 1.05/price)` as floor and skip fast if held < floor.
- Confidence: high

**[2026-04-17] — Runtime env precedence / duplicate-key footgun**
- Observation: `config/runtime.local.env` had 5 `LEADER_WALLETS`, but `secrets.env` still had an older 3-wallet list. Because `load_runtime_env()` loaded `secrets.env` last with override enabled, stale non-sensitive keys in secrets silently overwrote the intended runtime-local public config.
- Action: Treat `secrets.env` as sensitive-only. In `config/runtime_env.py`, only allowlisted sensitive keys override from `secrets.env`; public keys like `LEADER_WALLETS` stay owned by `runtime.local.env`. Emit a warning when `secrets.env` redefines a non-sensitive key that conflicts with `runtime.local.env`.
- Confidence: high

**[2026-04-17] — Bounded concurrency for trade pipeline**
- Observation: The active pipeline was serializing read-heavy work (market-min/meta lookups, reconciliation polling) and paying avoidable latency per signal even before reaching the real order submit path.
- Action: Add bounded concurrency only to safe lanes first: market-min prefetch in `trade_pipeline`, parallel reconciliation of previously unconfirmed orders, and a small `EXECUTION_SUBMIT_CONCURRENCY` semaphore in `ExecutionService` to cap simultaneous external order submissions. Keep write concurrency low by default.
- Confidence: high

**[2026-04-17] — Monitor per-leader fetch guard / singleflight**
- Observation: The monitor loop was already one task per leader, but there was still no hard per-wallet fetch guard around Polymarket data API calls. Future concurrent callers or helper paths could dogpile `activity?user=` and `positions?user=` for the same leader.
- Action: In `services/tradeMonitor.py`, add keyed per-leader fetch locks + in-flight task sharing + minimum spacing for `activity` and `positions` fetches. Different leaders still run in parallel; same leader/kind reuses the same request and respects pacing.
- Confidence: high

**[2026-04-17] — Manual Polymarket sells create stale local SELL state**
- Observation: When positions are manually reduced/sold on Polymarket outside the bot, local SQLite positions can overstate held size. Subsequent bot SELLs hit CLOB `not enough balance / allowance` even though local position looks large, and repeated retries can trip the CLOB circuit for a non-network issue.
- Action: Treat SELL-side `insufficient_balance` / `not enough balance` as non-retryable, then immediately sync the local position from live `positions?user=` holdings for that token. If live size is zero, delete local position; if live size is smaller, overwrite local size so future SELL logic uses the real balance.
- Confidence: high

**[2026-04-17] — BUY min-order bump vs fake hard-cap bug**
- Observation: When account-cap policy is disabled, the pipeline was still passing the strategy target notional (for example $1 from BUY_MIN_AMOUNT) into the decision layer as if it were a hard max cap. After raising exchange min size to 5 shares, small-target BUYs got skipped with `risk_cap_too_low_for_min_order` even when the configured `BUY_MAX_AMOUNT` had plenty of headroom.
- Action: In `trade_pipeline`, when account-cap policy is off, let the decision layer use the configured `buy_max_amount` as the real cap so it can auto-bump tiny BUY targets up to the exchange minimum. Only let a true account-cap policy bind the cap when enabled.
- Confidence: high

**[2026-04-17] — BUY decision/execution notional basis mismatch**
- Observation: BUY decision logic could approve the exchange min size in token terms (for example 5 shares) but return only the live-price notional (`5 * live_price`). Execution later enforces min size using the protected worst price (`5 * worst_price`), so perfectly valid trades near the threshold were still rejected as `amount below min_order_size`.
- Action: In `CopyDecisionService`, carry forward worst-case protected notional as `target_notional_usdc` for BUY execution, while keeping live-price notional in `details` for observability.
- Confidence: high

**[2026-04-22] — #26 token-aware runtime position identity**
- Observation: `PositionsRepo` already persisted positions at `user_id + condition_id + token_id`, but `PositionManager` and rehydration still allowed condition-only runtime identity to collapse token-distinct positions into one in-memory bucket.
- Action: Treat `(condition_id, asset/token_id)` as the canonical active-runtime position identity. Keep condition-only lookup only as a compatibility path when exactly one token-scoped position exists for that condition, and update active risk/rehydration paths to pass token id explicitly.
- Confidence: high

**[2026-04-22] — #27 protective-exit lifecycle activation threshold**
- Observation: Writing the durable risk block before the protective SELL fills creates a fake completed-outcome state: re-entry gets blocked even when the exit was rejected or never filled.
- Action: In `services/riskMonitor.py`, capture trigger metadata first but persist the risk block only after a confirmed protective SELL fill. Keep trigger-time metadata and enrich it with fill-time metadata (`fill_price`, `fill_size_tokens`, `fill_order_id`) once the block actually activates.
- Confidence: high

**[2026-04-22] — #28 per-scoped-position action coordinator**
- Observation: Order-level idempotency in `executionService` does not prevent two concurrent runtime tasks (trade pipeline BUY + risk monitor protective SELL + reconciliation state-apply) from observing stale state and issuing contradictory actions on the same `(user_id, condition_id, token_id)` scope.
- Action: Introduce `services/position_action_coordinator.py` with a per-`TradingContext` coordinator that hands out an `asyncio.Lock` per `(condition_id, token_id)` scope. Wrap the three critical sections (per-signal body in `execute_trades`, per-position body in `run_risk_check_for_context`, and each record in `_reconcile_batch._run`) with `async with coord.acquire(scope):`. Unrelated scopes stay concurrent.
- Confidence: high

**[2026-04-22] — Loud refactors via deterministic Python reindent instead of manual editing**
- Observation: Wrapping a large existing loop body in a new `async with` block produces a diff that looks massive but is structurally trivial (+4 spaces per line). Doing the reindent manually is error-prone and makes the diff impossible to trust.
- Action: When a slice requires reindenting a large body uniformly, use a small one-shot Python script that reads the file, locates the body by line pattern, prepends the new wrapper, and rewrites lines with the new indentation. Follow up with `python3 -c "import ast; ast.parse(open(path).read())"` to catch syntax issues before running tests.
- Confidence: high

**[2026-04-22] — #29 reconciliation identity must use venue order_id, not idempotency key**
- Observation: `services/reconciliation.py` was classifying exchange orders by calling `orders_repo.get_by_idempotency_key(order_id)`, which confuses the venue order identifier with the locally generated submission idempotency key and makes unknown-order drift counts fundamentally ambiguous.
- Action: Add `OrdersRepo.get_by_order_id(...)` and make exchange reconciliation prefer that explicit venue-id lookup. Keep fallback-to-idempotency only for legacy mocks, and document that `services/reconciliation.py` is drift detection / stale-order management while `trade_pipeline.reconcile_submitted_unconfirmed(...)` remains the current state-mutating delayed-fill convergence path.
- Confidence: high

**[2026-04-22] — #29 restart convergence needs pre-loop unconfirmed reconciliation**
- Observation: Reconciling `submitted_unconfirmed` orders only inside `execute_trades(...)` leaves a restart-time gap where risk monitoring can evaluate stale DB state before delayed fills are converged, causing phantom open positions after filled SELLs or invisible new positions after filled BUYs.
- Action: Run startup reconciliation after contexts are built but before monitor loops start, and pre-reconcile inside `run_risk_check_for_context(...)` before `rehydrate_position_manager(...)` so the risk monitor never evaluates a stale pre-convergence snapshot.
- Confidence: medium

**[2026-04-22] — #29 risk-monitor recovery needs durable protective metadata in `orders.request_json`**
- Observation: Risk-monitor protective SELLs can land in `orders` as `submitted_unconfirmed` without any `processed_trades` row, so restart convergence cannot recover them unless the durable order submission itself carries enough scope metadata (`condition_id`) and protective trigger context (`recovery_metadata`). Token id + signal id alone is too brittle.
- Action: Extend `ExecutionRequest` persistence with `condition_id` and structured `recovery_metadata`, then use `OrdersRepo.get_unconfirmed_order_submissions(...)` + `update_submission_status(...)` to reconcile risk-originated pending SELLs on startup / pre-risk-check, closing the restart gap without inventing a giant global convergence refactor.
- Confidence: medium

**[2026-04-22] — #29 partial protective SELL must not activate risk block**
- Observation: The risk-monitor path was treating any positive protective SELL fill as a completed exit and writing a durable risk block before checking whether the scoped position was actually closed. Partial fills are real state changes, but they do not justify a permanent re-entry ban.
- Action: In both live risk exits and restart recovery reconciliation, apply the SELL fill first and activate the risk block only if the scoped position is fully closed (`PositionsRepo.apply_fill(...)` returns `None`). Cover both paths with focused tests.
- Confidence: high

**[2026-04-22] — #29 delayed-fill convergence must apply only incremental matched delta**
- Observation: Restart reconciliation was reading the venue's cumulative matched size and reapplying the whole amount every pass. If 2 tokens were already durably applied before restart and the venue later reported 5 matched, the old path would apply 5 again instead of the missing delta 3.
- Action: In `orchestrators.trade_pipeline._reconcile_one(...)`, compare the venue's current matched amount against the previously persisted submission response, apply only the positive delta, keep non-final partial progress in `submitted_unconfirmed`, and only flip to `filled_reconciled` once the venue outcome is actually final.
- Confidence: high

**[2026-04-22] — #29 risk-side delayed-fill progress needs explicit regression coverage**
- Observation: The risk-monitor recovery path now carries the same cumulative-matched vs incremental-delta logic as trade reconciliation, but without a direct regression test it is too easy for a future edit to reintroduce restart-time double-apply on partially filled protective SELLs.
- Action: Keep a focused startup-recovery test that seeds prior matched progress in `orders.response_json` and local fills, then asserts only the missing delta is applied, the order stays `submitted_unconfirmed` for non-final venue state, and no risk block is activated while position remains open.
- Confidence: high

**[2026-04-22] — #30 live bootstrap must inject typed config into active sizing paths**
- Observation: The active runtime bootstrap still had two config-honesty gaps: `build_user_state()` constructed `PositionManager()` without `AppConfig`, and `TradingContext.copy_notional()` fell back to raw `ENV` values even when typed sizing config was available.
- Action: Thread `AppConfig` through the live bootstrap/composition root, construct `PositionManager(config=...)` for active users, and have `TradingContext` prefer `config.sizing` whenever the typed config is present. Keep raw `ENV` reads only on the legacy/test-only path.
- Confidence: high
