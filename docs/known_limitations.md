# Known Limitations

The Polymarket copy-trading bot is fail-closed wherever a money-moving
decision is at stake, but the production path still has several
documented limitations that the operator needs to know before running it
live. Each item below was the subject of a deep-research review cycle and
is intentionally documented here (and scanned for by
`scripts/rc1_check.sh`'s test surface) so the bot does not silently
regress on any of them.

## 1. http polling for market data

The leader-market and order-book read path uses **http polling** of the
Polymarket gamma REST API and the venue's REST `/book` endpoint. Polling
configurable rate defaults match what the deep-research review found to
be safe under typical leader activity; aggressive operators tuning the
poll are responsible for the rate-limit cost. The bot does not silently
replace polling with WSS streaming — the WSS subscription is opt-in and
the code paths remain separate per the REST-primary execution contract
(see `tests/test_market_data_rest_primary_execution.py`).

Limitations:

- Polling latency is bounded by the REST round-trip + rate limit refresh;
  a slow gamma response can delay a leader fill's reflection in the
  funder wallet's strategy decisions. The bot's fail-closed book-quality
  gate (`tests/test_book_quality_gate.py`) catches widesp-read books
  before they can drive an off-mark order.
- No streaming tail-fallback: when the gamma API is down, the bot drops
  the leader-fill update rather than risking a stale snapshot. The next
  poll cycle is the sole recuperation path.
- http polling is the floor — the WSS subscription path is gated by the
  operator setting `MARKET_DATA_PRIMARY_SOURCE` to a streaming mode;
  default config keeps http polling as the executive source of truth.

## 2. execute_intent is the single live-entry surface

External code paths that want to suggest a trade MUST route through
`execute_intent`. Any other call to `execution_service.execute` is
forbidden by `scripts/rc1_check.sh`'s forbid scan (it Greps production
code for `execution_service.execute`, the `getattr` aliases, and rejects
any file that lacks the `execution_coordinator_missing` guard). The
single permitted call site is `services/execution/coordinator.py` — the
code-level single-entry-point invariant for live order submission. This
is a deliberate architectural limitation, not a missing feature.

Limitations:

- The single entry-point does not enforce policy (slippage, max sizing);
  that policy layer is upstream of `execute_intent` and must always be
  invoked before reaching this surface. The bot enforces this ordering
  via the orchestrator pipeline (`orchestrators/trade_pipeline.py`).
- The single entry-point also means recovery paths (rollback, canary,
  cancel-and-resubmit) MUST go through `execute_intent` — they cannot
  bypass it through a helper. The audit log surfaces this because each
  executed order carries the same canonical idempotency key.

## 3. external integrations

The bot depends on three external surfaces:

- The Polymarket gamma REST API (HTTP polling for leader discovery +
  order-book snapshots).
- The CLOB venue V2 REST API (order placement + order-status + fills).
- The Polygon RPC (HTTP/WebSocket for on-chain collateral, on-chain
  `OrderFilled` event detection in PR2, and Neg Risk exchange
  probes).

Each of these has documented limitations:

- Polymarket gamma: rate-limit bursts throttle the bot aggressively
  (Polymarket-side), and the gamma API does not document a backoff
  contract; the bot's polling cadence defaults to what was empirically
  safe + the deep-research gap analysis. The freeze on aggressive
  polling is enforced at typed-config level (`AppConfig.polling_interval`).
- CLOB venue V2: the venue's order endpoint does not return ack in the
  same response — the bot polls `/order/<id>` separately. Pending vs
  confirmed fill distinction is client-side; the order-status call is
  best-effort and the bot's risk pre-reconciliation compensates for late
  acks (PR #122).
- Polygon RPC: the bot must hold two distinct RPC URLs (primary +
  fallback) because Polygon-style RPC providers have a stale-block failure
  mode. The bot's collateral-readiness per-iteration probe reads primary
  first and falls back if the primary returns a stale block. On Pr2 the
  WebSocket subscription paths assume a fresh RPC.

The contract is: **the operator is responsible for keeping at minimum one
healthy RPC**; if all RPCs go dark, the bot goes fail-closed (collateral
readiness degrades; doctor blocks; live trading halts). This is the
intended behaviour.

## 4. Strategy profiler is dry-run by default

The Phase-11 strategy profiler (`services/strategy_profile/`) and the env
tuner (`services/strategy_profile/env_patch_writer.py`) are dry-run-by-
default. The bot does not auto-apply env patches; the operator must
invoke the env-patch writer with `apply=True` and review the proposed
patch explanation. PATCHES TO SECRET KEYS ARE NEVER MUTATED — see
`_PROTECTED_EXACT` and `_PROTECTED_SUBSTRINGS` in
`services/strategy_profile/env_patch_writer.py`; the writer's
idempotency-key set + lock in the on-chain dispatcher (`PR2 follow-up`)
do not interact with this surface.

## 5. KNOWN_LIMITATIONS scan contract

The release contract (`scripts/rc1_check.sh`) does NOT scan for any of
the literals in this document; the integrity of these limitations is
pinned by the unit test suite (`tests/test_market_data_rest_primary_;
execution.py::test_rest_primary_executes_with_wss_down`,
`tests/test_execution_coordinator_production.py::*`). If you add a new
limitation here, add a corresponding regression test to the unit suite.

## 6. Related docs

- `docs/live_runbook.md` — daily operator workflow including the pre-
  flight gate that enforces these limitations.
- `docs/recovery_playbook.md` — how `auto_released_ambiguous` interacts
  with the reconciliation state machine (one of the few invariants that
  survives the external-integration surface).
- `docs/emergency_shutdown.md` — operator response when an external
  integration drops (kill-switch + doctor).
- `docs/trader_discovery.md` — discovery data caveats, ranking evidence,
  and roster behavior.

## Leader detection: polling runs alongside the on-chain source

`CLAUDE.md` describes "one selected detection source". The runtime deliberately
deviates: `detection.mode` selects exactly one **on-chain** source
(`on_chain_pending` or `on_chain_confirmed`), but REST polling of the
Polymarket data API keeps running in every mode as a parallel fallback. This
is intentional — the data API is the only source that survives an Alchemy
outage — and it is safe because the copy layer dedups the same fill by trade
key: `CrossSourceTradeClaimPolicy` (`orchestrators/onchain_signal_dispatch.py`)
claims the tx-level identity `user|leader|tx|side|asset` before an on-chain
fill reaches the pipeline, so whichever source sees a fill first wins and the
loser is logged as `detection_signal_deduped` / counted in
`detection_signals_deduped_total` (on-chain loser) or
`duplicate_order_prevented_total` (polling loser).

Multi-fill transactions (measured on Polygon: ~3.4 % of `(tx, maker, token,
side)` groups hold ≥2 `OrderFilled` fills, ~5.3 % of all fills): every fill of
one settlement transaction is a distinct on-chain signal and each is copied at
its own size — the tx-level claim is held by the on-chain path itself, so a
sibling fill loses that claim to *us*, not to polling, and is dispatched
(`cross_source_sibling_fill`); the pipeline's per-fill `…|log:N` claim keeps
idempotency. Polling's aggregated row for that tx (which sums the sizes) is
refused, so the follower ends at the leader's full size exactly once. Sibling
detection uses bounded in-process memory plus the row status
(`cross_source_claimed`) after a restart; a lost claim whose holder cannot be
determined (probe error, no row) is skipped with `winner=unknown` and is never
attributed to polling.

The tx-level claim is released again (`cross_source_claim_released`) only when
the freshly claimed dispatch demonstrably submitted nothing, so REST polling
really is a fallback for that fill. "Submitted nothing" is deliberately narrow,
because releasing after a copy reached the venue would let the next polling
tick submit a second one:

- every pipeline result is a *pre-submission* skip — `status="skipped"` with no
  `order_id` and a reason other than `duplicate_ignored` (an empty result list
  counts as releasable);
- or the pipeline raised **and** the per-fill `processed_trades` row is
  absent, i.e. it raised before reaching its own per-fill claim
  (`try_claim_processed_trade`, taken before any submission attempt). Any
  existing row — regardless of status or `order_id` — proves that claim was
  already taken, so a raise after that point keeps the tx-level claim
  (possible missed copy, never a double copy): `mark_processed_trade` never
  writes an `order_id` onto the per-fill row, so the row's `order_id` cannot
  distinguish "claimed" from "submitted".

A `skipped/duplicate_ignored` result reports the opposite: the execution dedup,
a `ReservationConflict`, or an already-persisted fill — the copy happened, so
the claim stays. Anything the per-fill probe cannot prove also keeps the claim
(`cross_source_claim_kept`, WARN), and the release itself is a status-guarded
delete (`OrdersRepo.release_cross_source_claim`, `WHERE trade_key = $1 AND
status = 'cross_source_claimed'`) so it can never remove a row the policy no
longer owns; a no-op release logs `cross_source_claim_release_skipped`.

The claim is likewise deliberately **kept** when a sibling fill was already
admitted on it: releasing would let polling's aggregate copy the sibling a
second time. In that narrow case the failed first fill's size is not retried by
polling (same as the pre-existing single-channel behaviour for a skipped fill).

Consequences and limits:

- Per-signal provenance is visible as `detection_signal | source=<polling|
  on_chain_pending|on_chain_confirmed>` and `detection_signals_total{source}`.
- `on_chain_pending` runs the gen-1 `OnchainDetectionRuntime` with the mined
  `OrderFilled` subscription **off**: gen-1 mined signals carry a `log_index`
  that changes the copy identity, so enabling both would copy pending + mined
  twice. Consequently there is **no mined correction path** in that mode: the
  per-leader supersession check lives in the mined handler and never fires, so
  a provisional copy of a mempool sighting that is dropped or replaced before
  mining is not unwound. Run it only after a paper soak and with an explicit
  operator unwind procedure. The confirmed path is a strict superset and is
  the recommended first live mode.
- Alchemy CU cost: with the gen-2 pending subscription carrying full tx bodies
  and the `logs` subscription filtered by address only, the confirmed mode was
  measured at ~85k CU/min (≈120M CU/day at 24/7). Follow-up: filter the
  `logs` subscription on the `OrderFilled` topic and the indexed maker/taker
  leader addresses before treating 24/7 live operation as cost-settled.
- The gen-1 pending decoder matches direct `matchOrders` calldata only — no
  Safe `execTransaction` wrapper. That is sufficient for Polymarket proxy-wallet
  leaders (the operator relayer calls the exchange directly with the leader as
  `Order.maker`); Safe-wrapped leaders are covered by the gen-2 confirmed path.
- The deprecated `enable_onchain_*` booleans never select provisional copy;
  only an explicit `detection.mode: on_chain_pending` does.
- Changing `detection.mode` requires a restart; the control plane excludes it
  from editable settings.
