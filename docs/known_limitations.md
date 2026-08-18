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
  mining is not *unwound* by the detection runtime. It is now **detected and
  reported** by the pending-copy confirmation tracker (below), but unwinding
  remains an operator action.
- **`on_chain_pending` is the checked-in live default** as of
  `config/environments/live/detection.yml` (`mode: on_chain_pending`,
  `pending_tx_max_signal_age_ms: 700`, tracker on with a 60 s deadline and a
  2048-entry ceiling). That is a deliberate latency-for-certainty trade: the
  copy fires on an unmined sighting. It presumes the operator has run the paper
  soak and holds an explicit unwind procedure for a provisional copy whose
  transaction never mines. `on_chain_confirmed` remains the strict superset and
  the fallback to switch back to (`PMCOPY__DETECTION__MODE=on_chain_confirmed`,
  restart required) if `pending_copy_unconfirmed_total` climbs.
- The staleness gate `pending_tx_max_signal_age_ms` bounds only the signal's
  **internal queue age** — the time between the runtime decoding the sighting
  and the dispatcher picking it up. It is not a bound on how long the
  transaction has been in the mempool, nor on network propagation delay before
  the subscription delivered it.
- Switching the live mode does not touch the gen-2
  `AlchemyConnectionManager`/`PolygonSourceTransactionRuntime` path; that code
  is unchanged and still what `on_chain_confirmed` runs.
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

### Pending-copy confirmation (detect, report, do not unwind)

In `on_chain_pending`, every copy that actually reached the venue off a
provisional sighting is recorded by `PendingCopyConfirmationTracker`
(`services/onchain_detection/pending_copy_confirmation_tracker.py`) and polled
against the leader transaction's receipt over HTTP RPC
(`eth_getTransactionReceipt` through the read-only `FallbackWeb3Provider`).

- Recording is synchronous, O(1), and exception-guarded, so the
  detection-to-submit path is unchanged; all RPC work happens in the
  background `pending-copy-confirmation` task.
- Poll offsets are 5/15/30/60 s after the copy, with a
  `pending_copy_confirmation_deadline_seconds` (default 60 s) cutoff that
  always forces one final, terminal poll.
- Outcomes: receipt `status=1` -> DEBUG `pending_copy_confirmed` +
  `pending_copy_confirmed_total`; `status=0` -> `reverted`; no receipt by the
  deadline -> `not_mined`; only RPC failures up to the deadline ->
  `rpc_error`. Each of the latter three logs WARN `pending_copy_unconfirmed |
  tx=… leader=… age_s=… reason=…`, increments
  `pending_copy_unconfirmed_total{reason}`, and emits a bounded DETECTION
  `FailureEvent` (`pending_copy_<reason>`).
- `pending_copy_tracked` gauges the in-flight set; overflow past
  `pending_copy_confirmation_max_tracked` evicts the oldest entry and counts
  `pending_copy_tracker_dropped_total{reason="full"}`.

Residual risks the tracker does **not** remove (operator must accept):

- **No automatic unwind.** `pending_copy_auto_unwind_enabled` is false by
  default and the requester is design-only: the hook Protocol and the
  injection point exist, nothing is wired, so the flag cannot move money
  today. An unconfirmed copy is reported, not closed.
- **No reorg-after-success detection.** A copy confirmed at `status=1` is
  never re-checked; a later re-org that unwinds that transaction is invisible.
- **In-memory state.** Tracked copies do not survive a restart: anything
  in flight at shutdown is simply forgotten (no verdict, no report).
- **RPC dependence.** With no `polymarket.rpc_url` configured the tracker is
  not built at all (a WARN says so at startup); a full-window RPC outage
  reports `rpc_error`, which is "unknown", not "orphaned".
- The pending signal's price is the leader's calldata limit price; the
  pipeline still prices from the live decision quote.
- The gen-1 decoder still sees only direct `matchOrders` calls, so a
  Safe-wrapped leader is invisible to both detection and this tracker.
- The skipped-row / polling asymmetry for pending signals (`tx_key ==
  fill_key`) is unchanged.
- Alchemy CU for a full-body `alchemy_pendingTransactions` subscription
  without a `logs` subscription is still **unmeasured** (the ~85k CU/min
  figure above is the confirmed-mode baseline); measure it during the paper
  soak before a 24/7 live run.

WSS health is now visible too: `onchain_ws_connected{stream}`,
`onchain_ws_reconnect_total{stream}`, and the **non-required** readiness check
`onchain_ws_ok` (non-required on purpose — REST polling keeps copying while
the subscription is down, so a drop is degraded, not unready).

## Bankroll profile caveats (350 pUSD moderate)

The packaged `sizing` / `risk` / `execution` / `protective_exits` values encode
one operator profile — a 350 pUSD bankroll at moderate risk, derived in
`docs/02-configuration.md` and pinned by `tests/test_live_operator_tuning.py`.
Five properties of that profile are behavioural, not cosmetic, and the operator
has to know them before running it:

- **`stop_loss_threshold` is an absolute price, not a drawdown percentage.**
  `PositionManager.evaluate_risk_action` compares the current outcome price
  directly against the threshold (`services/positionManager.py`), so `0.08`
  means "exit when a share trades at or below 8 c", not "exit at −8 %". It is
  set below `execution.min_share_price` (`0.10`) on purpose: at or above the
  entry floor, every freshly opened position would become stop-eligible the
  moment `stop_loss_grace_period_seconds` expired. Raising the threshold
  without raising `min_share_price` re-creates that trap.
- **`sizing.max_position_pct` is measured against a stale free-cash basis.**
  The basis is the startup / 24 h free-cash snapshot the trade pipeline
  caches, not live equity. A restart while capital is deployed re-snapshots a
  smaller free-cash figure and silently tapers the per-position cap; it is left
  at `0.1` so it stays slack behind the 17.5 hard cap rather than becoming the
  binding constraint.
- **`max_category_exposure_usdc` is a documented no-op.** The active pipeline
  does not persist per-position categories, so `_resolve_buy_risk_cap` passes
  `category_exposure_usdc=0` and the cap can never bind. It is set to the 210
  exposure ceiling for consistency, not because it enforces anything.
- **`min_balance_headroom_usdc` is a static floor.** `BalanceRegime` compares
  `free_cash - notional` against a fixed 140, with no awareness of equity,
  open exposure, or the day's realized PnL. If the bankroll changes materially
  it must be re-derived by hand — nothing rescales it.
- **The per-leader cap assumes the two-leader roster.** 140 is 40 % of the
  bankroll, sized so two leaders can each hold 8 concurrent copies inside the
  210 ceiling. Adding a third leader without re-deriving the caps lets total
  exposure exceed the intended 60 %; the account-cap policy has no
  roster-aware aggregate.

Both BUY gates must clear and neither is a percentage of live equity: the
account-cap policy (`headroom_for_buy`) works off the balance snapshot the
pipeline already fetched, and the `BalanceRegime` belt is in-memory and
absolute. Neither adds IO to the detection-to-submit path.
