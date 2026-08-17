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
`duplicate_order_prevented_total` (polling loser). Accepted trade-off: a
multi-fill transaction with identical tx/side/asset is copied once from the
on-chain path, exactly as polling's aggregation copies it once.

Consequences and limits:

- Per-signal provenance is visible as `detection_signal | source=<polling|
  on_chain_pending|on_chain_confirmed>` and `detection_signals_total{source}`.
- `on_chain_pending` runs the gen-1 `OnchainDetectionRuntime` with the mined
  `OrderFilled` subscription **off**: gen-1 mined signals carry a `log_index`
  that changes the copy identity, so enabling both would copy pending + mined
  twice. The confirmed path is a strict superset and is the recommended first
  live mode.
- The gen-1 pending decoder matches direct `matchOrders` calldata only — no
  Safe `execTransaction` wrapper. That is sufficient for Polymarket proxy-wallet
  leaders (the operator relayer calls the exchange directly with the leader as
  `Order.maker`); Safe-wrapped leaders are covered by the gen-2 confirmed path.
- The deprecated `enable_onchain_*` booleans never select provisional copy;
  only an explicit `detection.mode: on_chain_pending` does.
- Changing `detection.mode` requires a restart; the control plane excludes it
  from editable settings.
