# Early on-chain trader detection — implemented in PR2

PR2 delivered the on-chain detection module behind
`ENABLE_ONCHAIN_PENDING_DETECTION` (default off). The mined `OrderFilled`
log subscription is always safe to run (authoritative); the pending-tx
subscription is provisional and flag-gated.

## Mode semantics (2026-08: `detection.mode` selector)

The booleans above are deprecated. `detection.mode` selects the on-chain
source through `orchestrators/detection_source_factory.py`:

| mode | runtime | what fires the copy | Postgres |
|---|---|---|---|
| `polling` | none | data-api REST polling only | required (always) |
| `on_chain_pending` | gen-1 `OnchainDetectionRuntime(enable_pending=True, enable_mined=False)` | `alchemy_pendingTransactions` sighting decoded from calldata; `PENDING_PROBABLE`, `provisional=True`; dropped when older than `pending_tx_max_signal_age_ms` or below `pending_tx_detection_confidence_required`. **No mined correction path** (mined subscription off for dedup): per-leader supersession never fires, so a provisional copy of a sighting that never mines is *detected and reported* by the pending-copy confirmation tracker (below) but never unwound automatically | required |
| `on_chain_confirmed` + `confirmed_source: mempool_reserved` (default) | gen-2 `PolygonSourceTransactionRuntime` | mempool sighting only *reserves*; execution after `eth_getTransactionReceipt` decodes the `OrderFilled` logs (`MINED_AUTHORITATIVE`, `provisional=False`) | required (`PostgresSourceTransactionStateStore`) |
| `on_chain_confirmed` + `confirmed_source: logs_only` | gen-1 `OnchainDetectionRuntime(enable_pending=False, enable_mined=True)` with two `logs` subscriptions topic-filtered on `OrderFilled` topic0 + the followed leaders (maker role topic2, taker role topic3; cap 100 per filter, deterministic prefix beyond) | the mined `OrderFilled` log itself (`MINED_AUTHORITATIVE`, `provisional=False`, `log_index`/`order_hash` set) — detection at block inclusion, no mempool lead, no provisional copy, no tracker | required (idempotency/copy keys) |

REST polling keeps running in every mode; `CrossSourceTradeClaimPolicy` in
`orchestrators/onchain_signal_dispatch.py` claims the tx-level trade key so a
fill is copied once whichever source saw it first. Startup logs
`detection_mode_selected | mode=... onchain_ws=set|unset polling_fallback=true
provider=alchemy` (the WS URL, which carries the Alchemy key, is never logged;
`clients/onchain_ws.py` logs the host only). Every dispatched on-chain signal
logs `detection_signal | source=... leader=... tx=...` and increments
`detection_signals_total{source}`.

Mempool visibility was measured, not assumed (read-only probe, 120 s,
2026-08-17): ~96 % of mined `OrderFilled` transactions had been seen pending
first, median mempool-to-mined lead 0.67 s.

## Rollout status (2026-08-18): `on_chain_pending` is the live default

> **Superseded later the same day — see the next section**: the live default is
> now `on_chain_confirmed` + `confirmed_source: logs_only`.

`config/environments/live/detection.yml` now pins `mode: on_chain_pending`
with `pending_tx_max_signal_age_ms: 700`, `pending_tx_provider: alchemy`,
`pending_tx_detection_confidence_required: pending_probable`, and the
pending-copy confirmation tracker on (`pending_copy_confirmation_enabled:
true`, 60 s deadline, 2048 tracked). Base `config/detection.yml` is unchanged:
`mode` stays unpinned (typed default `polling`) and
`enable_onchain_pending_detection` stays `false`, so paper keeps running pure
polling and no other deployment inherits provisional copy.

The earlier recommendation — first live run on `on_chain_confirmed`, switch to
pending only after a paper soak — is superseded by this operator decision. The
preconditions it named still hold as *operator obligations*, not as blockers
the config enforces:

- a paper soak showing pending sightings decode for the followed leaders;
- an explicit unwind procedure for a provisional copy whose transaction never
  mines — nothing supersedes or corrects it in this mode (the mined
  subscription is off) and polling never sees an unmined fill either. The
  tracker reports the orphan (`pending_copy_unconfirmed_total{reason}`,
  WARN `pending_copy_unconfirmed`); the operator closes the position;
- Alchemy CU cost for a full-body `alchemy_pendingTransactions` subscription
  *without* a `logs` subscription is still unmeasured — the ~85k CU/min figure
  is the confirmed-mode baseline. Measure it before a 24/7 live run
  (`docs/known_limitations.md`).

Rollback is a one-line environment change plus a restart:
`PMCOPY__DETECTION__MODE=on_chain_confirmed` (or edit the live file back).
`detection.mode` is deliberately not an editable control-plane setting.

## Rollout update (2026-08-18, later): live default is `logs_only` confirmed

`config/environments/live/detection.yml` now pins `mode: on_chain_confirmed`
with `onchain.confirmed_source: logs_only`. The live copy fires on the mined
`OrderFilled` event delivered by two topic-filtered `eth_subscribe` `logs`
subscriptions (`OrderFilled` topic0 + the followed leaders as indexed
maker/taker topics) on any plain WSS provider — the standard filter format
(`null` wildcard, per-position OR list) that Alchemy, Infura, QuickNode, and
self-hosted nodes all implement. The Alchemy endpoint and
`pending_tx_provider: alchemy` are kept (the operator's WSS is Alchemy;
`logs_only` also accepts `generic`).

Why: the mempool-reserved confirmed baseline measured ~85k CU/min (≈ $54/day
at $0.45/M CU) and `on_chain_pending` ≈ $27/day, while the filtered `logs`
volume is a handful of events per hour — inside the Alchemy 30M CU/month free
tier. The mempool lead being given up is the measured ~0.67 s median; the
mined event needs no confirmation and carries no orphan-position risk, so the
pending-copy tracker is not built in this mode.

Operator obligations carried over from the pending rollout: none of the
provisional-copy preconditions apply (no provisional copies exist in this
mode). The mempool path stays one environment variable away —
`PMCOPY__DETECTION__MODE=on_chain_pending` (restart required) — and the
pinned pending keys in the live file take effect again unchanged.

### The paper soak cannot exercise the pending-copy tracker

The first obligation above — "a paper soak showing pending sightings decode" —
covers detection only. It says nothing about the confirmation safeguard,
because **in paper mode the tracker records nothing at all.**

`OnchainSignalDispatchAdapter._track_pending_copy` is reached only for
followers whose pipeline outcome `_reached_venue()` — status `executed` or
`submitted_unconfirmed`. Every paper copy returns at the `paper` tier before
execution with no order id, so `track()` is never called, `pending_copy_tracked`
stays `0`, and no `pending_copy_*` counter ever moves. Composition announces
this once at startup: INFO `pending_copy_tracker_idle_in_paper_mode`.

Consequence for the soak report: **zero `pending_copy_*` output in paper is
NOT EXERCISED, not healthy.** Reading it as a green light is the specific
mistake this note exists to prevent.

The real gate is the first live session, over a bounded window — the first
60 minutes of live trading:

- `pending_copy_tracked` / `pending_copy_confirmed_total` must become non-zero
  once live copies are placed. Still `0` means the tracker is not actually
  wired (no RPC endpoint, feature off, wrong mode) — stop there.
- **Abort on any `pending_copy_unconfirmed_total{reason="reverted"}`**: the
  leader transaction failed and the copy is an orphan.
- **Abort when `pending_copy_unconfirmed_total{reason="not_mined"}` exceeds
  2**: the mempool sightings are not reaching a block, so the mode is copying
  noise.
- `reason="rpc_error"` is not a verdict on the leader transaction — it means
  the safeguard is blind. Sustained `rpc_error` is an RPC outage to fix, not a
  reason to keep trading unconfirmed.

Abort procedure: pause entries, close any orphan position manually (auto-unwind
is inert, see above), then apply the one-line rollback
`PMCOPY__DETECTION__MODE=on_chain_confirmed` and restart. The operator runbook
carries the same gate (`docs/live_runbook.md`).

## Module layout (shipped)
```
services/onchain_detection/
  __init__.py
  trade_detection_models.py    # DetectionConfidence + OnchainTradeSignal
  calldata_decoder.py          # decode matchOrders calldata -> watched match
  order_filled_detector.py     # OrderFilled log decode (both exchanges)
  pending_tx_detector.py       # Alchemy alchemy_pendingTransactions sub
  mined_fill_deduplicator.py   # (tx_hash, log_index) composite-key dedupe
clients/
  onchain_ws.py                # websockets + eth_subscribe + reconnect backoff
orchestrators/
  onchain_detection_runtime.py # owns WS tasks + queue + dispatcher -> execute_trades
```

## Confidence ladder
`DetectionConfidence(StrEnum)` = `market_unattributed`, `pending_probable`,
`mined_authoritative`, `confirmed_final`. Pending signals are **provisional**
and never treated as final truth. Mined signals are authoritative but deduped
by `(tx_hash, log_index)` because one settlement tx can emit multiple fill
logs.

## Exchange coverage (verified addresses, Polygon 137)
- CTF Exchange V2: `0xE111180000d2663C0091e4f400237545B87B996B`
- Neg Risk CTF Exchange V2: `0xe2222d279d744050d28e00520010520000310F59`

Both are in `config/addresses.py` (single source of truth), in the
`ONCHAIN_EXCHANGE_ADDRESSES` constant, in the pending-tx `toAddress` filter,
and in the `OrderFilled` log subscription `address` list. Verified against
the official Polymarket/ctf-exchange-v2 repository via Firecrawl.

## OrderFilled decode (verified from source)
`OrderFilled(bytes32 indexed orderHash, address indexed maker, address
indexed taker, uint8 side, uint256 tokenId, uint256 makerAmountFilled,
uint256 takerAmountFilled, uint256 fee, bytes32 builder, bytes32 metadata)`.
- Topic0 = `keccak256("OrderFilled(bytes32,address,address,uint8,uint256,
  uint256,uint256,uint256,bytes32,bytes32)")` =
  `0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee`.
- Indexed: `orderHash` (topic1), `maker` (topic2), `taker` (topic3).
- Non-indexed data block: 7 words (`side, tokenId, makerAmountFilled,
  takerAmountFilled, fee, builder, metadata`).
- `side` is the **maker** side (0=BUY, 1=SELL); the taker side is the opposite.
- Price: maker BUY -> `makerAmountFilled / takerAmountFilled`; maker SELL ->
  `takerAmountFilled / makerAmountFilled`.
- Watched-wallet match: `maker == watched` OR `taker == watched` (both
  checked; `maker` is the primary).

## matchOrders calldata decode (verified from source)
External selector `matchOrders(bytes32,Order,Order[],uint256,uint256[],
uint256,uint256[])` = `0x3c2b4399`. The selector WITHOUT the standalone
taker `Order` (`0x6d0fcd6e`) does NOT match production `matchOrders` calls
and was replaced (Greptile P1 re-review). `Order` struct tuple =
`(uint256 salt, address maker, address signer, uint256 tokenId, uint256
makerAmount, uint256 takerAmount, uint8 side, uint8 signatureType, uint256
timestamp, bytes32 metadata, bytes32 builder, bytes signature)`. The watched
wallet is matched against `takerOrder.maker` and every `makerOrders[i].maker`.

**Known limitation:** proxy/safe wallets where the EOA `signer` differs from
`maker` are not attributed to the EOA in PR2 (the prompt specifies matching on
`maker`, the source-of-funds address). Future work: resolve signer->proxy/safe
ownership via `PolyProxyLib`/`PolySafeLib` address derivation.

## Dedupe (the Phase 9 hardening)
`MinedFillDeduplicator` keys mined fills on `(tx_hash, log_index)` — one
settlement tx with multiple fill logs produces one logical event per log,
not one per tx. Pending signals are tracked by `(tx_hash, leader_wallet)`
(not `tx_hash` alone) so a single pending `matchOrders` tx containing
multiple watched-wallet matches (e.g. taker=walletA, maker=walletB) is
recorded per leader. A later mined signal for the same `(tx_hash,
leader_wallet)` supersedes only that leader's pending (per-leader
`supersede_pending`); a mined for leaderA does NOT suppress a pending for
leaderB (Greptile P1 re-review: "Pending matches are dropped").

## Runtime wiring
`OnchainDetectionRuntime` owns the two WS subscriptions + an
`asyncio.Queue` + a dispatcher. Mined signals (authoritative) always
dispatch to the `signal_sink` (a wrapper around `execute_trades` in
production). Pending signals only dispatch when
`ENABLE_ONCHAIN_PENDING_DETECTION=true`. The `condition_id` for mined
signals is resolved via an injectable `condition_id_resolver` (Gamma API
`/markets?clob_token_ids={tokenId}` in production); for pending signals it
is extracted directly from the `matchOrders` calldata first arg.

## Pending-copy confirmation (runtime wiring)

`on_chain_pending` copies off an *unmined* sighting, so a follow-up is the only
thing that can notice an orphaned copy. Composition (all in `app.py`, thin
glue only):

```text
_build_pending_copy_tracker(config, telemetry_service=…, metrics_registry=…)
  └── Web3ReceiptStatusFetcher.from_rpc_url(polymarket.rpc_url,
                                            polymarket.fallback_rpc_url)
        └── FallbackWeb3Provider -> AsyncWeb3Adapter.get_transaction_receipt
              (asyncio.to_thread; TransactionNotFound -> None)
OnchainSignalDispatchAdapter(pending_copy_tracker=tracker)
  └── _dispatch_legacy: after the results gather, a provisional signal whose
      results include executed / paper / submitted_unconfirmed calls
      tracker.track(TrackedPendingCopy(...)) — sync, never awaited, guarded
await tracker.start()   # after the on-chain runtime start
await tracker.stop()    # in _shutdown_runtime, next to onchain_runtime.stop()
```

The tracker is built **only** when `detection.mode=on_chain_pending`,
`detection.onchain.pending_copy_confirmation_enabled` is true, and
`polymarket.rpc_url` is set (otherwise startup logs
`pending_copy_tracker_disabled | reason=no_rpc_endpoint` and the feature stays
off). `recorded_at` is stamped with `time.monotonic` on both sides — the
dispatcher and the tracker must share one time base or ages go negative.

Auto-unwind (`pending_copy_auto_unwind_enabled`, default false) is design only:
the `PendingCopyUnwindHook` Protocol and the injection point exist, no
requester is wired, so the flag cannot move money. The intended shape stays
`PendingCopyUnwindRequester -> PositionManager.request_forced_exit(...,
reason="pending_copy_unconfirmed")` consulted from `evaluate_risk_action`, so
an unwind would ride the existing risk-monitor -> coordinator -> forced-sell
FAK path rather than adding a second execution path.

Operator signals: `pending_copy_confirmed_total`,
`pending_copy_unconfirmed_total{reason=reverted|not_mined|rpc_error}`,
`pending_copy_tracked`, `pending_copy_tracker_dropped_total{reason="full"}`,
the WARN line `pending_copy_unconfirmed | tx=… leader=… age_s=… reason=…`, and
a bounded DETECTION `FailureEvent` (`pending_copy_<reason>`) on the telemetry
stream. Subscription health: `onchain_ws_connected{stream}`,
`onchain_ws_reconnect_total{stream}`, readiness check `onchain_ws_ok`
(`required=False`). Residual risks are listed in `docs/known_limitations.md`.

## Config
`OnchainDetectionConfig` is typed in `config/app_config.py` and documented in
`config/detection.yml`; overrides belong in `config/environments/<env>/detection.yml`.
The `pending_copy_*` keys are consulted only in `on_chain_pending`.
