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
| `on_chain_pending` | gen-1 `OnchainDetectionRuntime(enable_pending=True, enable_mined=False)` | `alchemy_pendingTransactions` sighting decoded from calldata; `PENDING_PROBABLE`, `provisional=True`; dropped when older than `pending_tx_max_signal_age_ms`, below `pending_tx_detection_confidence_required`, or superseded per leader | required |
| `on_chain_confirmed` | gen-2 `PolygonSourceTransactionRuntime` | mempool sighting only *reserves*; execution after `eth_getTransactionReceipt` decodes the `OrderFilled` logs (`MINED_AUTHORITATIVE`, `provisional=False`) | required (`PostgresSourceTransactionStateStore`) |

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
first, median mempool-to-mined lead 0.67 s. Recommended rollout: first live
run `on_chain_confirmed`; switch to `on_chain_pending` only after a paper soak
shows pending sightings decode for the followed leader.

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

## Config
`OnchainDetectionConfig` is typed in `config/app_config.py` and documented in
`config/detection.yml`; overrides belong in `config/environments/<env>/detection.yml`.
