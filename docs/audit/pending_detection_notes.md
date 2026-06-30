# Early on-chain trader detection — implemented in PR2

PR2 delivered the on-chain detection module behind
`ENABLE_ONCHAIN_PENDING_DETECTION` (default off). The mined `OrderFilled`
log subscription is always safe to run (authoritative); the pending-tx
subscription is provisional and flag-gated.

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
uint256,uint256[])` = `0x6d0fcd6e`. `Order` struct tuple =
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
not one per tx. Pending signals are tracked by `tx_hash` so a later mined
signal with the same `tx_hash` supersedes them (no double dispatch).

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
`OnchainDetectionConfig` (7 typed fields) in `config/app_config.py`,
documented in `config/runtime.local.example`, covered by
`test_env_schema_coverage.py`.