# Early on-chain trader detection — design intent (PR2, not yet implemented)

This is the design note for the deferred **PR2** module. Nothing here is wired
into PR1; PR1 only laid the typed Neg Risk exchange address that PR2 will filter
on. Build behind `ENABLE_ONCHAIN_PENDING_DETECTION` (default off).

## Module layout (planned)
```
services/onchain_detection/
  pending_tx_detector.py     # Alchemy alchemy_pendingTransactions WS
  order_filled_detector.py   # OrderFilled log WS on BOTH exchanges
  calldata_decoder.py        # decode matchOrders calldata
  trade_detection_models.py  # DetectionConfidence + signal dataclasses
```

## Confidence ladder
`DetectionConfidence(StrEnum)` = `market_unattributed`, `pending_probable`,
`mined_authoritative`, `confirmed_final`. Pending signals are **provisional** and
must never be treated as final truth.

## Exchange coverage (verified addresses, Polygon 137)
- CTF Exchange V2: `0xE111180000d2663C0091e4f400237545B87B996B`
- Neg Risk CTF Exchange V2: `0xe2222d279d744050d28e00520010520000310F59`
  (verified against docs.polymarket.com/resources/contracts)
Both must be in the pending-tx `toAddress` filter and the `OrderFilled` log
subscription.

## OrderFilled decode (per report, verify ABI before coding)
`OrderFilled(bytes32 orderHash, address maker, address taker, uint8 side,
uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled,
uint256 fee, bytes32 builder, bytes32 metadata)`.
`side` is the **maker** side (0=BUY, 1=SELL); the taker side is the opposite.
Price: maker BUY → `makerAmountFilled/takerAmountFilled`; maker SELL → inverse.

## Dedupe (the reason this is PR2)
Mined fills dedupe by `(tx_hash, log_index)` — one settlement tx can emit
multiple fill logs, so tx_hash alone is insufficient. `log_index` does not exist
in the codebase today; it is introduced here.

Before implementing, verify the ABI, event topics, side semantics, and Alchemy
`alchemy_pendingTransactions` behaviour via Firecrawl / Context7.
