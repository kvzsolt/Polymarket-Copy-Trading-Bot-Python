"""Decode mined ``OrderFilled`` logs from both Polymarket CTF exchanges (PR2).

Verified event (from ``src/exchange/mixins/Events.sol`` and
``src/exchange/interfaces/ITrading.sol`` in Polymarket/ctf-exchange-v2)::

    event OrderFilled(
        bytes32 indexed orderHash,
        address indexed maker,
        address indexed taker,
        Side side,                    // uint8, *maker* side: 0=BUY, 1=SELL
        uint256 tokenId,
        uint256 makerAmountFilled,
        uint256 takerAmountFilled,
        uint256 fee,
        bytes32 builder,
        bytes32 metadata
    );

Topic0 = ``keccak256("OrderFilled(bytes32,address,address,uint8,uint256,
uint256,uint256,uint256,bytes32,bytes32)")`` =
``0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee``.

Indexed (``log4``): ``orderHash`` (topic1), ``maker`` (topic2), ``taker``
(topic3). The non-indexed data block is 7 words (``0xe0`` bytes):
``side, tokenId, makerAmountFilled, takerAmountFilled, fee, builder, metadata``.

A single settlement tx can emit multiple ``OrderFilled`` logs (one per
matched maker order, plus the taker fill), so the authoritative dedupe key is
``(tx_hash, log_index)`` — never ``tx_hash`` alone.

The watched wallet appears in the ``maker`` indexed topic for maker-emitted
fills and in the ``taker`` indexed topic for the taker-emitted fill, so both
topics are checked.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from eth_utils import keccak

from config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    price_from_fill,
    watched_side_from_maker_side,
)

ORDER_FILLED_TOPIC0: bytes = keccak(
    text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,"
    "uint256,uint256,bytes32,bytes32)"
)
# 7 non-indexed words = 0xe0 bytes.
_ORDER_FILLED_DATA_WORDS = 7
_ORDER_FILLED_DATA_BYTES = _ORDER_FILLED_DATA_WORDS * 32

# Both exchanges emit the identical event from the same source contract, so
# a single topic0 + the two exchange addresses fully scope the subscription.
ONCHAIN_EXCHANGE_ADDRESSES: tuple[str, ...] = (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
)


def _hexstr_to_addr(value: str) -> str:
    """Normalize a 32-byte topic word (hex) to a lowercase 0x-prefixed address."""
    if not value:
        return ""
    if value.startswith("0x") or value.startswith("0X"):
        value = value[2:]
    # Topic words are 32 bytes; the address is the last 20 bytes.
    if len(value) == 64:
        return "0x" + value[-40:].lower()
    if len(value) == 40:
        return "0x" + value.lower()
    return ("0x" + value[-40:].lower()) if len(value) >= 40 else ""


def _hexstr_to_bytes32(value: str) -> bytes:
    if not value:
        return b"\x00" * 32
    if value.startswith("0x") or value.startswith("0X"):
        value = value[2:]
    return bytes.fromhex(value.zfill(64))


def _word_to_int(data: bytes, word_index: int) -> int:
    start = word_index * 32
    return int.from_bytes(data[start : start + 32], "big")


@dataclass(frozen=True)
class DecodedOrderFilledLog:
    """Decoded fields from a single ``OrderFilled`` log."""

    order_hash: bytes
    maker: str
    taker: str
    maker_side: int
    token_id: int
    maker_amount_filled: int
    taker_amount_filled: int
    fee: int
    builder: bytes
    metadata: bytes
    tx_hash: str
    log_index: int
    block_number: int | None
    exchange: str


def decode_order_filled_log(log: dict) -> DecodedOrderFilledLog | None:
    """Decode a raw log dict (as delivered by an ``eth_subscribe`` ``logs`` sub).

    Expected keys: ``address``, ``topics`` (list[str]), ``data`` (hex str),
    ``transactionHash`` (or ``transaction_hash``), ``logIndex`` (or
    ``log_index``), ``blockNumber`` (or ``block_number``).

    Returns ``None`` if the log is not an ``OrderFilled`` event from a known
    exchange, or if the data block is malformed. Never raises.
    """
    if not isinstance(log, dict):
        return None
    address = str(log.get("address") or "").lower()
    if address not in ONCHAIN_EXCHANGE_ADDRESSES:
        return None
    topics = log.get("topics") or []
    if not isinstance(topics, list) or len(topics) < 4:
        return None
    topic0 = _hexstr_to_bytes32(str(topics[0]))
    if topic0 != ORDER_FILLED_TOPIC0:
        return None

    data_hex = str(log.get("data") or "")
    if data_hex.startswith("0x") or data_hex.startswith("0X"):
        data_hex = data_hex[2:]
    try:
        data = bytes.fromhex(data_hex)
    except ValueError:
        return None
    if len(data) < _ORDER_FILLED_DATA_BYTES:
        return None

    tx_hash = str(log.get("transactionHash") or log.get("transaction_hash") or "").lower()
    log_index_raw = log.get("logIndex", log.get("log_index", 0))
    try:
        log_index = int(log_index_raw, 16) if isinstance(log_index_raw, str) else int(log_index_raw)
    except (TypeError, ValueError):
        log_index = 0
    block_raw = log.get("blockNumber", log.get("block_number"))
    try:
        block_number = int(block_raw, 16) if isinstance(block_raw, str) else int(block_raw)
    except (TypeError, ValueError):
        block_number = None

    return DecodedOrderFilledLog(
        order_hash=_hexstr_to_bytes32(str(topics[1])),
        maker=_hexstr_to_addr(str(topics[2])),
        taker=_hexstr_to_addr(str(topics[3])),
        maker_side=_word_to_int(data, 0),
        token_id=_word_to_int(data, 1),
        maker_amount_filled=_word_to_int(data, 2),
        taker_amount_filled=_word_to_int(data, 3),
        fee=_word_to_int(data, 4),
        builder=bytes.fromhex(format(_word_to_int(data, 5), "064x")),
        metadata=bytes.fromhex(format(_word_to_int(data, 6), "064x")),
        tx_hash=tx_hash,
        log_index=log_index,
        block_number=block_number,
        exchange=address,
    )


def _amounts_to_size_usdc(
    maker_side: int, maker_filled: int, taker_filled: int
) -> tuple[Decimal, Decimal, Decimal]:
    """Return ``(size_tokens, price, usdc_size)`` from a mined fill.

    CTF outcome tokens are 6-decimal (matching USDC.e). ``size_tokens`` is the
    number of outcome tokens that changed hands (the maker-side "amount of
    tokens to be sold" for SELL, or the taker-amount for BUY — we use the
    token-side amount consistently). ``usdc_size`` is the collateral side.

    For maker BUY: maker pays USDC, receives tokens -> size = takerAmountFilled
    (tokens), usdc = makerAmountFilled.
    For maker SELL: maker pays tokens, receives USDC -> size = makerAmountFilled
    (tokens), usdc = takerAmountFilled.

    Both are scaled by 1e6 to get human-readable USDC / token counts.
    """
    if maker_side == MAKER_SIDE_BUY:
        size_raw = taker_filled
        usdc_raw = maker_filled
    elif maker_side == MAKER_SIDE_SELL:
        size_raw = maker_filled
        usdc_raw = taker_filled
    else:
        return Decimal(0), Decimal(0), Decimal(0)
    size = Decimal(size_raw) / Decimal(10**6)
    usdc = Decimal(usdc_raw) / Decimal(10**6)
    price = price_from_fill(maker_side, maker_filled, taker_filled)
    return size, price, usdc


def build_signal_from_log(
    log: DecodedOrderFilledLog,
    watched_wallets: set[str],
    event_ts: int,
    condition_id: str = "",
) -> OnchainTradeSignal | None:
    """Build a ``MINED_AUTHORITATIVE`` signal if the log references a watched wallet.

    Returns ``None`` if neither ``maker`` nor ``taker`` is in ``watched_wallets``
    (lowercase). The signal's ``log_index`` is always set (mined fills carry
    it), making the composite ``(tx_hash, log_index)`` dedupe key available.
    """
    watched_lower = {w.lower() for w in watched_wallets}
    maker = log.maker.lower()
    taker = log.taker.lower()
    if maker in watched_lower:
        watched_wallet = maker
        watched_role = "maker"
    elif taker in watched_lower:
        watched_wallet = taker
        watched_role = "taker"
    else:
        return None

    side = watched_side_from_maker_side(log.maker_side, watched_role)
    size, price, usdc = _amounts_to_size_usdc(
        log.maker_side, log.maker_amount_filled, log.taker_amount_filled
    )
    order_hash_hex = "0x" + log.order_hash.hex() if log.order_hash else None
    return OnchainTradeSignal(
        leader_wallet=watched_wallet,
        token_id=str(log.token_id),
        side=side,
        size=size,
        price=price,
        usdc_size=usdc,
        tx_hash=log.tx_hash,
        confidence=DetectionConfidence.MINED_AUTHORITATIVE,
        maker_side=log.maker_side,
        watched_role=watched_role,
        event_ts=event_ts,
        log_index=log.log_index,
        condition_id=condition_id,
        provisional=False,
        exchange=log.exchange,
        order_hash=order_hash_hex,
    )


def log_subscription_params(exchange_addresses: tuple[str, ...] | None = None) -> list:
    """Build the ``eth_subscribe`` JSON-RPC ``params`` list for mined logs.

    ``eth_subscribe`` expects ``params`` as a positional array like
    ``["logs", filter]``. A single subscription with both exchange
    addresses + the ``OrderFilled`` topic0 is sufficient; maker/taker
    filtering is done client-side because the watched set can change at
    runtime and a single sub cannot OR topic2/topic3.
    """
    addresses = tuple(a.lower() for a in (exchange_addresses or ONCHAIN_EXCHANGE_ADDRESSES))
    filter_obj = {
        "address": list(addresses),
        "topics": ["0x" + ORDER_FILLED_TOPIC0.hex()],
    }
    return ["logs", filter_obj]
