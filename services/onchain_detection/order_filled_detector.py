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

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from eth_utils.crypto import keccak

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
from utils.logger import get_logger

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
    block_hash: str
    exchange: str


@dataclass(frozen=True, slots=True)
class _MinedLogIdentity:
    """Identify a canonical included log without guessing missing coordinates."""

    transaction_hash: str
    log_index: int

    @classmethod
    def parse(cls, log: dict[str, object]) -> "_MinedLogIdentity | None":
        """Reject removal notifications, pending identities, and malformed fields."""
        if log.get("removed", False) is not False:
            return None
        transaction_hash = log.get("transactionHash", log.get("transaction_hash"))
        if (
            not isinstance(transaction_hash, str)
            or len(transaction_hash) != 66
            or not transaction_hash.lower().startswith("0x")
            or any(character not in "0123456789abcdefABCDEF" for character in transaction_hash[2:])
        ):
            return None
        raw_index = log.get("logIndex", log.get("log_index"))
        if isinstance(raw_index, bool) or not isinstance(raw_index, (str, int)):
            return None
        try:
            if isinstance(raw_index, str):
                if (
                    not raw_index.lower().startswith("0x")
                    or not raw_index[2:]
                    or any(character not in "0123456789abcdefABCDEF" for character in raw_index[2:])
                ):
                    return None
                index = int(raw_index, 16)
            else:
                index = raw_index
        except ValueError:
            return None
        if index < 0:
            return None
        return cls(transaction_hash.lower(), index)


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
    identity = _MinedLogIdentity.parse(log)
    if identity is None:
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

    block_raw = log.get("blockNumber", log.get("block_number"))
    try:
        block_number = (
            int(block_raw, 16)
            if isinstance(block_raw, str)
            else int(block_raw)
            if isinstance(block_raw, int)
            else None
        )
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
        tx_hash=identity.transaction_hash,
        log_index=identity.log_index,
        block_number=block_number,
        block_hash=str(log.get("blockHash") or log.get("block_hash") or "").lower(),
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
    event_ts: int | None,
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
        block_number=log.block_number,
        block_hash=log.block_hash,
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


#: Maximum watched wallets embedded per ``logs`` topic filter. A filter that
#: lists every followed leader keeps provider cost near zero; beyond this the
#: WARN (counts only) names the truncation and the REST-polling fallback
#: covers the leaders that no longer fit.
WATCHED_TOPIC_FILTER_CAP = 100

logger = get_logger("OrderFilledDetector")


def _address_to_topic(address: str) -> str:
    """Return the 32-byte topic word for an address (lowercase, left-padded)."""
    body = str(address).strip().lower()
    if body.startswith("0x"):
        body = body[2:]
    return "0x" + body.rjust(64, "0")


def log_subscription_params_for_watched(
    exchange_addresses: tuple[str, ...] | None = None,
    watched_wallets: Iterable[str] | None = None,
) -> tuple[list, ...]:
    """Build the leader-filtered ``logs`` subscriptions (maker role, taker role).

    ``OrderFilled`` indexes the maker as topic2 and the taker as topic3, and a
    single ``logs`` filter cannot OR two topic positions, so the caller opens
    one subscription per role:

    * maker role: ``topics = [[topic0], null, [<leader topics>]]``
    * taker role: ``topics = [[topic0], null, null, [<leader topics>]]``

    (``null`` is the positional wildcard; an array is OR-at-that-position —
    the standard ``eth_getLogs``/``eth_subscribe`` filter format every WSS
    provider implements.) The client-side maker/taker match in
    :func:`build_signal_from_log` stays as the second guard.

    Args:
        exchange_addresses: CTF exchange contracts to filter on; defaults to
            both Polymarket exchanges.
        watched_wallets: Followed leader wallets, any case. Truncated to
            :data:`WATCHED_TOPIC_FILTER_CAP` per filter (deterministic
            lexicographic prefix, WARN with counts only). ``None`` or empty
            returns ``()`` — an empty roster opens **no** subscription rather
            than re-opening the unfiltered feed, whose ~40k CU/min cost is
            exactly what the leader filter exists to avoid; the runtime idles
            until the roster delivers a leader again.

    Returns:
        A tuple with one ``eth_subscribe`` params list per subscription to
        open: two role filters with a watched set, none without.
    """
    watched = {
        str(wallet).strip().lower() for wallet in (watched_wallets or ()) if str(wallet).strip()
    }
    if not watched:
        return ()
    if len(watched) > WATCHED_TOPIC_FILTER_CAP:
        logger.warning(
            "orderfilled_topic_filter_capped | watched=%d cap=%d — leaders beyond the cap "
            "keep the REST-polling path only",
            len(watched),
            WATCHED_TOPIC_FILTER_CAP,
        )
        watched = set(sorted(watched)[:WATCHED_TOPIC_FILTER_CAP])
    leader_topics = sorted(_address_to_topic(wallet) for wallet in watched)
    addresses = [a.lower() for a in (exchange_addresses or ONCHAIN_EXCHANGE_ADDRESSES)]
    topic0 = ["0x" + ORDER_FILLED_TOPIC0.hex()]
    maker_role = ["logs", {"address": addresses, "topics": [topic0, None, leader_topics]}]
    taker_role = ["logs", {"address": addresses, "topics": [topic0, None, None, leader_topics]}]
    return (maker_role, taker_role)
