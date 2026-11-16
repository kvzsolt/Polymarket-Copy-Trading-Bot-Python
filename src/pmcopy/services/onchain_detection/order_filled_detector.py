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

from typing import Any

from eth_utils.crypto import keccak

from pmcopy.config.addresses import (
    ALL_VERIFIED_EXCHANGE_ADDRESSES,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    verified_polymarket_exchange_addresses,
)
from pmcopy.domain.tracked_users import TrackedUserDirectory
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    price_from_fill,
    watched_side_from_maker_side,
)
from pmcopy.utils.logger import get_logger

ORDER_FILLED_V2_TOPIC0: bytes = keccak(
    text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,"
    "uint256,uint256,bytes32,bytes32)"
)
ORDER_FILLED_TOPIC0: bytes = ORDER_FILLED_V2_TOPIC0

ORDER_FILLED_V1_TOPIC0: bytes = keccak(
    text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
)

# V2: 7 non-indexed words = 0xe0 bytes.
_ORDER_FILLED_V2_DATA_WORDS = 7
_ORDER_FILLED_V2_DATA_BYTES = _ORDER_FILLED_V2_DATA_WORDS * 32
# V1: 5 non-indexed words = 0xa0 bytes (makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee).
_ORDER_FILLED_V1_DATA_WORDS = 5
_ORDER_FILLED_V1_DATA_BYTES = _ORDER_FILLED_V1_DATA_WORDS * 32

# Default active V2 Exchange addresses.
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


def _hexstr_to_bytes32(value: str) -> bytes | None:
    if not value:
        return b"\x00" * 32
    if value.startswith("0x") or value.startswith("0X"):
        value = value[2:]
    if len(value) > 64:
        return None
    try:
        return bytes.fromhex(value.zfill(64))
    except ValueError:
        return None


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


def decode_order_filled_log(
    log: dict[str, Any],
    *,
    include_legacy_v1: bool = False,
) -> DecodedOrderFilledLog | None:
    """Decode a raw log dict (as delivered by an ``eth_subscribe`` ``logs`` sub).

    Supports Polymarket CLOB V2 ``OrderFilled`` (7 non-indexed words) by default,
    and additionally decodes V1 ``OrderFilled`` (5 non-indexed words) when
    ``include_legacy_v1=True``.

    Returns ``None`` if the log is not an ``OrderFilled`` event from an enabled
    verified exchange, or if the data block is malformed. Never raises.
    """
    if not isinstance(log, dict):
        return None
    identity = _MinedLogIdentity.parse(log)
    if identity is None:
        return None
    address = str(log.get("address") or "").lower()
    allowed_exchanges = (
        ALL_VERIFIED_EXCHANGE_ADDRESSES
        if include_legacy_v1
        else frozenset(ONCHAIN_EXCHANGE_ADDRESSES)
    )
    if address not in allowed_exchanges:
        return None
    topics = log.get("topics") or []
    if not isinstance(topics, list) or len(topics) < 4:
        return None
    topic0 = _hexstr_to_bytes32(str(topics[0]))
    order_hash = _hexstr_to_bytes32(str(topics[1]))
    if topic0 is None or order_hash is None:
        return None

    data_hex = str(log.get("data") or "")
    if data_hex.startswith("0x") or data_hex.startswith("0X"):
        data_hex = data_hex[2:]
    try:
        data = bytes.fromhex(data_hex)
    except ValueError:
        return None

    if topic0 == ORDER_FILLED_V2_TOPIC0:
        if len(data) < _ORDER_FILLED_V2_DATA_BYTES:
            return None
        maker_side = _word_to_int(data, 0)
        token_id = _word_to_int(data, 1)
        maker_amount_filled = _word_to_int(data, 2)
        taker_amount_filled = _word_to_int(data, 3)
        fee = _word_to_int(data, 4)
        builder = bytes.fromhex(format(_word_to_int(data, 5), "064x"))
        metadata = bytes.fromhex(format(_word_to_int(data, 6), "064x"))
    elif include_legacy_v1 and topic0 == ORDER_FILLED_V1_TOPIC0:
        if len(data) < _ORDER_FILLED_V1_DATA_BYTES:
            return None
        maker_asset_id = _word_to_int(data, 0)
        taker_asset_id = _word_to_int(data, 1)
        if maker_asset_id == 0 and taker_asset_id != 0:
            maker_side = MAKER_SIDE_BUY
            token_id = taker_asset_id
        elif taker_asset_id == 0 and maker_asset_id != 0:
            maker_side = MAKER_SIDE_SELL
            token_id = maker_asset_id
        else:
            return None
        maker_amount_filled = _word_to_int(data, 2)
        taker_amount_filled = _word_to_int(data, 3)
        fee = _word_to_int(data, 4)
        builder = b"\x00" * 32
        metadata = b"\x00" * 32
    else:
        return None

    if maker_side not in (MAKER_SIDE_BUY, MAKER_SIDE_SELL):
        return None
    if maker_amount_filled <= 0 or taker_amount_filled <= 0:
        return None

    block_raw = log.get("blockNumber", log.get("block_number"))
    block_number: int | None = None
    if not isinstance(block_raw, bool):
        try:
            if isinstance(block_raw, str):
                cleaned_blk = block_raw.strip()
                block_number = (
                    int(cleaned_blk, 16)
                    if cleaned_blk.lower().startswith("0x")
                    else int(cleaned_blk, 10)
                )
            elif isinstance(block_raw, int):
                block_number = block_raw
        except (TypeError, ValueError):
            block_number = None

    return DecodedOrderFilledLog(
        order_hash=order_hash,
        maker=_hexstr_to_addr(str(topics[2])),
        taker=_hexstr_to_addr(str(topics[3])),
        maker_side=maker_side,
        token_id=token_id,
        maker_amount_filled=maker_amount_filled,
        taker_amount_filled=taker_amount_filled,
        fee=fee,
        builder=builder,
        metadata=metadata,
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


def _make_mined_signal(
    log: DecodedOrderFilledLog,
    *,
    leader_wallet: str,
    watched_role: str,
    event_ts: int | None,
    condition_id: str = "",
    token_holder_wallet: str = "",
    source_receipt: Any = None,
) -> OnchainTradeSignal:
    side = watched_side_from_maker_side(log.maker_side, watched_role)
    size, price, usdc = _amounts_to_size_usdc(
        log.maker_side, log.maker_amount_filled, log.taker_amount_filled
    )
    order_hash_hex = "0x" + log.order_hash.hex() if log.order_hash else None
    holder = (token_holder_wallet or log.maker or leader_wallet).lower()
    return OnchainTradeSignal(
        leader_wallet=leader_wallet,
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
        source_receipt=source_receipt,
        token_holder_wallet=holder,
    )


def build_signal_from_log(
    log: DecodedOrderFilledLog,
    watched_wallets: set[str],
    event_ts: int | None,
    condition_id: str = "",
) -> OnchainTradeSignal | None:
    """Build a ``MINED_AUTHORITATIVE`` signal if the log references a watched wallet."""
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
    return _make_mined_signal(
        log,
        leader_wallet=watched_wallet,
        watched_role=watched_role,
        event_ts=event_ts,
        condition_id=condition_id,
    )


def build_signals_from_log_for_directory(
    log: DecodedOrderFilledLog,
    directory: TrackedUserDirectory,
    event_ts: int | None,
    condition_id: str = "",
    *,
    source_receipt: Any = None,
) -> tuple[OnchainTradeSignal, ...]:
    """Emit canonical leader signal from one deduplicated ``OrderFilled`` log.

    On Polymarket CTF Exchange (V1 and V2), every matched order owner (resting
    maker or active taker) receives its own canonical ``OrderFilled`` log with
    ``topics[2] (maker) == order_owner`` (for takers, ``topics[3] == exchange``).
    Suppressing counterparty-perspective logs (``topics[3] == leader`` where
    ``topics[2]`` is a counterparty maker) prevents 2x double execution and
    Mint/Merge token/side inversion while preserving additive maker fills.
    """
    if directory.is_empty:
        return ()
    maker_leader = directory.canonical_leader_for(log.maker)
    if maker_leader is None:
        return ()
    return (
        _make_mined_signal(
            log,
            leader_wallet=maker_leader,
            watched_role="maker",
            event_ts=event_ts,
            condition_id=condition_id,
            token_holder_wallet=log.maker,
            source_receipt=source_receipt,
        ),
    )


#: Maximum watched wallets embedded per ``logs`` topic filter in legacy helper.
WATCHED_TOPIC_FILTER_CAP = 100

logger = get_logger("OrderFilledDetector")

_HEX_CHARS = frozenset("0123456789abcdef")


def _address_to_topic(address: str) -> str:
    """Return the validated 32-byte topic word for an address or padded topic."""
    body = str(address or "").strip().lower()
    if body.startswith("0x"):
        body = body[2:]
    if len(body) == 64:
        if body[:24] != "0" * 24 or any(ch not in _HEX_CHARS for ch in body[24:]):
            raise ValueError(f"invalid 32-byte padded user topic: {address!r}")
        return "0x" + body
    if len(body) == 40 and all(ch in _HEX_CHARS for ch in body):
        return "0x" + ("0" * 24) + body
    raise ValueError(f"invalid tracked user address/topic: {address!r}")


def build_user_filtered_log_subscriptions(
    exchange_addresses: Iterable[str] | None = None,
    padded_user_topics: Iterable[str] | None = None,
    *,
    include_legacy_v1: bool = False,
) -> tuple[list[Any], ...]:
    """Build strict server-side maker & taker ``eth_subscribe("logs", ...)`` filters.

    NEVER builds an unfiltered Exchange ``logs`` subscription. If
    ``padded_user_topics`` is empty, returns ``()`` so zero subscriptions are
    opened until at least one tracked user exists.
    """
    raw_exchanges = (
        tuple(exchange_addresses)
        if exchange_addresses is not None
        else verified_polymarket_exchange_addresses(include_legacy_v1=include_legacy_v1)
    )
    normalized_exchanges: list[str] = []
    for raw_addr in raw_exchanges:
        cleaned = str(raw_addr or "").strip().lower()
        if cleaned not in ALL_VERIFIED_EXCHANGE_ADDRESSES:
            raise ValueError(f"unverified exchange address in logs filter: {raw_addr!r}")
        if cleaned not in normalized_exchanges:
            normalized_exchanges.append(cleaned)
    if not normalized_exchanges or len(normalized_exchanges) > 4:
        raise ValueError("logs filter requires 1..4 verified Polymarket Exchange addresses")

    topics_set = {
        _address_to_topic(entry) for entry in (padded_user_topics or ()) if str(entry or "").strip()
    }
    if not topics_set:
        return ()

    sorted_user_topics = sorted(topics_set)
    topic0_list = ["0x" + ORDER_FILLED_V2_TOPIC0.hex()]
    if include_legacy_v1:
        topic0_list.append("0x" + ORDER_FILLED_V1_TOPIC0.hex())

    maker_role: list[Any] = [
        "logs",
        {
            "address": normalized_exchanges,
            "topics": [topic0_list, None, sorted_user_topics],
        },
    ]
    taker_role: list[Any] = [
        "logs",
        {
            "address": normalized_exchanges,
            "topics": [topic0_list, None, None, list(sorted_user_topics)],
        },
    ]
    if not maker_role[1]["topics"][2] or not taker_role[1]["topics"][3]:
        raise AssertionError("logs subscription user topics must never be empty")
    return (maker_role, taker_role)


def log_subscription_params_for_watched(
    exchange_addresses: tuple[str, ...] | None = None,
    watched_wallets: Iterable[str] | None = None,
    *,
    include_legacy_v1: bool = False,
) -> tuple[list[Any], ...]:
    """Build leader-filtered ``logs`` subscriptions (maker role, taker role)."""
    watched = {
        str(wallet).strip().lower() for wallet in (watched_wallets or ()) if str(wallet).strip()
    }
    if not watched:
        return ()
    if len(watched) > WATCHED_TOPIC_FILTER_CAP:
        logger.warning(
            "orderfilled_topic_filter_capped | watched=%d cap=%d — truncating topic filter",
            len(watched),
            WATCHED_TOPIC_FILTER_CAP,
        )
        watched = set(sorted(watched)[:WATCHED_TOPIC_FILTER_CAP])
    return build_user_filtered_log_subscriptions(
        exchange_addresses=exchange_addresses,
        padded_user_topics=watched,
        include_legacy_v1=include_legacy_v1,
    )
