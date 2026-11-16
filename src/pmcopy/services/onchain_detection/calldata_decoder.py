"""Decode CTF Exchange V2 ``matchOrders`` calldata (PR2).

External entry point on both the CTF Exchange V2 and the Neg Risk CTF
Exchange V2 is ``matchOrders(bytes32,Order,Order[],uint256,uint256[],uint256,uint256[])``
(selector ``0x3c2b4399``). Safe/Gnosis calls may wrap that exact payload in
``execTransaction`` (selector ``0x6a761202``), where::

    struct Order {
        uint256 salt;
        address maker;       // source of funds
        address signer;      // may differ from maker for proxy/safe wallets
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint8   side;         // 0 = BUY, 1 = SELL  (maker side)
        uint8   signatureType;
        uint256 timestamp;
        bytes32 metadata;
        bytes32 builder;
        bytes  signature;    // dynamic, last
    }

The watched-wallet match checks ``takerOrder.maker`` and every
``makerOrders[i].maker`` against the watched set. (Proxy / Safe wallets where
the EOA ``signer`` differs from ``maker`` are not attributed to the EOA in
PR2 — documented limitation.)

Verified against the official Polymarket/ctf-exchange-v2 source:
``src/exchange/CTFExchange.sol`` (``matchOrders``) and
``src/exchange/libraries/Structs.sol`` (``Order``) via Firecrawl.
"""

from __future__ import annotations

from dataclasses import dataclass

from eth_abi.abi import decode as abi_decode
from eth_abi.exceptions import DecodingError
from eth_utils.abi import function_signature_to_4byte_selector

from pmcopy.domain.tracked_users import TrackedUserDirectory
from pmcopy.services.onchain_detection.trade_detection_models import (
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
)

# Canonical selector for ``matchOrders(bytes32,Order,Order[],
# uint256,uint256[],uint256,uint256[])``. The standalone taker ``Order``
# (2nd parameter) MUST be present — the selector without it
# (``0x6d0fcd6e``) does not match production ``matchOrders`` calls
# (``0x3c2b4399``), so the pending-tx decoder would reject every real
# matchOrders payload before ABI decoding (Greptile P1 re-review).
_ORDER_TUPLE = (
    "(uint256,address,address,uint256,uint256,uint256,uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
_MATCH_ORDERS_TYPE = (
    f"(bytes32,{_ORDER_TUPLE},{_ORDER_TUPLE}[],uint256,uint256[],uint256,uint256[])"
)
_MATCH_ORDERS_ARG_TYPES = (
    "bytes32",
    _ORDER_TUPLE,
    f"{_ORDER_TUPLE}[]",
    "uint256",
    "uint256[]",
    "uint256",
    "uint256[]",
)
MATCH_ORDERS_SELECTOR: bytes = function_signature_to_4byte_selector(
    "matchOrders(bytes32,"
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes),"
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes)[],"
    "uint256,uint256[],uint256,uint256[])"
)

# Legacy Polymarket CTF Exchange V1 Order & matchOrders selectors (4-arg and 6-arg).
_V1_ORDER_TUPLE = (
    "(uint256,address,address,address,uint256,uint256,uint256,"
    "uint256,uint256,uint256,uint8,uint8,bytes)"
)
_MATCH_ORDERS_V1_4ARG_TYPES = (
    _V1_ORDER_TUPLE,
    f"{_V1_ORDER_TUPLE}[]",
    "uint256",
    "uint256[]",
)
_MATCH_ORDERS_V1_6ARG_TYPES = (
    _V1_ORDER_TUPLE,
    f"{_V1_ORDER_TUPLE}[]",
    "uint256",
    "uint256[]",
    "uint256",
    "uint256[]",
)
MATCH_ORDERS_V1_4ARG_SELECTOR: bytes = function_signature_to_4byte_selector(
    f"matchOrders({_V1_ORDER_TUPLE},{_V1_ORDER_TUPLE}[],uint256,uint256[])"
)
MATCH_ORDERS_V1_6ARG_SELECTOR: bytes = function_signature_to_4byte_selector(
    f"matchOrders({_V1_ORDER_TUPLE},{_V1_ORDER_TUPLE}[],uint256,uint256[],uint256,uint256[])"
)

SAFE_EXEC_TRANSACTION_SELECTOR: bytes = function_signature_to_4byte_selector(
    "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)"
)
_SAFE_EXEC_TRANSACTION_TYPES = (
    "address",
    "uint256",
    "bytes",
    "uint8",
    "uint256",
    "uint256",
    "uint256",
    "address",
    "address",
    "bytes",
)

_SAFE_SELECTOR_HEX = "0x" + SAFE_EXEC_TRANSACTION_SELECTOR.hex()
_V2_ALLOWED_SELECTOR_HEX = frozenset(
    {
        "0x" + MATCH_ORDERS_SELECTOR.hex(),
        _SAFE_SELECTOR_HEX,
    }
)
_V1_AND_V2_ALLOWED_SELECTOR_HEX = _V2_ALLOWED_SELECTOR_HEX | frozenset(
    {
        "0x" + MATCH_ORDERS_V1_4ARG_SELECTOR.hex(),
        "0x" + MATCH_ORDERS_V1_6ARG_SELECTOR.hex(),
    }
)
_ZERO_PAD_24 = "0" * 24


def calldata_matches_tracked_users(
    tx_input: str,
    tracked_hex40: frozenset[str],
    *,
    include_legacy_v1: bool = False,
) -> bool:
    """Zero-struct-alloc hex pre-filter before ``bytes.fromhex`` / ``eth_abi.decode``.

    1. Verifies ``tx_input[:10]`` matches an enabled ``matchOrders`` / Safe selector
       without lowercasing or decoding the payload body.
    2. Scans 32-byte (64-hex-char) ABI word boundaries for ``000...000<40-hex-user>``
       against ``tracked_hex40`` in O(1) per word (plus +8 hex-char inner word alignment
       when wrapped in Gnosis Safe ``execTransaction``).
    """
    if not tracked_hex40 or not isinstance(tx_input, str) or len(tx_input) < 74:
        return False
    selector_hex = tx_input[:10].lower()
    allowed = _V1_AND_V2_ALLOWED_SELECTOR_HEX if include_legacy_v1 else _V2_ALLOWED_SELECTOR_HEX
    if selector_hex not in allowed:
        return False

    body = tx_input[10:].lower()
    limit = len(body) - 63
    for pos in range(0, limit, 64):
        if body[pos : pos + 24] == _ZERO_PAD_24 and body[pos + 24 : pos + 64] in tracked_hex40:
            return True

    if selector_hex == _SAFE_SELECTOR_HEX:
        for pos in range(8, limit, 64):
            if body[pos : pos + 24] == _ZERO_PAD_24 and body[pos + 24 : pos + 64] in tracked_hex40:
                return True
    return False


@dataclass(frozen=True)
class DecodedOrder:
    """Describe an immutable decoded Polymarket order."""

    salt: int
    maker: str
    signer: str
    token_id: int
    maker_amount: int
    taker_amount: int
    side: int
    signature_type: int
    timestamp: int
    metadata: bytes
    builder: bytes


@dataclass(frozen=True)
class MatchOrdersCall:
    """Describe a decoded match-orders calldata payload."""

    condition_id: bytes
    taker_order: DecodedOrder
    maker_orders: tuple[DecodedOrder, ...]
    taker_fill_amount: int
    maker_fill_amounts: tuple[int, ...]
    taker_fee_amount: int
    maker_fee_amounts: tuple[int, ...]


@dataclass(frozen=True)
class WatchedMatch:
    """A watched wallet found inside a decoded ``matchOrders`` call."""

    wallet: str
    role: str
    side: int
    token_id: int
    maker_amount: int
    taker_amount: int
    maker_index: int | None = None
    order_maker_wallet: str = ""


@dataclass(frozen=True)
class DecodedSourceCall:
    """Qualified direct or Safe-wrapped Polymarket settlement call."""

    exchange_address: str
    safe_address: str | None
    match_orders: MatchOrdersCall


class SourceTransactionCalldataDecoder:
    """Qualify direct and Safe-wrapped calls to configured CTF exchanges."""

    def __init__(self, exchange_addresses: frozenset[str]) -> None:
        """Initialize the decoder.

        Args:
            exchange_addresses: Allowed CTF Exchange V2 contract addresses.

        Raises:
            ValueError: If no syntactically valid exchange address is supplied.
        """

        normalized = frozenset(
            address
            for raw_address in exchange_addresses
            if (address := self._normalize_address(raw_address)) is not None
        )
        if not normalized:
            raise ValueError("exchange_addresses must contain a valid EVM address")
        self._exchange_addresses = normalized

    def decode(
        self,
        transaction_target: str,
        calldata: bytes,
    ) -> DecodedSourceCall | None:
        """Return a qualified settlement call or ``None``.

        Args:
            transaction_target: Outer pending transaction destination.
            calldata: Outer pending transaction calldata bytes.

        Returns:
            Decoded direct or Safe-wrapped matchOrders call. Delegate calls,
            malformed payloads, unconfigured exchanges, and other selectors
            return ``None``.
        """

        target = self._normalize_address(transaction_target)
        if target is None:
            return None
        if target in self._exchange_addresses:
            match_orders = decode_match_orders(calldata)
            if match_orders is None:
                return None
            return DecodedSourceCall(
                exchange_address=target,
                safe_address=None,
                match_orders=match_orders,
            )
        if len(calldata) < 4 or calldata[:4] != SAFE_EXEC_TRANSACTION_SELECTOR:
            return None
        try:
            decoded = abi_decode(list(_SAFE_EXEC_TRANSACTION_TYPES), calldata[4:])
            inner_target = self._normalize_address(str(decoded[0]))
            inner_calldata = bytes(decoded[2])
            operation = int(decoded[3])
        except (DecodingError, IndexError, TypeError, ValueError, OverflowError):
            return None
        if operation != 0 or inner_target not in self._exchange_addresses:
            return None
        match_orders = decode_match_orders(inner_calldata)
        if match_orders is None:
            return None
        return DecodedSourceCall(
            exchange_address=inner_target,
            safe_address=target,
            match_orders=match_orders,
        )

    @staticmethod
    def _normalize_address(value: str) -> str | None:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 42 or not normalized.startswith("0x"):
            return None
        try:
            int(normalized[2:], 16)
        except ValueError:
            return None
        return normalized


def _to_decoded_order(raw: tuple) -> DecodedOrder:
    return DecodedOrder(
        salt=int(raw[0]),
        maker=str(raw[1]).lower(),
        signer=str(raw[2]).lower(),
        token_id=int(raw[3]),
        maker_amount=int(raw[4]),
        taker_amount=int(raw[5]),
        side=int(raw[6]),
        signature_type=int(raw[7]),
        timestamp=int(raw[8]),
        metadata=bytes(raw[9]),
        builder=bytes(raw[10]),
    )


def _to_decoded_v1_order(raw: tuple) -> DecodedOrder:
    return DecodedOrder(
        salt=int(raw[0]),
        maker=str(raw[1]).lower(),
        signer=str(raw[2]).lower(),
        token_id=int(raw[4]),
        maker_amount=int(raw[5]),
        taker_amount=int(raw[6]),
        side=int(raw[10]),
        signature_type=int(raw[11]),
        timestamp=int(raw[7]),
        metadata=b"\x00" * 32,
        builder=b"\x00" * 32,
    )


def decode_match_orders(
    calldata: bytes,
    *,
    include_legacy_v1: bool = False,
) -> MatchOrdersCall | None:
    """Decode a V2 (or optional V1) ``matchOrders`` calldata payload."""
    if not calldata or len(calldata) < 4:
        return None
    selector = calldata[:4]
    payload = calldata[4:]

    if selector == MATCH_ORDERS_SELECTOR:
        outer = None
        try:
            outer = abi_decode([_MATCH_ORDERS_TYPE], payload)[0]
        except (DecodingError, ValueError, OverflowError):
            try:
                outer = abi_decode(list(_MATCH_ORDERS_ARG_TYPES), payload)
            except (DecodingError, ValueError, OverflowError):
                return None
        try:
            taker = _to_decoded_order(outer[1])
            makers = tuple(_to_decoded_order(m) for m in outer[2])
            return MatchOrdersCall(
                condition_id=bytes(outer[0]),
                taker_order=taker,
                maker_orders=makers,
                taker_fill_amount=int(outer[3]),
                maker_fill_amounts=tuple(int(x) for x in outer[4]),
                taker_fee_amount=int(outer[5]),
                maker_fee_amounts=tuple(int(x) for x in outer[6]),
            )
        except (IndexError, TypeError, ValueError):
            return None

    if not include_legacy_v1:
        return None

    if selector == MATCH_ORDERS_V1_4ARG_SELECTOR:
        try:
            decoded_v1 = abi_decode(list(_MATCH_ORDERS_V1_4ARG_TYPES), payload)
            taker = _to_decoded_v1_order(decoded_v1[0])
            makers = tuple(_to_decoded_v1_order(m) for m in decoded_v1[1])
            maker_fills = tuple(int(x) for x in decoded_v1[3])
            return MatchOrdersCall(
                condition_id=b"",
                taker_order=taker,
                maker_orders=makers,
                taker_fill_amount=int(decoded_v1[2]),
                maker_fill_amounts=maker_fills,
                taker_fee_amount=0,
                maker_fee_amounts=tuple(0 for _ in maker_fills),
            )
        except (DecodingError, IndexError, TypeError, ValueError, OverflowError):
            return None

    if selector == MATCH_ORDERS_V1_6ARG_SELECTOR:
        try:
            decoded_v1 = abi_decode(list(_MATCH_ORDERS_V1_6ARG_TYPES), payload)
            taker = _to_decoded_v1_order(decoded_v1[0])
            makers = tuple(_to_decoded_v1_order(m) for m in decoded_v1[1])
            return MatchOrdersCall(
                condition_id=b"",
                taker_order=taker,
                maker_orders=makers,
                taker_fill_amount=int(decoded_v1[2]),
                maker_fill_amounts=tuple(int(x) for x in decoded_v1[3]),
                taker_fee_amount=int(decoded_v1[4]),
                maker_fee_amounts=tuple(int(x) for x in decoded_v1[5]),
            )
        except (DecodingError, IndexError, TypeError, ValueError, OverflowError):
            return None

    return None


def find_directory_matches(
    call: MatchOrdersCall,
    directory: TrackedUserDirectory,
) -> list[WatchedMatch]:
    """Match taker/maker orders against ``directory`` (EOA signer + Proxy/Safe maker)."""
    if directory.is_empty:
        return []
    matches: list[WatchedMatch] = []
    seen_roles: set[tuple[str, str, int | None]] = set()

    taker_leader = directory.canonical_leader_for(
        call.taker_order.maker
    ) or directory.canonical_leader_for(call.taker_order.signer)
    if taker_leader is not None:
        seen_roles.add((taker_leader, "taker", None))
        matches.append(
            WatchedMatch(
                wallet=taker_leader,
                role="taker",
                side=call.taker_order.side,
                token_id=call.taker_order.token_id,
                maker_amount=call.taker_order.maker_amount,
                taker_amount=call.taker_order.taker_amount,
                maker_index=None,
                order_maker_wallet=call.taker_order.maker.lower(),
            )
        )

    for idx, maker_order in enumerate(call.maker_orders):
        maker_leader = directory.canonical_leader_for(
            maker_order.maker
        ) or directory.canonical_leader_for(maker_order.signer)
        if maker_leader is not None and (maker_leader, "maker", idx) not in seen_roles:
            seen_roles.add((maker_leader, "maker", idx))
            matches.append(
                WatchedMatch(
                    wallet=maker_leader,
                    role="maker",
                    side=maker_order.side,
                    token_id=maker_order.token_id,
                    maker_amount=maker_order.maker_amount,
                    taker_amount=maker_order.taker_amount,
                    maker_index=idx,
                    order_maker_wallet=maker_order.maker.lower(),
                )
            )
    return matches


def find_watched_wallets(
    call: MatchOrdersCall,
    watched: set[str],
) -> list[WatchedMatch]:
    """Return every watched-wallet match inside ``call``.

    ``watched`` must be a set of lowercase ``0x``-prefixed addresses. Both the
    taker order's ``maker`` and every maker order's ``maker`` are checked.

    For the taker match, the watched wallet's side is the *taker order's*
    side (the taker order is the active order; its ``side`` is its own maker
    side, which from the watched wallet's perspective is its own side). For
    each maker match, the watched wallet's side is the maker order's side.
    """
    watched_lower = {w.lower() for w in watched}
    matches: list[WatchedMatch] = []

    taker_maker = call.taker_order.maker
    if taker_maker in watched_lower:
        matches.append(
            WatchedMatch(
                wallet=taker_maker,
                role="taker",
                side=call.taker_order.side,
                token_id=call.taker_order.token_id,
                maker_amount=call.taker_order.maker_amount,
                taker_amount=call.taker_order.taker_amount,
                maker_index=None,
            )
        )

    for idx, maker_order in enumerate(call.maker_orders):
        if maker_order.maker in watched_lower:
            matches.append(
                WatchedMatch(
                    wallet=maker_order.maker,
                    role="maker",
                    side=maker_order.side,
                    token_id=maker_order.token_id,
                    maker_amount=maker_order.maker_amount,
                    taker_amount=maker_order.taker_amount,
                    maker_index=idx,
                )
            )

    return matches


def is_valid_maker_side(side: int) -> bool:
    """Return True if ``side`` is a recognized maker-side enum value."""
    return side in (MAKER_SIDE_BUY, MAKER_SIDE_SELL)
