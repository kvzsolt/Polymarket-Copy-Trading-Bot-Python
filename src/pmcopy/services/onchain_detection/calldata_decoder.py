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
MATCH_ORDERS_SELECTOR: bytes = function_signature_to_4byte_selector(
    "matchOrders(bytes32,"
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes),"
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes)[],"
    "uint256,uint256[],uint256,uint256[])"
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


def decode_match_orders(calldata: bytes) -> MatchOrdersCall | None:
    """Decode a ``matchOrders`` calldata payload.

    Returns ``None`` (no raise) when:
      * the calldata is shorter than a selector
      * the selector does not match ``MATCH_ORDERS_SELECTOR``
      * the ABI decode fails (malformed/truncated payload)

    On success the returned ``MatchOrdersCall`` has lowercase addresses and
    integer amounts.
    """
    if not calldata or len(calldata) < 4:
        return None
    if calldata[:4] != MATCH_ORDERS_SELECTOR:
        return None
    try:
        outer = abi_decode([_MATCH_ORDERS_TYPE], calldata[4:])[0]
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
