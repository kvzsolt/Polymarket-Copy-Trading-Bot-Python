"""Pending-transaction detector via Alchemy ``alchemy_pendingTransactions``.

Locks CLOB mempool streaming strictly to ``toAddress: [<1..4 verified Exchanges>]``
(never ``fromAddress: [tracked_users]``, which misses 100% of operator-submitted
``matchOrders`` transactions), deduplicates rebroadcast ``tx.hash`` values in a
bounded TTL cache, and applies a fast 10-char selector + 32-byte word hex
pre-filter before calling ``bytes.fromhex`` or ``eth_abi.abi.decode``.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pmcopy.config.addresses import (
    ALL_VERIFIED_EXCHANGE_ADDRESSES,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    verified_polymarket_exchange_addresses,
)
from pmcopy.domain.tracked_users import TrackedUserDirectory
from pmcopy.services.onchain_detection.calldata_decoder import (
    MatchOrdersCall,
    SAFE_EXEC_TRANSACTION_SELECTOR,
    SourceTransactionCalldataDecoder,
    WatchedMatch,
    calldata_matches_tracked_users,
    decode_match_orders,
    find_directory_matches,
    find_watched_wallets,
)
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    OnchainTradeSignal,
    price_from_fill,
)

DIRECT_TX_FROM_ADDRESS_CAP = 1000
DEFAULT_PENDING_TX_HASH_CACHE_MAX = 32_768
DEFAULT_PENDING_TX_HASH_TTL_SECONDS = 120.0
_HEX_CHARS = frozenset("0123456789abcdef")


@dataclass(frozen=True)
class PendingTxConfig:
    """Validate bounded pending-transaction detection settings."""

    max_signal_age_ms: int = 500
    hashes_only: bool = False
    confidence_required: DetectionConfidence = DetectionConfidence.PENDING_PROBABLE


_DEFAULT_EXCHANGES = (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
)


class PendingTxHashCache:
    """Bounded TTL+LRU cache deduplicating rebroadcast mempool ``tx.hash`` notifications."""

    def __init__(
        self,
        *,
        max_entries: int = DEFAULT_PENDING_TX_HASH_CACHE_MAX,
        ttl_seconds: float = DEFAULT_PENDING_TX_HASH_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        if ttl_seconds <= 0.0:
            raise ValueError("ttl_seconds must be > 0")
        self._max_entries = int(max_entries)
        self._ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._entries: OrderedDict[str, float] = OrderedDict()

    def seen_or_record(self, tx_hash: str) -> bool:
        """Return True if ``tx_hash`` was already seen within ``ttl_seconds``."""
        cleaned = str(tx_hash or "").strip().lower()
        if not cleaned:
            return False
        now = self._clock()
        self._evict_expired(now)
        if cleaned in self._entries:
            return True
        self._entries[cleaned] = now
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
        return False

    def clear(self) -> None:
        """Clear all retained hashes."""
        self._entries.clear()

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self._ttl_seconds
        while self._entries:
            _, first_ts = next(iter(self._entries.items()))
            if first_ts >= cutoff:
                break
            self._entries.popitem(last=False)


def _validate_exchange_to_addresses(
    exchange_addresses: Iterable[str] | None,
) -> list[str]:
    if exchange_addresses is None:
        raw_list = list(verified_polymarket_exchange_addresses(include_legacy_v1=False))
    else:
        raw_list = list(exchange_addresses)

    if not raw_list:
        raise ValueError(
            "alchemy_pendingTransactions toAddress must be non-empty "
            "(unfiltered mempool subscriptions are forbidden)"
        )
    if len(raw_list) > 4:
        raise ValueError(
            "alchemy_pendingTransactions toAddress accepts at most 4 verified Exchange addresses"
        )

    deduped: list[str] = []
    for raw_addr in raw_list:
        cleaned = str(raw_addr or "").strip().lower()
        if cleaned not in ALL_VERIFIED_EXCHANGE_ADDRESSES:
            raise ValueError(
                f"unverified Exchange address in alchemy_pendingTransactions toAddress: {raw_addr!r}"
            )
        if cleaned not in deduped:
            deduped.append(cleaned)

    if not deduped or len(deduped) > 4:
        raise ValueError(
            "alchemy_pendingTransactions toAddress must contain 1..4 verified Exchange addresses"
        )
    return deduped


def build_exchange_pending_subscription_params(
    exchange_addresses: Iterable[str] | None = None,
) -> list[Any]:
    """Build singleton ``toAddress``-locked ``alchemy_pendingTransactions`` subscription."""
    validated = _validate_exchange_to_addresses(exchange_addresses)
    return [
        "alchemy_pendingTransactions",
        {
            "toAddress": validated,
            "hashesOnly": False,
        },
    ]


def pending_subscription_params(
    exchange_addresses: tuple[str, ...] | None = None,
    hashes_only: bool = False,
) -> list[Any]:
    """Build ``alchemy_pendingTransactions`` params with hard ``toAddress`` validation."""
    validated = _validate_exchange_to_addresses(exchange_addresses)
    return [
        "alchemy_pendingTransactions",
        {
            "toAddress": validated,
            "hashesOnly": bool(hashes_only),
        },
    ]


def build_direct_user_pending_subscriptions(
    tracked_addresses: Iterable[str] | None,
    *,
    track_direct_user_txs: bool = False,
    chunk_size: int = DIRECT_TX_FROM_ADDRESS_CAP,
) -> tuple[list[Any], ...]:
    """Build optional ``fromAddress`` pending filters (disabled unless explicitly enabled)."""
    if not track_direct_user_txs:
        return ()
    cap = min(DIRECT_TX_FROM_ADDRESS_CAP, max(1, int(chunk_size)))
    normalized: set[str] = set()
    for raw_addr in tracked_addresses or ():
        cleaned = str(raw_addr or "").strip().lower()
        if (
            len(cleaned) == 42
            and cleaned.startswith("0x")
            and all(ch in _HEX_CHARS for ch in cleaned[2:])
        ):
            normalized.add(cleaned)
    if not normalized:
        return ()
    ordered = sorted(normalized)
    return tuple(
        [
            "alchemy_pendingTransactions",
            {
                "fromAddress": ordered[idx : idx + cap],
                "hashesOnly": False,
            },
        ]
        for idx in range(0, len(ordered), cap)
    )


def _tx_to_exchange(tx: dict[str, Any], *, include_legacy_v1: bool = False) -> str:
    to = str(tx.get("to") or "").lower()
    allowed = (
        ALL_VERIFIED_EXCHANGE_ADDRESSES if include_legacy_v1 else frozenset(_DEFAULT_EXCHANGES)
    )
    if to in allowed:
        return to
    return ""


def _amounts_for_pending(match: WatchedMatch, call: MatchOrdersCall) -> tuple[int, int]:
    """Return ``(maker_filled, taker_filled)`` raw amounts for a pending match.

    For taker matches, computes the exact counterparty fill across matched
    maker orders (Complementary, Mint, and Merge per CTF Exchange ``Trading.sol``)
    so marketable taker orders with loose price caps reflect actual execution
    shares and price rather than worst-case limit ratios.
    """
    role = match.role
    if role == "taker":
        taker_making = int(call.taker_fill_amount or 0)
        if taker_making <= 0:
            return 0, 0
        if call.maker_orders and len(call.maker_orders) == len(call.maker_fill_amounts):
            taker_side = call.taker_order.side
            taker_taking = 0
            implied_taker_making = 0
            mint_makers_usdc = 0
            has_mint = False
            valid_legs = True
            for maker_order, raw_mfill in zip(
                call.maker_orders, call.maker_fill_amounts, strict=True
            ):
                mfill = int(raw_mfill or 0)
                if mfill <= 0:
                    continue
                _, maker_taking = _fill_pair(mfill, maker_order)
                if maker_taking <= 0:
                    valid_legs = False
                    break
                if maker_order.side != taker_side:
                    # Complementary match: maker's making asset IS taker's taking asset,
                    # and maker's taking asset IS what taker pays.
                    taker_taking += mfill
                    implied_taker_making += maker_taking
                elif taker_side == MAKER_SIDE_BUY:
                    # Mint match (BUY + BUY): maker_taking shares == (taker_usdc_leg + mfill).
                    if maker_taking < mfill:
                        valid_legs = False
                        break
                    has_mint = True
                    mint_makers_usdc += mfill
                    implied_taker_making += maker_taking - mfill
                else:
                    # Merge match (SELL + SELL): mfill shares merged -> mfill USDC minus maker's USDC cut.
                    if maker_taking > mfill:
                        valid_legs = False
                        break
                    taker_taking += mfill - maker_taking
                    implied_taker_making += mfill
            if has_mint:
                taker_taking += taker_making + mint_makers_usdc
            tolerance = max(1, len(call.maker_orders))
            if (
                valid_legs
                and taker_taking > 0
                and abs(implied_taker_making - taker_making) <= tolerance
            ):
                return taker_making, taker_taking
        return _fill_pair(call.taker_fill_amount, call.taker_order)

    maker_index = getattr(match, "maker_index", None)
    if maker_index is None:
        return 0, 0
    try:
        fill_amount = call.maker_fill_amounts[int(maker_index)]
        maker_order = call.maker_orders[int(maker_index)]
    except (IndexError, TypeError, ValueError):
        return 0, 0
    return _fill_pair(fill_amount, maker_order)


def _fill_pair(maker_fill_amount: int, order: object) -> tuple[int, int]:
    maker_total = int(getattr(order, "maker_amount", 0) or 0)
    taker_total = int(getattr(order, "taker_amount", 0) or 0)
    maker_filled = int(maker_fill_amount or 0)
    if maker_total <= 0 or taker_total <= 0 or maker_filled <= 0:
        return 0, 0
    taker_filled = (maker_filled * taker_total) // maker_total
    return maker_filled, taker_filled


def _signals_from_matches(
    matches: list[WatchedMatch],
    call: MatchOrdersCall,
    *,
    tx_hash: str,
    exchange: str,
    now_ms: int,
    source_receipt: Any = None,
) -> list[OnchainTradeSignal]:
    condition_id = "0x" + call.condition_id.hex() if call.condition_id else ""
    signals: list[OnchainTradeSignal] = []
    for match in matches:
        maker_filled, taker_filled = _amounts_for_pending(match, call)
        if maker_filled <= 0 or taker_filled <= 0:
            continue
        maker_side = match.side
        if maker_side not in (MAKER_SIDE_BUY, MAKER_SIDE_SELL):
            continue
        if maker_side == MAKER_SIDE_BUY:
            size_raw = taker_filled
            usdc_raw = maker_filled
        else:
            size_raw = maker_filled
            usdc_raw = taker_filled
        size = Decimal(size_raw) / Decimal(10**6)
        usdc = Decimal(usdc_raw) / Decimal(10**6)
        price = price_from_fill(maker_side, maker_filled, taker_filled)
        side = "BUY" if maker_side == MAKER_SIDE_BUY else "SELL"
        holder = (match.order_maker_wallet or match.wallet).lower()
        signals.append(
            OnchainTradeSignal(
                leader_wallet=match.wallet,
                token_id=str(match.token_id),
                side=side,
                size=size,
                price=price,
                usdc_size=usdc,
                tx_hash=tx_hash,
                confidence=DetectionConfidence.PENDING_PROBABLE,
                maker_side=maker_side,
                watched_role=match.role,
                event_ts=now_ms,
                log_index=None,
                condition_id=condition_id,
                provisional=True,
                exchange=exchange,
                order_hash=None,
                source_receipt=source_receipt,
                token_holder_wallet=holder,
            )
        )
    return signals


def build_signals_from_pending_tx_for_directory(
    tx: dict[str, Any],
    directory: TrackedUserDirectory,
    *,
    include_legacy_v1: bool = False,
    tx_hash_cache: PendingTxHashCache | None = None,
    recv_ts_ms: int | None = None,
    config: PendingTxConfig | None = None,
) -> list[OnchainTradeSignal]:
    """Fast-prefiltered pending ``matchOrders`` decoder for ``TrackedUserDirectory``."""
    if not isinstance(tx, dict) or directory.is_empty:
        return []
    allowed_exchanges = (
        ALL_VERIFIED_EXCHANGE_ADDRESSES if include_legacy_v1 else frozenset(_DEFAULT_EXCHANGES)
    )
    direct_exchange = _tx_to_exchange(tx, include_legacy_v1=include_legacy_v1)
    input_hex = str(tx.get("input") or "")
    is_safe_tx = (
        not direct_exchange
        and len(input_hex) >= 10
        and input_hex[:10].lower() == "0x" + SAFE_EXEC_TRANSACTION_SELECTOR.hex()
    )
    if not direct_exchange and not is_safe_tx:
        return []

    tx_hash = str(tx.get("hash") or "").strip().lower()
    if tx_hash_cache is not None and tx_hash and tx_hash_cache.seen_or_record(tx_hash):
        return []

    if not calldata_matches_tracked_users(
        input_hex,
        directory.hex40_without_prefix,
        include_legacy_v1=include_legacy_v1,
    ):
        return []

    raw_hex = input_hex[2:] if input_hex[:2].lower() == "0x" else input_hex
    try:
        calldata = bytes.fromhex(raw_hex)
    except ValueError:
        return []

    if direct_exchange:
        exchange = direct_exchange
        call = decode_match_orders(calldata, include_legacy_v1=include_legacy_v1)
    else:
        source_call = SourceTransactionCalldataDecoder(allowed_exchanges).decode(
            str(tx.get("to") or ""),
            calldata,
        )
        if source_call is None:
            return []
        exchange = source_call.exchange_address
        call = source_call.match_orders

    if call is None:
        return []
    matches = find_directory_matches(call, directory)
    if not matches:
        return []

    _ = config or PendingTxConfig()
    now_ms = recv_ts_ms if recv_ts_ms is not None else int(time.time() * 1000)
    return _signals_from_matches(
        matches,
        call,
        tx_hash=tx_hash,
        exchange=exchange,
        now_ms=now_ms,
        source_receipt=tx.get("_source_receipt"),
    )


def build_signals_from_pending_tx(
    tx: dict[str, Any],
    watched_wallets: set[str],
    recv_ts_ms: int | None = None,
    config: PendingTxConfig | None = None,
) -> list[OnchainTradeSignal]:
    """Decode a pending tx and emit one ``PENDING_PROBABLE`` signal per watched match."""
    if not isinstance(tx, dict) or not watched_wallets:
        return []
    exchange = _tx_to_exchange(tx)
    if not exchange:
        return []
    input_hex = str(tx.get("input") or "")
    tracked_hex40 = frozenset(
        w.strip().lower().removeprefix("0x")
        for w in watched_wallets
        if len(w.strip().lower().removeprefix("0x")) == 40
    )
    if not calldata_matches_tracked_users(input_hex, tracked_hex40):
        return []
    raw_hex = input_hex[2:] if input_hex[:2].lower() == "0x" else input_hex
    try:
        calldata = bytes.fromhex(raw_hex)
    except ValueError:
        return []
    call = decode_match_orders(calldata)
    if call is None:
        return []

    matches = find_watched_wallets(call, watched_wallets)
    if not matches:
        return []

    now_ms = recv_ts_ms if recv_ts_ms is not None else int(time.time() * 1000)
    _ = config or PendingTxConfig()
    tx_hash = str(tx.get("hash") or "").lower()
    return _signals_from_matches(
        matches,
        call,
        tx_hash=tx_hash,
        exchange=exchange,
        now_ms=now_ms,
    )


def is_stale_pending(signal: OnchainTradeSignal, now_ms: int, max_age_ms: int) -> bool:
    """Return True if a pending signal is older than ``max_age_ms`` at dispatch."""
    if not signal.provisional:
        return False
    if signal.event_ts is None or signal.event_ts > now_ms:
        return True
    if max_age_ms <= 0:
        return False
    return (now_ms - signal.event_ts) > max_age_ms
