"""Pending-transaction detector via Alchemy ``alchemy_pendingTransactions`` (PR2).

Subscribes to pending Polygon transactions whose ``to`` is either the CTF
Exchange V2 or the Neg Risk CTF Exchange V2, with ``hashesOnly=false`` so the
full transaction object (including ``input`` calldata) is delivered. Each
pending tx is decoded as a ``matchOrders`` call; if a watched wallet appears
as ``takerOrder.maker`` or any ``makerOrders[i].maker``, a provisional
``PENDING_PROBABLE`` signal is emitted.

Verified Alchemy subscription shape (Firecrawl,
https://www.alchemy.com/docs/reference/alchemy-pendingtransactions):
``eth_subscribe`` method, ``params: {"alchemy_pendingTransactions": {
"toAddress": [...], "hashesOnly": false}}``. The notification payload is a
full transaction object (``hash, input, from, to, gas, gasPrice, nonce,
blockHash, blockNumber, transactionIndex, value, v, r, s``).

Pending signals are **provisional**: they may be dropped, reverted, or
replaced. They only feed the copy path when ``ENABLE_ONCHAIN_PENDING_DETECTION``
is on, and they are superseded by the mined ``OrderFilled`` signal that
shares their ``tx_hash``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from pmcopy.services.onchain_detection.calldata_decoder import (
    MatchOrdersCall,
    WatchedMatch,
    decode_match_orders,
    find_watched_wallets,
)
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    price_from_fill,
)
from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)


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


def pending_subscription_params(
    exchange_addresses: tuple[str, ...] | None = None,
    hashes_only: bool = False,
) -> list:
    """Build the ``eth_subscribe`` JSON-RPC ``params`` list for ``alchemy_pendingTransactions``.

    ``eth_subscribe`` expects ``params`` as a positional array like
    ``["alchemy_pendingTransactions", options]``. The notification payload
    is a full transaction object (``hash, input, from, to, ...``) when
    ``hashesOnly`` is False; hashes only when True.
    """
    addresses = tuple(a.lower() for a in (exchange_addresses or _DEFAULT_EXCHANGES))
    options = {
        "toAddress": list(addresses),
        "hashesOnly": bool(hashes_only),
    }
    return ["alchemy_pendingTransactions", options]


def _tx_to_exchange(tx: dict) -> str:
    to = str(tx.get("to") or "").lower()
    if to in _DEFAULT_EXCHANGES:
        return to
    return ""


def _amounts_for_pending(match: WatchedMatch, call: MatchOrdersCall) -> tuple[int, int]:
    """Return ``(maker_filled, taker_filled)`` raw amounts for a pending match.

    ``matchOrders`` fill arguments are expressed in maker-amount units. Compute
    the paired taker amount from the order ratio so partial pending matches do
    not copy the full order size.
    """
    role = match.role
    if role == "taker":
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


def build_signals_from_pending_tx(
    tx: dict,
    watched_wallets: set[str],
    recv_ts_ms: int | None = None,
    config: PendingTxConfig | None = None,
) -> list[OnchainTradeSignal]:
    """Decode a pending tx and emit one ``PENDING_PROBABLE`` signal per watched match.

    Returns an empty list if:
      * the tx is not a ``matchOrders`` call to a known exchange
      * no watched wallet appears in the decoded orders
      * the signal is older than ``config.max_signal_age_ms`` (when
        ``recv_ts_ms`` is provided, the tx's own timestamp is not known
        pre-inclusion, so the age gate is a no-op unless the caller supplies a
        receive time; in practice the runtime stamps ``event_ts = now`` and
        the gate is applied at dispatch time)

    The ``tx_hash`` is taken from ``tx.hash``; ``log_index`` is ``None``
    (pending); ``provisional=True``; ``condition_id`` is extracted from the
    calldata's first arg (the ``conditionId`` passed to ``matchOrders``).
    """
    if not isinstance(tx, dict):
        return []
    exchange = _tx_to_exchange(tx)
    if not exchange:
        return []
    input_hex = str(tx.get("input") or "")
    if not input_hex:
        return []
    if input_hex.startswith("0x") or input_hex.startswith("0X"):
        input_hex = input_hex[2:]
    try:
        calldata = bytes.fromhex(input_hex)
    except ValueError:
        return []
    call = decode_match_orders(calldata)
    if call is None:
        return []

    matches = find_watched_wallets(call, watched_wallets)
    if not matches:
        return []

    now_ms = recv_ts_ms if recv_ts_ms is not None else int(time.time() * 1000)
    config = config or PendingTxConfig()
    # Pending tx have no on-chain timestamp; the age gate is applied by the
    # runtime at dispatch time using recv_ts_ms. Here we just stamp event_ts.
    _ = config

    condition_id = "0x" + call.condition_id.hex() if call.condition_id else ""

    signals: list[OnchainTradeSignal] = []
    tx_hash = str(tx.get("hash") or "").lower()
    for match in matches:
        maker_filled, taker_filled = _amounts_for_pending(match, call)
        if maker_filled <= 0 or taker_filled <= 0:
            continue
        # Size/USDC scale: 6 decimals (CTF tokens + USDC.e).
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
        # In the calldata ``Order`` struct, ``side`` is the order's own side
        # (not the unified maker-side of the OrderFilled event). The taker
        # order's side is the taker's own side; a maker order's side is the
        # maker's own side. No inversion is needed for pending signals.
        side = "BUY" if maker_side == MAKER_SIDE_BUY else "SELL"
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
            )
        )
    return signals


def is_stale_pending(signal: OnchainTradeSignal, now_ms: int, max_age_ms: int) -> bool:
    """Return True if a pending signal is older than ``max_age_ms`` at dispatch."""
    if not signal.provisional:
        return False
    if signal.event_ts is None or signal.event_ts > now_ms:
        return True
    if max_age_ms <= 0:
        return False
    return (now_ms - signal.event_ts) > max_age_ms
