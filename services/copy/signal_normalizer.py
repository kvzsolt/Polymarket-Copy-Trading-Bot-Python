"""Normalize raw leader trade signals into canonical NormalizedSignal objects."""
from __future__ import annotations

from decimal import Decimal

from domain.trading_intent import NormalizedSignal


def normalize_trade_signal(raw: dict) -> NormalizedSignal | None:
    """Convert a raw trade event dict into a NormalizedSignal.

    Returns None if the signal cannot be normalized (missing required fields).
    """
    # Extract required fields
    leader_id = str(raw.get("leader_id") or raw.get("leader_wallet") or "").strip()
    condition_id = str(raw.get("condition_id") or "").strip()
    token_id = str(raw.get("token_id") or raw.get("asset_id") or "").strip()
    side = str(raw.get("side") or "").strip().upper()

    if not all([leader_id, condition_id, token_id, side]):
        return None
    if side not in ("BUY", "SELL"):
        return None

    try:
        size = Decimal(str(raw.get("size") or raw.get("size_tokens") or 0))
        price = Decimal(str(raw.get("price") or 0))
        usdc_size = Decimal(str(raw.get("usdc_size") or raw.get("notional") or 0))
    except Exception:
        return None

    if size <= 0 or price <= 0:
        return None
    if usdc_size <= 0:
        usdc_size = size * price

    return NormalizedSignal(
        leader_id=leader_id,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        size=size,
        price=price,
        usdc_size=usdc_size,
        tx_hash=str(raw.get("tx_hash") or ""),
        event_ts=int(raw.get("event_ts") or raw.get("timestamp") or 0),
        title=str(raw.get("title") or ""),
        event_slug=str(raw.get("event_slug") or ""),
        outcome=str(raw.get("outcome") or ""),
        submarket=str(raw.get("submarket") or ""),
    )
