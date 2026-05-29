"""Effective idempotency — post-sizing duplicate suppression (Issue #69).

Duplicate protection must represent the **final executable order**, not the
raw request.  An upsize from 3 → 5 tokens, a tick-rounded price, or a
different condition/token must all produce a different key so the bot
does not suppress a genuinely different executable order.

The key is computed **after** :func:`services.order_sizing.size_buy_order`
or :func:`services.order_sizing.size_sell_order` has produced the effective
size and price, ensuring the hash reflects what the venue will actually
receive.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class EffectiveOrderIdentity:
    """Structured identity of a sized order for idempotency."""

    idempotency_key: str
    user: str
    condition_id: str
    token_id: str
    side: str
    effective_size: Decimal
    effective_price: Decimal
    tick_size: Decimal
    intent_id: str
    policy_version: str


def compute_effective_key(
    *,
    user: str,
    condition_id: str,
    token_id: str,
    side: str,
    effective_size: Decimal,
    effective_price: Decimal,
    tick_size: Decimal,
    intent_id: str = "",
    policy_version: str = "",
) -> str:
    """Compute a SHA-256 idempotency key from the **effective adjusted** order.

    This must be called **after** sizing adjustments so that an upsize
    produces a different key than the original below-min request, and so
    that tick-rounded prices are part of the identity.

    Parameters
    ----------
    user:
        Wallet / user identifier.
    condition_id:
        Polymarket condition identifier.
    token_id:
        Polymarket token identifier.
    side:
        "BUY" or "SELL".
    effective_size:
        Final executable token count from :class:`SizingDecision`.
    effective_price:
        Worst-case protected price from :class:`SizingDecision`.
    tick_size:
        Market tick size used in price rounding.
    intent_id:
        Optional source/intent identifier for traceability.
    policy_version:
        Optional policy version to distinguish rule changes.

    Returns
    -------
    str
        SHA-256 hex digest representing the executable order identity.
    """
    # Issue #102: canonical Decimal form so that benign formatting
    # differences (10 vs 10.00, 0.5 vs 0.50) produce identical keys.
    # ``Decimal.normalize()`` strips trailing zeros and ``format(..., "f")``
    # converts any scientific intermediate back to fixed-point.
    def _canon(value: Decimal) -> str:
        return format(value.normalize(), "f")

    payload_dict = {
        "user": user,
        "condition_id": condition_id,
        "token_id": token_id,
        "side": side.upper(),
        "effective_size": _canon(effective_size),
        "effective_price": _canon(effective_price),
        "tick_size": _canon(tick_size),
        "intent_id": intent_id,
    }
    if policy_version:
        payload_dict["policy_version"] = policy_version
    payload = json.dumps(
        payload_dict,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_effective_identity(
    *,
    user: str,
    condition_id: str,
    token_id: str,
    side: str,
    effective_size: Decimal,
    effective_price: Decimal,
    tick_size: Decimal,
    intent_id: str = "",
    policy_version: str = "",
) -> EffectiveOrderIdentity:
    """Build a full :class:`EffectiveOrderIdentity` with the computed key.

    Convenience wrapper that returns the structured identity object along
    with the computed key so callers can persist all fields together.
    """
    key = compute_effective_key(
        user=user,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        effective_size=effective_size,
        effective_price=effective_price,
        tick_size=tick_size,
        intent_id=intent_id,
        policy_version=policy_version,
    )
    return EffectiveOrderIdentity(
        idempotency_key=key,
        user=user,
        condition_id=condition_id,
        token_id=token_id,
        side=side.upper(),
        effective_size=effective_size,
        effective_price=effective_price,
        tick_size=tick_size,
        intent_id=intent_id,
        policy_version=policy_version,
    )