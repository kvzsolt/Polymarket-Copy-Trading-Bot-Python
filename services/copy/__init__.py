"""Copy trading services — signal normalization, decisions, sizing, and idempotency."""
from __future__ import annotations

from services.copy.signal_normalizer import normalize_trade_signal
from services.copy.copy_decision import CopyDecision, CopyDecisionService, SizingDecision, effective_intent_key
from services.copy.sizing_policy import calculate_copy_notional, normalize_strategy, get_trade_multiplier
from services.order_sizing import SizingDecision as OrderSizingDecision, size_buy_order, size_sell_order
from services.idempotency import compute_effective_key, compute_effective_identity, EffectiveOrderIdentity

__all__ = [
    "CopyDecision",
    "CopyDecisionService",
    "EffectiveOrderIdentity",
    "OrderSizingDecision",
    "SizingDecision",
    "calculate_copy_notional",
    "compute_effective_key",
    "compute_effective_identity",
    "effective_intent_key",
    "get_trade_multiplier",
    "normalize_strategy",
    "normalize_trade_signal",
    "size_buy_order",
    "size_sell_order",
]
