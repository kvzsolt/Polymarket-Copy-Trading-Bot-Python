"""Copy trading services — signal normalization, decisions, and sizing."""
from __future__ import annotations

from services.copy.signal_normalizer import normalize_trade_signal
from services.copy.copy_decision import CopyDecision, CopyDecisionService, SizingDecision, effective_intent_key
from services.copy.sizing_policy import calculate_copy_notional, normalize_strategy, get_trade_multiplier

__all__ = [
    "CopyDecision",
    "CopyDecisionService",
    "SizingDecision",
    "calculate_copy_notional",
    "effective_intent_key",
    "get_trade_multiplier",
    "normalize_strategy",
    "normalize_trade_signal",
]
