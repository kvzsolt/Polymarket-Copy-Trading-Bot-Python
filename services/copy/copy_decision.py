"""Copy trading decision logic — thin facade over CopyDecisionService."""
from __future__ import annotations

# Re-export for new-style imports
from services.copyDecisionService import (
    CopyDecision,
    CopyDecisionService,
    SizingDecision,
    effective_intent_key,
)

__all__ = ["CopyDecision", "CopyDecisionService", "SizingDecision", "effective_intent_key"]
