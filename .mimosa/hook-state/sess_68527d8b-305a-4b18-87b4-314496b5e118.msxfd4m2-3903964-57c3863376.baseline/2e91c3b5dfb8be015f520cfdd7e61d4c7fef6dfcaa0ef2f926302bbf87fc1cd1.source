"""Early on-chain trader detection (PR2).

Provisional pending-transaction signals + authoritative mined ``OrderFilled``
log signals for watched Polymarket trader wallets, on both the CTF Exchange V2
and the Neg Risk CTF Exchange V2.

Behind ``ENABLE_ONCHAIN_PENDING_DETECTION`` (default off); the mined log
subscription is always safe to run because mined fills are authoritative.
"""

from __future__ import annotations

from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)
from services.onchain_detection.mined_fill_deduplicator import MinedFillDeduplicator

__all__ = [
    "DetectionConfidence",
    "OnchainTradeSignal",
    "MinedFillDeduplicator",
]
