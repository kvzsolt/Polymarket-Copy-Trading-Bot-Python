"""Redemption service — thin facade re-exporting from services.redemptionService."""
from __future__ import annotations

from services.redemptionService import RedemptionService

__all__ = ["RedemptionService"]
