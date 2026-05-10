"""Snake_case shim path for issue #67.

The plan.md PR 2 spec names ``services/position_action_coordinator.py``
explicitly. The implementation lives in :mod:`services.positionActionCoordinator`
(camelCase, retained for the 8+ existing call sites). This module re-exports
the public symbols so either import path resolves.
"""

from __future__ import annotations

from domain.position_key import PositionKey
from services.positionActionCoordinator import (
    PositionActionCoordinator,
    PositionScope,
)

__all__ = ["PositionActionCoordinator", "PositionScope", "PositionKey"]
