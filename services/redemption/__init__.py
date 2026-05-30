"""On-chain CTF auto-redemption subsystem.

Opt-in settlement loop that claims collateral on resolved Polymarket
positions via ``CTF.redeemPositions``. Lazily exported because the
implementation imports ``web3``/``eth_account`` at module load; keeping the
package import cheap means ``import services.redemption`` does not pull the
Web3 stack unless the service is actually constructed.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.redemption.redemption_service import (
        RedemptionPosition,
        RedemptionService,
        RedemptionTxResult,
    )

_LAZY_EXPORTS = {
    "RedemptionService": "services.redemption.redemption_service",
    "RedemptionPosition": "services.redemption.redemption_service",
    "RedemptionTxResult": "services.redemption.redemption_service",
}


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    value = getattr(importlib.import_module(target), name)
    globals()[name] = value
    return value


__all__ = [
    "RedemptionPosition",
    "RedemptionService",
    "RedemptionTxResult",
]
