"""Redemption hardening services."""
from __future__ import annotations

# Eagerly import lightweight modules
from services.redemption.redemption_reconciler import (
    RedemptionReconciler,
    RedemptionStatus,
    ReconciliationResult,
)

# ---------------------------------------------------------------------------
# Lazy imports for modules that depend on web3/py_clob_client_v2.
#
# SECURITY: This is a strict allowlist.  Only symbols explicitly listed
# below can be resolved via lazy import.
# ---------------------------------------------------------------------------
import threading as _threading

_LAZY_IMPORTS: dict[str, str] = {
    "RedemptionService": "services.redemption.redemption_service",
    "validate_redemption_wallet_mode": "services.redemption.wallet_validator",
}
_LAZY_IMPORT_LOCK = _threading.Lock()


def __getattr__(name: str):
    """Lazy-load heavy submodules only when accessed (strict allowlist)."""
    module_path = _LAZY_IMPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib
    mod = importlib.import_module(module_path)
    value = getattr(mod, name)
    with _LAZY_IMPORT_LOCK:
        if name not in globals():
            globals()[name] = value
    return value


__all__ = [
    "RedemptionReconciler",
    "RedemptionService",
    "RedemptionStatus",
    "ReconciliationResult",
    "validate_redemption_wallet_mode",
]
