"""CCXT-only health and telemetry terminology.

Health data describes the CCXT transport (``available``, ``transport``,
``ccxt_version``, ``authenticated``, ``live_execution_ready``,
``precision_patch``, ``capabilities``) and never carries SDK-era field names
or credential material.
"""

from __future__ import annotations

from typing import Mapping

TRANSPORT_LABEL = "ccxt.prediction.polymarket"
PRECISION_PATCH_ID = "pmcopy-arbitrary-tick-v1"

CCXT_CAPABILITY_METHODS = (
    "create_order",
    "cancel_order",
    "fetch_order",
    "fetch_open_orders",
    "fetch_my_trades",
)


def build_ccxt_health_payload(
    *,
    available: bool,
    ccxt_version: str | None,
    authenticated: bool,
    live_execution_ready: bool,
    capabilities: Mapping[str, bool],
    error: str | None = None,
    details: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Assemble the canonical CCXT venue health payload.

    Args:
        available: Whether the CCXT transport satisfies the compatibility gate.
        ccxt_version: Detected installed CCXT version, when known.
        authenticated: Whether the exchange instance carries live credentials.
        live_execution_ready: Whether authenticated live execution is permitted.
        capabilities: Per-method transport capability flags.
        error: Redacted error category, or ``None`` when healthy.
        details: Optional extra diagnostic fields merged into the payload.

    Returns:
        A JSON-safe dict using CCXT terminology only.
    """

    payload: dict[str, object] = {
        "available": bool(available),
        "transport": TRANSPORT_LABEL,
        "ccxt_version": ccxt_version,
        "authenticated": bool(authenticated),
        "live_execution_ready": bool(live_execution_ready),
        "precision_patch": PRECISION_PATCH_ID,
        "capabilities": {
            name: bool(capabilities.get(name, False)) for name in CCXT_CAPABILITY_METHODS
        },
        "error": error,
    }
    if details:
        payload.update(details)
    return payload


__all__ = [
    "CCXT_CAPABILITY_METHODS",
    "PRECISION_PATCH_ID",
    "TRANSPORT_LABEL",
    "build_ccxt_health_payload",
]
