from __future__ import annotations

import aiohttp

from domain.models import ComplianceDecision
from utils.fetchData import fetch_data
from utils.logger import get_logger

logger = get_logger("Compliance")


GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


async def check_geoblock() -> ComplianceDecision:
    """Return a ComplianceDecision; fail closed on any error or uncertainty."""
    try:
        data = await fetch_data(GEOBLOCK_URL, silent=True)
    except aiohttp.ClientResponseError as exc:
        status = int(getattr(exc, "status", 0) or 0)
        if status > 0:
            logger.warning(f"Geoblock check HTTP error {status}; failing closed")
            return ComplianceDecision(
                allowed=False,
                reason=f"geoblock_failed_closed_http_{status}",
                metadata={"error": str(exc)},
            )
        logger.warning(f"Geoblock check failed closed: {exc}")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed",
            metadata={"error": str(exc)},
        )
    except Exception as exc:
        logger.warning(f"Geoblock check failed closed: {exc}")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed",
            metadata={"error": str(exc)},
        )

    if not isinstance(data, dict):
        logger.warning("Geoblock check failed closed: invalid payload type")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed_invalid_payload",
            metadata={"payload_type": type(data).__name__},
        )

    if data.get("blocked") is None:
        logger.warning("Geoblock check failed closed: blocked field missing")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed_missing_field",
            metadata={"payload_keys": list(data.keys())},
        )

    blocked = bool(data.get("blocked", False))
    if blocked:
        return ComplianceDecision(
            allowed=False,
            reason="region_blocked_by_polymarket",
        )
    return ComplianceDecision(allowed=True, reason="ok")
