from __future__ import annotations

from datetime import datetime, timedelta, timezone

from domain.models import ComplianceDecision
from utils.fetchData import fetch_data
from utils.logger import get_logger

logger = get_logger("Compliance")

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

async def check_geoblock() -> ComplianceDecision:
    """Return a ComplianceDecision; fail closed on any error or uncertainty."""
    checked_at = utcnow()

    try:
        data = await fetch_data(GEOBLOCK_URL, silent=True)
    except Exception as exc:
        status = int(getattr(exc, "status", 0) or 0)
        if status > 0:
            logger.warning(f"Geoblock check HTTP error {status}; failing closed")
            return ComplianceDecision(
                allowed=False,
                reason=f"geoblock_failed_closed_http_{status}",
                source="polymarket",
                checked_at=checked_at,
                expires_at=None,
                raw={"error": str(exc)},
            )
        logger.warning(f"Geoblock check failed closed: {exc}")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed",
            source="polymarket",
            checked_at=checked_at,
            expires_at=None,
            raw={"error": str(exc)},
        )

    if not isinstance(data, dict):
        logger.warning("Geoblock check failed closed: invalid payload type")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed_invalid_payload",
            source="polymarket",
            checked_at=checked_at,
            expires_at=None,
            raw={"payload_type": type(data).__name__},
        )

    if data.get("blocked") is None:
        logger.warning("Geoblock check failed closed: blocked field missing")
        return ComplianceDecision(
            allowed=False,
            reason="geoblock_failed_closed_missing_field",
            source="polymarket",
            checked_at=checked_at,
            expires_at=None,
            raw={"payload_keys": list(data.keys())},
        )

    blocked = bool(data.get("blocked", False))
    if blocked:
        return ComplianceDecision(
            allowed=False,
            reason="region_blocked_by_polymarket",
            source="polymarket",
            checked_at=checked_at,
            expires_at=None,
            raw=data,
        )

    return ComplianceDecision(
        allowed=True,
        reason="ok",
        source="polymarket",
        checked_at=checked_at,
        expires_at=checked_at + timedelta(minutes=15),
        raw=data,
    )
