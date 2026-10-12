"""Compliance services — geoblock checks and regulatory gates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pmcopy.domain.models import ComplianceDecision
from pmcopy.utils.fetchData import fetch_data
from pmcopy.utils.logger import get_logger

logger = get_logger("Compliance")

GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


def utcnow() -> datetime:
    """Return the current timezone-aware UTC time."""
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


class GeoblockGate:
    """Trade-path geoblock gate with caching + expiry revalidation (P1-j).

    ``check_geoblock`` is fail-closed, but it was consulted only at startup and
    its 15-minute ``expires_at`` was consumed by nothing — so a region change
    mid-session was never re-evaluated and enforcement never touched the trade
    path. This gate caches the decision and revalidates it: an ``allowed``
    decision is trusted until its ``expires_at`` (~15 min); a blocked /
    fail-closed decision (``expires_at is None``) is re-checked after a short
    interval so a recovered region can resume and a transient block is not
    trusted forever. Strictly opt-in — when ``enabled`` is False it enforces
    nothing and returns ``None``.
    """

    def __init__(
        self,
        enabled: bool,
        *,
        check=check_geoblock,
        block_recheck_seconds: float = 60.0,
    ) -> None:
        self._enabled = bool(enabled)
        self._check = check
        self._block_recheck_seconds = float(block_recheck_seconds)
        self._decision: ComplianceDecision | None = None
        self._next_check_at: datetime | None = None

    @property
    def enabled(self) -> bool:
        """Return whether compliance enforcement is enabled."""
        return self._enabled

    async def ensure_allowed(self) -> ComplianceDecision | None:
        """Current ComplianceDecision (revalidating on expiry), or ``None`` when
        geoblock enforcement is disabled."""
        if not self._enabled:
            return None
        now = utcnow()
        if self._decision is None or self._next_check_at is None or now >= self._next_check_at:
            decision = await self._check()
            self._decision = decision
            if decision.allowed and decision.expires_at is not None:
                self._next_check_at = decision.expires_at
            else:
                self._next_check_at = now + timedelta(seconds=self._block_recheck_seconds)
        return self._decision


__all__ = ["GEOBLOCK_URL", "check_geoblock", "ComplianceDecision", "GeoblockGate"]
