"""P1-j: GeoblockGate caches the geoblock decision and revalidates on expiry so
enforcement is consulted on the trade path, not just at boot.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

import services.compliance as compliance
from domain.models import ComplianceDecision
from services.compliance import GeoblockGate

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _allowed(at=T0, ttl_min=15):
    return ComplianceDecision(
        allowed=True, reason="ok", source="polymarket",
        checked_at=at, expires_at=at + timedelta(minutes=ttl_min), raw={},
    )


def _blocked(at=T0):
    return ComplianceDecision(
        allowed=False, reason="region_blocked_by_polymarket", source="polymarket",
        checked_at=at, expires_at=None, raw={},
    )


@pytest.mark.asyncio
async def test_disabled_gate_enforces_nothing():
    check = AsyncMock(return_value=_blocked())
    gate = GeoblockGate(False, check=check)
    assert await gate.ensure_allowed() is None
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_allowed_decision_cached_until_expiry(monkeypatch):
    monkeypatch.setattr(compliance, "utcnow", lambda: T0)
    check = AsyncMock(return_value=_allowed(T0, 15))
    gate = GeoblockGate(True, check=check)

    assert (await gate.ensure_allowed()).allowed
    # Within the 15-minute TTL: cached, no re-check.
    monkeypatch.setattr(compliance, "utcnow", lambda: T0 + timedelta(minutes=5))
    await gate.ensure_allowed()
    assert check.await_count == 1
    # Past expiry: revalidate (the 15-minute expires_at is now consumed).
    monkeypatch.setattr(compliance, "utcnow", lambda: T0 + timedelta(minutes=16))
    await gate.ensure_allowed()
    assert check.await_count == 2


@pytest.mark.asyncio
async def test_blocked_decision_rechecked_after_interval(monkeypatch):
    monkeypatch.setattr(compliance, "utcnow", lambda: T0)
    check = AsyncMock(return_value=_blocked(T0))
    gate = GeoblockGate(True, check=check, block_recheck_seconds=60)

    assert not (await gate.ensure_allowed()).allowed
    monkeypatch.setattr(compliance, "utcnow", lambda: T0 + timedelta(seconds=30))
    await gate.ensure_allowed()
    assert check.await_count == 1  # cached within recheck window
    monkeypatch.setattr(compliance, "utcnow", lambda: T0 + timedelta(seconds=61))
    await gate.ensure_allowed()
    assert check.await_count == 2  # re-checked after the window


@pytest.mark.asyncio
async def test_recovered_region_resumes_after_block_recheck(monkeypatch):
    monkeypatch.setattr(compliance, "utcnow", lambda: T0)
    check = AsyncMock(side_effect=[_blocked(T0), _allowed(T0 + timedelta(seconds=61), 15)])
    gate = GeoblockGate(True, check=check, block_recheck_seconds=60)

    assert not (await gate.ensure_allowed()).allowed
    monkeypatch.setattr(compliance, "utcnow", lambda: T0 + timedelta(seconds=61))
    assert (await gate.ensure_allowed()).allowed
