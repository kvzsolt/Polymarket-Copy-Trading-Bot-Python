"""Regress premium Wallet360 position percentages at the discovery boundary."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from integrations.falcon.errors import FalconProtocolError
from integrations.falcon.models import Wallet360Snapshot
from services.discovery.falcon_candidate_service import (
    FalconCandidateService,
    FalconDiscoveryPolicy,
)
from tests.test_falcon_candidate_service import GOOD, NOW, FakeFalcon, wallet360


@pytest.mark.parametrize("raw", ["0", "0.3048", "1", "1.01", "21.225", "100"])
def test_position_percentage_always_normalizes_percentage_points(raw: str) -> None:
    """A sub-one percentage uses the same unit as observed premium values."""
    snapshot = Wallet360Snapshot.from_mapping({"max_position_pct": raw})

    assert snapshot.max_position_pct == Decimal(raw) / Decimal(100)
    assert snapshot.raw["max_position_pct"] == raw


@pytest.mark.parametrize("raw", [0, 1, 100, Decimal("21.225")])
def test_position_percentage_accepts_exact_numeric_values(raw: int | Decimal) -> None:
    """Exact wire numbers normalize without introducing binary float arithmetic."""
    assert Wallet360Snapshot.from_mapping({"max_position_pct": raw}).max_position_pct == (
        Decimal(raw) / Decimal(100)
    )


def test_missing_position_percentage_stays_missing() -> None:
    """Omission and explicit null must not invent a zero exposure observation."""
    assert Wallet360Snapshot.from_mapping({}).max_position_pct is None
    assert Wallet360Snapshot.from_mapping({"max_position_pct": None}).max_position_pct is None


@pytest.mark.parametrize(
    "raw",
    [True, "NaN", "Infinity", "-Infinity", "-0.01", "100.01", "", "invalid-secret", {}],
)
def test_invalid_position_percentage_fails_closed_without_echoing_input(raw: object) -> None:
    """Unit correction retains bounded, finite, non-boolean validation."""
    with pytest.raises(ValueError) as captured:
        Wallet360Snapshot.from_mapping({"max_position_pct": raw})

    assert "invalid-secret" not in str(captured.value)


class PositionPercentageFalcon(FakeFalcon):
    """Replay premium percentage ranges with synthetic identities and fixed time."""

    def __init__(self, historical_percentage: str = "21.225") -> None:
        super().__init__()
        self._historical_percentage = historical_percentage

    async def wallet_360(
        self, wallet: str, window_days: int, **_kwargs: object
    ) -> list[dict[str, object]]:
        """Return historical and latest rows in each requested evidence window."""
        old = wallet360(wallet, window_days, at=NOW - timedelta(days=1))[0]
        latest = wallet360(wallet, window_days)[0]
        old["max_position_pct"] = self._historical_percentage
        latest["max_position_pct"] = "0.3048"
        return [old, latest]


@pytest.mark.asyncio
async def test_discovery_accepts_percentage_history_across_all_wallet_windows() -> None:
    """Valid older rows above one cannot abort current multi-window discovery."""
    service = FalconCandidateService(
        PositionPercentageFalcon(),
        policy=FalconDiscoveryPolicy(enable_price_jump_risk=False),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"), wallets=[{"wallet": GOOD, "h_score": "82"}])

    assert result.best is not None
    assert result.best.wallet == GOOD
    assert result.best.evidence.wallet360_windows == (1, 3, 7, 15)
    assert result.best.evidence.source_errors == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["100.01", "invalid-secret"])
async def test_discovery_does_not_ignore_malformed_historical_percentages(invalid: str) -> None:
    """A valid latest row must not hide an invalid provider response history."""
    service = FalconCandidateService(
        PositionPercentageFalcon(historical_percentage=invalid),
        policy=FalconDiscoveryPolicy(enable_price_jump_risk=False),
        now=lambda: NOW,
    )

    with pytest.raises(FalconProtocolError) as captured:
        await service.discover(Decimal("500"), wallets=[{"wallet": GOOD, "h_score": "82"}])

    assert "invalid-secret" not in str(captured.value)
