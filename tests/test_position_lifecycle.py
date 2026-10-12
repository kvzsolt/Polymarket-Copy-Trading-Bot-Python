"""Settlement requires explicit evidence and never follows a zero market mark."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.integrations.polymarket.data_api_positions import DataApiPositionSizeReader


@pytest.mark.parametrize("payout", [Decimal("0"), Decimal("1"), Decimal("0.5")])
def test_redeemable_holding_is_settled_with_exact_payout(payout: Decimal) -> None:
    """Redeemable claims retain their shares and explicit settlement value."""
    from pmcopy.domain.position_lifecycle import PositionLifecycle, PositionLifecyclePolicy

    state = PositionLifecyclePolicy().classify(redeemable=True, settlement_price=payout)
    assert state.lifecycle is PositionLifecycle.SETTLED
    assert state.settlement_price == payout


@pytest.mark.parametrize("price", [None, Decimal("0"), Decimal("0.99")])
def test_unresolved_zero_or_missing_mark_does_not_settle(price: Decimal | None) -> None:
    """An active holding remains exposed even when its mark is zero or unknown."""
    from pmcopy.domain.position_lifecycle import PositionLifecycle, PositionLifecyclePolicy

    state = PositionLifecyclePolicy().classify(redeemable=False, settlement_price=price)
    assert state.lifecycle is PositionLifecycle.ACTIVE
    assert state.settlement_price is None


def test_missing_lifecycle_evidence_remains_unknown() -> None:
    """Legacy holdings remain conservatively included until classified."""
    from pmcopy.domain.position_lifecycle import PositionLifecycle, PositionLifecyclePolicy

    state = PositionLifecyclePolicy().classify(redeemable=None, settlement_price=Decimal("0"))
    assert state.lifecycle is PositionLifecycle.UNKNOWN
    assert state.settlement_price is None


@pytest.mark.parametrize("flag", ["true", "false", 0, 1, {}, []])
def test_malformed_settlement_flags_are_rejected(flag: object) -> None:
    """Truthiness cannot turn an arbitrary provider field into settlement evidence."""
    from pmcopy.domain.position_lifecycle import PositionLifecyclePolicy

    with pytest.raises(ValueError, match="redeemable"):
        PositionLifecyclePolicy().classify(redeemable=flag, settlement_price=Decimal("0"))


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-0.01", "1.01", True, 0.5])
def test_invalid_settlement_price_is_rejected(price: object) -> None:
    """Settlement prices must be finite exact values in the payout range."""
    from pmcopy.domain.position_lifecycle import PositionLifecyclePolicy

    with pytest.raises(ValueError, match="settlement"):
        PositionLifecyclePolicy().classify(redeemable=True, settlement_price=price)


@pytest.mark.asyncio
async def test_account_reader_rejects_malformed_settlement_before_snapshot_mutation() -> None:
    """The complete snapshot is unavailable when lifecycle metadata is malformed."""
    client = SimpleNamespace(
        get_json=AsyncMock(return_value=[{"asset": "123", "size": "10", "redeemable": "false"}])
    )
    reader = DataApiPositionSizeReader(lambda: client)
    assert await reader.positions("0x" + "1" * 40) is None


@pytest.mark.asyncio
async def test_account_reader_preserves_settled_positive_shares_and_zero_payout() -> None:
    """Zero-valued settlement stays visible in the complete physical inventory."""
    client = SimpleNamespace(
        get_json=AsyncMock(
            return_value=[{"asset": "123", "size": "10", "redeemable": True, "curPrice": "0"}]
        )
    )
    reader = DataApiPositionSizeReader(lambda: client)
    rows = await reader.positions("0x" + "1" * 40)
    assert rows is not None
    assert rows[0]["size"] == Decimal("10")
    assert rows[0]["lifecycle"] == "settled"
    assert rows[0]["settlement_price"] == Decimal("0")
