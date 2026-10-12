"""Restart-safe per-fill protective-risk lot contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.lots import (
    DurableRiskLot,
    InMemoryRiskLotRepository,
    ProtectiveLotExitFill,
    RiskLotService,
    RiskLotStatus,
)
from pmcopy.services.copy_tasks.protective_exits import (
    FillRiskLot,
    LayeredLevel,
    ProtectiveRiskPolicy,
)


def _lot() -> DurableRiskLot:
    """Return one deterministic persisted lot aggregate."""

    opened_at = datetime(2026, 8, 11, 12, tzinfo=UTC)
    return DurableRiskLot(
        follower_account="deposit-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        entry_fill_id="buy-fill-a",
        lot=FillRiskLot(
            lot_id="lot-a",
            task_id="task-a",
            market_id="market-a",
            outcome_id="token-a",
            entry_price=Decimal("0.40"),
            original_shares=Decimal("10"),
            remaining_shares=Decimal("10"),
            opened_at=opened_at,
            peak_price=Decimal("0.40"),
        ),
        policy=ProtectiveRiskPolicy(
            trailing_stop_percent=Decimal("10"),
            layered_take_profit=(LayeredLevel(Decimal("20"), Decimal("0.5")),),
        ),
        status=RiskLotStatus.OPEN,
        version=1,
    )


@pytest.mark.asyncio
async def test_entry_fill_creates_idempotent_durable_lot() -> None:
    """Use the authoritative entry fill as the exactly-once identity."""

    service = RiskLotService(InMemoryRiskLotRepository())

    assert await service.create_from_entry_fill(_lot()) is True
    assert await service.create_from_entry_fill(_lot()) is False
    assert await service.list_open("deposit-a") == (_lot(),)


@pytest.mark.asyncio
async def test_peak_price_is_monotonic_and_survives_repository_rehydration() -> None:
    """Never let an older quote lower a lot's trailing high-water mark."""

    repository = InMemoryRiskLotRepository()
    service = RiskLotService(repository)
    await service.create_from_entry_fill(_lot())

    raised = await service.observe_price("lot-a", Decimal("0.60"))
    stale = await service.observe_price("lot-a", Decimal("0.50"))
    restored = RiskLotService(repository)

    assert raised.lot.peak_price == Decimal("0.60")
    assert stale == raised
    assert (await restored.list_open("deposit-a"))[0].lot.peak_price == Decimal("0.60")


@pytest.mark.asyncio
async def test_partial_exit_is_exactly_once_and_closes_only_at_zero_remaining() -> None:
    """Persist partial layers and close the lot only after all shares fill."""

    service = RiskLotService(InMemoryRiskLotRepository())
    await service.create_from_entry_fill(_lot())
    first_time = _lot().lot.opened_at + timedelta(minutes=1)
    partial = ProtectiveLotExitFill(
        fill_id="exit-a",
        lot_id="lot-a",
        shares=Decimal("4"),
        price=Decimal("0.50"),
        occurred_at=first_time,
        level_id="tp:0",
    )

    first = await service.record_exit_fill(partial)
    duplicate = await service.record_exit_fill(partial)
    terminal = await service.record_exit_fill(
        ProtectiveLotExitFill(
            fill_id="exit-b",
            lot_id="lot-a",
            shares=Decimal("6"),
            price=Decimal("0.55"),
            occurred_at=first_time + timedelta(seconds=1),
            level_id=None,
        )
    )

    assert first.created is True
    assert first.lot.lot.remaining_shares == Decimal("6")
    assert first.lot.status is RiskLotStatus.OPEN
    assert first.lot.lot.triggered_levels == ()
    assert first.lot.lot.pending_exits[0].remaining_shares == Decimal("1")
    assert duplicate.created is False
    assert duplicate.lot == first.lot
    assert terminal.lot.lot.remaining_shares == Decimal("0")
    assert terminal.lot.status is RiskLotStatus.CLOSED


@pytest.mark.asyncio
async def test_exit_cannot_exceed_remaining_shares() -> None:
    """Fail closed when venue evidence contradicts durable remaining shares."""

    service = RiskLotService(InMemoryRiskLotRepository())
    await service.create_from_entry_fill(_lot())

    with pytest.raises(ValueError, match="remaining"):
        await service.record_exit_fill(
            ProtectiveLotExitFill(
                fill_id="exit-too-large",
                lot_id="lot-a",
                shares=Decimal("11"),
                price=Decimal("0.50"),
                occurred_at=_lot().lot.opened_at + timedelta(minutes=1),
            )
        )
