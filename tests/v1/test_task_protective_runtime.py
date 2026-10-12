"""Restored task lots use the existing protective evaluator and execution port."""

from dataclasses import replace
from decimal import Decimal

import pytest

from pmcopy.services.copy_tasks.lots import InMemoryRiskLotRepository, RiskLotService
from pmcopy.services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from pmcopy.services.copy_tasks.protective_runtime import TaskProtectivePositionManager
from tests.v1.test_durable_risk_lots import _lot
from pmcopy.services.copy_tasks.lots import ProtectiveLotExitFill


class Quotes:
    """Supply a validated bid or an explicit data failure."""

    def __init__(self, bid):
        self.bid = bid

    async def executable_bid(self, lot):
        """Return the fixture's executable bid."""
        if isinstance(self.bid, Exception):
            raise self.bid
        return self.bid


class Exits:
    """Capture requests at the canonical execution boundary."""

    def __init__(self):
        self.calls = []

    async def submit(self, lot, decision, bid):
        """Record one explicit allocation and hard floor."""
        self.calls.append((lot, decision, bid))


@pytest.mark.asyncio
async def test_restored_lot_remains_managed_without_an_active_task_or_leader_monitor():
    """Task lifecycle cannot abandon an already owned position."""
    lots = RiskLotService(InMemoryRiskLotRepository())
    lot = replace(_lot(), policy=ProtectiveRiskPolicy(take_profit_price=Decimal("0.7")))
    await lots.create_from_entry_fill(lot)
    exits = Exits()
    manager = TaskProtectivePositionManager(
        follower_account=lot.follower_account,
        lots=lots,
        quotes=Quotes(Decimal("0.75")),
        exits=exits,
    )
    await manager.run_once()
    assert len(exits.calls) == 1
    assert exits.calls[0][1].shares_to_exit == Decimal("10")
    assert exits.calls[0][1].reason == "take_profit"
    assert (await lots.get(lot.lot.lot_id)).lot.peak_price == Decimal("0.75")


@pytest.mark.asyncio
async def test_partial_layer_resumes_only_its_remaining_target_after_restart():
    """Filling two of a five-share layer leaves three shares to exit."""
    repository = InMemoryRiskLotRepository()
    lots = RiskLotService(repository)
    lot = _lot()
    await lots.create_from_entry_fill(lot)
    await lots.record_exit_fill(
        ProtectiveLotExitFill(
            fill_id="partial-layer",
            lot_id=lot.lot.lot_id,
            shares=Decimal("2"),
            price=Decimal("0.5"),
            occurred_at=lot.lot.opened_at,
            level_id="tp:0",
        )
    )
    exits = Exits()
    restored = TaskProtectivePositionManager(
        follower_account=lot.follower_account,
        lots=RiskLotService(repository),
        quotes=Quotes(Decimal("0.5")),
        exits=exits,
    )
    await restored.run_once()
    assert len(exits.calls) == 1
    assert exits.calls[0][1].shares_to_exit == Decimal("3")


@pytest.mark.asyncio
async def test_unavailable_executable_bid_does_not_submit_or_advance_peak():
    """An unavailable book is visible failure rather than a synthetic exit mark."""
    lots = RiskLotService(InMemoryRiskLotRepository())
    lot = _lot()
    await lots.create_from_entry_fill(lot)
    exits = Exits()
    manager = TaskProtectivePositionManager(
        follower_account=lot.follower_account,
        lots=lots,
        quotes=Quotes(RuntimeError("unavailable")),
        exits=exits,
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        await manager.run_once()
    assert exits.calls == []
    assert (await lots.get(lot.lot.lot_id)).lot.peak_price == lot.lot.peak_price


@pytest.mark.asyncio
async def test_empty_policy_needs_no_book_and_does_not_fail_other_lots():
    """Unconfigured optional protection must not request unrelated market data."""
    lots = RiskLotService(InMemoryRiskLotRepository())
    lot = replace(_lot(), policy=ProtectiveRiskPolicy())
    await lots.create_from_entry_fill(lot)
    manager = TaskProtectivePositionManager(
        follower_account=lot.follower_account,
        lots=lots,
        quotes=Quotes(RuntimeError("no book should be read")),
        exits=Exits(),
    )
    await manager.run_once()


@pytest.mark.asyncio
async def test_unavailable_lot_does_not_abandon_other_owned_inventory():
    """Continue independent lots while reporting an explicit incomplete risk pass."""

    class MixedQuotes:
        async def executable_bid(self, lot):
            if lot.lot.lot_id == "lot-a":
                raise RuntimeError("SYNTHETIC_PRIVATE_KEY")
            return Decimal("0.75")

    lots = RiskLotService(InMemoryRiskLotRepository())
    lot = replace(_lot(), policy=ProtectiveRiskPolicy(take_profit_price=Decimal("0.7")))
    await lots.create_from_entry_fill(lot)
    await lots.create_from_entry_fill(
        replace(
            lot,
            entry_fill_id="buy-b",
            lot=replace(lot.lot, lot_id="lot-b"),
        )
    )
    exits = Exits()
    manager = TaskProtectivePositionManager(
        follower_account=lot.follower_account,
        lots=lots,
        quotes=MixedQuotes(),
        exits=exits,
    )
    with pytest.raises(RuntimeError) as failure:
        await manager.run_once()
    assert len(exits.calls) == 1
    assert exits.calls[0][0].lot.lot_id == "lot-b"
    assert "SYNTHETIC_PRIVATE_KEY" not in str(failure.value)
