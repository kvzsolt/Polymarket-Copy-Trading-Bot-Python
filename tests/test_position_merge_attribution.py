"""Adding a fill cannot invent ownership or cost for existing unknown inventory."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.storage.postgres.positions_repo import PositionRecord
from pmcopy.orchestrators.pipeline.trade_pipeline import _sync_sell_position_from_live_holdings
from pmcopy.domain.position_ownership import PositionOwnership
from tests.test_positions_repo_set_position_size import _FakePool, _FakeTx, VALID_TOKEN


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_leader", "incoming_leader", "expected"),
    [
        (None, "leader-a", None),
        ("leader-a", "leader-b", None),
        ("leader-a", None, None),
        ("leader-a", "leader-a", "leader-a"),
    ],
)
async def test_confirmed_buy_does_not_assign_mixed_inventory_to_the_latest_leader(
    existing_leader, incoming_leader, expected
):
    row = {
        "user_id": "u",
        "condition_id": "condition",
        "token_id": VALID_TOKEN,
        "leader_id": existing_leader,
        "size_tokens": Decimal(10),
        "avg_entry_price": Decimal(".4"),
    }
    connection = SimpleNamespace(
        transaction=_FakeTx, fetchrow=AsyncMock(return_value=row), execute=AsyncMock()
    )
    position = await PositionsRepo(_FakePool(connection)).apply_fill(
        user_id="u",
        leader_id=incoming_leader,
        condition_id="condition",
        token_id=VALID_TOKEN,
        side="BUY",
        fill_size=Decimal(2),
        fill_price=Decimal(".5"),
    )
    assert position is not None
    assert position.size_tokens == 12
    assert position.leader_id == expected
    assert position.avg_entry_price == Decimal(5) / Decimal(12)


@pytest.mark.asyncio
async def test_buy_into_unknown_cost_inventory_does_not_turn_zero_into_an_average_cost():
    row = {
        "user_id": "u",
        "condition_id": "condition",
        "token_id": VALID_TOKEN,
        "leader_id": None,
        "size_tokens": Decimal(10),
        "avg_entry_price": Decimal(0),
    }
    connection = SimpleNamespace(
        transaction=_FakeTx, fetchrow=AsyncMock(return_value=row), execute=AsyncMock()
    )
    position = await PositionsRepo(_FakePool(connection)).apply_fill(
        user_id="u",
        leader_id="leader-a",
        condition_id="condition",
        token_id=VALID_TOKEN,
        side="BUY",
        fill_size=Decimal(2),
        fill_price=Decimal(".5"),
    )
    assert position is not None
    assert position.size_tokens == 12
    assert position.avg_entry_price == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("live_size", "owner"), [("12", None), ("8", "leader-a")])
async def test_holdings_recovery_does_not_assign_inventory_to_the_current_sell_signal(
    live_size, owner
):
    current = PositionRecord("u", "condition", VALID_TOKEN, "leader-a", Decimal(10), Decimal(".4"))
    positions = SimpleNamespace(
        get_position=AsyncMock(return_value=current), upsert_position=AsyncMock()
    )
    context = SimpleNamespace(
        user_id="u",
        wallet_address="wallet",
        positions_repo=positions,
        holdings_reader=SimpleNamespace(shares=AsyncMock(return_value=Decimal(live_size))),
    )
    await _sync_sell_position_from_live_holdings(
        context, leader_id="leader-b", condition_id="condition", token_id=VALID_TOKEN
    )
    assert positions.upsert_position.await_args.args[0].leader_id == owner


@pytest.mark.parametrize("blank", ["", " ", "\t"])
def test_blank_owner_is_missing_evidence(blank):
    assert PositionOwnership.matches(blank, blank) is False
    assert PositionOwnership.after_buy(None, blank, Decimal(0)) is None
