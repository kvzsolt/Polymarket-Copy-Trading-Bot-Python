"""Confirmed fill episodes must retain account, task, and leader attribution."""

from decimal import Decimal

import pytest

from domain.detection import TradeSide
from services.leader_performance import (
    ClosedEpisodeAccumulator,
    FollowerLeaderKey,
    PerformanceFill,
)


def fill(
    identity: str, side: str, shares: str, price: str, *, task: str = "task-a", fee: str = "0"
):
    return PerformanceFill(
        identity, "token", task, TradeSide(side), Decimal(shares), Decimal(price), Decimal(fee)
    )


def test_partial_exits_count_one_prediction_only_after_closure() -> None:
    accumulator = ClosedEpisodeAccumulator()
    accumulator.apply(fill("buy", "BUY", "200", ".50"))
    for index in range(10):
        accumulator.apply(fill(str(index), "SELL", "10", ".55"))
    assert accumulator.stats.closed_episodes == 0
    assert accumulator.stats.win_rate_fraction is None
    accumulator.apply(fill("final", "SELL", "100", ".55", fee="1"))
    assert accumulator.stats.closed_episodes == 1
    assert accumulator.stats.winning_episodes == 1
    assert accumulator.stats.realized_pnl == Decimal("9")


def test_task_lots_are_not_combined_into_one_prediction() -> None:
    accumulator = ClosedEpisodeAccumulator()
    for task in ("task-a", "task-b"):
        accumulator.apply(fill(task + "buy", "BUY", "10", ".50", task=task))
    accumulator.apply(fill("sell", "SELL", "10", ".40"))
    assert accumulator.stats.closed_episodes == 1
    assert accumulator.stats.win_rate_fraction == Decimal(0)
    assert accumulator.stats.realized_pnl == Decimal("-1")


def test_imported_sell_is_missing_cost_basis_not_a_win() -> None:
    accumulator = ClosedEpisodeAccumulator()
    accumulator.apply(fill("imported", "SELL", "10", ".80"))
    assert accumulator.stats.closed_episodes == 0
    assert accumulator.stats.excluded_scopes == 1


@pytest.mark.parametrize("raw", [True, "NaN", "Infinity", "-1"])
def test_fill_boundary_rejects_invalid_money(raw) -> None:
    with pytest.raises(ValueError):
        PerformanceFill("fill", "token", None, TradeSide.BUY, raw, Decimal(".5"), Decimal(0))


def test_key_preserves_account_and_normalizes_leader() -> None:
    key = FollowerLeaderKey("account-a", "0x" + "AB" * 20)
    assert key.follower_account == "account-a"
    assert key.leader_wallet == "0x" + "ab" * 20
