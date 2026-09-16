"""Deterministic automatic-roster selection and degradation contracts."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from domain.auto_trader import (
    AutoTraderPolicy,
    CopyPerformance,
    FalconObservation,
    Trader,
    TraderStatus,
)


NOW = datetime(2026, 9, 16, tzinfo=UTC)
A = "0x" + "a" * 40
B = "0x" + "b" * 40
C = "0x" + "c" * 40


def trader(wallet=A, score="0.8", status=TraderStatus.RESERVE):
    return Trader(
        wallet=wallet,
        status=status,
        score=Decimal(score),
        observed_at=NOW,
        selected_at=NOW - timedelta(hours=4),
        qualification_streak=3,
    )


def performance(**updates):
    values = dict(
        evaluation_id="paper-20",
        observed_at=NOW,
        started_at=NOW - timedelta(days=7),
        closed_episodes=20,
        realized_pnl=Decimal("5"),
        recent_pnl=Decimal("5"),
        drawdown=Decimal("0.03"),
        watermark="20",
        complete=True,
    )
    values.update(updates)
    return CopyPerformance(**values)


def test_best_falcon_candidates_activate_without_paper_history():
    policy = AutoTraderPolicy(target_count=1)
    result = policy.select((trader(A, "0.7"), trader(B, "0.9")), NOW)
    assert [row.wallet for row in result if row.status is TraderStatus.ACTIVE] == [B]
    assert next(row for row in result if row.wallet == B).activation_source == "falcon"


def test_healthy_incumbent_is_not_replaced_by_better_rank():
    policy = AutoTraderPolicy(target_count=1)
    active = trader(A, "0.7", TraderStatus.ACTIVE)
    result = policy.select((active, trader(B, "0.99")), NOW)
    assert next(row for row in result if row.wallet == A).status is TraderStatus.ACTIVE


def test_lower_count_preserves_exits_by_draining_excess():
    result = AutoTraderPolicy(target_count=1).select(
        (trader(A, "0.7", TraderStatus.ACTIVE), trader(B, "0.9", TraderStatus.ACTIVE)), NOW
    )
    assert next(row for row in result if row.wallet == A).status is TraderStatus.DRAINING
    assert next(row for row in result if row.wallet == B).status is TraderStatus.ACTIVE


def test_three_fresh_bad_falcon_results_demote_even_if_copy_results_are_good():
    policy = AutoTraderPolicy(target_count=1)
    current = trader(status=TraderStatus.ACTIVE)
    for index in range(3):
        current = policy.observe_falcon(
            current,
            FalconObservation(A, Decimal("0.4"), NOW + timedelta(minutes=index), str(index)),
        )
    assert current.status is TraderStatus.DEGRADED
    assert current.degradation_reason == "falcon_quality"


def test_repeated_falcon_observation_does_not_advance_strikes():
    policy = AutoTraderPolicy(target_count=1)
    observation = FalconObservation(A, Decimal("0.4"), NOW, "same")
    current = trader(status=TraderStatus.ACTIVE)
    for _ in range(5):
        current = policy.observe_falcon(current, observation)
    assert current.status is TraderStatus.ACTIVE
    assert current.falcon_strikes == 1


def test_losses_demote_despite_good_falcon_score_only_with_new_closed_evidence():
    policy = AutoTraderPolicy(target_count=1)
    current = trader(status=TraderStatus.ACTIVE)
    bad = performance(realized_pnl=Decimal("-5"), recent_pnl=Decimal("-5"))
    for _ in range(5):
        current = policy.observe_results(current, bad, NOW)
    assert current.result_strikes == 1
    for index in (21, 22):
        current = policy.observe_results(
            current, replace(bad, watermark=str(index), closed_episodes=index), NOW
        )
    assert current.status is TraderStatus.DEGRADED
    assert current.degradation_reason == "copy_losses"


@pytest.mark.parametrize("sample,complete", [(0, True), (19, True), (20, False)])
def test_missing_or_insufficient_results_never_demote(sample, complete):
    policy = AutoTraderPolicy(target_count=1)
    current = trader(status=TraderStatus.ACTIVE)
    for index in range(5):
        current = policy.observe_results(
            current,
            performance(
                closed_episodes=sample,
                complete=complete,
                recent_pnl=Decimal("-5"),
                watermark=str(index),
            ),
            NOW,
        )
    assert current.status is TraderStatus.ACTIVE
    assert current.result_strikes == 0


def test_paper_promotion_requires_observation_sample_profit_drawdown_and_falcon():
    policy = AutoTraderPolicy(target_count=1)
    assert policy.paper_qualification(trader(), performance(), NOW) == ()
    failures = policy.paper_qualification(
        trader(score="0.4"),
        performance(
            started_at=NOW, closed_episodes=1, realized_pnl=Decimal("-1"), drawdown=Decimal("0.10")
        ),
        NOW,
    )
    assert set(failures) == {
        "observation_too_short",
        "insufficient_episodes",
        "not_profitable",
        "drawdown",
        "falcon_quality",
    }


def test_new_candidate_needs_distinct_qualifying_falcon_cycles():
    policy = AutoTraderPolicy(target_count=1)
    current = Trader(wallet=A)
    for index in range(2):
        current = policy.observe_falcon(
            current, FalconObservation(A, Decimal("0.8"), NOW, str(index))
        )
    assert policy.select((current,), NOW)[0].status is not TraderStatus.ACTIVE
    current = policy.observe_falcon(current, FalconObservation(A, Decimal("0.8"), NOW, "third"))
    assert policy.select((current,), NOW)[0].status is TraderStatus.ACTIVE


def test_stale_reserve_cannot_be_newly_activated():
    stale = replace(trader(), observed_at=NOW - timedelta(hours=2))
    assert (
        AutoTraderPolicy(target_count=1).select((stale,), NOW)[0].status is not TraderStatus.ACTIVE
    )


def test_live_episode_history_counts_only_fresh_closed_fills():
    from services.leader_performance import PerformanceFill
    from services.auto_trader_performance import RecentEpisodeAccumulator
    from domain.detection import TradeSide

    accumulator = RecentEpisodeAccumulator()
    for i in range(25):
        accumulator.apply(
            PerformanceFill(
                f"{i}:buy", str(i), None, TradeSide.BUY, Decimal("2"), Decimal(".5"), Decimal(".01")
            )
        )
        accumulator.apply(
            PerformanceFill(
                f"{i}:sell",
                str(i),
                None,
                TradeSide.SELL,
                Decimal("2"),
                Decimal(".4"),
                Decimal(".01"),
            )
        )
    assert accumulator.recent_pnls == (Decimal("-.22"),) * 20
    assert accumulator.closed_watermark == "24:sell"
    accumulator.apply(
        PerformanceFill(
            "new:buy", "new", None, TradeSide.BUY, Decimal("1"), Decimal(".5"), Decimal(0)
        )
    )
    assert accumulator.closed_watermark == "24:sell"


def test_changed_evaluation_identity_without_a_new_episode_does_not_add_loss_strikes():
    policy = AutoTraderPolicy(target_count=1)
    current = trader(status=TraderStatus.ACTIVE)
    bad = performance(recent_pnl=Decimal("-5"))
    current = policy.observe_results(current, bad, NOW)
    for index in range(5):
        current = policy.observe_results(current, replace(bad, watermark=f"changed:{index}"), NOW)
    assert current.result_strikes == 1
    assert current.status is TraderStatus.ACTIVE
