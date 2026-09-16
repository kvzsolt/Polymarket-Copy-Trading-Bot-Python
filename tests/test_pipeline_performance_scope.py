"""The actual copy pipeline must not treat account PnL as leader evidence."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrators import trade_pipeline
from services.leader_performance import ClosedEpisodeStats, FollowerLeaderKey
from tests.config_helpers import load_test_config


class ScoringReached(RuntimeError):
    """Stop the pipeline after recording the production scoring call."""


@pytest.mark.asyncio
async def test_pipeline_reads_scoped_performance_before_scoring(monkeypatch):
    leader = "0x" + "a" * 40
    reader = SimpleNamespace(
        get_closed_episode_stats=AsyncMock(return_value=ClosedEpisodeStats(1, 0))
    )
    scorer = SimpleNamespace(score_leader=Mock(side_effect=ScoringReached))
    tracker = SimpleNamespace(metrics=Mock(return_value={"win_rate": 100, "net_pnl": 10000}))
    context = SimpleNamespace(
        user_id="account-a",
        config=load_test_config({}),
        leader_scorer=scorer,
        copy_decision_service=Mock(),
        pnl_tracker=tracker,
        leader_performance=reader,
        execution_service=Mock(),
        extra={},
    )
    monkeypatch.setattr(
        trade_pipeline, "reconcile_submitted_unconfirmed", AsyncMock(return_value=0)
    )
    monkeypatch.setattr(
        trade_pipeline, "_resolve_leader_tier", AsyncMock(return_value="paper_follow")
    )
    with pytest.raises(ScoringReached):
        await trade_pipeline._execute_trades_impl(context, leader, [], 1_700_000_000)
    reader.get_closed_episode_stats.assert_awaited_once_with(FollowerLeaderKey("account-a", leader))
    assert scorer.score_leader.call_args.kwargs["analytics"]["win_rate"] == 0
    tracker.metrics.assert_not_called()
