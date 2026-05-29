from __future__ import annotations

from types import SimpleNamespace

from services.discovery.leader_trust_store import LeaderTrustRow, LeaderTrustStore
from services.leaderScoringService import LeaderScoringService


def _config():
    return SimpleNamespace(
        sizing=SimpleNamespace(
            enable_trust_size_multipliers=True,
            trust_size_multiplier_min=0.5,
            trust_size_multiplier_max=1.5,
        ),
        leader=SimpleNamespace(
            leader_score_floor=0.1,
            leader_score_activity_target=10,
        ),
    )


def test_score_leader_blends_trust_store_without_async_db():
    store = LeaderTrustStore()
    store._rows = {
        "0x1111111111111111111111111111111111111111": LeaderTrustRow(
            wallet="0x1111111111111111111111111111111111111111",
            status="live_eligible",
            final_score=0.8,
            falcon_quality_score=0.9,
            copyability_score=0.7,
            copyable_signal_ratio=0.75,
            copyable_notional_ratio=0.6,
        )
    }
    scorer = LeaderScoringService(config=_config(), trust_store=store)

    score = scorer.score_leader(
        "0x1111111111111111111111111111111111111111",
        analytics={"net_pnl": 10, "win_rate": 60},
        recent_trade_count=3,
    )

    assert score.discovery_score == 0.8
    assert score.score > 0.6
    assert scorer.trust_multiplier(0.5) == 1.0

