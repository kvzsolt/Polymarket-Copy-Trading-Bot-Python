from __future__ import annotations

from types import SimpleNamespace

import pytest

from config.app_config import DiscoveryConfig
from services.discovery.models import discovery_config_hash
from services.leaderScoringService import LeaderScoringService


def _config() -> SimpleNamespace:
    return SimpleNamespace(
        discovery=SimpleNamespace(scoring=DiscoveryConfig().scoring),
        sizing=SimpleNamespace(
            enable_trust_size_multipliers=True,
            trust_size_multiplier_min=0.5,
            trust_size_multiplier_max=1.25,
        ),
    )


def test_runtime_signal_is_dominated_by_persisted_roster_score() -> None:
    weak = LeaderScoringService(config=_config(), roster_score=0.2).score_leader(
        "0x" + "1" * 40,
        analytics={"win_rate": 0.6, "net_pnl": 10},
        recent_trade_count=4,
    )
    strong = LeaderScoringService(config=_config(), roster_score=0.9).score_leader(
        "0x" + "1" * 40,
        analytics={"win_rate": 0.6, "net_pnl": 10},
        recent_trade_count=4,
    )

    assert strong.score - weak.score == pytest.approx(0.7 * (0.9 - 0.2))
    assert strong.roster_score == 0.9


def test_manual_leader_uses_configured_prior_and_floor() -> None:
    result = LeaderScoringService(config=_config()).score_leader(
        "0x" + "2" * 40,
        analytics={"win_rate": 0, "net_pnl": -1_000},
        recent_trade_count=0,
    )

    assert result.roster_score == 0.5
    assert result.score >= DiscoveryConfig().scoring.runtime_score_floor


def test_config_hash_tracks_decisions_not_transport_tuning() -> None:
    baseline = DiscoveryConfig()
    transport_tuned = baseline.model_copy(
        update={
            "api": baseline.api.model_copy(
                update={"requests_per_second": 2.0}
            )
        }
    )
    scoring_tuned = baseline.model_copy(
        update={
            "scoring": baseline.scoring.model_copy(
                update={"runtime_score_floor": 0.2}
            )
        }
    )

    assert discovery_config_hash(transport_tuned) == discovery_config_hash(baseline)
    assert discovery_config_hash(scoring_tuned) != discovery_config_hash(baseline)
