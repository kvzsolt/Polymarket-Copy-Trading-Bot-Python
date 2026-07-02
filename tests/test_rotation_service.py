from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.discovery.candidate_scoring import FalconDiscoveryEvaluation
from services.discovery.copyability_replay import StaticBalanceProvider
from services.discovery.rotation_service import RotationService


def _config():
    return SimpleNamespace(
        discovery=SimpleNamespace(
            discovery_signal_group_window_seconds=60,
            discovery_signal_group_max_gap_seconds=120,
            discovery_min_group_notional_usdc=5,
            discovery_history_trade_limit=50000,
            rotate_keep_live_if_7d_pnl_positive=True,
            rotate_reject_live_if_7d_pnl_below=0,
            leader_score_version="LEADER_SCORE_V1",
        ),
        leader=SimpleNamespace(auto_discover_top_n=5),
        sizing=SimpleNamespace(
            mode="proportional",
            percentage=100,
            fixed_amount=Decimal("1"),
            buy_min_amount=1,
            buy_max_amount=100,
            trade_multiplier=1,
            tiered_trade_multipliers="",
            adaptive_basis="trade_size",
            adaptive_min_percent=0,
            adaptive_max_percent=0,
            adaptive_threshold_usdc=500,
        ),
        execution=SimpleNamespace(
            max_position_size_usdc=0,
            min_balance_headroom_usdc=0,
        ),
        risk=SimpleNamespace(max_open_positions=10),
    )


def _eval(wallet: str, *, final_score: float = 0.1, pnl_7d: Decimal = Decimal("0")):
    return FalconDiscoveryEvaluation(
        wallet=wallet,
        status="live_eligible",
        final_score=final_score,
        falcon_quality_score=0.8,
        copyability_score=0.0,
        recent_state_score=0.5,
        market_fit_score=0.5,
        h_score=80,
        roi_15d=10,
        win_rate_15d=60,
        sharpe_15d=2,
        pnl_15d=100,
        total_trades_15d=50,
        markets_traded_15d=10,
        trajectory="stable",
        total_grouped_signals=0,
        meaningful_signals=0,
        copyable_signals=0,
        copyable_signal_ratio=0.0,
        copyable_notional_ratio=0.0,
        sports_esports_ratio=0.0,
        fast_crypto_signal_ratio=0.0,
        fast_crypto_notional_ratio=0.0,
        pnl_7d=pnl_7d,
        pnl_3d=Decimal("0"),
        rejection_reason=None,
        eligible_live=True,
        eligible_shadow=True,
    )


def _trade(wallet: str):
    return SimpleNamespace(
        trader_wallet=wallet,
        condition_id="cond-1",
        token_id="token-1",
        side="BUY",
        event_ts=100,
        tx_hash="tx-1",
        trade_id="trade-1",
        size=Decimal("10"),
        usdc_size=Decimal("10"),
        title="NBA test",
        market_slug="nba-test",
        event_slug="nba-test",
        outcome="YES",
    )


@pytest.mark.asyncio
async def test_rotation_replays_candidate_with_current_config_before_ranking():
    current_wallet = "0x" + "1" * 40
    candidate_wallet = "0x" + "2" * 40

    class _Repo:
        async def list_evaluations(self, *, statuses=None, limit=50, eligible_live=None):
            return [_eval(candidate_wallet, final_score=0.1)]

        async def get_evaluation(self, wallet: str):
            if wallet.lower() == current_wallet:
                return _eval(current_wallet, pnl_7d=Decimal("-1"))
            return None

        async def list_trades(self, wallet: str, *, limit: int):
            return [_trade(wallet)]

    svc = RotationService(
        pool=object(),
        config=_config(),
        balance_provider=StaticBalanceProvider("100"),
    )
    svc.discovery_repo = _Repo()

    rec = await svc.recommend([current_wallet])

    assert rec.replace == ((current_wallet, candidate_wallet),)
    assert rec.candidates[0].copyable_signal_ratio == 1.0
    assert rec.candidates[0].copyable_notional_ratio == 1.0
    assert rec.candidates[0].score_version == "leader_score_v1"
    assert rec.candidates[0].final_score > 0.7
