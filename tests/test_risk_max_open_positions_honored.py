"""C2 — risk.max_open_positions must be honored when the tiered policy is off.

The audit found ``risk.max_open_positions`` was dead: ``_policy_from_config``
always used ``balance_policy_max_open_positions`` (12) regardless of the
``enable_balance_tiered_policy`` flag, so the operator's configured 40-position
limit was silently capped at 12. When the tiered policy is OFF, the
open-positions cap must come from ``risk.max_open_positions``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from pmcopy.services.risk.risk_engine import _policy_from_config, headroom_for_buy


def _config(
    *,
    enable_tiered: bool = False,
    max_open_positions: int = 40,
    balance_policy_max_open_positions: int = 12,
) -> SimpleNamespace:
    return SimpleNamespace(
        risk=SimpleNamespace(
            enable_balance_tiered_policy=enable_tiered,
            max_open_positions=max_open_positions,
            balance_policy_max_open_positions=balance_policy_max_open_positions,
            balance_policy_reserve_pct=0.12,
            balance_policy_reserve_floor=7.0,
            balance_policy_hard_cap_pct=0.15,
            balance_policy_hard_cap_ceiling=8.0,
            balance_policy_allow_partial_exits=False,
            balance_policy_allow_pyramiding=False,
            balance_policy_max_adds_per_position=1,
            balance_policy_base_min_score=0.22,
            balance_policy_min_notional_usdc=0.00001,
            balance_policy_dynamic_threshold_raise=False,
            max_market_exposure_usdc=300.0,
            max_category_exposure_usdc=600.0,
            max_leader_exposure_usdc=500.0,
            day_start_equity_usdc=0.0,
            max_daily_drawdown_pct=20.0,
        )
    )


def test_tiered_off_uses_risk_max_open_positions():
    policy = _policy_from_config(_config(enable_tiered=False, max_open_positions=40))
    assert policy.max_open_positions == 40


def test_tiered_on_uses_balance_policy_max_open_positions():
    policy = _policy_from_config(
        _config(enable_tiered=True, max_open_positions=40, balance_policy_max_open_positions=12)
    )
    assert policy.max_open_positions == 12


def test_headroom_blocks_buy_at_risk_max_open_positions_when_tiered_off():
    """With 40 open positions and risk.max_open_positions=40 (tiered off), the
    41st position must be blocked — previously the cap was 12 so 40 was never
    reachable via this gate, but the operator's intent (40) must now bind."""
    policy = _policy_from_config(_config(enable_tiered=False, max_open_positions=40))
    h = headroom_for_buy(
        equity_usdc=Decimal("1000"),
        free_cash_usdc=Decimal("900"),
        open_positions_count=40,
        policy=policy,
        config=_config(enable_tiered=False, max_open_positions=40),
    )
    assert not h.allow
    assert h.reason == "max_open_positions_reached"
