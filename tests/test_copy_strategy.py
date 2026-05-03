from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from domain.trading_context import TradingContext
from legacy.tradeExecutor import _calc_buy_amount
from services.copyStrategy import (
    calculate_copy_notional,
    get_trade_multiplier,
    parse_tiered_multipliers,
)

_UNSET = object()


def _make_config(
    *,
    mode: str = "proportional",
    fixed_amount: float = 10.0,
    trade_multiplier: float = 1.0,
    tiered_trade_multipliers: str = "",
    adaptive_basis: str = "trade_size",
    adaptive_min_percent: float = 0.0,
    adaptive_max_percent: float = 0.0,
    adaptive_threshold_usdc: float = 500.0,
) -> SimpleNamespace:
    return SimpleNamespace(
        sizing=SimpleNamespace(
            mode=mode,
            fixed_amount=fixed_amount,
            trade_multiplier=trade_multiplier,
            tiered_trade_multipliers=tiered_trade_multipliers,
            adaptive_basis=adaptive_basis,
            adaptive_min_percent=adaptive_min_percent,
            adaptive_max_percent=adaptive_max_percent,
            adaptive_threshold_usdc=adaptive_threshold_usdc,
        )
    )


def _make_context(
    *,
    allocation_pct: float = 0.10,
    buy_min: float = 1.0,
    buy_max: float = 100.0,
    config: object = _UNSET,
) -> TradingContext:
    kwargs = dict(
        user_id="u1",
        wallet_address="0x" + "1" * 40,
        client=object(),
        positions_repo=object(),
        fills_repo=object(),
        orders_repo=object(),
        risk_blocks_repo=object(),
        pnl_tracker=object(),
        risk_engine=object(),
        accounting_service=object(),
        position_manager=object(),
        execution_service=None,
        allocation_pct=allocation_pct,
        buy_min_amount=buy_min,
        buy_max_amount=buy_max,
    )
    if config is not _UNSET:
        kwargs["config"] = config
        kwargs["copy_decision_service"] = SimpleNamespace(_config=object(), leader_scorer=None)
    return TradingContext(**kwargs)


def test_parse_tiered_multipliers_and_pick_matching_multiplier():
    tiers = parse_tiered_multipliers("0-10:2.0,10-100:1.0,100+:0.25")
    assert len(tiers) == 3
    assert get_trade_multiplier(leader_notional_usdc=Decimal("5"), tiered_multipliers=tiers) == Decimal("2.0")
    assert get_trade_multiplier(leader_notional_usdc=Decimal("50"), tiered_multipliers=tiers) == Decimal("1.0")
    assert get_trade_multiplier(leader_notional_usdc=Decimal("500"), tiered_multipliers=tiers) == Decimal("0.25")


def test_parse_tiered_multipliers_rejects_overlap():
    with pytest.raises(ValueError, match="Overlapping tiers"):
        parse_tiered_multipliers("0-100:1.0,50-200:0.5")


def test_calculate_copy_notional_supports_trade_size_adaptive_logic():
    result = calculate_copy_notional(
        strategy="adaptive",
        leader_notional_usdc=Decimal("100"),
        copy_percentage=Decimal("10"),
        fixed_amount=Decimal("10"),
        min_order_usdc=Decimal("1"),
        max_order_usdc=Decimal("100"),
        adaptive_basis="trade_size",
        adaptive_min_percent=Decimal("5"),
        adaptive_max_percent=Decimal("15"),
        adaptive_threshold_usdc=Decimal("300"),
    )

    # Factor = 100 / 300 = 0.3333, so lerp(15, 10, 0.3333) = 13.3333%
    assert float(result.base_notional_usdc) == pytest.approx(13.3333, abs=1e-3)
    assert float(result.final_notional_usdc) == pytest.approx(13.3333, abs=1e-3)
    assert "adaptive-trade-size" in result.reasoning


def test_trading_context_copy_notional_applies_tiered_multiplier():
    ctx = _make_context(
        allocation_pct=0.10,
        buy_min=1.0,
        buy_max=100.0,
        config=_make_config(
            tiered_trade_multipliers="0-100:1.5,100+:0.5",
        ),
    )

    result = ctx.copy_notional(Decimal("200"))

    # 10% of 200 = 20, 100+ tier applies 0.5x => 10
    assert float(result.final_notional_usdc) == pytest.approx(10.0)


def test_trading_context_copy_notional_requires_typed_config():
    ctx = _make_context(allocation_pct=0.10, buy_min=1.0, buy_max=100.0)

    with pytest.raises(RuntimeError, match="requires injected AppConfig"):
        ctx.copy_notional(Decimal("200"))


def test_legacy_calc_buy_amount_supports_tiered_multiplier():
    with patch(
        "legacy.tradeExecutor.ENV",
        {
            "SIZING_MODE": "proportional",
            "FIXED_AMOUNT": 10,
            "TRADE_MULTIPLIER": 1.0,
            "TIERED_TRADE_MULTIPLIERS": "0-100:1.5,100+:0.5",
        },
    ):
        result = _calc_buy_amount({"usdcSize": 200}, 0.10, 1.0, 100.0)

    assert result == pytest.approx(10.0)
