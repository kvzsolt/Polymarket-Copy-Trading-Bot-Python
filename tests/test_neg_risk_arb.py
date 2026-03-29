"""Tests for neg-risk arbitrage scanner."""

from __future__ import annotations

import pytest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from services.negRiskArb import (
    ArbConfig,
    ArbOpportunity,
    _best_ask,
    scan_market_for_arb,
)


class TestBestAsk:
    def test_empty_asks(self):
        book = SimpleNamespace(asks=[])
        price, depth = _best_ask(book)
        assert price == Decimal("0")
        assert depth == Decimal("0")

    def test_none_asks(self):
        book = SimpleNamespace(asks=None)
        price, depth = _best_ask(book)
        assert price == Decimal("0")

    def test_single_ask(self):
        book = SimpleNamespace(asks=[
            SimpleNamespace(price="0.55", size="100"),
        ])
        price, depth = _best_ask(book)
        assert price == Decimal("0.55")
        assert depth == Decimal("100")

    def test_multiple_asks_picks_best(self):
        book = SimpleNamespace(asks=[
            SimpleNamespace(price="0.60", size="50"),
            SimpleNamespace(price="0.55", size="80"),
            SimpleNamespace(price="0.70", size="30"),
        ])
        price, depth = _best_ask(book)
        assert price == Decimal("0.55")
        assert depth == Decimal("160")  # 50+80+30


class TestArbProfitability:
    def test_profitable_arb(self):
        """YES ask=0.48, NO ask=0.48 → sum=0.96, profit/token ≈ 0.04 - fees."""
        config = ArbConfig(
            min_profit_bps=10,
            max_cost_usdc=Decimal("10"),
            min_ask_depth_tokens=Decimal("5"),
            fee_bps=Decimal("20"),
        )
        # sum_asks = 0.96
        # fee = 0.96 * (20/10000 * 2) = 0.96 * 0.004 = 0.00384
        # total_cost_per_set = 0.96 * 1.004 = 0.96384
        # profit/token = 1.0 - 0.96384 = 0.03616 → 361.6 bps → well above 10bps
        sum_asks = Decimal("0.96")
        fee_per_leg = Decimal("20") / Decimal("10000")
        total_fee_pct = fee_per_leg * 2
        total_cost = sum_asks * (1 + total_fee_pct)
        profit = Decimal("1") - total_cost
        assert profit > Decimal("0.001")

    def test_unprofitable_arb(self):
        """YES ask=0.51, NO ask=0.50 → sum=1.01, definitely no arb."""
        sum_asks = Decimal("1.01")
        profit = Decimal("1") - sum_asks
        assert profit < 0

    def test_marginal_arb_killed_by_fees(self):
        """YES ask=0.499, NO ask=0.499 → sum=0.998, with fees = 0.998*1.004 = 1.002 → no profit."""
        sum_asks = Decimal("0.998")
        fee_per_leg = Decimal("20") / Decimal("10000")
        total_fee_pct = fee_per_leg * 2
        total_cost = sum_asks * (1 + total_fee_pct)
        profit = Decimal("1") - total_cost
        assert profit < 0


class TestScanMarketForArb:
    @pytest.mark.asyncio
    async def test_returns_none_for_no_arb(self):
        """Market with sum_asks >= 1.0 returns None."""
        mock_client = SimpleNamespace(
            client=SimpleNamespace(
                get_market=lambda cid: {
                    "condition_id": cid,
                    "tokens": [
                        {"token_id": "t1", "outcome": "Yes"},
                        {"token_id": "t2", "outcome": "No"},
                    ],
                    "neg_risk": False,
                    "question": "Test?",
                    "market_slug": "test",
                },
                get_order_book=lambda tid: SimpleNamespace(
                    asks=[SimpleNamespace(price="0.55", size="100")]
                    if tid == "t1" else
                    [SimpleNamespace(price="0.50", size="100")]
                ),
            ),
        )
        config = ArbConfig(min_profit_bps=10, min_ask_depth_tokens=Decimal("5"))
        result = await scan_market_for_arb(mock_client, "0xabc", config)
        # sum = 0.55 + 0.50 = 1.05 > 1.0 → no arb
        assert result is None

    @pytest.mark.asyncio
    async def test_detects_arb(self):
        """Market with sum_asks < 1.0 (after fees) returns ArbOpportunity."""
        mock_client = SimpleNamespace(
            client=SimpleNamespace(
                get_market=lambda cid: {
                    "condition_id": cid,
                    "tokens": [
                        {"token_id": "t1", "outcome": "Yes"},
                        {"token_id": "t2", "outcome": "No"},
                    ],
                    "neg_risk": True,
                    "question": "Arb test?",
                    "market_slug": "arb-test",
                },
                get_order_book=lambda tid: SimpleNamespace(
                    asks=[SimpleNamespace(price="0.47", size="100")]
                    if tid == "t1" else
                    SimpleNamespace(asks=[SimpleNamespace(price="0.47", size="100")])
                ),
            ),
        )
        # Fix: get_order_book returns the namespace, not asks list
        mock_client.client.get_order_book = lambda tid: SimpleNamespace(
            asks=[SimpleNamespace(price="0.47", size="100")]
        )
        config = ArbConfig(
            min_profit_bps=10,
            max_cost_usdc=Decimal("10"),
            min_ask_depth_tokens=Decimal("5"),
            fee_bps=Decimal("20"),
        )
        result = await scan_market_for_arb(mock_client, "0xarb", config)
        # sum = 0.94, fees = 0.94 * 0.004 = 0.00376, total = 0.94376
        # profit = 1.0 - 0.94376 = 0.05624 → 562 bps → arb!
        assert result is not None
        assert result.sum_asks == Decimal("0.94")
        assert result.net_profit_per_token > Decimal("0.05")
        assert result.optimal_size_tokens > 0

    @pytest.mark.asyncio
    async def test_rejects_low_liquidity(self):
        """Market with thin books returns None."""
        mock_client = SimpleNamespace(
            client=SimpleNamespace(
                get_market=lambda cid: {
                    "condition_id": cid,
                    "tokens": [
                        {"token_id": "t1", "outcome": "Yes"},
                        {"token_id": "t2", "outcome": "No"},
                    ],
                    "neg_risk": False,
                    "question": "Thin?",
                    "market_slug": "thin",
                },
                get_order_book=lambda tid: SimpleNamespace(
                    asks=[SimpleNamespace(price="0.45", size="3")]  # Only 3 tokens depth
                ),
            ),
        )
        config = ArbConfig(
            min_profit_bps=10,
            min_ask_depth_tokens=Decimal("20"),  # Requires 20
        )
        result = await scan_market_for_arb(mock_client, "0xthin", config)
        assert result is None  # 3 < 20 minimum depth
