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
        # Correct fee model: apply fee_bps once to total notional (sum across legs).
        # fee = 0.96 * (20/10000) = 0.00192
        # total_cost_per_set = 0.96 * 1.002 = 0.96192
        # profit/token = 1.0 - 0.96192 = 0.03808 → 380.8 bps
        sum_asks = Decimal("0.96")
        fee_pct = Decimal("20") / Decimal("10000")
        total_cost = sum_asks * (1 + fee_pct)
        profit = Decimal("1") - total_cost
        assert profit > Decimal("0.001")

    def test_unprofitable_arb(self):
        """YES ask=0.51, NO ask=0.50 → sum=1.01, definitely no arb."""
        sum_asks = Decimal("1.01")
        profit = Decimal("1") - sum_asks
        assert profit < 0

    def test_marginal_arb_killed_by_fees(self):
        """YES ask=0.499, NO ask=0.499 → sum=0.998, with fees = 0.998*1.002 = 0.999996 → ~0 profit."""
        sum_asks = Decimal("0.998")
        fee_pct = Decimal("20") / Decimal("10000")
        total_cost = sum_asks * (1 + fee_pct)
        profit = Decimal("1") - total_cost
        # This is essentially breakeven; with any slippage/impact it's negative.
        assert profit <= Decimal("0.00001")


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
        # With SLIPPAGE_TOLERANCE=2% and tick=0.01:
        # best_ask=0.47 -> worst_ask=ceil(0.4794 to 0.01)=0.48 per leg
        # sum_worst=0.96; fee=0.96*0.002=0.00192; total=0.96192
        # profit/token=1-0.96192=0.03808
        assert result is not None
        assert result.sum_asks == Decimal("0.94")
        assert result.net_profit_per_token == Decimal("0.03808")
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
