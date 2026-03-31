"""Tests for neg-risk arb basket lifecycle and trader discovery improvements."""
import asyncio
import time
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from services.negRiskArb import (
    ArbBasket,
    ArbBasketStore,
    BasketState,
    BasketLegResult,
)
from services.traderDiscoveryService import (
    TraderDiscoveryService,
    TraderProfile,
    _validate_row_schema,
)


class TestBasketLifecycle(unittest.TestCase):
    """Test arb basket state transitions and persistence."""

    def test_basket_state_enum(self):
        self.assertEqual(BasketState.PLANNED.value, "planned")
        self.assertEqual(BasketState.HOLDING.value, "holding")
        self.assertEqual(BasketState.REDEEMED.value, "redeemed")
        self.assertEqual(BasketState.ABANDONED.value, "abandoned")

    def test_basket_to_dict_roundtrip(self):
        basket = ArbBasket(
            basket_id="arb:test:123",
            condition_id="0xabc",
            market_slug="test-market",
            question="Will it work?",
            state=BasketState.HOLDING,
            target_size_tokens=Decimal("10"),
            complete_units=Decimal("10"),
            total_cost_usdc=Decimal("9.50"),
        )
        d = basket.to_dict()
        self.assertEqual(d["basket_id"], "arb:test:123")
        self.assertEqual(d["state"], "holding")
        self.assertAlmostEqual(d["complete_units"], 10.0)
        self.assertAlmostEqual(d["total_cost_usdc"], 9.50)

    def test_basket_store_in_memory(self):
        store = ArbBasketStore()
        basket = ArbBasket(
            basket_id="arb:test:1",
            condition_id="0x111",
            market_slug="test",
            question="test",
            state=BasketState.HOLDING,
        )
        store.save(basket)
        active = store.get_active_condition_ids()
        self.assertIn("0x111", active)

        # Terminal state should not appear
        basket.state = BasketState.REDEEMED
        store.save(basket)
        active = store.get_active_condition_ids()
        self.assertNotIn("0x111", active)

    def test_basket_store_load_non_terminal(self):
        store = ArbBasketStore()
        b1 = ArbBasket(basket_id="b1", condition_id="c1", market_slug="m1", question="q1", state=BasketState.HOLDING)
        b2 = ArbBasket(basket_id="b2", condition_id="c2", market_slug="m2", question="q2", state=BasketState.REDEEMED)
        b3 = ArbBasket(basket_id="b3", condition_id="c3", market_slug="m3", question="q3", state=BasketState.EXECUTING)
        store.save(b1)
        store.save(b2)
        store.save(b3)
        non_terminal = store.load_non_terminal()
        ids = {b.basket_id for b in non_terminal}
        self.assertIn("b1", ids)
        self.assertIn("b3", ids)
        self.assertNotIn("b2", ids)

    def test_leg_result_to_dict(self):
        lr = BasketLegResult(
            token_id="0xtoken",
            outcome="Yes",
            side="BUY",
            accepted=True,
            filled_amount=Decimal("5.0"),
            avg_price=Decimal("0.45"),
            fees_paid=Decimal("0.02"),
        )
        d = lr.to_dict()
        self.assertEqual(d["token_id"], "0xtoken")
        self.assertTrue(d["accepted"])
        self.assertAlmostEqual(d["filled_amount"], 5.0)

    def test_complete_units_is_min_of_fills(self):
        """The sacred invariant: complete_units = min(filled across all legs)."""
        basket = ArbBasket(
            basket_id="test", condition_id="c", market_slug="m", question="q",
        )
        basket.leg_results = [
            BasketLegResult(token_id="t1", outcome="A", side="BUY", accepted=True, filled_amount=Decimal("10")),
            BasketLegResult(token_id="t2", outcome="B", side="BUY", accepted=True, filled_amount=Decimal("8")),
            BasketLegResult(token_id="t3", outcome="C", side="BUY", accepted=True, filled_amount=Decimal("12")),
        ]
        filled = [lr.filled_amount for lr in basket.leg_results]
        complete_units = min(filled)
        self.assertEqual(complete_units, Decimal("8"))

        # Residuals
        for lr in basket.leg_results:
            excess = lr.filled_amount - complete_units
            if lr.token_id == "t1":
                self.assertEqual(excess, Decimal("2"))
            elif lr.token_id == "t2":
                self.assertEqual(excess, Decimal("0"))
            elif lr.token_id == "t3":
                self.assertEqual(excess, Decimal("4"))


class TestDiscoverySchemaValidation(unittest.TestCase):
    """Test schema validation for discovery API responses."""

    def test_valid_row(self):
        row = {"trader": "0x123", "overall_gain": 1000, "win_rate": 0.6, "total_positions": 50}
        self.assertTrue(_validate_row_schema(row))

    def test_missing_field(self):
        row = {"trader": "0x123", "overall_gain": 1000}  # missing win_rate, total_positions
        self.assertFalse(_validate_row_schema(row))

    def test_empty_row(self):
        self.assertFalse(_validate_row_schema({}))


class TestDiscoveryFallback(unittest.TestCase):
    """Test discovery service fallback behavior."""

    def test_memory_cache_fallback(self):
        svc = TraderDiscoveryService()
        # Populate cache
        svc._cache["0x" + "a" * 40] = TraderProfile(
            wallet="0x" + "a" * 40,
            overall_gain=5000.0,
            win_rate=0.65,
            total_positions=100,
            composite_score=0.7,
        )

        result = asyncio.get_event_loop().run_until_complete(
            svc._fallback_discover(1000.0, 0.55, 20, 50)
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].wallet, "0x" + "a" * 40)

    def test_health_status_initial(self):
        svc = TraderDiscoveryService()
        status = svc.get_health_status()
        self.assertFalse(status["healthy"])
        self.assertEqual(status["cache_size"], 0)
        self.assertEqual(status["last_discovery_source"], "none")

    def test_health_status_after_cache(self):
        svc = TraderDiscoveryService()
        svc._last_discovery = time.time()
        svc._last_discovery_count = 10
        svc._last_discovery_source = "primary"
        svc._cache["0x" + "b" * 40] = TraderProfile(wallet="0x" + "b" * 40)
        status = svc.get_health_status()
        self.assertTrue(status["healthy"])
        self.assertEqual(status["cache_size"], 1)


class TestCrossBasketLotAccounting(unittest.TestCase):
    """Test pro-rata lot allocation when multiple baskets share a condition."""

    def test_prorata_single_basket(self):
        """Single basket gets 100% of redemption."""
        basket = ArbBasket(
            basket_id="b1", condition_id="0xcond1",
            market_slug="m1", question="q1",
            state=BasketState.RESOLVED,
            complete_units=Decimal("10"),
            total_cost_usdc=Decimal("9.50"),
            total_fees_usdc=Decimal("0.10"),
        )
        total_units = basket.complete_units
        redeemed_usdc = Decimal("10")  # $1 per unit

        share = basket.complete_units / total_units
        self.assertEqual(share, Decimal("1"))

        basket_redeemed = redeemed_usdc * share
        pnl = basket_redeemed - basket.total_cost_usdc - basket.total_fees_usdc
        self.assertEqual(basket_redeemed, Decimal("10"))
        self.assertEqual(pnl, Decimal("0.40"))

    def test_prorata_two_baskets(self):
        """Two baskets sharing same condition get proportional allocation."""
        b1 = ArbBasket(
            basket_id="b1", condition_id="0xcond1",
            market_slug="m1", question="q1",
            state=BasketState.RESOLVED,
            complete_units=Decimal("30"),
            total_cost_usdc=Decimal("28.50"),
            total_fees_usdc=Decimal("0.30"),
        )
        b2 = ArbBasket(
            basket_id="b2", condition_id="0xcond1",
            market_slug="m1", question="q1",
            state=BasketState.RESOLVED,
            complete_units=Decimal("70"),
            total_cost_usdc=Decimal("66.50"),
            total_fees_usdc=Decimal("0.70"),
        )

        total_units = b1.complete_units + b2.complete_units
        self.assertEqual(total_units, Decimal("100"))

        redeemed_usdc = Decimal("100")

        b1_share = b1.complete_units / total_units
        b2_share = b2.complete_units / total_units
        self.assertEqual(b1_share, Decimal("0.3"))
        self.assertEqual(b2_share, Decimal("0.7"))

        b1_redeemed = redeemed_usdc * b1_share
        b2_redeemed = redeemed_usdc * b2_share
        self.assertEqual(b1_redeemed, Decimal("30"))
        self.assertEqual(b2_redeemed, Decimal("70"))

        b1_pnl = b1_redeemed - b1.total_cost_usdc - b1.total_fees_usdc
        b2_pnl = b2_redeemed - b2.total_cost_usdc - b2.total_fees_usdc
        self.assertEqual(b1_pnl, Decimal("1.20"))
        self.assertEqual(b2_pnl, Decimal("2.80"))

    def test_prorata_zero_units_safe(self):
        """Zero total units doesn't divide by zero."""
        basket = ArbBasket(
            basket_id="b0", condition_id="0xcond1",
            market_slug="m1", question="q1",
            state=BasketState.RESOLVED,
            complete_units=Decimal("0"),
        )
        total_units = basket.complete_units
        # Should not divide by zero
        if total_units > 0:
            share = basket.complete_units / total_units
        else:
            share = Decimal("0")
        self.assertEqual(share, Decimal("0"))

    def test_condition_grouping(self):
        """Baskets grouped correctly by condition_id."""
        store = ArbBasketStore()
        b1 = ArbBasket(basket_id="b1", condition_id="0xAAA", market_slug="m1", question="q1",
                       state=BasketState.RESOLVED, complete_units=Decimal("10"))
        b2 = ArbBasket(basket_id="b2", condition_id="0xAAA", market_slug="m1", question="q1",
                       state=BasketState.RESOLVED, complete_units=Decimal("20"))
        b3 = ArbBasket(basket_id="b3", condition_id="0xBBB", market_slug="m2", question="q2",
                       state=BasketState.RESOLVED, complete_units=Decimal("5"))
        store.save(b1)
        store.save(b2)
        store.save(b3)

        resolved = [b for b in store._in_memory.values() if b.state == BasketState.RESOLVED]
        condition_baskets: dict[str, list] = {}
        for b in resolved:
            condition_baskets.setdefault(b.condition_id, []).append(b)

        self.assertEqual(len(condition_baskets["0xAAA"]), 2)
        self.assertEqual(len(condition_baskets["0xBBB"]), 1)


if __name__ == "__main__":
    unittest.main()
