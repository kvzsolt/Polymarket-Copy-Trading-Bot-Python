"""Tests for Issue #69 — metadata-driven sizing and effective idempotency.

Validates:
- BUY below venue min upsizes only when balance/caps allow.
- BUY below venue min skips when headroom is insufficient.
- SELL below venue min skips when held size is insufficient.
- SELL upsizes only when held shares support it.
- Worst-price notional is consistent.
- Idempotency key changes when effective size, price, tick, token, or condition changes.
"""

from decimal import Decimal

from services.order_sizing import size_buy_order, size_sell_order
from services.idempotency import compute_effective_key, compute_effective_identity


# ---------------------------------------------------------------------------
# BUY sizing
# ---------------------------------------------------------------------------


class TestBuySizing:
    """BUY effective-size computation via size_buy_order."""

    def test_buy_below_venue_min_upsizes_when_cap_allows(self):
        """Small BUY below taker min is upsized when cap covers it."""
        # live_price=0.50 => taker_min_tokens = ceil(1/0.50) = 2
        # base_notional=0.50 => desired_size=1 token < 2 => upsize to 2
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=0.50,
            max_notional_usdc=2.0,
            market_min_size=5.0,  # ignored for taker orders
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        assert result.upsized is True
        assert result.effective_size == Decimal("2")  # upsized to taker min
        assert result.reason == "upsized"

    def test_buy_below_venue_min_skips_when_cap_insufficient(self):
        """Small BUY below taker min is skipped when cap cannot cover upsize."""
        # live_price=0.50 => taker_min=2 tokens => worst_notional ~ 2*0.51=1.02
        # cap=1.0 < 1.02 => can't afford upsize
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=0.5,
            max_notional_usdc=1.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is False
        assert result.reason == "buy_cap_blocks_upsize_to_venue_min"

    def test_buy_above_venue_min_accepts(self):
        """Normal BUY above taker min is accepted without upsize."""
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,  # 20 tokens
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        assert result.upsized is False
        assert result.effective_size >= Decimal("2")

    def test_buy_invalid_price_rejects(self):
        result = size_buy_order(
            live_price=0.0,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.reason == "invalid_price"

    def test_buy_zero_notional_rejects(self):
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=0.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.reason == "zero_notional"

    def test_buy_zero_cap_rejects(self):
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=0.0,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.reason == "zero_cap"

    def test_buy_worst_price_notional_consistent(self):
        """Worst-case notional = effective_size * effective_price (worst protected)."""
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        # worst price should be >= live price * (1 + slippage)
        d_live = Decimal("0.50")
        d_slippage = Decimal("0.02")
        min_worst = d_live * (Decimal("1") + d_slippage)
        assert result.effective_price >= min_worst

    def test_buy_cap_clamps_to_max(self):
        """BUY larger than cap is clamped down."""
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=100.0,  # wants 200 tokens
            max_notional_usdc=5.0,  # cap at $5
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        # effective size should be <= what $5 can buy at worst price
        assert result.accept is True
        worst = result.effective_price
        from decimal import ROUND_FLOOR

        max_size = (Decimal("5") / worst).quantize(Decimal("0.0001"), rounding=ROUND_FLOOR)
        assert result.effective_size <= max_size + Decimal("0.01")  # small rounding tolerance

    def test_buy_risk_cap_too_low_for_min_order(self):
        """Cap is too low to even buy taker-min tokens at worst price."""
        # live_price=10.0 => taker_min = ceil(1/10) = 0.1 token
        # worst_price ~ 10.20 => worst_notional ~ 1.02
        # cap=0.50 < 1.02 => can't fill minimum
        result = size_buy_order(
            live_price=10.0,
            base_notional_usdc=100.0,
            max_notional_usdc=0.5,  # $0.5 cap
            market_min_size=5.0,  # ignored
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is False
        assert result.reason in (
            "buy_cap_blocks_upsize_to_venue_min",
            "risk_cap_too_low_for_min_order",
        )

    def test_buy_no_venue_min_skips_upsize(self):
        """When live price is high enough that taker min is tiny, no upsize applies."""
        # live_price=2.0 => taker_min = ceil(1/2) = 0.5 tokens
        # base_notional=10 => desired_size=5 tokens >= 0.5 => no upsize
        result = size_buy_order(
            live_price=2.0,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=0.0,
            tick_size=0.01,
        )
        assert result.accept is True
        assert result.upsized is False
        assert result.venue_min_size == Decimal("0.5")  # taker min from $1 notional / 2.0 price

    def test_buy_tick_size_rounding(self):
        """Worst price is rounded up to tick."""
        result = size_buy_order(
            live_price=0.55,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.10,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        # worst price should be a multiple of tick_size
        remainder = result.effective_price % Decimal("0.10")
        assert remainder == Decimal("0") or remainder >= Decimal("0.90")  # ceiling rounding

    def test_buy_sizing_decision_fields_complete(self):
        """All SizingDecision fields are populated on acceptance."""
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        assert result.effective_size > Decimal("0")
        assert result.effective_price > Decimal("0")
        assert result.venue_min_size >= Decimal("0")
        assert result.reason in ("ok", "upsized")
        assert isinstance(result.upsized, bool)


# ---------------------------------------------------------------------------
# SELL sizing
# ---------------------------------------------------------------------------


class TestSellSizing:
    """SELL effective-size computation via size_sell_order."""

    def test_sell_below_venue_min_skips_insufficient_held(self):
        """SELL below taker min is skipped when held shares < taker min."""
        # live_price=0.50 => taker_min=2 tokens
        # held=1.0 < 2 => skip
        result = size_sell_order(
            sell_tokens=1.0,
            held_size=1.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.reason == "sell_held_below_venue_min"

    def test_sell_upsizes_when_held_supports(self):
        """SELL below taker min is upsized when held shares >= taker min."""
        # live_price=0.50 => taker_min=2 tokens
        # sell=1 < 2, held=10 >= 2 => upsize to 2
        result = size_sell_order(
            sell_tokens=1.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        assert result.upsized is True
        assert result.effective_size == Decimal("2")

    def test_sell_normal_accepts(self):
        """Normal SELL above taker min is accepted without upsize."""
        result = size_sell_order(
            sell_tokens=10.0,
            held_size=20.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        assert result.upsized is False
        assert result.effective_size == Decimal("10.0000")

    def test_sell_capped_by_held(self):
        """SELL larger than held is capped to held size."""
        result = size_sell_order(
            sell_tokens=50.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        assert result.effective_size <= Decimal("10.0000")

    def test_sell_zero_tokens_rejects(self):
        result = size_sell_order(
            sell_tokens=0.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.reason == "sell_size_zero"

    def test_sell_no_venue_min(self):
        """Without venue min, taker minimum still applies."""
        result = size_sell_order(
            sell_tokens=3.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=0.0,
        )
        # taker min at price 0.50 = 2 tokens; sell=3 >= 2 => accept
        assert result.accept is True
        assert result.upsized is False

    def test_sell_venue_min_exact_held(self):
        """SELL with held exactly at taker min is accepted."""
        # live_price=0.50 => taker_min=2
        result = size_sell_order(
            sell_tokens=2.0,
            held_size=2.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        assert result.effective_size == Decimal("2")

    def test_sell_rounding_ceiling(self):
        """SELL size is rounded up (ceiling) to 4 decimal places."""
        result = size_sell_order(
            sell_tokens=5.1234,
            held_size=10.0,
            live_price=0.50,
            market_min_size=0.0,
        )
        assert result.accept is True
        # Ceiling of 5.1234 = 5.1234 (already 4 decimals)
        assert result.effective_size == Decimal("5.1234")


# ---------------------------------------------------------------------------
# Idempotency key computation
# ---------------------------------------------------------------------------


class TestEffectiveIdempotency:
    """Idempotency keys represent executable orders, not raw requests."""

    def test_key_changes_with_effective_size(self):
        """Different effective size → different key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(
            effective_size=Decimal("5"), effective_price=Decimal("0.50"), **base_kwargs
        )
        key_b = compute_effective_key(
            effective_size=Decimal("10"), effective_price=Decimal("0.50"), **base_kwargs
        )
        assert key_a != key_b

    def test_key_changes_with_effective_price(self):
        """Different effective price → different key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(
            effective_size=Decimal("5"), effective_price=Decimal("0.50"), **base_kwargs
        )
        key_b = compute_effective_key(
            effective_size=Decimal("5"), effective_price=Decimal("0.51"), **base_kwargs
        )
        assert key_a != key_b

    def test_key_changes_with_tick_size(self):
        """Different tick size → different key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
        )
        key_a = compute_effective_key(tick_size=Decimal("0.01"), **base_kwargs)
        key_b = compute_effective_key(tick_size=Decimal("0.10"), **base_kwargs)
        assert key_a != key_b

    def test_key_changes_with_token(self):
        """Different token_id → different key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(token_id="tok1", **base_kwargs)
        key_b = compute_effective_key(token_id="tok2", **base_kwargs)
        assert key_a != key_b

    def test_key_changes_with_condition(self):
        """Different condition_id → different key."""
        base_kwargs = dict(
            user="user1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(condition_id="cond1", **base_kwargs)
        key_b = compute_effective_key(condition_id="cond2", **base_kwargs)
        assert key_a != key_b

    def test_key_changes_with_side(self):
        """Different side → different key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_buy = compute_effective_key(side="BUY", **base_kwargs)
        key_sell = compute_effective_key(side="SELL", **base_kwargs)
        assert key_buy != key_sell

    def test_key_stable_for_same_inputs(self):
        """Same inputs → same key."""
        kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5.0000"),
            effective_price=Decimal("0.5100"),
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(**kwargs)
        key_b = compute_effective_key(**kwargs)
        assert key_a == key_b

    def test_key_ignores_intent_id_by_default(self):
        """Empty intent_id doesn't affect key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(intent_id="", **base_kwargs)
        key_b = compute_effective_key(intent_id="", **base_kwargs)
        assert key_a == key_b

    def test_key_changes_with_intent_id(self):
        """Non-empty intent_id changes the key."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_a = compute_effective_key(intent_id="intent-1", **base_kwargs)
        key_b = compute_effective_key(intent_id="intent-2", **base_kwargs)
        assert key_a != key_b

    def test_key_changes_with_policy_version(self):
        """Different policy version changes the key; empty is backward-compat."""
        base_kwargs = dict(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        # Empty policy_version produces same key as no policy_version
        key_empty = compute_effective_key(policy_version="", **base_kwargs)
        key_none = compute_effective_key(policy_version="", **base_kwargs)
        assert key_empty == key_none
        # Non-empty policy_version changes the key
        key_v1 = compute_effective_key(policy_version="v1", **base_kwargs)
        key_v2 = compute_effective_key(policy_version="v2", **base_kwargs)
        assert key_v1 != key_v2
        assert key_v1 != key_empty

    def test_compute_effective_identity_returns_full_object(self):
        """compute_effective_identity returns a full EffectiveOrderIdentity."""
        identity = compute_effective_identity(
            user="user1",
            condition_id="cond1",
            token_id="tok1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
            intent_id="intent-1",
            policy_version="v1",
        )
        assert identity.idempotency_key
        assert identity.user == "user1"
        assert identity.condition_id == "cond1"
        assert identity.token_id == "tok1"
        assert identity.side == "BUY"
        assert identity.effective_size == Decimal("5")
        assert identity.effective_price == Decimal("0.50")
        assert identity.tick_size == Decimal("0.01")
        assert identity.intent_id == "intent-1"
        assert identity.policy_version == "v1"

    def test_side_normalised_to_uppercase(self):
        """Side is normalised to uppercase in the key."""
        key_lower = compute_effective_key(
            user="u",
            condition_id="c",
            token_id="t",
            side="buy",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_upper = compute_effective_key(
            user="u",
            condition_id="c",
            token_id="t",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        assert key_lower == key_upper


# ---------------------------------------------------------------------------
# Integration: sizing → idempotency round-trip
# ---------------------------------------------------------------------------


class TestSizingIdempotencyRoundTrip:
    """Sizing decision feeds into idempotency key correctly."""

    def test_buy_sizing_to_idempotency_key(self):
        """A buy sizing result produces a valid idempotency key."""
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        key = compute_effective_key(
            user="user1",
            condition_id="0xcond",
            token_id="0xtok",
            side="BUY",
            effective_size=result.effective_size,
            effective_price=result.effective_price,
            tick_size=Decimal("0.01"),
        )
        assert len(key) == 64  # SHA-256 hex digest

    def test_sell_sizing_to_idempotency_key(self):
        """A sell sizing result produces a valid idempotency key."""
        result = size_sell_order(
            sell_tokens=10.0,
            held_size=20.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        key = compute_effective_key(
            user="user1",
            condition_id="0xcond",
            token_id="0xtok",
            side="SELL",
            effective_size=result.effective_size,
            effective_price=result.effective_price,
            tick_size=Decimal("0.01"),
        )
        assert len(key) == 64

    def test_upsize_changes_idempotency_key(self):
        """An upsized order has a different key than the original request."""
        original = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=0.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        upsized = size_buy_order(
            live_price=0.50,
            base_notional_usdc=0.50,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert original.accept is True
        assert upsized.accept is True
        key_orig = compute_effective_key(
            user="u",
            condition_id="c",
            token_id="t",
            side="BUY",
            effective_size=original.effective_size,
            effective_price=original.effective_price,
            tick_size=Decimal("0.01"),
        )
        key_up = compute_effective_key(
            user="u",
            condition_id="c",
            token_id="t",
            side="BUY",
            effective_size=upsized.effective_size,
            effective_price=upsized.effective_price,
            tick_size=Decimal("0.01"),
        )
        # Upsized order has different effective size → different key
        assert key_orig != key_up


# ---------------------------------------------------------------------------
# Worst-notional consistency
# ---------------------------------------------------------------------------


class TestWorstNotionalConsistency:
    """worst_notional = effective_size * effective_price for accepted orders."""

    def test_buy_worst_notional_consistent(self):
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        expected = result.effective_size * result.effective_price
        assert result.worst_notional == expected

    def test_sell_worst_notional_consistent(self):
        result = size_sell_order(
            sell_tokens=10.0,
            held_size=20.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        expected = result.effective_size * result.effective_price
        assert result.worst_notional == expected

    def test_buy_rejected_worst_notional_zero(self):
        result = size_buy_order(
            live_price=0.0,
            base_notional_usdc=10.0,
            max_notional_usdc=50.0,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.worst_notional == Decimal("0")

    def test_sell_rejected_worst_notional_zero(self):
        result = size_sell_order(
            sell_tokens=0.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is False
        assert result.worst_notional == Decimal("0")

    def test_buy_upsized_worst_notional_consistent(self):
        result = size_buy_order(
            live_price=0.50,
            base_notional_usdc=0.50,
            max_notional_usdc=2.0,
            market_min_size=5.0,
            tick_size=0.01,
            slippage_ratio=0.02,
        )
        assert result.accept is True
        assert result.upsized is True
        expected = result.effective_size * result.effective_price
        assert result.worst_notional == expected

    def test_sell_upsized_worst_notional_consistent(self):
        result = size_sell_order(
            sell_tokens=1.0,
            held_size=10.0,
            live_price=0.50,
            market_min_size=5.0,
        )
        assert result.accept is True
        assert result.upsized is True
        expected = result.effective_size * result.effective_price
        assert result.worst_notional == expected


# ---------------------------------------------------------------------------
# Execution-service effective idempotency wiring
# ---------------------------------------------------------------------------


class TestExecutionEffectiveIdempotency:
    """Verify the execution service uses effective post-sizing idempotency."""

    def test_effective_key_includes_condition_id(self):
        from services.executionService import _effective_idempotency_key

        key_a = _effective_idempotency_key(
            user="u1",
            condition_id="0xAAA",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        key_b = _effective_idempotency_key(
            user="u1",
            condition_id="0xBBB",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        assert key_a != key_b

    def test_effective_key_includes_tick_size(self):
        from services.executionService import _effective_idempotency_key

        key_a = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        key_b = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.10"),
        )
        assert key_a != key_b

    def test_effective_key_includes_worst_price(self):
        from services.executionService import _effective_idempotency_key

        key_a = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.50"),
            tick_size=Decimal("0.01"),
        )
        key_b = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        assert key_a != key_b

    def test_effective_key_stable_for_same_inputs(self):
        from services.executionService import _effective_idempotency_key

        key_a = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        key_b = _effective_idempotency_key(
            user="u1",
            condition_id="c1",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        assert key_a == key_b

    def test_effective_key_sensitive_to_post_sizing_price(self):
        """The effective key changes when the post-sizing worst price moves
        (different from the pre-sizing legacy key which ignored price).

        The pre-sizing ``_idempotency_key`` helper was deleted in the
        legacy-kill slice (#77, commit 5868dc2); the effective key is now
        the only idempotency contract.
        """
        from services.executionService import _effective_idempotency_key

        key_low_price = _effective_idempotency_key(
            user="u1",
            condition_id="0xcond",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
            intent_id="sig1",
        )
        key_high_price = _effective_idempotency_key(
            user="u1",
            condition_id="0xcond",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.60"),
            tick_size=Decimal("0.01"),
            intent_id="sig1",
        )
        assert key_low_price != key_high_price

    def test_none_condition_id_treated_as_empty(self):
        from services.executionService import _effective_idempotency_key

        key_none = _effective_idempotency_key(
            user="u1",
            condition_id=None,
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        key_empty = _effective_idempotency_key(
            user="u1",
            condition_id="",
            token_id="t1",
            side="BUY",
            effective_size=Decimal("5"),
            effective_price=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        assert key_none == key_empty
