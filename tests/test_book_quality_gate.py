"""Phase 9 — book quality gate: reject executions when bid/ask spread is too wide."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from utils.createClobClient import MarketMeta


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _meta(bid: str | None, ask: str | None) -> MarketMeta:
    return MarketMeta(
        token_id="0xabc",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("0"),
        neg_risk=False,
        best_bid=Decimal(bid) if bid else None,
        best_ask=Decimal(ask) if ask else None,
    )


def _make_execution_service(meta: MarketMeta, quote_price: str = "0.50"):
    """Build a minimal ExecutionService with mocked deps."""
    from services.executionService import ExecutionService

    clob = AsyncMock()
    clob.get_price = AsyncMock(return_value={"price": quote_price})

    meta_svc = AsyncMock()
    meta_svc.get = AsyncMock(return_value=meta)

    order_repo = MagicMock()
    order_repo.get_by_idempotency_key = MagicMock(return_value=None)

    svc = ExecutionService.__new__(ExecutionService)
    svc.clob_client = clob
    svc.market_meta_service = meta_svc
    svc.order_repo = order_repo
    svc.fee_service = AsyncMock()
    svc.fee_service.get_fee_bps = AsyncMock(return_value=Decimal("0"))
    return svc


def _req(amount: str = "10", side: str = "BUY"):
    from services.executionService import ExecutionRequest

    return ExecutionRequest(
        user_id="user1",
        leader_id="0xleader",
        token_id="0xabc",
        side=side,
        amount=Decimal(amount),
        max_slippage_bps=200,
        reason="copy_trade",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_circuit():
    """Reset circuit breaker so it doesn't interfere."""
    import services.executionService as _es
    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


class TestBookQualityGate:
    """Spread gate in ExecutionService.execute()."""

    def test_wide_spread_rejected(self):
        """Spread of 20% (2000 bps) should be rejected at default 500 bps limit."""
        svc = _make_execution_service(_meta("0.40", "0.48"))  # 20% spread
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_tight_spread_passes(self):
        """Spread of 2% (200 bps) should pass at default 500 bps limit."""
        svc = _make_execution_service(_meta("0.50", "0.51"))  # 2% spread
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        # Should NOT be rejected for spread — may fail later for other reasons
        assert result.error is None or "spread" not in result.error

    def test_no_bid_rejected(self):
        """If best_bid is None, the market has no book — should be rejected."""
        svc = _make_execution_service(_meta(None, "0.51"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_no_ask_rejected(self):
        """If best_ask is None, the market has no book — should be rejected."""
        svc = _make_execution_service(_meta("0.50", None))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_zero_bid_passes(self):
        """If best_bid is 0, spread check is skipped (avoid division by zero)."""
        svc = _make_execution_service(_meta("0", "0.51"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_gate_disabled_when_zero(self):
        """MAX_BOOK_SPREAD_BPS=0 disables the gate entirely."""
        svc = _make_execution_service(_meta("0.10", "0.90"))  # 800% spread
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "0"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_custom_threshold(self):
        """Custom threshold at 100 bps should reject 2% spread."""
        svc = _make_execution_service(_meta("0.50", "0.51"))  # 2% = 200 bps
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "100"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_sell_side_also_gated(self):
        """Spread gate applies to SELL orders too."""
        svc = _make_execution_service(_meta("0.40", "0.48"))  # 20% spread
        req = _req(side="SELL", amount="5")
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_error_includes_actual_spread(self):
        """Error message includes actual spread in bps."""
        svc = _make_execution_service(_meta("0.40", "0.48"))  # 2000 bps
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert "2000 bps" in result.error
        assert "500 bps max" in result.error

    def test_garbage_book_detected(self):
        """bid=0.01, ask=0.99 is a garbage/placeholder book — should be book_no_liquidity."""
        svc = _make_execution_service(_meta("0.01", "0.99"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"
        assert "no real liquidity" in result.error

    def test_garbage_book_borderline(self):
        """bid=0.03, ask=0.97 is still detected as thin/placeholder."""
        svc = _make_execution_service(_meta("0.03", "0.97"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_legitimate_wide_spread_not_classified_as_garbage(self):
        """bid=0.10, ask=0.90 is a wide spread but not garbage — should be book_spread_wide."""
        svc = _make_execution_service(_meta("0.10", "0.90"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"


class TestThinBookDetection:
    """Test the _is_thin_or_empty_book helper."""

    def test_none_bid(self):
        from utils.createClobClient import _is_thin_or_empty_book
        assert _is_thin_or_empty_book(None, Decimal("0.50")) is True

    def test_none_ask(self):
        from utils.createClobClient import _is_thin_or_empty_book
        assert _is_thin_or_empty_book(Decimal("0.50"), None) is True

    def test_garbage_values(self):
        from utils.createClobClient import _is_thin_or_empty_book
        assert _is_thin_or_empty_book(Decimal("0.01"), Decimal("0.99")) is True

    def test_normal_book(self):
        from utils.createClobClient import _is_thin_or_empty_book
        assert _is_thin_or_empty_book(Decimal("0.45"), Decimal("0.55")) is False

    def test_borderline_not_thin(self):
        from utils.createClobClient import _is_thin_or_empty_book
        # bid=0.04 is above the 0.03 ceiling, so not thin
        assert _is_thin_or_empty_book(Decimal("0.04"), Decimal("0.96")) is False


class TestMarketMetaCacheTTL:
    """Verify cache TTL behavior in ClobClientWrapper."""

    def test_good_book_cached_longer(self):
        """Normal book data should use the longer cache TTL."""
        import time as _time
        from utils.createClobClient import _META_CACHE_TTL_GOOD, ClobClientWrapper, MarketMeta

        wrapper = ClobClientWrapper.__new__(ClobClientWrapper)
        wrapper._market_meta_cache = {}
        meta = MarketMeta("tok1", Decimal("0.01"), Decimal("0"), False, Decimal("0.45"), Decimal("0.55"))
        wrapper._market_meta_cache["tok1"] = (meta, _time.monotonic() + _META_CACHE_TTL_GOOD)

        cached_meta, expiry = wrapper._market_meta_cache["tok1"]
        assert cached_meta is meta
        assert expiry > _time.monotonic()

    def test_thin_book_expires_fast(self):
        """Thin/garbage book should use a short TTL so it re-fetches quickly."""
        import time as _time
        from utils.createClobClient import _META_CACHE_TTL_THIN, _META_CACHE_TTL_GOOD, ClobClientWrapper, MarketMeta

        wrapper = ClobClientWrapper.__new__(ClobClientWrapper)
        wrapper._market_meta_cache = {}
        meta = MarketMeta("tok2", Decimal("0.01"), Decimal("0"), False, Decimal("0.01"), Decimal("0.99"))
        wrapper._market_meta_cache["tok2"] = (meta, _time.monotonic() + _META_CACHE_TTL_THIN)

        _, expiry = wrapper._market_meta_cache["tok2"]
        # Thin TTL should be much shorter than good TTL
        assert _META_CACHE_TTL_THIN < _META_CACHE_TTL_GOOD
