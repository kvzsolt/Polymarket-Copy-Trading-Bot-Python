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
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "rejected"
        assert "spread too wide" in result.error

    def test_tight_spread_passes(self):
        """Spread of 2% (200 bps) should pass at default 500 bps limit."""
        svc = _make_execution_service(_meta("0.50", "0.51"))  # 2% spread
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        # Should NOT be rejected for spread — may fail later for other reasons
        assert result.error is None or "spread" not in result.error

    def test_no_bid_passes(self):
        """If best_bid is None, spread check is skipped (can't compute)."""
        svc = _make_execution_service(_meta(None, "0.51"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_no_ask_passes(self):
        """If best_ask is None, spread check is skipped."""
        svc = _make_execution_service(_meta("0.50", None))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_zero_bid_passes(self):
        """If best_bid is 0, spread check is skipped (avoid division by zero)."""
        svc = _make_execution_service(_meta("0", "0.51"))
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_gate_disabled_when_zero(self):
        """MAX_BOOK_SPREAD_BPS=0 disables the gate entirely."""
        svc = _make_execution_service(_meta("0.10", "0.90"))  # 800% spread
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "0"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_custom_threshold(self):
        """Custom threshold at 100 bps should reject 2% spread."""
        svc = _make_execution_service(_meta("0.50", "0.51"))  # 2% = 200 bps
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "100"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert "spread too wide" in result.error

    def test_sell_side_also_gated(self):
        """Spread gate applies to SELL orders too."""
        svc = _make_execution_service(_meta("0.40", "0.48"))  # 20% spread
        req = _req(side="SELL", amount="5")
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert "spread too wide" in result.error

    def test_error_includes_actual_spread(self):
        """Error message includes actual spread in bps."""
        svc = _make_execution_service(_meta("0.40", "0.48"))  # 2000 bps
        req = _req()
        with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}):
            result = asyncio.get_event_loop().run_until_complete(svc.execute(req))
        assert "2000 bps" in result.error
        assert "500 bps max" in result.error
