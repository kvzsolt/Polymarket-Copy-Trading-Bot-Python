"""Phase 9 — book quality gate: reject executions when bid/ask spread is too wide."""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from services.market_data_service import MarketDataBundle, MarketDataPolicy, MarketDataSource
from utils.createClobClient import _top_of_book_from_levels


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_bundle(bid: str | None, ask: str | None) -> MarketDataBundle:
    now = time.monotonic()
    best_bid = Decimal(bid) if bid else None
    best_ask = Decimal(ask) if ask else None
    return MarketDataBundle(
        token_id="0xabc",
        quote=QuoteSnapshot(
            token_id="0xabc",
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id="0xabc",
            bids={best_bid: Decimal("100")} if best_bid else {},
            asks={best_ask: Decimal("100")} if best_ask else {},
            best_bid=best_bid,
            best_ask=best_ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id="0xabc",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


def _mock_mds(bid: str | None, ask: str | None) -> AsyncMock:
    mds = AsyncMock()
    mds._policy = MarketDataPolicy(rest_snapshot_max_spread_bps=300)
    mds.require_fresh_data = AsyncMock(return_value=_make_bundle(bid, ask))
    return mds


def _make_execution_service(bid: str | None, ask: str | None, quote_price: str = "0.50"):
    """Build a minimal ExecutionService with mocked deps."""
    from services.executionService import ExecutionService

    clob = AsyncMock()
    clob.get_price = AsyncMock(return_value={"price": quote_price})
    clob.create_market_order = AsyncMock(return_value={"signed": True})
    clob.post_signed_order = AsyncMock(
        return_value={"success": True, "orderID": "ord-book", "status": "submitted"}
    )
    clob.client = SimpleNamespace(
        get_order=lambda _order_id: {
            "status": "filled",
            "size_matched": "10",
            "avg_price": quote_price,
        }
    )

    order_repo = MagicMock()
    order_repo.get_by_idempotency_key = AsyncMock(return_value=None)
    order_repo.is_submission_locked = AsyncMock(return_value=False)
    order_repo.try_acquire_submission_lock = AsyncMock(return_value=True)
    order_repo.save_submission = AsyncMock(return_value=None)

    # Minimal AppConfig-shaped object with concrete values. MagicMock attributes
    # leak into the effective-idempotency payload and are not JSON serializable.
    mock_cfg = SimpleNamespace(
        execution=SimpleNamespace(
            execution_require_fresh_book=True,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=2500,
            max_book_spread_ticks=0,
            min_share_price=0.03,
            max_share_price=0.97,
            slippage_tolerance=2.0,
            max_retries=3,
            retry_backoff_base=1.0,
            pending_submission_ttl_seconds=120,
            non_terminal_failure_max_attempts=3,
            transaction_fee_bps=20,
            gas_cost_usdc=0.03,
            risk_buffer_pct=0.10,
            paper_trade_only=False,
            enable_copy_decision_gate=False,
            enable_near_resolution_gate=False,
            near_resolution_gate_price=0.95,
            near_resolution_max_age_seconds=15,
            meta_cache_ttl_good=300,
            meta_cache_ttl_thin=30,
        ),
        policy_version="test",
        raw={},
    )

    return ExecutionService(
        clob,
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal("0"))),
        order_repo,
        config=mock_cfg,
        market_data_service=_mock_mds(bid, ask),
    )


def _req(amount: str = "10", side: str = "BUY", **overrides):
    from services.executionService import ExecutionRequest

    data = dict(
        user_id="user1",
        leader_id="0xleader",
        token_id="0xabc",
        side=side,
        amount=Decimal(amount),
        max_slippage_bps=200,
        reason="copy_trade",
    )
    data.update(overrides)
    return ExecutionRequest(
        **data,
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
        """Spread of 20% (2000 bps) should be rejected at 500 bps limit."""
        svc = _make_execution_service("0.40", "0.48")  # 20% spread
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_tight_spread_passes(self):
        """Spread of 2% (200 bps) should pass at 500 bps limit."""
        svc = _make_execution_service("0.50", "0.51")  # 2% spread
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_no_bid_rejected_for_sell(self):
        """If best_bid is None, SELL orders should be rejected (need bid to sell into)."""
        svc = _make_execution_service(None, "0.51")
        svc._config.execution.max_book_spread_bps = 500
        req = _req(side="SELL", amount="5")
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_no_ask_rejected_for_buy(self):
        """If best_ask is None, BUY orders should be rejected (need ask to buy into)."""
        svc = _make_execution_service("0.50", None)
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_one_sided_buy_with_no_bid_allowed(self):
        """BUY with best_bid=None but valid best_ask should pass the spread gate."""
        svc = _make_execution_service(None, "0.51")
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error
        assert result.status != "book_no_liquidity"

    def test_one_sided_sell_with_no_ask_allowed(self):
        """SELL with best_ask=None but valid best_bid should pass the spread gate."""
        svc = _make_execution_service("0.50", None)
        svc._config.execution.max_book_spread_bps = 500
        req = _req(side="SELL", amount="5")
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error
        assert result.status != "book_no_liquidity"

    def test_zero_bid_passes(self):
        """If best_bid is 0, spread check is skipped (avoid division by zero)."""
        svc = _make_execution_service("0", "0.51")
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_gate_disabled_when_zero(self):
        """MAX_BOOK_SPREAD_BPS=0 disables the gate entirely."""
        svc = _make_execution_service("0.10", "0.90")  # 800% spread
        svc._config.execution.max_book_spread_bps = 0
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_stop_loss_sell_bypasses_spread_gate_and_uses_fak(self):
        """Risk exits liquidate into the bid even when the opposite ask makes
        the book too wide for normal copy-trade entries."""
        svc = _make_execution_service("0.10", "0.90")
        svc._config.execution.max_book_spread_bps = 500
        req = _req(
            side="SELL",
            amount="5",
            reason="stop_loss",
            recovery_metadata={"source": "risk_exit"},
        )

        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))

        assert result.accepted is True
        assert result.status == "submitted_unconfirmed"
        svc.clob_client.create_market_order.assert_awaited_once()
        assert svc.clob_client.create_market_order.await_args.kwargs["order_type"] == "FAK"
        assert svc.clob_client.post_signed_order.await_args.kwargs["order_type"] == "FAK"

    def test_copy_trade_fak_uses_fresh_snapshot_without_spread_gate(self):
        """Copy FAKs read current top-of-book but do not reject on wide spread."""
        svc = _make_execution_service("0.10", "0.90")
        svc.clob_gateway.get_execution_metadata = AsyncMock(
            side_effect=AssertionError("metadata-only path should not run")
        )
        svc.clob_client.post_signed_order = AsyncMock(
            return_value={
                "success": True,
                "orderID": "ord-fak",
                "status": "matched",
                "size_matched": "4",
                "avg_price": "0.25",
            }
        )
        req = _req(
            amount="1",
            order_type="FAK",
            price_limit=Decimal("0.25"),
            recovery_metadata={"order_type": "FAK", "price_limit": "0.25"},
        )

        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))

        assert result.accepted is True
        assert result.status == "filled"
        assert result.filled_amount == Decimal("4")
        svc._market_data_service.require_fresh_data.assert_awaited_once()
        _, snapshot_kwargs = svc._market_data_service.require_fresh_data.await_args
        assert snapshot_kwargs["side"] == "BUY"
        assert snapshot_kwargs["policy"].rest_snapshot_max_spread_bps == 1_000_000
        assert snapshot_kwargs["policy"].require_book_depth is False
        svc.clob_client.create_market_order.assert_awaited_once()
        assert svc.clob_client.create_market_order.await_args.kwargs["order_type"] == "FAK"
        assert svc.clob_client.create_market_order.await_args.kwargs["max_price"] == 0.92

    def test_copy_trade_fak_reposts_remaining_partial_fill(self):
        """Partial FAK fills refresh the book before bounded FAK reposts."""
        svc = _make_execution_service("0.10", "0.90")
        svc.clob_gateway.get_execution_metadata = AsyncMock(
            side_effect=AssertionError("metadata-only path should not run")
        )
        svc.clob_client.post_signed_order = AsyncMock(
            side_effect=[
                {
                    "success": True,
                    "orderID": "ord-fak-1",
                    "status": "matched",
                    "size_matched": "4",
                    "avg_price": "0.25",
                },
                {
                    "success": True,
                    "orderID": "ord-fak-2",
                    "status": "matched",
                    "size_matched": "4",
                    "avg_price": "0.25",
                },
            ]
        )
        req = _req(
            amount="2",
            order_type="FAK",
            price_limit=Decimal("0.25"),
            recovery_metadata={"order_type": "FAK", "price_limit": "0.25"},
        )

        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))

        assert result.accepted is True
        assert result.status == "filled"
        assert result.filled_amount == Decimal("8")
        assert result.order_id == "ord-fak-1,ord-fak-2"
        assert svc.clob_client.create_market_order.await_count == 2
        for call in svc.clob_client.post_signed_order.await_args_list:
            assert call.kwargs["order_type"] == "FAK"
        assert svc._market_data_service.require_fresh_data.await_count == 2

    def test_copy_trade_fak_no_match_does_not_retry_or_trip_circuit(self):
        """A venue FAK no-match cancellation is terminal, not a CLOB outage."""
        svc = _make_execution_service("0.10", "0.90")
        svc.clob_client.post_signed_order = AsyncMock(
            return_value={
                "success": False,
                "status": "cancelled",
                "error": "fak_no_match",
                "non_retryable": True,
            }
        )
        req = _req(
            amount="2",
            order_type="FAK",
            price_limit=Decimal("0.25"),
            recovery_metadata={"order_type": "FAK", "price_limit": "0.25"},
        )

        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))

        assert result.accepted is False
        assert result.status == "cancelled"
        assert result.error == "fak_no_match"
        assert svc.clob_client.post_signed_order.await_count == 1
        assert svc.circuit_breaker.allow_request() is True

    def test_custom_threshold(self):
        """Relative mode: custom threshold at 100 bps should reject 2% spread."""
        svc = _make_execution_service("0.50", "0.51")  # 2% = 200 bps
        svc._config.execution.max_book_spread_bps = 100
        svc._config.execution.book_spread_mode = "relative_bps"
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_low_price_one_cent_spread_passes_in_absolute_mode(self):
        """absolute_bps mode should allow a 1-cent spread when threshold is 100 bps."""
        svc = _make_execution_service("0.04", "0.05")
        svc._config.execution.max_book_spread_bps = 100
        svc._config.execution.book_spread_mode = "absolute_bps"
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.error is None or "spread" not in result.error

    def test_low_price_one_cent_spread_rejects_in_relative_mode(self):
        """relative_bps mode should reject the same 1-cent spread at a low threshold."""
        svc = _make_execution_service("0.04", "0.05")
        svc._config.execution.max_book_spread_bps = 1000
        svc._config.execution.book_spread_mode = "relative_bps"
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"

    def test_spread_gate_requests_fresh_meta(self):
        svc = _make_execution_service("0.40", "0.48")
        svc._config.execution.max_book_spread_bps = 500
        svc._config.execution.execution_require_fresh_book = True
        req = _req()
        asyncio.new_event_loop().run_until_complete(svc.execute(req))
        svc._market_data_service.require_fresh_data.assert_awaited_once_with("0xabc", side="BUY", condition_id=None)

    def test_sell_side_also_gated(self):
        """Spread gate applies to SELL orders too."""
        svc = _make_execution_service("0.40", "0.48")  # 20% spread
        svc._config.execution.max_book_spread_bps = 500
        req = _req(side="SELL", amount="5")
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"
        assert "spread too wide" in result.error

    def test_error_includes_actual_spread(self):
        """Error message includes actual spread in bps."""
        svc = _make_execution_service("0.40", "0.48")  # 2000 bps
        svc._config.execution.max_book_spread_bps = 500
        svc._config.execution.book_spread_mode = "relative_bps"
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert "2000 bps" in result.error
        assert "500 bps max" in result.error

    def test_garbage_book_detected(self):
        """bid=0.01, ask=0.99 is a garbage/placeholder book — should be book_no_liquidity."""
        svc = _make_execution_service("0.01", "0.99")
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_garbage_book_borderline(self):
        """bid=0.02, ask=0.98 should still be flagged as garbage."""
        svc = _make_execution_service("0.02", "0.98")
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_no_liquidity"

    def test_legitimate_wide_spread_not_classified_as_garbage(self):
        """bid=0.04, ask=0.96 is NOT garbage — gets the normal spread rejection."""
        svc = _make_execution_service("0.04", "0.96")
        svc._config.execution.max_book_spread_bps = 500
        req = _req()
        result = asyncio.new_event_loop().run_until_complete(svc.execute(req))
        assert result.accepted is False
        assert result.status == "book_spread_wide"


class TestSpreadMetrics:
    """Test the _calculate_book_spread_metrics helper."""

    def test_spread_metrics_include_absolute_relative_and_ticks(self):
        from services.executionService import _calculate_book_spread_metrics
        metrics = _calculate_book_spread_metrics(
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.51"),
            tick_size=Decimal("0.01"),
        )
        assert metrics.absolute_bps > 0
        assert metrics.relative_bps > 0
        assert metrics.ticks == 1


class TestDictMappedLevels:
    """Test _top_of_book_from_levels with dict-mapped level data."""

    def test_dict_mapped_bids(self):
        result = _top_of_book_from_levels({"0.45": 100, "0.50": 200}, side="bid")
        assert result == Decimal("0.50")

    def test_dict_mapped_asks(self):
        result = _top_of_book_from_levels({"0.55": 100, "0.51": 200}, side="ask")
        assert result == Decimal("0.51")

    def test_empty_dict_returns_none(self):
        assert _top_of_book_from_levels({}, side="bid") is None
        assert _top_of_book_from_levels({}, side="ask") is None

    def test_dict_mapped_with_invalid_key(self):
        result = _top_of_book_from_levels({"abc": 100, "0.50": 200}, side="bid")
        assert result == Decimal("0.50")
