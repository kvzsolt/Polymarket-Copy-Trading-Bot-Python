"""Phase 4 — book depth gate tests.

Spread gate behavior is covered in ``tests/test_book_quality_gate.py``;
this module focuses on the depth-only behavior: aggregated side liquidity
within a percentage band of the mid must be at least ``MIN_BOOK_DEPTH_USDC``
for the order to be sent.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from integrations.polymarket.models import VenueSubmitResult
from services.execution.book_spread import calculate_book_depth_metrics
from services.market_data_service import MarketDataBundle, MarketDataSource


# ---------------------------------------------------------------------------
# Pure helper tests
# ---------------------------------------------------------------------------
class TestCalculateBookDepthMetrics:
    def test_depth_summed_within_band(self) -> None:
        metrics = calculate_book_depth_metrics(
            bids={Decimal("0.49"): Decimal("100"), Decimal("0.40"): Decimal("500")},
            asks={Decimal("0.51"): Decimal("100"), Decimal("0.60"): Decimal("500")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            band_pct=Decimal("5"),
        )
        # mid = 0.50, band = +/-5% = [0.475, 0.525]. Only the inside levels qualify.
        assert metrics.ask_notional_usdc == Decimal("0.51") * Decimal("100")
        assert metrics.bid_notional_usdc == Decimal("0.49") * Decimal("100")
        assert metrics.ask_levels_in_band == 1
        assert metrics.bid_levels_in_band == 1

    def test_string_keys_normalized(self) -> None:
        metrics = calculate_book_depth_metrics(
            bids={"0.49": "100"},
            asks={"0.51": "100"},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            band_pct=5,
        )
        assert metrics.bid_notional_usdc == Decimal("0.49") * Decimal("100")
        assert metrics.ask_notional_usdc == Decimal("0.51") * Decimal("100")

    def test_no_book_returns_zero(self) -> None:
        metrics = calculate_book_depth_metrics(
            bids={},
            asks={},
            best_bid=None,
            best_ask=None,
            band_pct=5,
        )
        assert metrics.bid_notional_usdc == Decimal("0")
        assert metrics.ask_notional_usdc == Decimal("0")

    def test_negative_or_zero_levels_ignored(self) -> None:
        metrics = calculate_book_depth_metrics(
            bids={Decimal("0"): Decimal("100"), Decimal("0.49"): Decimal("0")},
            asks={Decimal("-0.01"): Decimal("100")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            band_pct=5,
        )
        assert metrics.bid_notional_usdc == Decimal("0")
        assert metrics.ask_notional_usdc == Decimal("0")

    def test_side_notional_dispatch(self) -> None:
        metrics = calculate_book_depth_metrics(
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("200")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            band_pct=5,
        )
        assert metrics.side_notional_usdc("BUY") == metrics.ask_notional_usdc
        assert metrics.side_notional_usdc("SELL") == metrics.bid_notional_usdc


# ---------------------------------------------------------------------------
# ExecutionService gate behavior
# ---------------------------------------------------------------------------
def _make_bundle_with_book(bids: dict, asks: dict) -> MarketDataBundle:
    now = time.monotonic()
    best_bid = max(bids.keys()) if bids else None
    best_ask = min(asks.keys()) if asks else None
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
            bids=bids,
            asks=asks,
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


def _make_execution_service(bids: dict, asks: dict, *, min_depth: float = 0.0):
    from services.executionService import ExecutionService

    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=_make_bundle_with_book(bids, asks))

    clob = AsyncMock()
    gateway = SimpleNamespace(
        submit_marketable_order=AsyncMock(
            return_value=VenueSubmitResult(
                success=True,
                order_id="ord-x",
                status="submitted",
            )
        )
    )

    order_repo = MagicMock()
    order_repo.get_by_idempotency_key = AsyncMock(return_value=None)
    order_repo.is_submission_locked = AsyncMock(return_value=False)
    order_repo.try_acquire_submission_lock = AsyncMock(return_value=True)
    order_repo.save_submission = AsyncMock(return_value=None)

    mock_cfg = SimpleNamespace(
        execution=SimpleNamespace(
            execution_require_fresh_book=True,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=10_000,  # disable spread gate for these tests
            max_book_spread_ticks=0,
            min_share_price=0.01,
            max_share_price=0.99,
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
            min_book_depth_usdc=float(min_depth),
            book_depth_band_pct=5.0,
        ),
        policy_version="test",
        raw={},
    )

    return ExecutionService(
        clob,
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal("0"))),
        order_repo,
        config=mock_cfg,
        clob_gateway=gateway,
        market_data_service=mds,
    )


def _req(side: str = "BUY", amount: str = "5"):
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


@pytest.fixture(autouse=True)
def _reset_circuit():
    import services.executionService as _es

    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


class TestBookDepthGate:
    def test_disabled_when_min_depth_zero(self):
        svc = _make_execution_service(
            bids={Decimal("0.49"): Decimal("1")},
            asks={Decimal("0.51"): Decimal("1")},
            min_depth=0.0,
        )
        result = asyncio.run(svc.execute(_req()))
        assert result.status != "book_thin_depth"

    def test_thin_depth_rejected_for_buy(self):
        # Ask side has only 1 token at 0.51 → notional ~0.51 USDC, far below 50.
        svc = _make_execution_service(
            bids={Decimal("0.49"): Decimal("1000")},
            asks={Decimal("0.51"): Decimal("1")},
            min_depth=50.0,
        )
        result = asyncio.run(svc.execute(_req("BUY")))
        assert result.accepted is False
        assert result.status == "book_thin_depth"
        assert "depth too thin" in result.error

    def test_thin_depth_rejected_for_sell(self):
        svc = _make_execution_service(
            bids={Decimal("0.49"): Decimal("1")},
            asks={Decimal("0.51"): Decimal("1000")},
            min_depth=50.0,
        )
        result = asyncio.run(svc.execute(_req("SELL", "1")))
        assert result.accepted is False
        assert result.status == "book_thin_depth"

    def test_sufficient_depth_passes(self):
        svc = _make_execution_service(
            bids={Decimal("0.49"): Decimal("1000")},
            asks={Decimal("0.51"): Decimal("1000")},
            min_depth=50.0,
        )
        result = asyncio.run(svc.execute(_req("BUY")))
        assert result.status != "book_thin_depth"

    def test_outside_band_levels_ignored(self):
        # Only level is at 0.65, far outside the +/-5% band of mid 0.50.
        svc = _make_execution_service(
            bids={Decimal("0.49"): Decimal("1000")},
            asks={Decimal("0.65"): Decimal("10_000")},
            min_depth=100.0,
        )
        result = asyncio.run(svc.execute(_req("BUY")))
        assert result.accepted is False
        assert result.status == "book_thin_depth"
