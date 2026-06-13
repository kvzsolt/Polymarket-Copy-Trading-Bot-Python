"""P1-m: a venue outage that surfaces as ``VenueUnavailableError`` (ambiguous,
submitted-unconfirmed) must still record a circuit-breaker failure.

The submission retry loop broke out of the loop on ``VenueUnavailableError`` /
unclassified exceptions WITHOUT calling ``circuit_breaker.record_failure()`` —
only the retry-exhausted ``for/else`` recorded one. So the dominant outage mode
(venue down -> ambiguous) never tripped the breaker, and the bot kept POSTing
into a dead venue, piling up ``submitted_unconfirmed`` rows.

This harness mocks the orders repo so the path runs without Postgres.
"""

from __future__ import annotations

import time as _time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from integrations.polymarket.errors import VenueUnavailableError
from interfaces.clob_gateway import ClobGateway
from services.circuitBreaker import CircuitBreaker
from services.executionService import ExecutionRequest, ExecutionService
from services.market_data_service import MarketDataBundle, MarketDataSource


def _make_exec_config() -> SimpleNamespace:
    ec = SimpleNamespace(
        min_share_price=0.0,
        max_share_price=0.0,
        pending_submission_ttl_seconds=120,
        enable_executable_order_guard=True,
        circuit_failure_threshold=3,
        circuit_cooldown_seconds=60,
        execution_require_fresh_book=False,
        book_spread_mode="absolute_bps",
        max_book_spread_bps=5000,
        max_book_spread_ticks=0,
        slippage_tolerance=2.0,
        max_retries=3,
        retry_backoff_base=1.0,
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
    )
    return SimpleNamespace(execution=ec, raw={"EXECUTION_SUBMIT_CONCURRENCY": 2})


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


def _make_mds(token_id: str = "t1") -> AsyncMock:
    now = _time.monotonic()
    bid, ask = Decimal("0.49"), Decimal("0.51")
    bundle = MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(token_id=token_id, best_bid=bid, best_ask=ask, last_trade_price=None, source="wss", monotonic_ts=now),
        book=BookSnapshot(token_id=token_id, bids={bid: Decimal("100")}, asks={ask: Decimal("100")}, best_bid=bid, best_ask=ask, source="wss", monotonic_ts=now),
        tick=TickSnapshot(token_id=token_id, tick_size=Decimal("0.01"), min_order_size=Decimal("1"), neg_risk=False, source="wss", monotonic_ts=now),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    return mds


class _StubClob:
    def __init__(self) -> None:
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.client = SimpleNamespace(
            get_order=lambda _oid: {"status": "filled", "size_matched": "5", "avg_price": "0.50"},
        )


def _mock_order_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_by_idempotency_key = AsyncMock(return_value=None)
    repo.try_acquire_submission_lock = AsyncMock(return_value=True)
    repo.is_submission_locked = AsyncMock(return_value=False)
    repo.save_submission = AsyncMock()
    repo.update_submission_status = AsyncMock()
    repo.release_submission_lock = AsyncMock()
    return repo


def _make_req(**overrides) -> ExecutionRequest:
    defaults = dict(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-1",
    )
    defaults.update(overrides)
    return ExecutionRequest(**defaults)


def _build_svc(breaker: CircuitBreaker) -> ExecutionService:
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(side_effect=VenueUnavailableError("venue down"))
    gateway.get_order = AsyncMock()
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
        circuit_breaker=breaker,
    )


@pytest.mark.asyncio
async def test_venue_unavailable_trips_circuit_after_threshold():
    breaker = CircuitBreaker("test_clob", failure_threshold=3, cooldown_seconds=60)
    svc = _build_svc(breaker)

    assert breaker.allow_request() is True
    for i in range(3):
        assert breaker.allow_request() is True, f"breaker opened too early at call {i}"
        await svc.execute(_make_req(signal_id=f"sig-{i}"))

    # Each ambiguous venue-unavailable submission must have recorded a failure;
    # after the threshold the breaker opens and stops POSTing into a dead venue.
    assert breaker.allow_request() is False
