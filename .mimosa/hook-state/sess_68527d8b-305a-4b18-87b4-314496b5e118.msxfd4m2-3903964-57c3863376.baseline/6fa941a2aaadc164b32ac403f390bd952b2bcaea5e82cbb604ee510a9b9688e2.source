"""P1-j: ExecutionService consults the geoblock gate on the trade path.

A BUY *entry* is refused in a geoblocked region; a protective SELL exit is never
blocked (closing a position is risk-reducing and must not be stranded). When no
gate is wired, behaviour is unchanged.
"""

from __future__ import annotations

import time as _time
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from domain.models import ComplianceDecision
from integrations.polymarket.models import VenueOrderState, VenueSubmitResult
from interfaces.clob_gateway import ClobGateway
from services.executionService import ExecutionRequest, ExecutionService
from services.market_data_service import MarketDataBundle, MarketDataSource


def _make_exec_config() -> SimpleNamespace:
    ec = SimpleNamespace(
        min_share_price=0.0,
        max_share_price=0.0,
        pending_submission_ttl_seconds=120,
        enable_executable_order_guard=True,
        circuit_failure_threshold=5,
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
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=bid,
            best_ask=ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={bid: Decimal("100")},
            asks={ask: Decimal("100")},
            best_bid=bid,
            best_ask=ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("1"),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
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


def _success_gateway() -> MagicMock:
    gw = MagicMock(spec=ClobGateway)
    gw.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(success=True, order_id="ord-1", status="submitted")
    )
    gw.get_order = AsyncMock(
        return_value=VenueOrderState(
            order_id="ord-1", status="filled", size=Decimal("5"), filled_size=Decimal("5")
        )
    )
    return gw


class _StubGate:
    def __init__(self, decision):
        self._decision = decision

    async def ensure_allowed(self):
        return self._decision


def _blocked_decision():
    return ComplianceDecision(
        allowed=False,
        reason="region_blocked_by_polymarket",
        source="polymarket",
        checked_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        expires_at=None,
        raw={},
    )


def _req(side: str = "BUY") -> ExecutionRequest:
    return ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side=side,
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id=f"sig-{side}",
    )


def _build_svc(gateway, geoblock_gate):
    return ExecutionService(
        _StubClob(),
        _FeeSvc(),
        _mock_order_repo(),
        config=_make_exec_config(),
        clob_gateway=gateway,
        market_data_service=_make_mds(),
        geoblock_gate=geoblock_gate,
    )


@pytest.mark.asyncio
async def test_buy_entry_refused_when_geoblocked():
    gw = _success_gateway()
    svc = _build_svc(gw, _StubGate(_blocked_decision()))

    result = await svc.execute(_req("BUY"))

    assert result.accepted is False
    assert result.status == "geoblocked"
    gw.submit_marketable_order.assert_not_awaited()  # never reached the venue


@pytest.mark.asyncio
async def test_protective_sell_exit_not_blocked_by_geoblock():
    gw = _success_gateway()
    svc = _build_svc(gw, _StubGate(_blocked_decision()))

    await svc.execute(_req("SELL"))

    gw.submit_marketable_order.assert_awaited()  # exit proceeds despite geoblock


@pytest.mark.asyncio
async def test_no_gate_means_no_geoblock_enforcement():
    gw = _success_gateway()
    svc = _build_svc(gw, None)

    await svc.execute(_req("BUY"))

    gw.submit_marketable_order.assert_awaited()
