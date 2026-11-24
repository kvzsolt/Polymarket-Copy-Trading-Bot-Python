"""Attempted order-id persistence and multi-candidate reconciliation probing.

Every order id the venue hands back during submission attempts is persisted on
the row response (``attempted_order_ids``) so reconciliation can probe fills and
order state under any candidate hash — sibling attempts included — while
auto-release probes treat candidate fills as blocking evidence.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.integrations.polymarket.errors import VenueOrderNotFoundError
from pmcopy.services.execution.reconciliation_service import ReconciliationService
from pmcopy.services.execution.submission import sanitize_submission_response

ROW_ORDER_ID = "0xROWHASH"
ATTEMPTED_ORDER_ID = "0xATT1"


def _order_row(
    now: float,
    *,
    attempted: list[str] | None = None,
    age_seconds: float = 120.0,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "status": "ambiguous_timeout",
        "orderID": ROW_ORDER_ID,
        "size_matched": "0",
    }
    if attempted is not None:
        response["attempted_order_ids"] = attempted
    return {
        "order_id": ROW_ORDER_ID,
        "user_id": "0xUSER",
        "leader_id": "0xLEADER",
        "status": "ambiguous_timeout",
        "created_at": datetime.fromtimestamp(now - age_seconds, tz=timezone.utc),
        "request": {
            "side": "BUY",
            "amount": "4.056",
            "price_limit": "0.3570",
            "condition_id": "cond-1",
            "token_id": "tok-1",
            "reservation_id": "res-1",
        },
        "response": response,
        "matched_total": "0",
    }


def _fill(order_id: str = ATTEMPTED_ORDER_ID, *, size: str = "11.58", price: str = "0.35") -> dict[str, Any]:
    return {
        "fill_id": "tr-att-1",
        "order_id": order_id,
        "side": "BUY",
        "size": size,
        "price": price,
        "timestamp": int(time.time()),
        "status": "CONFIRMED",
        "transaction_hash": "0xTX-att-1",
        "fee": None,
    }


class _CandidateGateway:
    """Gateway answering per candidate order id like the incident shape."""

    def __init__(
        self,
        *,
        fills_by_order: dict[str, list[dict[str, Any]]] | None = None,
        order_states_by_id: dict[str, Any] | None = None,
        open_by_id: dict[str, list[Any]] | None = None,
    ):
        self.fills_by_order = fills_by_order or {}
        self.order_states_by_id = order_states_by_id or {}
        self.open_by_id = open_by_id or {}
        self.market_calls = 0

    async def get_order(self, order_id: str) -> Any:
        state = self.order_states_by_id.get(order_id)
        if state is None:
            raise VenueOrderNotFoundError("order_not_found")
        return state

    async def list_open_orders(self, *, order_id: str | None = None, market: str | None = None):
        if order_id is None:
            return []
        return self.open_by_id.get(order_id, [])

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ):
        if order_id is not None:
            return self.fills_by_order.get(order_id, [])
        if trade_id is not None:
            return []
        self.market_calls += 1
        return []


class _OrdersRepo:
    def __init__(self, row: dict[str, Any]):
        self.row = row

    async def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        if order_id == self.row.get("order_id"):
            return self.row
        return None


class _AuditLogger:
    def __init__(self):
        self.events: list[tuple[str, dict]] = []

    def log_event(self, event_name: str, payload: dict) -> None:
        self.events.append((event_name, payload))


def _service(gateway: _CandidateGateway, row: dict[str, Any], *, positions_repo=None):
    return ReconciliationService(
        orders_repo=_OrdersRepo(row),
        fills_repo=MagicMock(),
        positions_repo=positions_repo or MagicMock(),
        reservation_service=MagicMock(),
        clob_gateway=gateway,
        audit_logger=_AuditLogger(),
    )


@pytest.mark.asyncio
async def test_confirm_fill_finds_fills_under_attempted_order_id():
    now = time.time()
    row = _order_row(now, attempted=[ATTEMPTED_ORDER_ID])
    gateway = _CandidateGateway(
        fills_by_order={ROW_ORDER_ID: [], ATTEMPTED_ORDER_ID: [_fill()]}
    )
    svc = _service(gateway, row)

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "account_trades"
    assert result["status"] == "confirmed"
    assert result["size_matched"] == "11.58"
    assert gateway.market_calls == 0


@pytest.mark.asyncio
async def test_confirm_fill_resolves_order_state_from_attempted_candidate():
    now = time.time()
    row = _order_row(now, attempted=[ATTEMPTED_ORDER_ID])
    gateway = _CandidateGateway(
        order_states_by_id={
            ATTEMPTED_ORDER_ID: {"status": "filled", "size_matched": "11.58"}
        }
    )
    svc = _service(gateway, row)

    result = await svc.confirm_fill(ROW_ORDER_ID)

    assert result["confirmation_source"] == "order"
    assert result["status"] == "filled"
    assert result["size_matched"] == "11.58"


@pytest.mark.asyncio
async def test_auto_release_probe_blocks_when_fills_under_attempted_candidate():
    now = time.time()
    row = _order_row(now, attempted=[ATTEMPTED_ORDER_ID])
    gateway = _CandidateGateway(
        fills_by_order={ROW_ORDER_ID: [], ATTEMPTED_ORDER_ID: [_fill()]}
    )
    positions_repo = MagicMock()
    positions_repo.get_position = AsyncMock(
        return_value=MagicMock(size_tokens=Decimal("0"))
    )
    svc = _service(gateway, row, positions_repo=positions_repo)

    probe = await svc._run_auto_release_probes(
        order_id=ROW_ORDER_ID,
        venue_order_id=None,
        market_id="cond-1",
        trade_ids=[],
        user_id="0xUSER",
        condition_id="cond-1",
        token_id="tok-1",
        expected_size=Decimal("4.056"),
        side="BUY",
        pre_submit_position_size=Decimal("0"),
        order_id_candidates=[ROW_ORDER_ID, ATTEMPTED_ORDER_ID],
    )

    assert probe["details"]["get_fills"] == "fills_present:1"
    assert probe["inconclusive"] >= 1


def test_sanitize_submission_response_preserves_attempted_order_ids():
    sanitized = sanitize_submission_response(
        {
            "success": True,
            "status": "submitted",
            "orderID": "0xB",
            "attempted_order_ids": ["0xA", "0xB"],
        }
    )
    assert sanitized["attempted_order_ids"] == ["0xA", "0xB"]


@pytest.fixture(autouse=True)
def _reset_circuit():
    """Reset the process-wide circuit breaker around every test."""
    import pmcopy.services.execution.execution_service as _es

    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


def _make_exec_config() -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(
            min_share_price=0,
            max_share_price=0,
            pending_submission_ttl_seconds=120,
            circuit_failure_threshold=5,
            circuit_cooldown_seconds=60,
            execution_require_fresh_book=False,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=500,
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
        ),
        policy_version="test",
        raw={},
    )


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


@pytest.mark.asyncio
async def test_execution_service_persists_attempted_order_ids():
    from pmcopy.cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
    from pmcopy.services.execution.execution_service import ExecutionRequest, ExecutionService
    from pmcopy.services.market_data.market_data_service import MarketDataBundle, MarketDataSource
    from pmcopy.integrations.polymarket.models import VenueSubmitResult

    now = time.monotonic()
    bundle = MarketDataBundle(
        token_id="t1",
        quote=QuoteSnapshot(
            token_id="t1",
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id="t1",
            bids={Decimal("0.49"): Decimal("100")},
            asks={Decimal("0.51"): Decimal("100")},
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id="t1",
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

    gateway = MagicMock()
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            order_id=ATTEMPTED_ORDER_ID,
            status="canceled",
            error="order_not_fillable",
            non_retryable=True,
        )
    )

    repo = MagicMock()
    repo.get_by_idempotency_key = AsyncMock(return_value=None)
    repo.try_acquire_submission_lock = AsyncMock(return_value=True)
    repo.is_submission_locked = AsyncMock(return_value=False)
    repo.save_submission = AsyncMock()

    svc = ExecutionService(
        None,
        _FeeSvc(),
        repo,
        clob_gateway=gateway,
        config=_make_exec_config(),
        market_data_service=mds,
    )
    req = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-att-1",
    )

    result = await svc.execute(req)

    assert result.accepted is False
    record = repo.save_submission.await_args.args[0]
    assert record.response_payload["attempted_order_ids"] == [ATTEMPTED_ORDER_ID]
