"""Regression tests for the copy-trade ``ExecutionIntent`` built by the pipeline.

The pipeline sizes a copy with the risk engine (``gated_notional``) and hands
that number to ``_submit_copy_through_coordinator`` as
``ExecutionRequest.amount``. ``ExecutionCoordinator`` re-derives the BUY
``ExecutionRequest.amount`` from ``ExecutionIntent.notional_usdc``, so the
intent must carry the *follower's* risk-sized pUSD spend — never the leader's
notional (``signal.usdc_size``).

These tests drive the real chain
``_submit_copy_through_coordinator -> ExecutionCoordinator -> execution_service``
with in-memory doubles so the money value is asserted where it is actually
spent.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.execution.coordinator import ExecutionCoordinator
from services.executionService import ExecutionResult
from orchestrators.trade_pipeline import _submit_copy_through_coordinator


LEADER = "0x" + "d" * 40
TOKEN = "0x" + "e" * 40


class _Reservation:
    """Minimal reservation handle exposing only the id the coordinator reads."""

    def __init__(self, reservation_id: str) -> None:
        self.id = reservation_id


class _RecordingReservationService:
    """Reservation double capturing the share-shaped OrderIntent it receives."""

    enabled = False

    def __init__(self) -> None:
        self.intents: list[object] = []
        self.released: list[tuple[str, str]] = []

    async def reserve_for_intent(self, intent):
        self.intents.append(intent)
        return _Reservation("res-1")

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        self.released.append((reservation_id, reason))


class _RecordingExecutionService:
    """ExecutionService double recording every ExecutionRequest it is handed."""

    def __init__(self) -> None:
        self.requests: list[object] = []

    async def execute(self, request):
        self.requests.append(request)
        return ExecutionResult(
            accepted=True,
            order_id="ord-1",
            status="filled",
            requested_amount=Decimal(str(request.amount)),
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=None,
        )


def _coordinator(execution_service) -> ExecutionCoordinator:
    return ExecutionCoordinator(
        reservation_service=_RecordingReservationService(),
        orders_repo=None,
        fills_repo=None,
        positions_repo=None,
        execution_service=execution_service,
    )


def _context(coordinator) -> SimpleNamespace:
    return SimpleNamespace(
        user_id="user1",
        config=SimpleNamespace(dry_run=False, execution=SimpleNamespace(paper_trade_only=False)),
        execution_coordinator=coordinator,
        execution_service=None,
    )


def _exec_req(*, side: str, amount: Decimal) -> SimpleNamespace:
    return SimpleNamespace(
        user_id="user1",
        leader_id=LEADER,
        token_id=TOKEN,
        side=side,
        amount=amount,
        max_slippage_bps=200,
        reason="copy_trade",
        signal_id="0xsig",
        condition_id="cond-1",
        recovery_metadata={"source": "copy_trade", "order_type": "FAK"},
    )


def _signal(*, side: str, price: float, usdc_size: float, size: float) -> SimpleNamespace:
    return SimpleNamespace(
        side=side,
        price=price,
        size=Decimal(str(size)),
        usdc_size=Decimal(str(usdc_size)),
        token_id=TOKEN,
        condition_id="cond-1",
        tx_hash="0xsig",
    )


@pytest.mark.asyncio
async def test_buy_submits_risk_gated_notional_not_leader_notional() -> None:
    """A $102.65 leader BUY sized down to $5.33 must spend $5.33."""
    exec_svc = _RecordingExecutionService()
    coordinator = _coordinator(exec_svc)

    await _submit_copy_through_coordinator(
        _context(coordinator),
        _exec_req(side="BUY", amount=Decimal("5.33")),
        _signal(side="BUY", price=0.45, usdc_size=102.65, size=228.11),
        leader_id=LEADER,
    )

    [submitted] = exec_svc.requests
    assert submitted.side == "BUY"
    assert Decimal(str(submitted.amount)) == Decimal("5.33")


@pytest.mark.asyncio
async def test_buy_reservation_uses_follower_share_count() -> None:
    """The reservation is share-shaped: gated pUSD / price, not leader shares."""
    exec_svc = _RecordingExecutionService()
    coordinator = _coordinator(exec_svc)
    reservations = coordinator.reservation_service

    await _submit_copy_through_coordinator(
        _context(coordinator),
        _exec_req(side="BUY", amount=Decimal("5.33")),
        _signal(side="BUY", price=0.45, usdc_size=102.65, size=228.11),
        leader_id=LEADER,
    )

    [order_intent] = reservations.intents
    assert order_intent.price == Decimal("0.45")
    assert order_intent.size == Decimal("5.33") / Decimal("0.45")


@pytest.mark.asyncio
async def test_sell_still_submits_share_amount() -> None:
    """SELL amount stays share-denominated end to end."""
    exec_svc = _RecordingExecutionService()
    coordinator = _coordinator(exec_svc)

    await _submit_copy_through_coordinator(
        _context(coordinator),
        _exec_req(side="SELL", amount=Decimal("12.5")),
        _signal(side="SELL", price=0.55, usdc_size=102.65, size=186.6),
        leader_id=LEADER,
    )

    [submitted] = exec_svc.requests
    assert submitted.side == "SELL"
    assert Decimal(str(submitted.amount)) == Decimal("12.5")
