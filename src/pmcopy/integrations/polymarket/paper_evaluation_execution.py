"""Depth-aware automatic evaluations behind the structural paper write boundary."""

import hashlib
from datetime import datetime
from decimal import Decimal

from pmcopy.domain.paper_evaluation import PaperBook, PaperFill, PaperSimulation
from pmcopy.integrations.polymarket.models import (
    MarketableOrderRequest,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from pmcopy.integrations.polymarket.paper_execution import (
    PaperExecutionGateway,
    PaperOrderSimulator,
)
from pmcopy.interfaces.clob_gateway import ClobGateway


class DepthPaperOrderSimulator(PaperOrderSimulator):
    """Extend the local simulator with one captured depth snapshot and stable identity."""

    def __init__(self, *, book: PaperBook, event: str, now: datetime) -> None:
        """Inject the entire deterministic evaluation input."""
        super().__init__(clock=lambda: now)
        self._book = book
        self._event = event
        self._now = now
        self.result = PaperFill()

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        """Simulate a bounded FAK fill; no network method exists on this object."""
        if request.price_limit is None:
            raise ValueError("paper evaluation requires an explicit price limit")
        fill = PaperSimulation().fill(
            self._book, side=request.side, size=request.size, limit=request.price_limit
        )
        self.result = fill
        order_id = "paper-eval-" + hashlib.sha256(self._event.encode()).hexdigest()
        average = fill.notional / fill.shares if fill.shares else Decimal(0)
        raw = {"paper": True, "size_matched": str(fill.shares), "avg_price": str(average)}
        self._orders[order_id] = VenueOrderState(
            order_id=order_id,
            status="closed",
            side=request.side,
            price=request.price_limit,
            size=fill.shares,
            filled_size=fill.shares,
            avg_fill_price=average,
            created_at=self._now,
            raw=raw,
        )
        if fill.shares:
            self._fills.append(
                VenueFill(
                    fill_id=order_id + ":fill",
                    order_id=order_id,
                    token_id=request.token_id,
                    side=request.side,
                    size=fill.shares,
                    price=average,
                    fee=fill.fee,
                    timestamp=int(self._now.timestamp()),
                    status="CONFIRMED",
                    trader_side="TAKER",
                    raw=raw,
                )
            )
        return VenueSubmitResult(success=True, order_id=order_id, status="closed", raw=raw)


class PaperEvaluationExecution:
    """Compose PaperExecutionGateway for each independently persisted evaluation."""

    def __init__(self, market_gateway: ClobGateway) -> None:
        """Retain the existing CCXT market reader behind the paper boundary."""
        self._market_gateway = market_gateway

    async def fill(
        self,
        book: PaperBook,
        *,
        token_id: str,
        side: str,
        size: Decimal,
        limit: Decimal,
        event: str,
        now: datetime,
    ) -> PaperFill:
        """Execute only local simulated writes and return exact accounting evidence."""
        simulator = DepthPaperOrderSimulator(book=book, event=event, now=now)
        gateway = PaperExecutionGateway(
            market_data_gateway=self._market_gateway, simulator=simulator
        )
        await gateway.submit_marketable_order(
            MarketableOrderRequest(token_id=token_id, side=side, size=size, price_limit=limit)
        )
        return simulator.result
