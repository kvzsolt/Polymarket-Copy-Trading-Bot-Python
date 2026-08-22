"""Paper-mode execution boundary: CCXT reads, local simulated writes.

Paper mode keeps live CCXT market data while routing every order write to a
local simulator. The boundary is structural — the paper gateway has no code
path that forwards a write to the CCXT transport — so missing credentials can
never be the paper safety mechanism and a configuration mistake cannot place
a live order from a paper runtime.
"""

from __future__ import annotations

import inspect
import itertools
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any

from domain.execution import ExecutionMetadata
from integrations.polymarket.errors import VenueOrderNotFoundError
from integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from interfaces.clob_gateway import ClobGateway

_SHARE_QUANTUM = Decimal("0.01")
_ORDER_IDS = itertools.count(1)
_FILL_IDS = itertools.count(1)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _positive(value: Decimal, *, field: str) -> Decimal:
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field} must be a positive finite decimal")
    return value


class PaperOrderSimulator:
    """Deterministic local order and fill simulator.

    Marketable orders fill immediately and completely at the caller's price
    limit; limit orders rest until canceled. Fills use exact ``Decimal``
    arithmetic and BUY cost semantics (size is USDC spend, shares are derived
    from the limit price).
    """

    def __init__(self, *, clock: Callable[[], datetime] = _utc_now) -> None:
        self._clock = clock
        self._orders: dict[str, VenueOrderState] = {}
        self._fills: list[VenueFill] = []

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        side = str(request.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported order side: {request.side!r}")
        price_limit = _positive(Decimal(str(request.price_limit)), field="price_limit")
        size = _positive(Decimal(str(request.size)), field="size")

        if side == "BUY":
            # BUY size is USDC spend; shares received are bounded by the cap.
            shares = (size / price_limit).quantize(_SHARE_QUANTUM, rounding=ROUND_DOWN)
            _positive(shares, field="derived share size")
        else:
            shares = size

        now = self._clock()
        order_id = f"paper-{next(_ORDER_IDS):06d}"
        fill_id = f"paper-fill-{next(_FILL_IDS):06d}"
        state = VenueOrderState(
            order_id=order_id,
            status="closed",
            side=side,
            price=price_limit,
            size=shares,
            filled_size=shares,
            avg_fill_price=price_limit,
            created_at=now,
            raw={"paper": True},
        )
        self._orders[order_id] = state
        self._fills.append(
            VenueFill(
                fill_id=fill_id,
                order_id=order_id,
                token_id=str(request.token_id),
                side=side,
                size=shares,
                price=price_limit,
                fee=Decimal("0"),
                timestamp=int(now.timestamp()),
                status="MATCHED",
                trader_side="TAKER",
                raw={"paper": True},
            )
        )
        # ExecutionService reads matched size and average price from the raw
        # map; without them a fully simulated FAK fill reads as a zero fill.
        return VenueSubmitResult(
            success=True,
            order_id=order_id,
            status="closed",
            raw={
                "paper": True,
                "status": "matched",
                "original_size": format(shares, "f"),
                "size_matched": format(shares, "f"),
                "avg_price": format(price_limit, "f"),
            },
        )

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        side = str(request.side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported order side: {request.side!r}")
        price = _positive(Decimal(str(request.price)), field="price")
        size = _positive(Decimal(str(request.size)), field="size")

        order_id = f"paper-{next(_ORDER_IDS):06d}"
        self._orders[order_id] = VenueOrderState(
            order_id=order_id,
            status="open",
            side=side,
            price=price,
            size=size,
            filled_size=Decimal("0"),
            avg_fill_price=Decimal("0"),
            created_at=self._clock(),
            raw={"paper": True},
        )
        # A resting limit order has legitimately matched nothing yet.
        return VenueSubmitResult(
            success=True,
            order_id=order_id,
            status="open",
            raw={
                "paper": True,
                "status": "live",
                "original_size": format(size, "f"),
                "size_matched": "0",
                "avg_price": "0",
            },
        )

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Return the simulated state, or the gateway-contract not-found error.

        ``VenueOrderNotFoundError`` is what reconciliation classifies as "venue
        affirmatively knows nothing about this id"; any other exception is a
        fail-closed probe error, so a bare ``KeyError`` would block release.
        """
        state = self._orders.get(order_id)
        if state is None:
            raise VenueOrderNotFoundError(f"paper order not found: {order_id}")
        return state

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        del market  # The simulator does not model per-market scoping.
        result = [
            state
            for state in self._orders.values()
            if state.status == "open" and (order_id is None or state.order_id == order_id)
        ]
        return sorted(result, key=lambda state: state.order_id)

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        del market
        return [
            fill
            for fill in self._fills
            if (trade_id is None or fill.fill_id == trade_id)
            and (order_id is None or fill.order_id == order_id)
        ]

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        state = self._orders.get(order_id)
        if state is None or state.status != "open":
            # Mirror the venue adapter's cancel-404 / cancel-after-fill
            # idempotency: anything not resting is already gone.
            return VenueCancelResult(
                success=True,
                order_id=order_id,
                status="already_gone",
            )
        self._orders[order_id] = VenueOrderState(
            order_id=state.order_id,
            status="canceled",
            side=state.side,
            price=state.price,
            size=state.size,
            filled_size=state.filled_size,
            avg_fill_price=state.avg_fill_price,
            created_at=state.created_at,
            raw=state.raw,
        )
        return VenueCancelResult(success=True, order_id=order_id, status="canceled")


class PaperExecutionGateway:
    """Route reads to the CCXT market gateway and writes to the simulator."""

    def __init__(
        self,
        *,
        market_data_gateway: ClobGateway,
        simulator: PaperOrderSimulator,
    ) -> None:
        self._market_data_gateway = market_data_gateway
        self._simulator = simulator

    @property
    def market_data_gateway(self) -> ClobGateway:
        """Return the CCXT-backed read-only market gateway."""

        return self._market_data_gateway

    @property
    def simulator(self) -> PaperOrderSimulator:
        """Return the local write simulator."""

        return self._simulator

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
        return await self._market_data_gateway.get_market_info(token_id)

    async def get_orderbook(self, token_id: str) -> OrderBook:
        return await self._market_data_gateway.get_orderbook(token_id)

    async def get_execution_metadata(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
    ) -> ExecutionMetadata:
        """Return live venue metadata while keeping paper writes local."""

        return await self._market_data_gateway.get_execution_metadata(
            token_id,
            condition_id=condition_id,
        )

    async def get_price(self, token_id: str, side: str) -> dict[str, str] | object:
        getter = getattr(self._market_data_gateway, "get_price", None)
        if getter is None:
            raise AttributeError("paper market-data gateway must provide get_price")
        return await getter(token_id, side)

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        return await self._simulator.submit_marketable_order(request)

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        return await self._simulator.submit_limit_order(request)

    async def get_order(self, order_id: str) -> VenueOrderState:
        return await self._simulator.get_order(order_id)

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        return await self._simulator.list_open_orders(order_id=order_id, market=market)

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        return await self._simulator.get_fills(
            market,
            trade_id=trade_id,
            order_id=order_id,
        )

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        return await self._simulator.cancel_order(order_id)

    async def check_compatibility(self) -> dict[str, Any]:
        report = await self._market_data_gateway.check_compatibility()
        # Paper mode is never live-execution-ready regardless of the read
        # transport's capabilities.
        return {**report, "authenticated": False, "live_execution_ready": False}

    async def close(self) -> None:
        close = getattr(self._market_data_gateway, "close", None)
        if close is not None and callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result


__all__ = [
    "PaperExecutionGateway",
    "PaperOrderSimulator",
]
