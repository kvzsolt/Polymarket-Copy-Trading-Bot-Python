"""Canonical Polymarket venue gateway protocol and narrow role sub-protocols.

``ClobGateway`` remains the single canonical venue-facing Protocol used by full
runtime composition and adapter implementations (``CcxtPredictionGateway`` and
``PaperExecutionGateway``).  Role-segregated sub-protocols allow read-only,
query-only, or control-only consumers to depend strictly on the methods they
actually invoke without maintaining duplicate ``Protocol`` declarations:

* :class:`MarketDataGateway` — public book, market info, and execution metadata;
* :class:`PriceQuoteGateway` — directional best-price quote read;
* :class:`PrivateHistoryGateway` — authenticated fill and open-order reads;
* :class:`OrderQueryGateway` — extends :class:`PrivateHistoryGateway` with
  single-order state lookup;
* :class:`OrderControlGateway` — open-order listing and idempotent cancellation;
* :class:`OrderSubmissionGateway` — marketable and limit order submission;
* :class:`VenueCompatibilityProbe` — non-raising startup readiness probe.

Vendor transport details never leak past these protocols.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)


@runtime_checkable
class MarketDataGateway(Protocol):
    """Read-only public market metadata and orderbook access."""

    async def get_market_info(self, token_id: str) -> dict[str, Any]: ...

    async def get_orderbook(self, token_id: str) -> OrderBook: ...

    async def get_execution_metadata(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
    ) -> ExecutionMetadata: ...


@runtime_checkable
class PriceQuoteGateway(Protocol):
    """Directional quote price read used by protective risk monitoring."""

    async def get_price(self, token_id: str, side: str) -> Decimal | None: ...


@runtime_checkable
class OrderSubmissionGateway(Protocol):
    """Authenticated (live) or simulated (paper) order submission."""

    async def submit_marketable_order(
        self, request: MarketableOrderRequest
    ) -> VenueSubmitResult: ...

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult: ...


@runtime_checkable
class PrivateHistoryGateway(Protocol):
    """Authenticated read-only fill and open-order history for recovery."""

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> Sequence[VenueOrderState]: ...

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> Sequence[VenueFill]: ...


@runtime_checkable
class OrderQueryGateway(PrivateHistoryGateway, Protocol):
    """Authenticated read-only single-order and history queries for reconciliation."""

    async def get_order(self, order_id: str) -> VenueOrderState: ...


@runtime_checkable
class OrderControlGateway(Protocol):
    """Read and idempotently cancel resting venue orders during full-stop control."""

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]: ...

    async def cancel_order(self, order_id: str) -> VenueCancelResult: ...


@runtime_checkable
class VenueCompatibilityProbe(Protocol):
    """Non-raising venue readiness diagnostic surface."""

    async def check_compatibility(self) -> dict[str, Any]:
        """Probe venue transport readiness; used by startup health gating.

        Implementations must not raise from this method — they should always
        return a dict with at minimum ``available: bool`` (plus
        ``authenticated`` / ``live_execution_ready`` for the CCXT transport).
        """
        ...


@runtime_checkable
class ClobGateway(
    MarketDataGateway,
    OrderSubmissionGateway,
    OrderQueryGateway,
    OrderControlGateway,
    VenueCompatibilityProbe,
    Protocol,
):
    """Single venue-facing surface used by full runtime trading composition.

    All methods are coroutine functions; failures raise the structured
    ``integrations.polymarket.errors`` hierarchy
    (``VenueRateLimitError`` / ``VenueUnavailableError`` are retryable;
    ``VenueAuthError`` / ``VenueInvalidOrderError`` are non-retryable).
    Successful submission may still carry
    ``VenueSubmitResult.ambiguous_post_failure=True`` when the venue
    acknowledged the POST timed out after the order was sent — callers must
    persist a ``submitted_unconfirmed`` record and let reconciliation resolve
    the terminal state.
    """

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]: ...

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]: ...


__all__ = [
    "ClobGateway",
    "MarketDataGateway",
    "OrderControlGateway",
    "OrderQueryGateway",
    "OrderSubmissionGateway",
    "PriceQuoteGateway",
    "PrivateHistoryGateway",
    "VenueCompatibilityProbe",
]
