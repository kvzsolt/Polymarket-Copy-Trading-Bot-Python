"""Canonical Polymarket venue gateway protocol.

`ExecutionService`, the execution coordinator, the reconciler, and the risk
monitor must depend on this Protocol instead of a concrete transport adapter.
Vendor transport details live behind the CCXT adapter in
``integrations/polymarket/ccxt_prediction_gateway.py`` (and the paper
simulator gateway) and never leak past this boundary.

This module is the single source of truth for the gateway shape; previously
each call site declared its own duplicate ``ClobGateway`` Protocol (see
``services/execution/coordinator.py`` and
``services/execution/reconciliation_service.py`` prior to issue #66). Those
duplicates are removed.
"""

from __future__ import annotations

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
class ClobGateway(Protocol):
    """Single venue-facing surface used by all production trading code.

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

    async def get_market_info(self, token_id: str) -> dict[str, Any]: ...

    async def get_orderbook(self, token_id: str) -> OrderBook: ...

    async def get_execution_metadata(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
    ) -> ExecutionMetadata: ...

    async def submit_marketable_order(
        self, request: MarketableOrderRequest
    ) -> VenueSubmitResult: ...

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult: ...

    async def get_order(self, order_id: str) -> VenueOrderState: ...

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

    async def cancel_order(self, order_id: str) -> VenueCancelResult: ...

    async def check_compatibility(self) -> dict[str, Any]:
        """Probe venue transport readiness; used by startup health gating.

        Implementations must not raise from this method — they should always
        return a dict with at minimum ``available: bool`` (plus
        ``authenticated`` / ``live_execution_ready`` for the CCXT transport).
        The orchestrator decides whether to fail closed (live mode) or warn
        (paper mode).
        """
        ...


__all__ = ["ClobGateway"]
