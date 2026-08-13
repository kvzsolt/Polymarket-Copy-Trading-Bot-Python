"""Official-SDK primary gateway with read-only CCXT shadow comparisons."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from inspect import isawaitable
from collections.abc import Awaitable
from typing import Protocol

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
from utils.logger import get_logger, redact_error_detail

logger = get_logger("HybridPolymarketGateway")


class ShadowClock(Protocol):
    """Provide a timestamp for a shadow observation."""

    def now(self) -> datetime:
        """Return a timezone-aware current instant."""

        ...


class SystemShadowClock:
    """Production UTC clock for shadow comparison observations."""

    def now(self) -> datetime:
        """Return the current timezone-aware UTC instant."""

        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ShadowMismatch:
    """One redacted SDK-primary versus CCXT-shadow disagreement."""

    operation: str
    key: str
    primary: object
    shadow: object
    error: str | None
    observed_at: datetime


class MismatchSink(Protocol):
    """Consume structured shadow mismatches."""

    def record(self, mismatch: ShadowMismatch) -> None:
        """Record one comparison mismatch."""

        ...


class ShadowMismatchRecorder:
    """In-memory mismatch recorder for tests and paper-mode telemetry."""

    def __init__(self) -> None:
        """Initialize an empty observation list."""

        self.items: list[ShadowMismatch] = []

    def record(self, mismatch: ShadowMismatch) -> None:
        """Append one structured mismatch."""

        self.items.append(mismatch)


class HybridClobGateway:
    """Return SDK results and compare only read operations against CCXT."""

    def __init__(
        self,
        primary: ClobGateway,
        shadow: ClobGateway,
        *,
        mismatch_sink: MismatchSink | None = None,
        clock: ShadowClock | None = None,
        max_pending_comparisons: int = 64,
    ) -> None:
        """Initialize an SDK-primary, read-only-shadow composition.

        Args:
            primary: Official SDK gateway used for every returned value and write.
            shadow: CCXT gateway used only for read comparison.
            mismatch_sink: Optional structured mismatch consumer.
            clock: Injectable observation clock.
            max_pending_comparisons: Bound on background read comparisons.

        Raises:
            ValueError: If the comparison bound is not positive.
        """

        if max_pending_comparisons < 1:
            raise ValueError("max_pending_comparisons must be positive")
        self._primary = primary
        self._shadow = shadow
        self._sink = mismatch_sink
        self._clock = clock or SystemShadowClock()
        self._max_pending = max_pending_comparisons
        self._tasks: set[asyncio.Task[None]] = set()
        self.dropped_comparisons = 0

    @property
    def primary(self) -> ClobGateway:
        """Return the authoritative SDK gateway."""

        return self._primary

    @property
    def shadow(self) -> ClobGateway:
        """Return the read-only comparison gateway."""

        return self._shadow

    async def get_market_info(self, token_id: str) -> dict[str, object]:
        """Return SDK market info and schedule a shadow read comparison."""

        value = await self._primary.get_market_info(token_id)
        self._schedule("get_market_info", token_id, value, self._shadow.get_market_info(token_id))
        return value

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Return SDK orderbook and schedule a shadow read comparison."""

        value = await self._primary.get_orderbook(token_id)
        self._schedule("get_orderbook", token_id, value, self._shadow.get_orderbook(token_id))
        return value

    async def submit_marketable_order(
        self,
        request: MarketableOrderRequest,
    ) -> VenueSubmitResult:
        """Submit once through the SDK primary; never call the shadow."""

        return await self._primary.submit_marketable_order(request)

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        """Submit once through the SDK primary; never call the shadow."""

        return await self._primary.submit_limit_order(request)

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Return SDK order state and schedule a shadow read comparison."""

        value = await self._primary.get_order(order_id)
        self._schedule("get_order", order_id, value, self._shadow.get_order(order_id))
        return value

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        """Return SDK open orders and schedule a shadow read comparison."""

        value = await self._primary.list_open_orders(order_id=order_id, market=market)
        key = f"{order_id or '*'}:{market or '*'}"
        self._schedule(
            "list_open_orders",
            key,
            value,
            self._shadow.list_open_orders(order_id=order_id, market=market),
        )
        return value

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        """Return SDK fills and schedule a shadow read comparison."""

        value = await self._primary.get_fills(
            market,
            trade_id=trade_id,
            order_id=order_id,
        )
        key = f"{market or '*'}:{trade_id or '*'}:{order_id or '*'}"
        self._schedule(
            "get_fills",
            key,
            value,
            self._shadow.get_fills(market, trade_id=trade_id, order_id=order_id),
        )
        return value

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Cancel only through the SDK primary; never call the shadow."""

        return await self._primary.cancel_order(order_id)

    async def check_compatibility(self) -> dict[str, object]:
        """Return primary compatibility and schedule a shadow probe."""

        value = await self._primary.check_compatibility()
        self._schedule(
            "check_compatibility",
            "runtime",
            value,
            self._shadow.check_compatibility(),
        )
        return value

    async def close(self) -> None:
        """Drain comparisons and close each owned gateway at most once."""

        await self.drain()
        seen: set[int] = set()
        for gateway in (self._primary, self._shadow):
            if id(gateway) in seen:
                continue
            seen.add(id(gateway))
            close = getattr(gateway, "close", None)
            if callable(close):
                result = close()
                if isawaitable(result):
                    await result

    async def drain(self, timeout_seconds: float = 5.0) -> None:
        """Wait briefly for pending comparisons and cancel stragglers.

        Args:
            timeout_seconds: Maximum monotonic duration to await comparisons.

        Raises:
            ValueError: If ``timeout_seconds`` is negative.
        """

        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        tasks = tuple(self._tasks)
        if not tasks:
            return
        done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
        if pending:
            self.dropped_comparisons += len(pending)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if done:
            await asyncio.gather(*done, return_exceptions=True)

    def _schedule(
        self,
        operation: str,
        key: str,
        primary_value: object,
        shadow_call: Awaitable[object],
    ) -> None:
        if len(self._tasks) >= self._max_pending:
            self.dropped_comparisons += 1
            close = getattr(shadow_call, "close", None)
            if callable(close):
                close()
            return
        task = asyncio.create_task(
            self._compare(operation, key, primary_value, shadow_call),
            name=f"polymarket-shadow:{operation}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _compare(
        self,
        operation: str,
        key: str,
        primary_value: object,
        shadow_call: Awaitable[object],
    ) -> None:
        try:
            shadow_value = await shadow_call
            primary_shape = self._normalize(primary_value)
            shadow_shape = self._normalize(shadow_value)
            if primary_shape != shadow_shape:
                self._record(operation, key, primary_shape, shadow_shape, None)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._record(
                operation,
                key,
                self._normalize(primary_value),
                None,
                redact_error_detail(exc),
            )

    def _record(
        self,
        operation: str,
        key: str,
        primary: object,
        shadow: object,
        error_category: str | None,
    ) -> None:
        mismatch = ShadowMismatch(
            operation=operation,
            key=key,
            primary=primary,
            shadow=shadow,
            error=error_category,
            observed_at=self._clock.now(),
        )
        if self._sink is not None:
            self._sink.record(mismatch)
        logger.warning(
            "polymarket_shadow_mismatch | operation=%s key=%s error=%s",
            operation,
            key,
            error_category or "value_mismatch",
        )

    @classmethod
    def _normalize(cls, value: object) -> object:
        if isinstance(value, Decimal):
            return str(value.normalize())
        if isinstance(value, datetime):
            return value.astimezone(UTC).isoformat()
        if isinstance(value, dict):
            return {
                str(key): cls._normalize(item)
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        if isinstance(value, (list, tuple)):
            return tuple(cls._normalize(item) for item in value)
        return value


__all__ = [
    "HybridClobGateway",
    "MismatchSink",
    "ShadowMismatch",
    "ShadowMismatchRecorder",
]
