"""SDK-primary hybrid gateway with read-only CCXT shadow comparisons."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

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

MismatchSink = Callable[["ShadowMismatch"], Any]


@dataclass(frozen=True, slots=True)
class ShadowMismatch:
    operation: str
    key: str
    primary: Any
    shadow: Any
    error: str | None
    observed_at: datetime


class ShadowMismatchRecorder:
    """In-memory recorder useful for tests, metrics bridges, and paper soaks."""

    def __init__(self) -> None:
        self.items: list[ShadowMismatch] = []

    def __call__(self, mismatch: ShadowMismatch) -> None:
        self.items.append(mismatch)


def _normalize(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def _comparison_shape(operation: str, value: Any) -> Any:
    if operation == "get_orderbook" and isinstance(value, OrderBook):
        return _normalize(
            {
                "token_id": value.token_id,
                "tick_size": value.tick_size,
                "min_order_size": value.min_order_size,
                "neg_risk": value.neg_risk,
                "bids": value.bids,
                "asks": value.asks,
                "best_bid": value.best_bid,
                "best_ask": value.best_ask,
                "spread_bps": value.spread_bps,
            }
        )
    if operation == "get_order" and isinstance(value, VenueOrderState):
        return _normalize(
            {
                "order_id": value.order_id,
                "status": value.status,
                "side": value.side,
                "price": value.price,
                "size": value.size,
                "filled_size": value.filled_size,
                "avg_fill_price": value.avg_fill_price,
            }
        )
    if operation == "list_open_orders":
        rows = [_comparison_shape("get_order", row) for row in value]
        return sorted(rows, key=lambda row: str(row.get("order_id")))
    if operation == "get_fills":
        rows = [
            _normalize(
                {
                    "fill_id": row.fill_id,
                    "order_id": row.order_id,
                    "token_id": row.token_id,
                    "side": row.side,
                    "size": row.size,
                    "price": row.price,
                    "fee": row.fee,
                    "status": row.status,
                    "transaction_hash": row.transaction_hash,
                    "trader_side": row.trader_side,
                    "bucket_index": row.bucket_index,
                }
            )
            for row in value
        ]
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("fill_id")),
                str(row.get("order_id")),
                str(row.get("bucket_index")),
            ),
        )
    return _normalize(value)


async def _await_maybe(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class HybridClobGateway:
    """Return SDK-primary results and compare read operations in the background.

    Submit and cancel methods are *never* mirrored.  Therefore enabling shadow
    mode cannot duplicate, cancel, or otherwise mutate a venue order.
    """

    def __init__(
        self,
        primary: ClobGateway,
        shadow: ClobGateway,
        *,
        mismatch_sink: MismatchSink | None = None,
        max_pending_comparisons: int = 64,
    ) -> None:
        if max_pending_comparisons < 1:
            raise ValueError("max_pending_comparisons must be positive")
        self.primary = primary
        self.shadow = shadow
        self._sink = mismatch_sink
        self._max_pending = max_pending_comparisons
        self._tasks: set[asyncio.Task[Any]] = set()
        self.dropped_comparisons = 0

    def _record(self, mismatch: ShadowMismatch) -> None:
        if self._sink is not None:
            try:
                self._sink(mismatch)
            except Exception as exc:
                logger.warning(
                    "shadow_mismatch_sink_failed | error=%s",
                    redact_error_detail(exc),
                )
        # Mismatch payloads stay in the in-process sink. Only the operation and
        # a redacted error category reach the log stream, so a CCXT exception
        # body can never carry order payloads or key material there.
        logger.warning(
            "Polymarket shadow mismatch operation=%s key=%s error=%s",
            mismatch.operation,
            mismatch.key,
            redact_error_detail(mismatch.error) if mismatch.error else "value_mismatch",
        )

    async def _compare(
        self,
        operation: str,
        key: str,
        primary_value: Any,
        shadow_call: Awaitable[Any],
    ) -> None:
        try:
            shadow_value = await shadow_call
            primary_shape = _comparison_shape(operation, primary_value)
            shadow_shape = _comparison_shape(operation, shadow_value)
            if primary_shape != shadow_shape:
                self._record(
                    ShadowMismatch(
                        operation=operation,
                        key=key,
                        primary=primary_shape,
                        shadow=shadow_shape,
                        error=None,
                        observed_at=datetime.now(timezone.utc),
                    )
                )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._record(
                ShadowMismatch(
                    operation=operation,
                    key=key,
                    primary=_comparison_shape(operation, primary_value),
                    shadow=None,
                    error=f"{type(exc).__name__}: {exc}",
                    observed_at=datetime.now(timezone.utc),
                )
            )

    def _schedule(
        self,
        operation: str,
        key: str,
        primary_value: Any,
        shadow_call: Awaitable[Any],
    ) -> None:
        if len(self._tasks) >= self._max_pending:
            self.dropped_comparisons += 1
            close = getattr(shadow_call, "close", None)
            if callable(close):
                close()
            return
        task = asyncio.create_task(
            self._compare(operation, key, primary_value, shadow_call),
            name=f"polymarket-shadow:{operation}:{key}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
        value = await self.primary.get_market_info(token_id)
        self._schedule(
            "get_market_info",
            token_id,
            value,
            self.shadow.get_market_info(token_id),
        )
        return value

    async def get_orderbook(self, token_id: str) -> OrderBook:
        value = await self.primary.get_orderbook(token_id)
        self._schedule("get_orderbook", token_id, value, self.shadow.get_orderbook(token_id))
        return value

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        return await self.primary.submit_marketable_order(request)

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        return await self.primary.submit_limit_order(request)

    async def get_order(self, order_id: str) -> VenueOrderState:
        value = await self.primary.get_order(order_id)
        self._schedule("get_order", order_id, value, self.shadow.get_order(order_id))
        return value

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        value = await self.primary.list_open_orders(order_id=order_id, market=market)
        key = f"{order_id or '*'}:{market or '*'}"
        self._schedule(
            "list_open_orders",
            key,
            value,
            self.shadow.list_open_orders(order_id=order_id, market=market),
        )
        return value

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        value = await self.primary.get_fills(
            market,
            trade_id=trade_id,
            order_id=order_id,
        )
        key = f"{market or '*'}:{trade_id or '*'}:{order_id or '*'}"
        self._schedule(
            "get_fills",
            key,
            value,
            self.shadow.get_fills(
                market,
                trade_id=trade_id,
                order_id=order_id,
            ),
        )
        return value

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        return await self.primary.cancel_order(order_id)

    async def check_compatibility(self) -> dict[str, Any]:
        primary, shadow = await asyncio.gather(
            self.primary.check_compatibility(),
            self.shadow.check_compatibility(),
            return_exceptions=True,
        )
        return {
            "sdk_available": not isinstance(primary, BaseException)
            and bool(primary.get("sdk_available")),
            "backend": "sdk-shadow-ccxt",
            # Compatibility reports are surfaced by startup health gating and
            # end up in logs, so probe failures carry a redacted category only.
            "primary": (
                primary
                if not isinstance(primary, BaseException)
                else {"sdk_available": False, "error": redact_error_detail(primary)}
            ),
            "shadow": (
                shadow
                if not isinstance(shadow, BaseException)
                else {"sdk_available": False, "error": redact_error_detail(shadow)}
            ),
            "pending_comparisons": len(self._tasks),
            "dropped_comparisons": self.dropped_comparisons,
        }

    async def close(self) -> None:
        await self.drain()
        seen: set[int] = set()
        for gateway in (self.primary, self.shadow):
            if id(gateway) in seen:
                continue
            seen.add(id(gateway))
            close = getattr(gateway, "close", None)
            if close is not None:
                await _await_maybe(close())


__all__ = [
    "HybridClobGateway",
    "ShadowMismatch",
    "ShadowMismatchRecorder",
]
