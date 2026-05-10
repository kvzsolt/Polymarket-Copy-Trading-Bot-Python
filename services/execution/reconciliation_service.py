from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional, Protocol

from domain.order_lifecycle import OrderLifecycleStatus
from domain.position_key import PositionKey

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReconciliationResult:
    order_id: str
    is_resolved: bool
    status: OrderLifecycleStatus
    error: str = ""

    @classmethod
    def resolved(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=True, status=status)

    @classmethod
    def pending(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status)

    @classmethod
    def unresolved(cls, order_id: str, status: OrderLifecycleStatus, error: str = "") -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status, error=error)

class OrderRecord(Protocol):
    id: str
    venue_order_id: str | None
    reservation_id: str
    status: str
    market_id: str | None

class OrdersRepo(Protocol):
    async def get(self, order_id: str) -> OrderRecord: ...
    async def mark_submitted_unconfirmed(self, order_id: str) -> None: ...
    async def mark_terminal(self, order_id: str, status: str) -> None: ...
    async def mark_reconciliation_required(self, order_id: str) -> None: ...
    async def mark_filled_or_partially_filled(self, order_id: str) -> None: ...

class FillsRepo(Protocol):
    async def upsert_venue_fill(self, order: OrderRecord, venue_fill: Any) -> Any: ...

class PositionsRepo(Protocol):
    def upsert_position(self, fill: Any) -> None: ...

class ReservationService(Protocol):
    def release_terminal(self, reservation_id: str, reason: str) -> None: ...

# NOTE: The canonical CLOB venue gateway Protocol lives in
# ``interfaces.clob_gateway.ClobGateway``. ``OrderQueryGateway`` is the
# query-only subset used by the reconciler; satisfied by the canonical
# gateway directly (``get_order`` and ``get_fills`` signatures match).
# See issue #66 for context.
class OrderQueryGateway(Protocol):
    async def get_order(self, venue_order_id: str) -> Any: ...
    async def get_fills(self, market: str | None = None) -> list[Any]: ...


# Backward-compatible alias for any out-of-tree imports that referenced the
# previous local name. New code should import ``OrderQueryGateway`` directly
# or the canonical ``interfaces.clob_gateway.ClobGateway``.
ClobGateway = OrderQueryGateway

class DbTransaction(Protocol):
    async def __aenter__(self) -> None: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...

class DbProvider(Protocol):
    def transaction(self) -> DbTransaction: ...


# Issue #67: structural Protocol for the per-position coordinator so this
# module does not import the heavier ``services.positionCoordinator``
# composition layer (would create a circular import). Any object exposing an
# ``acquire(scope)`` async context manager satisfies this.
class _PositionCoordinatorLike(Protocol):
    def acquire(self, scope: PositionKey) -> Any: ...


class ReconciliationService:
    def __init__(
        self,
        orders_repo: OrdersRepo,
        fills_repo: FillsRepo,
        positions_repo: PositionsRepo,
        reservation_service: ReservationService,
        clob_gateway: OrderQueryGateway,
        db: DbProvider,
        audit_logger: Any,
        *,
        position_coordinator: Optional[_PositionCoordinatorLike] = None,
    ):
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.reservation_service = reservation_service
        self.clob_gateway = clob_gateway
        self.db = db
        self.audit_logger = audit_logger
        self.position_coordinator = position_coordinator

    @staticmethod
    def _try_build_key(order: OrderRecord) -> Optional[PositionKey]:
        """Best-effort PositionKey from an order record.

        Returns None when the record lacks the canonical-position fields
        (legacy fixtures, partial-state records). Callers must degrade
        gracefully — the per-scope lock is defense-in-depth; the reconciler
        remains the sole writer of terminal state regardless.
        """
        user_id = getattr(order, "user_id", None)
        condition_id = getattr(order, "condition_id", None)
        token_id = (
            getattr(order, "asset", None)
            or getattr(order, "token_id", None)
        )
        outcome = getattr(order, "outcome", None)
        if not user_id or not condition_id or not token_id:
            return None
        try:
            return PositionKey(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                outcome=outcome,
            )
        except ValueError:
            return None

    @asynccontextmanager
    async def _scope_lock(self, key: Optional[PositionKey]) -> AsyncIterator[None]:
        """Acquire the per-scope coordinator lock when both coordinator and key are present."""
        if self.position_coordinator is None or key is None:
            yield
            return
        async with self.position_coordinator.acquire(key):
            logger.debug(
                "reconcile.scope_lock_acquired",
                extra=key.log_fields(),
            )
            yield

    async def reconcile_order(self, order_id: str) -> ReconciliationResult:
        order = await self.orders_repo.get(order_id)
        
        # If no venue_order_id, we can't query the CLOB. We must check if it was submitted at all.
        if not order.venue_order_id:
            await self.orders_repo.mark_reconciliation_required(order.id)
            return ReconciliationResult.unresolved(order.id, OrderLifecycleStatus.RECONCILIATION_REQUIRED, "no_venue_order_id")

        try:
            venue_order = await self.clob_gateway.get_order(order.venue_order_id)
            venue_fills = await self.clob_gateway.get_fills(market=order.market_id)
        except Exception as exc:
            # Venue unreachable -> keep safe reserved state
            self.audit_logger.log_event("reconciliation_unreachable", {"order_id": order.id, "error": str(exc)})
            return ReconciliationResult.unresolved(order.id, OrderLifecycleStatus.UNKNOWN_UNCONFIRMED, str(exc))

        # Per-position coordinator gate (issue #67). Held across the full
        # mutation block so a concurrent risk-monitor SELL or copy-trade BUY
        # on the same scoped position cannot interleave with terminal state
        # apply / reservation release.
        scope_key = self._try_build_key(order)

        if venue_fills:
            async with self._scope_lock(scope_key):
                async with self.db.transaction():
                    for venue_fill in venue_fills:
                        fill = await self.fills_repo.upsert_venue_fill(order, venue_fill)
                        self.positions_repo.upsert_position(fill)

                    await self.orders_repo.mark_filled_or_partially_filled(order.id)
                    self.reservation_service.release_terminal(
                        order.reservation_id,
                        reason="venue_fill_confirmed",
                    )
                self.audit_logger.log_event(
                    "reconciliation_resolved_filled",
                    {"order_id": order.id, **(scope_key.log_fields() if scope_key else {})},
                )
            return ReconciliationResult.resolved(order.id, OrderLifecycleStatus.FILLED)

        if venue_order and venue_order.status.lower() in {"live", "open", "pending"}:
            await self.orders_repo.mark_submitted_unconfirmed(order.id)
            return ReconciliationResult.pending(order.id, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED)

        if venue_order and venue_order.status.lower() in {"cancelled", "expired", "rejected"}:
            async with self._scope_lock(scope_key):
                async with self.db.transaction():
                    await self.orders_repo.mark_terminal(order.id, venue_order.status.lower())
                    self.reservation_service.release_terminal(
                        order.reservation_id,
                        reason=venue_order.status.lower(),
                    )
                self.audit_logger.log_event(
                    "reconciliation_resolved_terminal",
                    {
                        "order_id": order.id,
                        "status": venue_order.status.lower(),
                        **(scope_key.log_fields() if scope_key else {}),
                    },
                )
            try:
                status = OrderLifecycleStatus(venue_order.status.lower())
            except ValueError:
                status = OrderLifecycleStatus.TERMINAL_FAILED
            return ReconciliationResult.resolved(order.id, status)

        # Unrecognized status or no fills but matched status
        await self.orders_repo.mark_reconciliation_required(order.id)
        return ReconciliationResult.unresolved(order.id, OrderLifecycleStatus.RECONCILIATION_REQUIRED, f"unknown_venue_status:{venue_order.status if venue_order else 'None'}")