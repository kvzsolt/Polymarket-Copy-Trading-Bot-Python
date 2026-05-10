from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Protocol

from domain.order_lifecycle import OrderIntent, OrderLifecycleStatus, classify_venue_status
from integrations.polymarket.errors import (
    RetryableVenueError,
    TerminalVenueError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from services.execution.reservation_service import ReservationService, Clock, DefaultClock
from services.execution.state_machine import assert_transition_allowed

logger = logging.getLogger(__name__)

_INSUFFICIENT_BALANCE_PATTERNS = (
    "insufficient_balance",
    "insufficient balance",
    "not enough balance",
    "allowance",
)


def _is_insufficient_balance_error(msg: str) -> bool:
    lower = msg.lower()
    return any(p in lower for p in _INSUFFICIENT_BALANCE_PATTERNS)


@dataclass(frozen=True)
class VenueSubmitResult:
    order_id: str
    status: str
    error_msg: str = ""


@dataclass(frozen=True)
class ExecutionResult:
    order_id: str
    status: OrderLifecycleStatus
    error: str = ""

    @classmethod
    def retryable_failed(cls, order_id: str, error: str) -> ExecutionResult:
        return cls(order_id=order_id, status=OrderLifecycleStatus.RETRYABLE_FAILED, error=error)

    @classmethod
    def filled(cls, order_id: str) -> ExecutionResult:
        return cls(order_id=order_id, status=OrderLifecycleStatus.FILLED)

    @classmethod
    def unconfirmed(cls, order_id: str, status: OrderLifecycleStatus) -> ExecutionResult:
        return cls(order_id=order_id, status=status)


# NOTE: The canonical CLOB venue gateway Protocol lives in
# ``interfaces.clob_gateway.ClobGateway`` and exposes venue-shaped methods
# (``submit_marketable_order(MarketableOrderRequest)``, ``get_order``,
# ``get_fills``, etc.). ``OrderSubmitter`` below is a higher-level,
# intent-shaped adapter satisfied by the execution-coordinator's binding glue;
# do not confuse the two. See issue #66 for context.
class OrderSubmitter(Protocol):
    async def submit_order(self, intent: OrderIntent) -> VenueSubmitResult: ...


# Backward-compatible alias for any out-of-tree imports that referenced the
# previous local name. New code should import ``OrderSubmitter`` directly or
# the canonical ``interfaces.clob_gateway.ClobGateway``.
ClobGateway = OrderSubmitter


class Reconciler(Protocol):
    async def enqueue(self, order_id: str) -> None: ...


class AuditLogger(Protocol):
    def log_event(self, event_name: str, payload: dict[str, Any]) -> None: ...


class ExecutionCoordinator:
    def __init__(
        self,
        reservation_service: ReservationService,
        orders_repo: Any,
        fills_repo: Any,
        positions_repo: Any,
        clob_gateway: OrderSubmitter,
        reconciler: Reconciler,
        audit_logger: AuditLogger,
        clock: Clock = DefaultClock(),
        reconciliation_service: Any | None = None,
        live_position_fetcher: Callable[[str, str], Awaitable[Decimal | None]] | None = None,
    ):
        self.reservation_service = reservation_service
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.clob_gateway = clob_gateway
        self.reconciler = reconciler
        self.audit_logger = audit_logger
        self.clock = clock
        self.reconciliation_service = reconciliation_service
        self.live_position_fetcher = live_position_fetcher

    async def execute_intent(self, intent: OrderIntent) -> ExecutionResult:
        reservation = await self.reservation_service.reserve_for_intent(intent)
        logger.info(
            "coordinator.reservation_acquired reservation_id=%s key=%s side=%s",
            reservation.id, intent.key.stable_key(), intent.key.side.value,
        )

        try:
            order = await self.orders_repo.create_from_intent(
                intent=intent,
                reservation_id=reservation.id,
                status=OrderLifecycleStatus.RESERVED,
            )
        except Exception as exc:
            self.reservation_service.release_terminal(
                reservation.id, f"create_failed:{exc}"
            )
            self._log_audit(
                "execution_create_failed", intent, "none", reservation.id,
                error=str(exc),
            )
            raise

        self._link_reservation_order(reservation.id, order_id=order.id)

        await self._transition_order(
            order.id,
            from_status=OrderLifecycleStatus.RESERVED,
            to_status=OrderLifecycleStatus.SUBMITTING,
        )

        try:
            venue_result = await self.clob_gateway.submit_order(intent)
        except RetryableVenueError as exc:
            await self._transition_order(
                order.id,
                from_status=OrderLifecycleStatus.SUBMITTING,
                to_status=OrderLifecycleStatus.RETRYABLE_FAILED,
                metadata={"error": str(exc)},
            )
            self._log_audit("execution_retryable_failed", intent, order.id, reservation.id, str(exc))
            return ExecutionResult.retryable_failed(order_id=order.id, error=str(exc))
        except TerminalVenueError as exc:
            await self._transition_order(
                order.id,
                from_status=OrderLifecycleStatus.SUBMITTING,
                to_status=OrderLifecycleStatus.TERMINAL_FAILED,
                metadata={"error": str(exc)},
            )
            self.reservation_service.release_terminal(reservation.id, "terminal_failed")
            self._log_audit("execution_terminal_failed", intent, order.id, reservation.id, str(exc))
            if (
                intent.key.side.value == "SELL"
                and self.reconciliation_service is not None
                and self.live_position_fetcher is not None
                and _is_insufficient_balance_error(str(exc))
            ):
                await self._reconcile_drift_from_intent(intent)
            raise
        except Exception as exc:
            await self._transition_order(
                order.id,
                from_status=OrderLifecycleStatus.SUBMITTING,
                to_status=OrderLifecycleStatus.TERMINAL_FAILED,
                metadata={"error": str(exc)},
            )
            self.reservation_service.release_terminal(reservation.id, "terminal_failed")
            self._log_audit("execution_terminal_failed", intent, order.id, reservation.id, str(exc))
            raise  # Reraise unknown errors, but ensure reservation is released

        self._link_reservation_order(
            reservation.id, order_id=order.id, venue_order_id=venue_result.order_id
        )

        status = classify_venue_status(venue_result.status)

        if status == OrderLifecycleStatus.FILLED:
            fill = await self.fills_repo.persist_from_venue_result(
                order_id=order.id,
                venue_result=venue_result,
            )
            self.positions_repo.upsert_position(fill) # Simplified position repo usage
            self.reservation_service.release_terminal(
                reservation.id,
                reason="filled",
            )
            await self._transition_order(
                order.id,
                from_status=OrderLifecycleStatus.SUBMITTING,
                to_status=OrderLifecycleStatus.FILLED,
                venue_order_id=venue_result.order_id,
            )
            self._log_audit("execution_filled", intent, order.id, reservation.id, venue_order_id=venue_result.order_id)
            return ExecutionResult.filled(order_id=order.id)

        # Preserve UNKNOWN_UNCONFIRMED distinctly from ordinary submitted/live
        # statuses. Unknown venue responses are ambiguous and must stay visible
        # to reconciliation/telemetry instead of being collapsed into
        # SUBMITTED_UNCONFIRMED.
        unresolved_status = status

        await self._transition_order(
            order.id,
            from_status=OrderLifecycleStatus.SUBMITTING,
            to_status=unresolved_status,
            venue_order_id=venue_result.order_id,
            metadata={"raw_status": venue_result.status},
        )
        
        # If terminal status (e.g. REJECTED), we release the reservation
        if unresolved_status in {OrderLifecycleStatus.CANCELLED, OrderLifecycleStatus.REJECTED, OrderLifecycleStatus.EXPIRED, OrderLifecycleStatus.TERMINAL_FAILED}:
            self.reservation_service.release_terminal(reservation.id, str(unresolved_status.value))
            logger.info(
                "coordinator.reservation_released reservation_id=%s reason=%s",
                reservation.id, unresolved_status.value,
            )
        else:
            await self.reconciler.enqueue(order.id)
            logger.info(
                "coordinator.reservation_pending reservation_id=%s "
                "status=%s — awaiting reconciliation",
                reservation.id, unresolved_status.value,
            )

        self._log_audit(f"execution_{unresolved_status.value}", intent, order.id, reservation.id, venue_order_id=venue_result.order_id)
        
        return ExecutionResult.unconfirmed(
            order_id=order.id,
            status=unresolved_status,
        )

    def _link_reservation_order(
        self,
        reservation_id: str,
        *,
        order_id: str | None = None,
        venue_order_id: str | None = None,
    ) -> None:
        repo = getattr(self.reservation_service, "reservations_repo", None)
        linker = getattr(repo, "link_order", None)
        if linker is not None:
            linker(reservation_id, order_id=order_id, venue_order_id=venue_order_id)

    async def _transition_order(self, order_id: str, from_status: OrderLifecycleStatus, to_status: OrderLifecycleStatus, venue_order_id: str | None = None, metadata: Dict[str, Any] | None = None) -> None:
        assert_transition_allowed(from_status, to_status)
        await self.orders_repo.transition(
            order_id,
            from_status=from_status,
            to_status=to_status,
            venue_order_id=venue_order_id,
            metadata=metadata,
        )
        
    def _log_audit(self, event_name: str, intent: OrderIntent, order_id: str, reservation_id: str, error: str = "", venue_order_id: str = ""):
        payload = {
            "user_id": intent.key.user_id,
            "leader_wallet": intent.key.leader_wallet,
            "condition_id": intent.key.condition_id,
            "token_id": intent.key.token_id,
            "side": intent.key.side.value,
            "intent_key": intent.key.stable_key(),
            "order_id": order_id,
            "reservation_id": reservation_id,
            "venue_order_id": venue_order_id,
            "source_signal_id": intent.source_signal_id,
            "error": error
        }
        self.audit_logger.log_event(event_name, payload)

    async def _reconcile_drift_from_intent(self, intent: OrderIntent) -> None:
        try:
            user_id = intent.key.user_id
            token_id = intent.key.token_id

            async def _fetcher() -> Decimal | None:
                return await self.live_position_fetcher(user_id, token_id)

            await self.reconciliation_service.reconcile_position_drift(
                user_id=user_id,
                condition_id=intent.key.condition_id,
                token_id=token_id,
                leader_id=intent.key.leader_wallet or "",
                live_position_fetcher=_fetcher,
                reservation_service=self.reservation_service,
            )
        except Exception:
            logger.debug("coordinator_drift_reconciliation_failed", exc_info=True)