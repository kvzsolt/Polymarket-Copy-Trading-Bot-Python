from __future__ import annotations

import logging
from inspect import isawaitable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict, Protocol

from domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
    OrderLifecycleStatus,
    classify_venue_status,
)
from integrations.polymarket.errors import (
    RetryableVenueError,
    TerminalVenueError,
)
from services.execution.reservation_service import (
    Clock,
    DefaultClock,
    InsufficientFreeQuote,
    InsufficientFreeShares,
    ReservationConflict,
    ReservationService,
)
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
class ExecutionIntent:
    """Production execution intent.

    Carries everything the coordinator needs to build a production
    ``ExecutionRequest`` and a ``services.execution.reservation_service``
    ``OrderIntent``. ``source`` drives the ``IntentReason`` mapping used by
    the reservation key. Distinct from the V2-shaped ``OrderIntent`` consumed
    by ``ExecutionCoordinator.execute_intent``.
    """

    user_id: str
    condition_id: str
    token_id: str
    side: str  # "BUY" | "SELL"
    size: Decimal
    price: Decimal
    notional_usdc: Decimal
    idempotency_key: str | None = None
    source: str = "copy_trade"  # copy_trade | risk_exit | near_one_exit | protective_exit | manual
    signal_id: str | None = None
    leader_wallet: str | None = None
    max_slippage_bps: int = 200
    reason: str | None = None
    recovery_metadata: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class ExecutionCoordinatorResult:
    """Structured result returned by every production coordinator submit_*."""

    accepted: bool
    status: str
    skip_code: str | None = None
    reason: str | None = None
    order_id: str | None = None
    reservation_id: str | None = None
    submission_state: str | None = None
    lifecycle_status: str = ""
    raw_result: Any | None = None


def is_live_mode(config: Any) -> bool:
    """Return True when the bot is configured for real venue submissions.

    Convention matches the rest of the codebase: ``config.execution.paper_trade_only``
    and ``config.dry_run``. A missing / None config is treated as non-live so
    test fixtures that build partial TradingContexts keep their direct
    ExecutionService fallback.
    """
    if config is None:
        return False
    try:
        if bool(getattr(config, "dry_run", False)):
            return False
        execution = getattr(config, "execution", None)
        if execution is not None and bool(getattr(execution, "paper_trade_only", False)):
            return False
    except Exception:
        return False
    return True


_SOURCE_TO_REASON: dict[str, IntentReason] = {
    "copy_trade": IntentReason.COPY_BUY,  # overridden for SELL below
    "risk_exit": IntentReason.RISK_EXIT,
    "near_one_exit": IntentReason.NEAR_ONE_EXIT,
    "protective_exit": IntentReason.RISK_EXIT,
    "manual": IntentReason.RISK_EXIT,
}


_NON_TERMINAL_SUBMIT_STATUSES = frozenset({
    "submitted_unconfirmed", "submitted_delayed", "delayed",
    "live", "order_live", "matched", "matched_pending_confirmation",
    "mined", "trade_mined", "retrying", "trade_retrying",
})

_TERMINAL_FAILURE_SUBMIT_STATUSES = frozenset({
    "rejected", "failed", "cancelled", "canceled", "expired",
    "book_no_liquidity", "book_spread_wide",
    "stale_market_data", "market_not_ready", "circuit_open",
})


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
        clob_gateway: OrderSubmitter | None = None,
        reconciler: Reconciler | None = None,
        audit_logger: AuditLogger | None = None,
        clock: Clock | None = None,
        reconciliation_service: Any | None = None,
        live_position_fetcher: Callable[[str, str], Awaitable[Decimal | None]] | None = None,
        execution_service: Any | None = None,
        position_coordinator: Any | None = None,
    ):
        self.reservation_service = reservation_service
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.clob_gateway = clob_gateway
        self.reconciler = reconciler
        self.audit_logger = audit_logger
        # Mutable default DefaultClock() removed — Python anti-pattern. If
        # Clock ever gains state, the shared instance would silently leak
        # across constructors.
        self.clock = clock if clock is not None else DefaultClock()
        self.reconciliation_service = reconciliation_service
        self.live_position_fetcher = live_position_fetcher
        # Production wrapper deps. ``execute_intent`` (V2) does not use these.
        self.execution_service = execution_service
        self.position_coordinator = position_coordinator
        # PR7 — wired per-context by ``build_trading_context``.
        self.position_drift_service: Any | None = None
        # Cache the underlying reservations repo for direct order-id lookups.
        self.reservations_repo = getattr(reservation_service, "reservations_repo", None)

    async def execute_intent(self, intent: OrderIntent) -> ExecutionResult:
        """V2 entry point: acquire per-scope lock, then run the execution body.

        Lock acquisition matches ``submit_near_one_exit`` so concurrent V2
        intents on the same ``(user_id, condition_id, token_id)`` scope
        serialize against each other and against the production wrapper paths.
        """
        if self.position_coordinator is None:
            # Fail-closed in live mode: serialization is a correctness
            # requirement, never a best-effort optimization. Two
            # concurrent same-scope intents on the V2 path with no
            # coordinator would race through reserve_for_intent and both
            # POST to the venue (review finding #15). Mirror the
            # PositionScope-import fail-closed branch below.
            if is_live_mode(getattr(self.execution_service, "_config", None)):
                raise RuntimeError(
                    "ExecutionCoordinator.execute_intent: position_coordinator is None; "
                    "refusing to bypass per-scope serialization in live mode"
                )
            return await self._execute_intent_locked(intent)
        try:
            from services.positionActionCoordinator import PositionScope
        except Exception as exc:
            # Fail-closed in live mode: serialization is a correctness
            # requirement, never a best-effort optimization.
            if is_live_mode(getattr(self.execution_service, "_config", None)):
                raise RuntimeError(
                    "ExecutionCoordinator.execute_intent: cannot import PositionScope; "
                    "refusing to bypass per-scope serialization in live mode"
                ) from exc
            return await self._execute_intent_locked(intent)
        scope = PositionScope(
            user_id=intent.key.user_id,
            condition_id=intent.key.condition_id,
            token_id=intent.key.token_id,
        )
        async with self.position_coordinator.acquire(scope):
            return await self._execute_intent_locked(intent)

    async def _execute_intent_locked(self, intent: OrderIntent) -> ExecutionResult:
        reservation = await self.reservation_service.reserve_for_intent(intent)
        logger.info(
            "coordinator.reservation_acquired reservation_id=%s key=%s side=%s",
            reservation.id, intent.key.stable_key(), intent.key.side.value,
        )

        # C2: track release state so a stray exception cannot leak the
        # reservation. ``released`` flips true on every explicit release;
        # ``keep_reservation`` marks non-release returns (retryable / pending
        # reconciliation) so the cleanup branch knows not to release.
        released = False
        keep_reservation = False

        async def _release(reason: str) -> None:
            nonlocal released
            if released:
                return
            try:
                await self.reservation_service.release_terminal(reservation.id, reason)
            finally:
                released = True

        try:
            try:
                order = await self.orders_repo.create_from_intent(
                    intent=intent,
                    reservation_id=reservation.id,
                    status=OrderLifecycleStatus.RESERVED,
                )
            except Exception as exc:
                await _release(f"create_failed:{exc}")
                self._log_audit(
                    "execution_create_failed", intent, "none", reservation.id,
                    error=str(exc),
                )
                raise

            await self._link_reservation_order(reservation.id, order_id=order.id)

            await self._transition_order(
                order.id,
                from_status=OrderLifecycleStatus.RESERVED,
                to_status=OrderLifecycleStatus.SUBMITTING,
            )

            try:
                venue_result = await self.clob_gateway.submit_order(intent)
            except RetryableVenueError as exc:
                # Issue #98: RetryableVenueError before a live venue_order_id
                # exists means there is no durable retry owner for the
                # reservation — leaving it locked would strand capital/shares
                # indefinitely. Release immediately; the order stays in
                # RETRYABLE_FAILED for telemetry/sweeper pickup.
                await _release(f"retryable_venue_error:{exc}")
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
                await _release("terminal_failed")
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
                await _release("terminal_failed")
                self._log_audit("execution_terminal_failed", intent, order.id, reservation.id, str(exc))
                raise  # Reraise unknown errors, but ensure reservation is released

            await self._link_reservation_order(
                reservation.id, order_id=order.id, venue_order_id=venue_result.order_id
            )

            status = classify_venue_status(venue_result.status)

            if status == OrderLifecycleStatus.FILLED:
                fill = await self.fills_repo.persist_from_venue_result(
                    order_id=order.id,
                    venue_result=venue_result,
                )
                await self.positions_repo.upsert_position(fill)
                await _release("filled")
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

            if unresolved_status in {
                OrderLifecycleStatus.CANCELLED,
                OrderLifecycleStatus.REJECTED,
                OrderLifecycleStatus.EXPIRED,
                OrderLifecycleStatus.TERMINAL_FAILED,
            }:
                await _release(str(unresolved_status.value))
                logger.info(
                    "coordinator.reservation_released reservation_id=%s reason=%s",
                    reservation.id, unresolved_status.value,
                )
            else:
                # C4: reconciler may be unset in test/partial wiring contexts.
                if self.reconciler is not None:
                    await self.reconciler.enqueue(order.id)
                else:
                    logger.warning(
                        "coordinator.reconciler_missing order_id=%s status=%s — "
                        "reservation will remain pending without reconciliation",
                        order.id, unresolved_status.value,
                    )
                keep_reservation = True
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
        except BaseException:
            # C2 safety net: any path that escapes via exception must not leak
            # the reservation unless we explicitly chose to keep it (retryable).
            if not released and not keep_reservation:
                try:
                    await self.reservation_service.release_terminal(
                        reservation.id, "execute_intent_unexpected_failure"
                    )
                except Exception:
                    logger.warning(
                        "coordinator.cleanup_release_failed reservation_id=%s",
                        reservation.id, exc_info=True,
                    )
            raise

    async def _link_reservation_order(
        self,
        reservation_id: str,
        *,
        order_id: str | None = None,
        venue_order_id: str | None = None,
    ) -> None:
        repo = getattr(self.reservation_service, "reservations_repo", None)
        linker = getattr(repo, "link_order", None)
        if linker is not None:
            result = linker(reservation_id, order_id=order_id, venue_order_id=venue_order_id)
            if isawaitable(result):
                await result

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
        if self.audit_logger is None:
            return
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
            "error": error,
        }
        try:
            self.audit_logger.log_event(event_name, payload)
        except Exception:
            # Audit failures must never break execution.
            logger.debug("coordinator_audit_failed event=%s", event_name, exc_info=True)

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

    # ------------------------------------------------------------------
    # Production wrapper around the existing ExecutionService primitive.
    # ``execute_intent`` is the future V2 intent-shaped path; the methods
    # below are the current production compatibility surface.
    # ------------------------------------------------------------------
    async def submit_copy_trade(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        return await self._submit_with_reservation(intent, source_override="copy_trade")

    async def submit_risk_exit(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        return await self._submit_with_reservation(intent, source_override="risk_exit")

    async def submit_protective_exit(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        return await self._submit_with_reservation(intent, source_override="protective_exit")

    async def submit_near_one_exit(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        # Near-one is the only path whose caller does not already hold the
        # PositionActionCoordinator scope. Acquire it here when available so
        # this SELL serialises against copy SELL and risk SELL on the same
        # (user, condition, token).
        if self.position_coordinator is None:
            return await self._submit_with_reservation(intent, source_override="near_one_exit")
        try:
            from services.positionActionCoordinator import PositionScope
        except Exception as exc:
            # Fail-closed in live mode — SELL submitter must hold the
            # per-scope lock, never silently downgrade to no-lock.
            if is_live_mode(getattr(self.execution_service, "_config", None)):
                raise RuntimeError(
                    "ExecutionCoordinator.submit_near_one_exit: cannot import "
                    "PositionScope; refusing to bypass per-scope serialization "
                    "in live mode"
                ) from exc
            PositionScope = None  # type: ignore[assignment]
        if PositionScope is None:
            return await self._submit_with_reservation(intent, source_override="near_one_exit")
        scope = PositionScope(
            user_id=intent.user_id,
            condition_id=intent.condition_id,
            token_id=intent.token_id,
        )
        async with self.position_coordinator.acquire(scope):
            return await self._submit_with_reservation(intent, source_override="near_one_exit")

    # ------------------------------------------------------------------
    # Internal — single production funnel.
    # ------------------------------------------------------------------
    async def _submit_with_reservation(
        self,
        intent: ExecutionIntent,
        *,
        source_override: str,
    ) -> ExecutionCoordinatorResult:
        if self.execution_service is None:
            raise RuntimeError(
                "ExecutionCoordinator.submit_*: execution_service is required for production submits"
            )

        # PR7 — drift gate. Pause acts at the per-scope level; honoured by
        # every production submit path before reservation acquisition so an
        # unresolved drift never converts into venue activity.
        drift_svc = self.position_drift_service
        if drift_svc is not None and drift_svc.is_scope_paused(
            user_id=intent.user_id,
            condition_id=intent.condition_id,
            token_id=intent.token_id,
        ):
            self._safe_audit(
                "coordinator_position_drift_blocked",
                {
                    "user_id": intent.user_id,
                    "condition_id": intent.condition_id,
                    "token_id": intent.token_id,
                    "side": intent.side,
                    "source": source_override,
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="position_drift_unresolved",
                skip_code="position_drift_unresolved",
                reason="scope paused by position drift policy",
                lifecycle_status="duplicate_ignored",
            )

        order_intent = self._build_order_intent(intent, source=source_override)

        # ---- Reservation acquire (dup-guard layer #1) -----------------
        try:
            reservation = await self.reservation_service.reserve_for_intent(order_intent)
        except ReservationConflict as exc:
            self._safe_audit(
                "coordinator_duplicate_reservation_conflict",
                {
                    "user_id": intent.user_id,
                    "condition_id": intent.condition_id,
                    "token_id": intent.token_id,
                    "side": intent.side,
                    "source": source_override,
                    "error": str(exc),
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="duplicate_ignored",
                skip_code="reservation_conflict",
                reason=str(exc),
                lifecycle_status="duplicate_ignored",
            )
        except (InsufficientFreeQuote, InsufficientFreeShares) as exc:
            self._safe_audit(
                "coordinator_reservation_insufficient",
                {
                    "user_id": intent.user_id,
                    "side": intent.side,
                    "source": source_override,
                    "error": str(exc),
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="reservation_failed",
                skip_code="reservation_insufficient",
                reason=str(exc),
                lifecycle_status="failed",
            )

        reservation_id = reservation.id
        logger.info(
            "coordinator.production_reserved id=%s side=%s source=%s scope=%s/%s",
            reservation_id, intent.side, source_override,
            intent.condition_id, intent.token_id,
        )

        # ---- Build ExecutionRequest, thread reservation_id ------------
        from services.execution.types import ExecutionRequest

        recovery_metadata = dict(intent.recovery_metadata or {})
        recovery_metadata["reservation_id"] = reservation_id
        recovery_metadata.setdefault("source", source_override)
        price_limit = None
        raw_price_limit = recovery_metadata.get("price_limit")
        if raw_price_limit is not None:
            try:
                price_limit = Decimal(str(raw_price_limit))
            except Exception:
                price_limit = None

        exec_request = ExecutionRequest(
            user_id=intent.user_id,
            leader_id=intent.leader_wallet or "",
            token_id=intent.token_id,
            side=intent.side.upper(),
            amount=Decimal(str(intent.size)),
            max_slippage_bps=int(intent.max_slippage_bps),
            reason=intent.reason or source_override,
            signal_id=intent.signal_id,
            condition_id=intent.condition_id,
            recovery_metadata=recovery_metadata,
            order_type=recovery_metadata.get("order_type"),
            price_limit=price_limit,
        )

        # ---- Submit through ExecutionService (dup-guard layer #2) -----
        try:
            result = await self.execution_service.execute(exec_request)
        except Exception as exc:
            await self._release_reservation(reservation_id, reason="execute_exception")
            self._safe_audit(
                "coordinator_execute_exception",
                {
                    "reservation_id": reservation_id,
                    "side": intent.side,
                    "source": source_override,
                    "error": repr(exc),
                },
            )
            raise

        status = (getattr(result, "status", "") or "").lower()
        order_id = getattr(result, "order_id", None)

        if status == "duplicate_ignored":
            # ExecutionService found a pre-existing in-flight order via
            # idempotency-key lookup. Release the just-taken reservation;
            # never touch a pre-existing one that may already be linked.
            await self._release_reservation(reservation_id, reason="idempotency_duplicate")
            self._safe_audit(
                "coordinator_duplicate_idempotency",
                {
                    "reservation_id": reservation_id,
                    "existing_order_id": order_id,
                    "source": source_override,
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="duplicate_ignored",
                skip_code="idempotency_duplicate",
                reason="existing in-flight order",
                order_id=order_id,
                reservation_id=reservation_id,
                lifecycle_status="duplicate_ignored",
                raw_result=result,
            )

        if status == "inflight_lock":
            await self._release_reservation(reservation_id, reason="inflight_lock")
            return ExecutionCoordinatorResult(
                accepted=False,
                status="inflight_lock",
                skip_code="inflight_lock",
                reason=getattr(result, "error", None),
                reservation_id=reservation_id,
                lifecycle_status="duplicate_ignored",
                raw_result=result,
            )

        if status in _TERMINAL_FAILURE_SUBMIT_STATUSES:
            await self._release_reservation(reservation_id, reason=status)
            return ExecutionCoordinatorResult(
                accepted=False,
                status=status,
                skip_code=status,
                reason=getattr(result, "error", None),
                order_id=order_id,
                reservation_id=reservation_id,
                lifecycle_status="failed",
                raw_result=result,
            )

        if status in {"filled", "partially_filled"}:
            await self._release_reservation(reservation_id, reason=status)
            if order_id and self.reservations_repo is not None:
                try:
                    await self.reservations_repo.link_order(reservation_id, order_id=order_id)
                except Exception as exc:
                    logger.debug(
                        "coordinator_link_reservation_failed | reservation_id=%s order_id=%s error=%r",
                        reservation_id, order_id, exc,
                    )
            return ExecutionCoordinatorResult(
                accepted=True,
                status=status,
                order_id=order_id,
                reservation_id=reservation_id,
                submission_state=status,
                lifecycle_status=status,
                raw_result=result,
            )

        # Non-terminal venue ack — leave reservation active. Reconciler
        # releases on terminal status (PR5 _release_reservation_if_present).
        if status in _NON_TERMINAL_SUBMIT_STATUSES or getattr(result, "accepted", False):
            if order_id and self.reservations_repo is not None:
                try:
                    await self.reservations_repo.link_order(reservation_id, order_id=order_id)
                except Exception as exc:
                    logger.debug(
                        "coordinator_link_reservation_failed | reservation_id=%s order_id=%s error=%r",
                        reservation_id, order_id, exc,
                    )
            # Enqueue reconciliation immediately so an ambiguous forced-exit
            # ack (e.g. a fully-killed FAK that returned submitted_unconfirmed)
            # is settled — and its reservation released — within the next
            # reconcile pass, rather than stranding the reservation until its
            # TTL expires and the risk-loop reaper picks it up. Mirrors the
            # _execute_intent_locked non-terminal path.
            if order_id and self.reconciler is not None:
                try:
                    await self.reconciler.enqueue(order_id)
                except Exception as exc:
                    logger.debug(
                        "coordinator_reconcile_enqueue_failed | reservation_id=%s order_id=%s error=%r",
                        reservation_id, order_id, exc,
                    )
            return ExecutionCoordinatorResult(
                accepted=True,
                status=status or "submitted_unconfirmed",
                order_id=order_id,
                reservation_id=reservation_id,
                submission_state=status or "submitted_unconfirmed",
                lifecycle_status=status or "submitted_unconfirmed",
                raw_result=result,
            )

        # Catch-all: status not recognised — treat as failure, release.
        await self._release_reservation(reservation_id, reason=status or "unknown")
        return ExecutionCoordinatorResult(
            accepted=False,
            status=status or "unknown",
            skip_code=status or "unknown",
            reason=getattr(result, "error", None),
            order_id=order_id,
            reservation_id=reservation_id,
            lifecycle_status="failed",
            raw_result=result,
        )

    # ------------------------------------------------------------------
    # Helpers for the production wrapper.
    # ------------------------------------------------------------------
    def _build_order_intent(self, intent: ExecutionIntent, *, source: str) -> OrderIntent:
        side = IntentSide.BUY if intent.side.upper() == "BUY" else IntentSide.SELL
        if source == "copy_trade":
            reason = IntentReason.COPY_BUY if side == IntentSide.BUY else IntentReason.COPY_SELL
        else:
            reason = _SOURCE_TO_REASON.get(source, IntentReason.RISK_EXIT)
        key = OrderIntentKey(
            user_id=intent.user_id,
            leader_wallet=intent.leader_wallet,
            condition_id=intent.condition_id,
            token_id=intent.token_id,
            side=side,
            reason=reason,
        )
        return OrderIntent(
            key=key,
            size=Decimal(str(intent.size)),
            price=Decimal(str(intent.price)),
            max_slippage_bps=int(intent.max_slippage_bps),
            created_at=datetime.fromtimestamp(self.clock.utcnow_timestamp(), tz=timezone.utc),
            source_signal_id=intent.signal_id,
        )

    async def _release_reservation(self, reservation_id: str, *, reason: str) -> None:
        try:
            await self.reservation_service.release_terminal(reservation_id, reason=reason)
        except Exception as exc:
            logger.warning(
                "coordinator_release_failed reservation_id=%s reason=%s error=%r",
                reservation_id, reason, exc,
            )

    def _safe_audit(self, event: str, payload: dict[str, Any]) -> None:
        if self.audit_logger is None:
            return
        try:
            self.audit_logger.log_event(event, payload)
        except Exception:
            # Audit failures must never break execution.
            pass
