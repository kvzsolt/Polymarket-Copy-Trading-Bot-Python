"""Coordinate idempotent submission, reservations, and reconciliation."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Protocol

from pmcopy.domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntent,
    OrderIntentKey,
)
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.utils.logger import redact_error_detail
from pmcopy.services.execution.reservation_service import (
    Clock,
    CopierOpenExposureCapExceeded,
    DefaultClock,
    InsufficientFreeQuote,
    InsufficientFreeShares,
    ReservationConflict,
    ReservationService,
)
from pmcopy.services.observability.metrics import LatencyPhase
from pmcopy.services.telemetry.latency import PipelineLatency

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionIntent:
    """Production execution intent.

    Carries everything the coordinator needs to build a production
    ``ExecutionRequest`` and a ``services.execution.reservation_service``
    ``OrderIntent``. ``source`` drives the ``IntentReason`` mapping used by
    the reservation key. Distinct from the reservation layer's share-shaped
    ``OrderIntent`` built internally by ``_build_order_intent``.
    """

    user_id: str
    condition_id: str
    token_id: str
    side: str  # "BUY" | "SELL"
    size: Decimal  # BUY collateral pUSD; SELL outcome shares
    price: Decimal
    notional_usdc: Decimal
    idempotency_key: str | None = None
    source: str = "copy_trade"  # copy_trade | risk_exit | protective_exit | manual
    signal_id: str | None = None
    leader_wallet: str | None = None
    max_slippage_bps: int = 200
    reason: str | None = None
    recovery_metadata: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    order_type: ExecutionOrderType | None = None
    price_limit: Decimal | None = None
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool | None = None
    leader_sell_fraction: Decimal | None = None
    live_position_size: Decimal | None = None
    sub_min_sell_policy: str | None = None
    task_id: str | None = None
    protective_lot_id: str | None = None


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
    "protective_exit": IntentReason.RISK_EXIT,
    "manual": IntentReason.RISK_EXIT,
}


_NON_TERMINAL_SUBMIT_STATUSES = frozenset(
    {
        "submitted_unconfirmed",
        "submitted_delayed",
        "delayed",
        "live",
        "order_live",
        "matched",
        "matched_pending_confirmation",
        "mined",
        "trade_mined",
        "retrying",
        "trade_retrying",
    }
)

_TERMINAL_FAILURE_SUBMIT_STATUSES = frozenset(
    {
        "rejected",
        "failed",
        "cancelled",
        "canceled",
        "expired",
        "book_no_liquidity",
        "book_spread_wide",
        "stale_market_data",
        "market_not_ready",
        "market_metadata_unavailable",
        "geoblocked",
        "circuit_open",
        "price_limit_unexecutable",
        "task_deleted",
        "task_paused",
        "task_account_mismatch",
        "task_leader_mismatch",
        "task_version_conflict",
        "task_state_unavailable",
        "task_state_invalid",
        "task_protection_unavailable",
        "task_counter_outcome_unavailable",
        "outside_schedule",
        "trading_paused",
        "kill_switch",
    }
)


class Reconciler(Protocol):
    """Define the Reconciler port used by application services."""

    async def enqueue(self, order_id: str) -> None:
        """Enqueue an ambiguous order for reconciliation."""
        ...


class AuditLogger(Protocol):
    """Define the AuditLogger port used by application services."""

    def log_event(self, event_name: str, payload: dict[str, Any]) -> None:
        """Persist a sanitized execution audit event."""
        ...


class CopyAdmission(Protocol):
    """Check durable automatic membership before copied order reservations."""

    async def admit(self, follower_account: str, wallet: str, side: str) -> bool:
        """Return whether this exact account/leader may submit the copied side."""
        ...


class ExecutionCoordinator:
    """Coordinate guarded order execution through injected ports."""

    def __init__(
        self,
        reservation_service: ReservationService,
        positions_repo: Any,
        reconciler: Reconciler | None = None,
        audit_logger: AuditLogger | None = None,
        clock: Clock | None = None,
        execution_service: Any | None = None,
        position_coordinator: Any | None = None,
        latency: PipelineLatency | None = None,
        copy_admission: CopyAdmission | None = None,
        position_drift_service: Any | None = None,
    ) -> None:
        self.reservation_service = reservation_service
        self.positions_repo = positions_repo
        self.reconciler = reconciler
        self.audit_logger = audit_logger
        # Mutable default DefaultClock() removed — Python anti-pattern. If
        # Clock ever gains state, the shared instance would silently leak
        # across constructors.
        self.clock = clock if clock is not None else DefaultClock()
        # Production wrapper deps used by the submit_* funnel.
        self.execution_service = execution_service
        # A04 wiring contract: the execution coordinator is BORN with the
        # account-owned position coordinator so the execution funnel shares
        # the same lock map as every per-binding TradingContext (production
        # scopes serialize via ``context.position_coordinator`` around the
        # submit_* calls; post-construction rewiring of this attribute is
        # forbidden).
        self.position_coordinator = position_coordinator
        self._latency = latency
        self._copy_admission = copy_admission
        self.leader_sell_target_service: object | None = None
        # PR7 — the account-owned drift gate (one PositionDriftService per
        # user, built in ``build_user_state``). Injected at construction
        # with the same wiring contract as ``position_coordinator``:
        # per-binding rewiring of this attribute is forbidden.
        self.position_drift_service = position_drift_service
        # Cache the underlying reservations repo for direct order-id lookups.
        self.reservations_repo = (
            None
            if getattr(reservation_service, "enabled", True) is False
            else getattr(reservation_service, "reservations_repo", None)
        )

    # ------------------------------------------------------------------
    # Production surface around the existing ExecutionService primitive.
    # The ``submit_*`` methods below funnel through
    # ``_submit_with_reservation`` — the single guarded production path.
    # ------------------------------------------------------------------
    async def submit_copy_trade(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        """Submit a copied entry through the guarded coordinator."""
        if self._copy_admission is not None and not await self._copy_admission.admit(
            intent.user_id, intent.leader_wallet or "", intent.side
        ):
            return ExecutionCoordinatorResult(
                accepted=False,
                status="rejected",
                skip_code="auto_trader_not_active",
                reason="automatic trader membership does not permit this copy",
                lifecycle_status="rejected",
            )
        return await self._submit_with_reservation(intent, source_override="copy_trade")

    async def submit_risk_exit(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        """Submit a risk exit through the guarded coordinator."""
        return await self._submit_with_reservation(intent, source_override="risk_exit")

    async def submit_protective_exit(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        """Submit a protective exit through the guarded coordinator."""
        return await self._submit_with_reservation(intent, source_override="protective_exit")

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
            with (
                self._latency.span(LatencyPhase.RESERVATION)
                if self._latency is not None
                else nullcontext()
            ):
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
                    "error": redact_error_detail(exc),
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="duplicate_ignored",
                skip_code="reservation_conflict",
                reason=redact_error_detail(exc),
                lifecycle_status="duplicate_ignored",
            )
        except CopierOpenExposureCapExceeded as exc:
            self._safe_audit(
                "coordinator_copier_exposure_cap",
                {
                    "user_id": intent.user_id,
                    "side": intent.side,
                    "source": source_override,
                    "error": redact_error_detail(exc),
                },
            )
            return ExecutionCoordinatorResult(
                accepted=False,
                status="copier_open_exposure_cap",
                skip_code="copier_open_exposure_cap",
                reason=str(exc),
                lifecycle_status="failed",
            )
        except (InsufficientFreeQuote, InsufficientFreeShares) as exc:
            self._safe_audit(
                "coordinator_reservation_insufficient",
                {
                    "user_id": intent.user_id,
                    "side": intent.side,
                    "source": source_override,
                    "error": redact_error_detail(exc),
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
            reservation_id,
            intent.side,
            source_override,
            intent.condition_id,
            intent.token_id,
        )

        # ---- Build ExecutionRequest, thread reservation_id ------------
        from pmcopy.services.execution.types import ExecutionRequest

        recovery_metadata = dict(intent.recovery_metadata or {})
        recovery_metadata["reservation_id"] = reservation_id
        recovery_metadata.setdefault("source", source_override)
        requested_sell_size = Decimal(str(intent.size))
        reserved_sell_size = requested_sell_size
        if intent.side.upper() == "SELL":
            raw_reserved_size = getattr(reservation, "reserved_size", Decimal("0"))
            try:
                candidate_reserved_size = Decimal(str(raw_reserved_size))
            except (InvalidOperation, TypeError, ValueError):
                candidate_reserved_size = Decimal("0")
            if candidate_reserved_size > 0:
                reserved_sell_size = candidate_reserved_size
            recovery_metadata["requested_sell_size"] = str(requested_sell_size)
            recovery_metadata["reserved_sell_size"] = str(reserved_sell_size)
        baseline_position_size = intent.live_position_size
        if baseline_position_size is None:
            baseline_position_size = await self._local_position_baseline(intent)
        if baseline_position_size is not None:
            recovery_metadata.setdefault(
                "pre_submit_live_position_size",
                str(baseline_position_size),
            )
        price_limit = intent.price_limit
        raw_price_limit = recovery_metadata.get("price_limit")
        if price_limit is None and raw_price_limit is not None:
            try:
                price_limit = Decimal(str(raw_price_limit))
            except Exception:
                price_limit = None

        raw_order_type = recovery_metadata.get("order_type")
        order_type = intent.order_type
        if order_type is None and raw_order_type is not None:
            try:
                order_type = ExecutionOrderType(str(raw_order_type).upper())
            except ValueError:
                order_type = None

        exec_request = ExecutionRequest(
            user_id=intent.user_id,
            leader_id=intent.leader_wallet or "",
            token_id=intent.token_id,
            side=intent.side.upper(),
            amount=(
                Decimal(str(intent.notional_usdc))
                if intent.side.upper() == "BUY"
                else reserved_sell_size
            ),
            max_slippage_bps=int(intent.max_slippage_bps),
            reason=intent.reason or source_override,
            signal_id=intent.signal_id,
            condition_id=intent.condition_id,
            recovery_metadata=recovery_metadata,
            order_type=order_type,
            price_limit=price_limit,
            tick_size=intent.tick_size,
            min_order_size=intent.min_order_size,
            neg_risk=intent.neg_risk,
            leader_sell_fraction=intent.leader_sell_fraction,
            live_position_size=intent.live_position_size,
            sub_min_sell_policy=intent.sub_min_sell_policy,
        )

        # ---- Submit through ExecutionService (dup-guard layer #2) -----
        try:
            result = await self.execution_service.execute(exec_request)
        except Exception as exc:
            self._safe_audit(
                "coordinator_execute_exception",
                {
                    "reservation_id": reservation_id,
                    "side": intent.side,
                    "source": source_override,
                    "error_type": type(exc).__name__,
                },
            )
            raise RuntimeError("Submission outcome requires reconciliation") from None

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
            if order_id and self.reservations_repo is not None:
                try:
                    await self.reservations_repo.link_order(reservation_id, order_id=order_id)
                except Exception as exc:
                    logger.debug(
                        "coordinator_link_reservation_failed | reservation_id=%s order_id=%s error=%r",
                        reservation_id,
                        order_id,
                        exc,
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
                        reservation_id,
                        order_id,
                        exc,
                    )
            # Enqueue reconciliation immediately so an ambiguous forced-exit
            # ack (e.g. a fully-killed FAK that returned submitted_unconfirmed)
            # is settled — and its reservation released — within the next
            # reconcile pass, rather than stranding the reservation until its
            # TTL expires and the risk-loop reaper picks it up.
            if order_id and self.reconciler is not None:
                try:
                    await self.reconciler.enqueue(order_id)
                except Exception as exc:
                    logger.debug(
                        "coordinator_reconcile_enqueue_failed | reservation_id=%s order_id=%s error=%r",
                        reservation_id,
                        order_id,
                        exc,
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

        # An unrecognized acknowledgement does not establish rejection.
        return ExecutionCoordinatorResult(
            accepted=False,
            status="submitted_unconfirmed",
            skip_code="submission_outcome_unknown",
            reason="Submission outcome requires reconciliation",
            order_id=order_id,
            reservation_id=reservation_id,
            submission_state="submitted_unconfirmed",
            lifecycle_status="submitted_unconfirmed",
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
            source_event_id=intent.signal_id,
        )
        price = Decimal(str(intent.price))
        amount = Decimal(str(intent.size))
        if side is IntentSide.BUY:
            notional = Decimal(str(intent.notional_usdc))
            if price <= 0 or notional <= 0:
                raise ValueError("BUY execution intent requires positive price and pUSD notional")
            # ReservationService owns a share-shaped OrderIntent and reserves
            # ``size * price`` quote. Convert the public BUY collateral amount
            # once at this internal boundary so the reserved and submitted
            # pUSD values remain identical.
            amount = notional / price
        return OrderIntent(
            key=key,
            size=amount,
            price=price,
            max_slippage_bps=int(intent.max_slippage_bps),
            created_at=datetime.fromtimestamp(self.clock.utcnow_timestamp(), tz=timezone.utc),
            source_signal_id=intent.signal_id,
            sell_fraction=intent.leader_sell_fraction,
            live_position_size=intent.live_position_size,
            min_order_size=intent.min_order_size,
            round_up_to_minimum=intent.sub_min_sell_policy == "round_up",
            task_id=intent.task_id,
            protective_lot_id=intent.protective_lot_id,
        )

    async def _local_position_baseline(self, intent: Any) -> Decimal | None:
        """Return the locally held size for this intent's token, or ``None``.

        The ambiguous-order auto-release compares the local position against a
        baseline captured before the POST. Without a baseline that probe is
        permanently inconclusive, and since release requires every probe clean,
        the reservation stays parked forever — so a token we hold nothing in
        must record an explicit zero rather than nothing at all.

        Args:
            intent: The copy intent about to be submitted.

        Returns:
            The held size, ``Decimal("0")`` when no position exists, or ``None``
            when the lookup is unavailable. Capture is best-effort: a repository
            failure must never block a submission.
        """
        repo = getattr(self, "positions_repo", None)
        getter: Callable[..., Awaitable[object]] | None = getattr(repo, "get_position", None)
        if not callable(getter):
            return None
        try:
            position = await getter(
                intent.user_id,
                intent.condition_id,
                token_id=intent.token_id,
            )
        except Exception as exc:  # noqa: BLE001 - best-effort telemetry capture
            logger.debug(
                "coordinator.position_baseline_unavailable token=%s reason=%s",
                intent.token_id,
                redact_error_detail(exc),
            )
            return None
        if position is None:
            return Decimal("0")
        try:
            return Decimal(str(getattr(position, "size_tokens", 0) or 0))
        except (InvalidOperation, TypeError, ValueError):
            return None

    async def _release_reservation(self, reservation_id: str, *, reason: str) -> None:
        try:
            await self.reservation_service.release_terminal(reservation_id, reason=reason)
        except Exception as exc:
            logger.warning(
                "coordinator_release_failed reservation_id=%s reason=%s error=%r",
                reservation_id,
                reason,
                exc,
            )

    def _safe_audit(self, event: str, payload: dict[str, Any]) -> None:
        if self.audit_logger is None:
            return
        try:
            self.audit_logger.log_event(event, payload)
        except Exception:
            # Audit failures must never break execution.
            pass
