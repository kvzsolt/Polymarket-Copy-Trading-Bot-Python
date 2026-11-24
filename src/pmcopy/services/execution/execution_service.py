"""Execute validated intents through the venue-neutral CLOB gateway (WP-A / WP-F).

``ExecutionService`` orchestrates five single-responsibility collaborators:

1. :class:`~pmcopy.services.execution.admission_gates.ExecutionAdmissionEvaluator`
   — pre-flight side/amount, kill-switch, trading-permission, breaker, & geoblock;
2. :class:`~pmcopy.services.execution.order_planner.ExecutionOrderPlanner`
   — metadata/book resolution, spread/depth gates, price bounds, & min-size gates;
3. :class:`~pmcopy.services.execution.idempotency_lock.SubmissionLockCoordinator`
   — deterministic effective-key derivation, dedup lookup, & atomic DB locking;
4. :class:`~pmcopy.services.execution.gateway_submitter.BoundedGatewaySubmitter`
   — write-ahead pending persistence, bounded retries, & ambiguous-state shielding;
5. :class:`~pmcopy.services.execution.settlement_projector.ExecutionSettlementProjector`
   — paper FAK fill/repost projection and live ``submitted_unconfirmed`` handoff.
"""

from __future__ import annotations

import asyncio
import random
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.config.app_config import AppConfig
from pmcopy.domain.price_bound import MARKETABLE_TICK_POLICY
from pmcopy.integrations.polymarket.models import MarketableOrderRequest, VenueSubmitResult
from pmcopy.services.execution.circuit_breaker import CircuitBreaker
from pmcopy.services.copy_tasks.entry_permission import TaskEntryPermissionService
from pmcopy.services.execution.admission_gates import (
    ExecutionAdmissionEvaluator,
    is_forced_sell_exit_request as _is_forced_sell_exit_request,
    normalize_side as _normalize_side,
)
from pmcopy.services.execution.book_spread import (
    calculate_book_depth_metrics as _calculate_book_depth_metrics,
    calculate_book_spread_metrics as _calculate_book_spread_metrics,
    resolve_book_spread_mode as _resolve_book_spread_mode,
)
from pmcopy.services.execution.gateway_submitter import (
    BoundedGatewaySubmitter,
    merge_attempted_order_ids as _merge_attempted_order_ids,
    response_avg_price as _response_avg_price,
    response_decimal as _response_decimal,
    response_error_text as _response_error_text,
    response_matched_size as _response_matched_size,
    response_order_id as _response_order_id,
    seed_attempted_order_ids as _seed_attempted_order_ids,
    submit_result_to_response as _submit_result_to_response,
)
from pmcopy.services.execution.idempotency_lock import (
    SubmissionLockCoordinator,
    derive_intent_id_from_request as _derive_intent_id_from_request,
)
from pmcopy.services.execution.order_planner import (
    ExecutionOrderPlanner,
    copy_fak_limit_on_tick as _copy_fak_limit_on_tick,
    parse_decimal_or_default as _parse_decimal_or_default,
    quantize_marketable_price as _quantize_price,
    quote_outside_band as _quote_outside_band,
)
from pmcopy.services.execution.settlement_projector import (
    ExecutionSettlementProjector,
    is_deferred_order_ack as _is_deferred_order_ack,
    is_fak_repostable_no_fill as _is_fak_repostable_no_fill,
    level_size_at_price as _level_size_at_price,
    non_negative_int as _non_negative_int,
    positive_int as _positive_int,
    top_level_marketable_amount as _top_level_marketable_amount,
)
from pmcopy.services.execution.submission import (
    build_pending_response as _build_pending_response,
    is_pending_submission_expired as _is_pending_submission_expired,
    is_retry_blocking_existing as _is_retry_blocking_existing,
    pending_submission_ttl_seconds as _pending_submission_ttl_seconds,
    sanitize_submission_request as _sanitize_submission_request,
    sanitize_submission_response as _sanitize_submission_response,
    truncate_text as _truncate_text,
)
from pmcopy.services.execution.types import (
    ExecutionAmountProjection,
    ExecutionRequest,
    ExecutionResult,
)
from pmcopy.services.execution.idempotency import compute_effective_key as _compute_effective_key
from pmcopy.services.observability.metrics import LatencyPhase
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.services.trading.trading_permission import TradingPermissionService
from pmcopy.utils.logger import get_logger

if TYPE_CHECKING:
    from pmcopy.interfaces.clob_gateway import ClobGateway
    from pmcopy.services.market_data.market_data_service import MarketDataService
    from pmcopy.services.telemetry import ExecutionTelemetryEmitter

logger = get_logger("ExecutionService")

# Legacy compatibility symbol retained for existing test-reset fixtures.
# Production multi-copier runtimes compose and inject a dedicated shared
# ``CircuitBreaker`` from ``orchestrators.runtime_bootstrap``; standalone
# ``ExecutionService`` instances default to an isolated breaker configured from
# ``config.execution.circuit_failure_threshold`` so unrelated instantiations
# never mutate process-global state.
_CLOB_CIRCUIT = CircuitBreaker(
    name="clob_api",
    failure_threshold=5,
    cooldown_seconds=60,
)


def _effective_idempotency_key(
    *,
    user: str,
    condition_id: str | None,
    token_id: str,
    side: str,
    effective_size: Decimal,
    effective_price: Decimal,
    tick_size: Decimal,
    intent_id: str = "",
    policy_version: str = "",
    environment: str = "",
) -> str:
    """Compute idempotency key from effective post-sizing order parameters."""
    return _compute_effective_key(
        user=user,
        condition_id=condition_id or "",
        token_id=token_id,
        side=side,
        effective_size=effective_size,
        effective_price=effective_price,
        tick_size=tick_size,
        intent_id=intent_id,
        policy_version=policy_version,
        environment=environment,
    )


async def _sleep_rate_limit_backoff(attempt: int) -> None:
    """Sleep with bounded jitter before retrying a rate-limited submission."""
    await asyncio.sleep(min(0.15 * attempt, 0.30) + random.uniform(0, 0.1))


class ExecutionService:
    """Submit copy and protective orders through one venue-neutral gateway."""

    def __init__(
        self,
        clob_client: Any,
        fee_service: Any = None,
        order_repo: Any = None,
        *,
        circuit_breaker: CircuitBreaker | None = None,
        config: AppConfig,
        clob_gateway: ClobGateway | None = None,
        market_data_service: MarketDataService | None = None,
        geoblock_gate: Any = None,
        trading_permission: TradingPermissionService | None = None,
        telemetry: ExecutionTelemetryEmitter | None = None,
        task_entry_permission: TaskEntryPermissionService | None = None,
        latency: PipelineLatency | None = None,
        admission_evaluator: ExecutionAdmissionEvaluator | None = None,
        order_planner: ExecutionOrderPlanner | None = None,
        lock_coordinator: SubmissionLockCoordinator | None = None,
        gateway_submitter: BoundedGatewaySubmitter | None = None,
        settlement_projector: ExecutionSettlementProjector | None = None,
    ) -> None:
        if config is None:
            raise TypeError("ExecutionService requires a typed AppConfig")
        if clob_gateway is None:
            raise TypeError("ExecutionService requires an injected clob_gateway")

        self.clob_client = clob_client
        self.fee_service = fee_service
        self.order_repo = order_repo
        self._config = config
        self._market_data_service = market_data_service
        self._geoblock_gate = geoblock_gate
        self._trading_permission = trading_permission
        self._task_entry_permission = task_entry_permission
        self._telemetry = telemetry
        self._latency = latency
        self.clob_gateway = clob_gateway

        self.circuit_breaker = (
            circuit_breaker
            if circuit_breaker is not None
            else CircuitBreaker.for_venue_config(config)
        )

        submit_concurrency = getattr(
            getattr(config, "execution", None),
            "execution_submit_concurrency",
            None,
        )
        if not isinstance(submit_concurrency, int) or submit_concurrency <= 0:
            submit_concurrency = 2
        self._submit_semaphore = asyncio.Semaphore(submit_concurrency)

        self._admission_evaluator = admission_evaluator or ExecutionAdmissionEvaluator()
        self._order_planner = order_planner or ExecutionOrderPlanner()
        self._lock_coordinator = lock_coordinator or SubmissionLockCoordinator()
        self._gateway_submitter = gateway_submitter or BoundedGatewaySubmitter()
        self._settlement_projector = settlement_projector or ExecutionSettlementProjector()

    def _get_submit_semaphore(self) -> asyncio.Semaphore:
        if not hasattr(self, "_submit_semaphore"):
            cap = getattr(
                getattr(self._config, "execution", None),
                "execution_submit_concurrency",
                None,
            )
            if not isinstance(cap, int) or cap <= 0:
                cap = 2
            self._submit_semaphore = asyncio.Semaphore(cap)
        return self._submit_semaphore

    @staticmethod
    def _apply_marketable_price_protection(
        *,
        calculated_price: Decimal,
        caller_limit: Decimal,
        tick_size: Decimal,
        side: str,
    ) -> Decimal:
        """Clamp and quantize a FAK limit without weakening its cap or floor."""
        return MARKETABLE_TICK_POLICY.apply_caller_protection(
            calculated_price=calculated_price,
            caller_limit=caller_limit,
            tick_size=tick_size,
            side=side,
        )

    async def _task_entry_denial(self, request: ExecutionRequest) -> str | None:
        if request.side.upper() != "BUY":
            return None
        recovery = request.recovery_metadata or {}
        task_id = recovery.get("task_id")
        if task_id is None:
            return None
        version = recovery.get("task_version")
        if (
            not isinstance(task_id, str)
            or not task_id.strip()
            or not isinstance(version, int)
            or isinstance(version, bool)
            or version <= 0
        ):
            return "task_state_invalid"
        if self._task_entry_permission is None:
            return "task_state_unavailable"
        return await self._task_entry_permission.denial(task_id, version, request.leader_id)

    async def _save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any] | Any,
        attempted_order_ids: list[str] | None = None,
    ) -> None:
        from pmcopy.storage.postgres.orders_repo import OrderSubmissionRecord

        sanitized_request = _sanitize_submission_request(request)
        sanitized_response = _sanitize_submission_response(response)
        if attempted_order_ids:
            _merge_attempted_order_ids(sanitized_response, attempted_order_ids)
        status = str(
            sanitized_response.get("status")
            or ("submitted" if sanitized_response.get("success") else "failed")
        )
        record = OrderSubmissionRecord(
            idempotency_key=idempotency_key,
            user_id=str(sanitized_request.get("user_id") or sanitized_request.get("userId") or ""),
            leader_id=str(
                sanitized_request.get("leader_id") or sanitized_request.get("leaderWallet") or ""
            )
            or None,
            order_id=_response_order_id(sanitized_response),
            status=status,
            request_payload=sanitized_request,
            response_payload=sanitized_response,
            last_error=str(sanitized_response.get("error"))
            if sanitized_response.get("error")
            else None,
        )
        order_repo = self.order_repo
        if order_repo is None:
            raise RuntimeError("ExecutionService requires an injected order repository")
        await order_repo.save_submission(record)

    async def _submit_gateway(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        timing = self._latency
        started = timing.capture() if timing is not None else None
        try:
            result = await self.clob_gateway.submit_marketable_order(request)
        finally:
            if timing is not None:
                timing.observe(LatencyPhase.VENUE_SUBMISSION, started)
        if (
            timing is not None
            and result.success
            and result.order_id
            and not result.ambiguous_post_failure
        ):
            timing.acknowledged(result.order_id, started)
        return result

    async def execute(self, req: ExecutionRequest) -> ExecutionResult:
        """Submit one request and project its outcome onto one operator event.

        Single telemetry choke point: emits ``order_exception`` on escaped
        ``Exception`` (passing ``CancelledError`` through unreported) or
        ``order_result`` once ``_execute_impl`` returns.
        """
        telemetry = self._telemetry
        try:
            result = await self._execute_impl(req)
        except Exception as exc:
            if telemetry is not None:
                telemetry.order_exception(req, exc)
            raise
        if telemetry is not None:
            telemetry.order_result(req, result)
        return result

    async def _execute_impl(self, req: ExecutionRequest) -> ExecutionResult:
        """Validate, plan, lock, submit, and classify one execution request."""
        evaluator = getattr(self, "_admission_evaluator", None) or ExecutionAdmissionEvaluator()
        planner = getattr(self, "_order_planner", None) or ExecutionOrderPlanner()
        locker = getattr(self, "_lock_coordinator", None) or SubmissionLockCoordinator()
        submitter = getattr(self, "_gateway_submitter", None) or BoundedGatewaySubmitter()
        projector = getattr(self, "_settlement_projector", None) or ExecutionSettlementProjector()

        admission = await evaluator.evaluate(
            req,
            config=self._config,
            trading_permission=getattr(self, "_trading_permission", None),
            circuit_breaker=self.circuit_breaker,
            geoblock_gate=getattr(self, "_geoblock_gate", None),
        )
        if isinstance(admission, ExecutionResult):
            return admission

        plan = await planner.plan(
            req,
            admission,
            config=self._config,
            market_data_service=getattr(self, "_market_data_service", None),
            logger=logger,
        )
        if isinstance(plan, ExecutionResult):
            return plan

        lock = await locker.acquire(
            req,
            plan,
            config=self._config,
            order_repo=self.order_repo,
            idempotency_key_fn=_effective_idempotency_key,
            logger=logger,
        )
        if isinstance(lock, ExecutionResult):
            return lock

        async with self._get_submit_semaphore():
            outcome = await submitter.submit(
                req,
                plan,
                lock,
                config=self._config,
                circuit_breaker=self.circuit_breaker,
                trading_permission=getattr(self, "_trading_permission", None),
                task_entry_denial_fn=self._task_entry_denial,
                save_submission_fn=self._save_submission,
                submit_gateway_fn=self._submit_gateway,
                sleep_backoff_fn=_sleep_rate_limit_backoff,
                logger=logger,
            )
            if isinstance(outcome, ExecutionResult):
                return outcome

            return await projector.project(
                req,
                plan,
                lock,
                outcome,
                config=self._config,
                market_data_service=getattr(self, "_market_data_service", None),
                trading_permission=getattr(self, "_trading_permission", None),
                save_submission_fn=self._save_submission,
                submit_gateway_fn=self._submit_gateway,
                emit_lifecycle_fn=self._emit_post_lifecycle,
                logger=logger,
            )

    @staticmethod
    def _emit_post_lifecycle(
        *,
        req: ExecutionRequest,
        side: str,
        order_id: str | None,
        worst_price: Decimal,
        venue_status: str,
        lifecycle_status: str,
    ) -> None:
        """Emit a lifecycle log after the gateway POST. Idempotent via dedup."""
        try:
            from pmcopy.utils.logger import log_order_lifecycle

            projection = ExecutionAmountProjection.from_request(
                side=side,
                amount=req.amount,
                price=worst_price,
            )
            log_order_lifecycle(
                logger,
                status=lifecycle_status,
                side=side,
                condition_id=getattr(req, "condition_id", None),
                asset=req.token_id,
                order_id=order_id,
                size=(float(projection.token_size) if projection.token_size is not None else None),
                price=float(worst_price) if worst_price is not None else None,
                usdc_value=(
                    float(projection.usdc_value) if projection.usdc_value is not None else None
                ),
                venue_status=venue_status,
                submission_state=lifecycle_status,
                signal_id=getattr(req, "signal_id", None),
                account=getattr(req, "user_id", None),
                meta={
                    "sizeEstimated": projection.token_size_estimated,
                    "requestAmount": str(projection.request_amount),
                    "requestAmountUnit": projection.request_amount_unit,
                },
            )
        except Exception as exc:
            logger.debug("lifecycle_log_failed | %r", exc)


__all__ = [
    "ExecutionAmountProjection",
    "ExecutionRequest",
    "ExecutionResult",
    "ExecutionService",
    "_CLOB_CIRCUIT",
    "_build_pending_response",
    "_calculate_book_depth_metrics",
    "_calculate_book_spread_metrics",
    "_copy_fak_limit_on_tick",
    "_derive_intent_id_from_request",
    "_effective_idempotency_key",
    "_is_deferred_order_ack",
    "_is_fak_repostable_no_fill",
    "_is_forced_sell_exit_request",
    "_is_pending_submission_expired",
    "_is_retry_blocking_existing",
    "_level_size_at_price",
    "_merge_attempted_order_ids",
    "_non_negative_int",
    "_normalize_side",
    "_parse_decimal_or_default",
    "_pending_submission_ttl_seconds",
    "_positive_int",
    "_quantize_price",
    "_quote_outside_band",
    "_resolve_book_spread_mode",
    "_response_avg_price",
    "_response_decimal",
    "_response_error_text",
    "_response_matched_size",
    "_response_order_id",
    "_sanitize_submission_request",
    "_sanitize_submission_response",
    "_seed_attempted_order_ids",
    "_submit_result_to_response",
    "_top_level_marketable_amount",
    "_truncate_text",
]
