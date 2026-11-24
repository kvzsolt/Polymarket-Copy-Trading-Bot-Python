"""Bounded gateway submission, error classification, and ambiguous-state persistence (WP-A)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from pmcopy.integrations.polymarket.models import MarketableOrderRequest, VenueSubmitResult
from pmcopy.services.execution.circuit_breaker import CircuitBreaker
from pmcopy.services.execution.idempotency_lock import AcquiredSubmissionLock
from pmcopy.services.execution.order_planner import PlannedExecutionOrder
from pmcopy.services.execution.submission import build_pending_response
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult
from pmcopy.services.trading.trading_permission import TradingPausedError, TradingPermissionService
from pmcopy.utils.logger import (
    log_api_error,
    log_api_response,
    redact_error_detail,
    safe_error_detail,
)

MAX_ATTEMPTED_ORDER_IDS = 8


def seed_attempted_order_ids(existing: dict[str, Any] | None) -> list[str]:
    """Seed the bounded attempted-order-id list from a persisted submission row."""
    response = existing.get("response") if isinstance(existing, dict) else None
    if not isinstance(response, dict):
        return []
    raw = response.get("attempted_order_ids")
    if not isinstance(raw, (list, tuple)):
        return []
    seeded: list[str] = []
    for item in raw:
        value = str(item or "").strip()
        if value and value not in seeded:
            seeded.append(value)
    return seeded[:MAX_ATTEMPTED_ORDER_IDS]


def merge_attempted_order_ids(
    response: dict[str, Any],
    attempted_order_ids: list[str] | None,
) -> dict[str, Any]:
    """Attach a deduplicated, bounded attempted-order-id list onto ``response``."""
    if not attempted_order_ids:
        return response
    merged: list[str] = []
    for item in attempted_order_ids:
        value = str(item or "").strip()
        if value and value not in merged:
            merged.append(value)
    if merged:
        response["attempted_order_ids"] = merged[:MAX_ATTEMPTED_ORDER_IDS]
    return response


def submit_result_to_response(submit_result: VenueSubmitResult) -> dict[str, Any]:
    """Translate ``VenueSubmitResult`` into the persisted response dictionary shape."""
    default_status = (
        submit_result.status or "submitted"
        if submit_result.success
        else submit_result.status or "failed"
    )
    raw = submit_result.raw if isinstance(submit_result.raw, dict) else {}
    raw_data_value = raw.get("data")
    raw_data = raw_data_value if isinstance(raw_data_value, dict) else {}

    def _pick_raw(*keys: str) -> Any:
        for key in keys:
            value = raw_data.get(key)
            if value is not None:
                return value
            value = raw.get(key)
            if value is not None:
                return value
        return None

    return {
        "success": submit_result.success,
        "orderID": submit_result.order_id,
        "status": default_status,
        "size_matched": _pick_raw("size_matched", "sizeMatched"),
        "filled_size": _pick_raw("filled_size", "filledSize"),
        "matched_size": _pick_raw("matched_size", "matchedSize"),
        "matched_amount": _pick_raw("matched_amount", "matchedAmount"),
        "avg_price": _pick_raw("avg_price", "average_price", "averagePrice"),
        "avg_fill_price": _pick_raw("avg_fill_price", "avgFillPrice"),
        "error": submit_result.error,
        "ambiguous_post_failure": submit_result.ambiguous_post_failure,
        "non_retryable": submit_result.non_retryable,
        "effective_price": (
            str(submit_result.effective_price)
            if submit_result.effective_price is not None
            else None
        ),
        "trade_ids": submit_result.trade_ids,
        "transactions_hashes": submit_result.transactions_hashes,
        "raw": submit_result.raw,
    }


def response_order_id(response: dict[str, Any]) -> Any:
    """Extract canonical venue order ID from a persisted/gateway response dict."""
    return response.get("orderID") or response.get("order_id") or response.get("id")


def response_decimal(response: dict[str, Any], *keys: str) -> Decimal:
    """Parse the first valid Decimal value across ``keys``, defaulting to ``0``."""
    for key in keys:
        value = response.get(key)
        if value is None:
            continue
        try:
            return Decimal(str(value))
        except Exception:
            continue
    return Decimal("0")


def response_matched_size(response: dict[str, Any]) -> Decimal:
    """Extract matched size from a venue response dictionary."""
    return response_decimal(
        response,
        "size_matched",
        "filled_size",
        "matched_size",
        "matched_amount",
    )


def response_avg_price(response: dict[str, Any]) -> Decimal | None:
    """Extract positive average fill price or ``None`` when unknown."""
    value = response_decimal(response, "avg_price", "avg_fill_price")
    return value if value > 0 else None


def response_error_text(response: dict[str, Any]) -> str:
    """Return normalized lowercase error string from ``response``."""
    value = response.get("error")
    return str(value).strip().lower() if value is not None else ""


@dataclass(frozen=True, slots=True)
class GatewaySubmissionOutcome:
    """Persisted initial submission response ready for settlement projection."""

    response: dict[str, Any]
    last_error: str | None


class BoundedGatewaySubmitter:
    """Execute write-ahead pending persistence and bounded venue submission attempts."""

    async def submit(
        self,
        req: ExecutionRequest,
        plan: PlannedExecutionOrder,
        lock: AcquiredSubmissionLock,
        *,
        config: Any,
        circuit_breaker: CircuitBreaker,
        trading_permission: TradingPermissionService | None,
        task_entry_denial_fn: Callable[[ExecutionRequest], Awaitable[str | None]],
        save_submission_fn: Callable[..., Awaitable[None]],
        submit_gateway_fn: Callable[[MarketableOrderRequest], Awaitable[VenueSubmitResult]],
        sleep_backoff_fn: Callable[[int], Awaitable[None]],
        logger: logging.Logger,
    ) -> GatewaySubmissionOutcome | ExecutionResult:
        """Submit order with write-ahead ambiguity protection and non-blind-retry policy."""
        idem_key = lock.idem_key
        request_doc = lock.request_doc
        attempted_order_ids = seed_attempted_order_ids(lock.existing)
        pending_since = time.time()
        await save_submission_fn(
            idempotency_key=idem_key,
            request=request_doc,
            response=build_pending_response(pending_since=pending_since, config=config),
            attempted_order_ids=attempted_order_ids,
        )

        gateway_request = MarketableOrderRequest(
            token_id=req.token_id,
            side=plan.side,
            size=plan.effective_order_amount,
            price_limit=plan.worst_price,
            condition_id=getattr(req, "condition_id", None),
            tick_size=plan.tick_size,
            neg_risk=plan.neg_risk,
            order_type=plan.requested_order_type
            or (ExecutionOrderType.FAK if plan.forced_sell_exit else None),
        )

        last_error: str | None = None
        response: dict[str, Any] = {}
        submission_attempts = 1 if plan.requested_order_type is ExecutionOrderType.FAK else 3

        for attempt in range(1, submission_attempts + 1):
            task_denial = await task_entry_denial_fn(req)
            if task_denial is not None:
                await save_submission_fn(
                    idempotency_key=idem_key,
                    request=request_doc,
                    response={"success": False, "status": task_denial, "non_retryable": True},
                    attempted_order_ids=attempted_order_ids,
                )
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status=task_denial,
                    requested_amount=req.amount,
                    filled_amount=Decimal(0),
                    avg_price=None,
                    fees_paid=Decimal(0),
                    error=task_denial,
                )
            if trading_permission is not None:
                try:
                    await trading_permission.require_submission_allowed(
                        purpose=plan.submission_purpose
                    )
                except TradingPausedError as exc:
                    response = {
                        "success": False,
                        "status": "trading_paused",
                        "error": str(exc),
                        "non_retryable": True,
                    }
                    await save_submission_fn(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                        attempted_order_ids=attempted_order_ids,
                    )
                    return ExecutionResult(
                        accepted=False,
                        order_id=None,
                        status="trading_paused",
                        requested_amount=req.amount,
                        filled_amount=Decimal("0"),
                        avg_price=None,
                        fees_paid=Decimal("0"),
                        error=str(exc),
                    )
            try:
                submit_result = await submit_gateway_fn(gateway_request)
                response = submit_result_to_response(submit_result)
                if submit_result.order_id:
                    candidate_id = str(submit_result.order_id)
                    if candidate_id not in attempted_order_ids:
                        attempted_order_ids.append(candidate_id)
                if submit_result.success:
                    circuit_breaker.record_success()
                    await save_submission_fn(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                        attempted_order_ids=attempted_order_ids,
                    )
                    log_api_response(
                        logger,
                        method="POST",
                        url="post_signed_order",
                        status=200,
                        data=response,
                        meta={"side": plan.side, "asset": req.token_id},
                    )
                    break
                if submit_result.ambiguous_post_failure:
                    last_error = submit_result.error or "ambiguous post failure"
                    response["status"] = "submitted_unconfirmed"
                    await save_submission_fn(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                        attempted_order_ids=attempted_order_ids,
                    )
                    logger.warning(
                        "ambiguous post failure (timeout/connection after POST): %s",
                        response.get("error"),
                    )
                    break
                last_error = submit_result.error or "unknown order submission error"
                if submit_result.non_retryable:
                    terminal_status = submit_result.status or "rejected"
                    venue_reason = submit_result.venue_reason
                    response = {
                        "success": False,
                        "status": terminal_status,
                        "error": last_error,
                        "venue_reason": venue_reason,
                        "non_retryable": True,
                    }
                    await save_submission_fn(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                        attempted_order_ids=attempted_order_ids,
                    )
                    logger.warning(
                        "non-retryable CLOB rejection, skipping retries: %s%s",
                        last_error,
                        f" | venue_reason={venue_reason}" if venue_reason else "",
                    )
                    break
            except asyncio.CancelledError:
                cancelled_response = {
                    "success": False,
                    "status": "submitted_unconfirmed",
                    "error": "cancelled_during_submit",
                }
                try:
                    await asyncio.shield(
                        save_submission_fn(
                            idempotency_key=idem_key,
                            request=request_doc,
                            response=cancelled_response,
                            attempted_order_ids=attempted_order_ids,
                        )
                    )
                except Exception:
                    logger.warning(
                        "cancelled_submit_ambiguity_persist_failed | idem_key=%s",
                        idem_key,
                        exc_info=True,
                    )
                raise
            except VenueRateLimitError as exc:
                last_error = redact_error_detail(exc)
                logger.warning("clob rate limited (attempt %s): %s", attempt, last_error)
            except (VenueAuthError, VenueInvalidOrderError) as exc:
                venue_reason = (
                    safe_error_detail(exc)
                    if isinstance(exc, VenueInvalidOrderError)
                    else redact_error_detail(exc)
                )
                last_error = venue_reason
                if isinstance(exc, VenueInsufficientBalanceError):
                    last_error = f"insufficient_balance | {last_error}"
                response = {
                    "success": False,
                    "status": "rejected",
                    "error": last_error,
                    "venue_reason": venue_reason,
                    "non_retryable": True,
                }
                await save_submission_fn(
                    idempotency_key=idem_key,
                    request=request_doc,
                    response=response,
                    attempted_order_ids=attempted_order_ids,
                )
                logger.warning(
                    "non-retryable CLOB rejection, skipping retries: %s",
                    last_error,
                )
                break
            except VenueUnavailableError as exc:
                last_error = redact_error_detail(exc)
                response = {
                    "success": False,
                    "status": "submitted_unconfirmed",
                    "error": last_error,
                }
                await save_submission_fn(
                    idempotency_key=idem_key,
                    request=request_doc,
                    response=response,
                    attempted_order_ids=attempted_order_ids,
                )
                log_api_response(
                    logger,
                    method="POST",
                    url="post_signed_order",
                    status=503,
                    error=last_error,
                    meta={
                        "side": plan.side,
                        "asset": req.token_id,
                        "state": "submitted_unconfirmed",
                    },
                )
                logger.warning(
                    "ambiguous submission (POST possibly delivered): %s",
                    last_error,
                )
                circuit_breaker.record_failure()
                break
            except Exception as exc:
                last_error = redact_error_detail(exc)
                response = {
                    "success": False,
                    "status": "submitted_unconfirmed",
                    "error": last_error,
                }
                await save_submission_fn(
                    idempotency_key=idem_key,
                    request=request_doc,
                    response=response,
                    attempted_order_ids=attempted_order_ids,
                )
                logger.warning(
                    "unclassified gateway exception (treating as ambiguous): %s",
                    last_error,
                )
                circuit_breaker.record_failure()
                break
            if attempt < submission_attempts:
                await sleep_backoff_fn(attempt)
        else:
            circuit_breaker.record_failure()
            await save_submission_fn(
                idempotency_key=idem_key,
                request=request_doc,
                response=response or {"success": False, "error": last_error, "status": "failed"},
                attempted_order_ids=attempted_order_ids,
            )
            log_api_error(
                logger,
                side=req.side,
                error=last_error,
                asset=req.token_id,
                retry=submission_attempts,
                max_retries=submission_attempts,
            )

        return GatewaySubmissionOutcome(response=response, last_error=last_error)


__all__ = [
    "BoundedGatewaySubmitter",
    "GatewaySubmissionOutcome",
    "MAX_ATTEMPTED_ORDER_IDS",
    "merge_attempted_order_ids",
    "response_avg_price",
    "response_decimal",
    "response_error_text",
    "response_matched_size",
    "response_order_id",
    "seed_attempted_order_ids",
    "submit_result_to_response",
]
