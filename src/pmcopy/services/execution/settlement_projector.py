"""Post-submission classification, protective FAK reposting, and reconciler handoff (WP-A)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.domain.price_bound import MARKETABLE_TICK_POLICY
from pmcopy.integrations.polymarket.models import MarketableOrderRequest, VenueSubmitResult
from pmcopy.services.execution.gateway_submitter import (
    GatewaySubmissionOutcome,
    response_avg_price,
    response_decimal,
    response_error_text,
    response_matched_size,
    response_order_id,
    submit_result_to_response,
)
from pmcopy.services.execution.idempotency_lock import AcquiredSubmissionLock
from pmcopy.services.execution.order_planner import (
    PlannedExecutionOrder,
    fetch_fresh_market_bundle,
    quantize_marketable_price,
    quote_outside_band,
)
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult
from pmcopy.services.trading.trading_permission import TradingPausedError, TradingPermissionService
from pmcopy.utils.logger import redact_error_detail

if TYPE_CHECKING:
    from pmcopy.services.market_data.market_data_service import MarketDataService

_DEFERRED_ORDER_ACK_STATUSES = frozenset(
    {
        "live",
        "matched",
        "delayed",
        "submitted",
        "submitted_unconfirmed",
        "submitted_delayed",
        "filled",
        "order_status_live",
        "order_status_matched",
    }
)


def positive_int(raw: Any, default: int) -> int:
    """Coerce ``raw`` into a positive integer (`>= 1`), falling back to ``default``."""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(1, value)


def non_negative_int(raw: Any, default: int) -> int:
    """Coerce ``raw`` into a non-negative integer (`>= 0`), falling back to ``default``."""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(0, value)


def is_deferred_order_ack(response: dict[str, Any]) -> bool:
    """Return True when the venue accepted an order whose fill is not yet reported."""
    if response_matched_size(response) > 0:
        return False
    if not response_order_id(response):
        return False
    status = str(response.get("status") or "").strip().lower()
    return status in _DEFERRED_ORDER_ACK_STATUSES


def is_fak_repostable_no_fill(response: dict[str, Any]) -> bool:
    """Return True when a zero-fill FAK response safely permits refreshing the book."""
    if bool(response.get("ambiguous_post_failure")):
        return False
    if is_deferred_order_ack(response):
        return False
    if response_matched_size(response) > 0:
        return False
    status = str(response.get("status") or "").strip().lower()
    error = response_error_text(response)
    if "fak_no_match" in error or "order_not_fully_fillable" in error:
        return True
    if response_order_id(response):
        return False
    return status == "cancelled"


def level_size_at_price(levels: Any, price: Decimal | None) -> Decimal:
    """Return liquidity size at ``price`` from dict or sequence book levels."""
    if price is None:
        return Decimal("0")
    iterable: Iterable[tuple[object, object]]
    if isinstance(levels, dict):
        iterable = levels.items()
    else:
        iterable = ()
        if levels:
            iterable = (
                (
                    getattr(level, "price", None)
                    if not isinstance(level, dict)
                    else level.get("price"),
                    getattr(level, "size", None)
                    if not isinstance(level, dict)
                    else level.get("size"),
                )
                for level in levels
            )
    for raw_price, raw_size in iterable:
        try:
            level_price = Decimal(str(raw_price))
            level_size = Decimal(str(raw_size))
        except Exception:
            continue
        if level_price == price and level_size > 0:
            return level_size
    return Decimal("0")


def top_level_marketable_amount(bundle: Any, side: str, quote_price: Decimal) -> Decimal:
    """Return marketable amount at best level (USDC quote for BUY, shares for SELL)."""
    book = getattr(bundle, "book", None)
    if book is None:
        return Decimal("0")
    if side == "BUY":
        size = level_size_at_price(getattr(book, "asks", {}), quote_price)
        return size * quote_price
    return level_size_at_price(getattr(book, "bids", {}), quote_price)


class ExecutionSettlementProjector:
    """Project gateway responses into ExecutionResult and run non-live FAK reposts."""

    async def project(
        self,
        req: ExecutionRequest,
        plan: PlannedExecutionOrder,
        lock: AcquiredSubmissionLock,
        outcome: GatewaySubmissionOutcome,
        *,
        config: Any,
        market_data_service: MarketDataService | None,
        trading_permission: TradingPermissionService | None,
        save_submission_fn: Callable[..., Awaitable[None]],
        submit_gateway_fn: Callable[[MarketableOrderRequest], Awaitable[VenueSubmitResult]],
        emit_lifecycle_fn: Callable[..., None],
        logger: logging.Logger,
    ) -> ExecutionResult:
        """Project ``outcome`` onto a paper FAK result or ``submitted_unconfirmed`` handoff."""
        response = outcome.response
        last_error = outcome.last_error
        side = plan.side
        ec = getattr(config, "execution", None)

        effective_price = response_decimal(response, "effective_price")
        submitted_price = effective_price if effective_price > 0 else plan.worst_price

        if plan.requested_order_type is ExecutionOrderType.FAK and is_deferred_order_ack(response):
            venue_status = str(response.get("status") or "submitted_unconfirmed")
            emit_lifecycle_fn(
                req=req,
                side=side,
                order_id=response_order_id(response),
                worst_price=submitted_price,
                venue_status=venue_status,
                lifecycle_status=venue_status,
            )
            return ExecutionResult(
                accepted=True,
                order_id=response_order_id(response),
                status="submitted_unconfirmed",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=None,
                effective_price=submitted_price,
            )

        if not response.get("success"):
            if str(response.get("status") or "").lower() == "submitted_unconfirmed":
                emit_lifecycle_fn(
                    req=req,
                    side=side,
                    order_id=response_order_id(response),
                    worst_price=submitted_price,
                    venue_status=str(response.get("status") or "submitted_unconfirmed"),
                    lifecycle_status="submitted_unconfirmed",
                )
                return ExecutionResult(
                    accepted=True,
                    order_id=response_order_id(response),
                    status="submitted_unconfirmed",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=last_error,
                    effective_price=submitted_price,
                )
            if not (
                plan.requested_order_type is ExecutionOrderType.FAK
                and is_fak_repostable_no_fill(response)
            ):
                emit_lifecycle_fn(
                    req=req,
                    side=side,
                    order_id=None,
                    worst_price=submitted_price,
                    venue_status=str(response.get("status") or "failed"),
                    lifecycle_status=str(response.get("status") or "failed"),
                )
                failure_status = str(response.get("status") or "failed").lower()
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status=failure_status,
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=last_error or "order submission failed",
                    effective_price=submitted_price,
                )
            logger.info(
                "copy_fak_initial_zero_fill_retry | token=%s side=%s status=%s error=%s",
                req.token_id,
                side,
                str(response.get("status") or "cancelled").lower(),
                response.get("error"),
            )

        order_id = response_order_id(response)
        immediate_fak_fill = (
            plan.requested_order_type is ExecutionOrderType.FAK
            and getattr(ec, "trading_mode", None) != "live"
        )
        if immediate_fak_fill:
            return await self._project_immediate_fak_fill(
                req=req,
                plan=plan,
                lock=lock,
                response=response,
                order_id=order_id,
                submitted_price=submitted_price,
                ec=ec,
                market_data_service=market_data_service,
                trading_permission=trading_permission,
                save_submission_fn=save_submission_fn,
                submit_gateway_fn=submit_gateway_fn,
                emit_lifecycle_fn=emit_lifecycle_fn,
                logger=logger,
            )

        venue_raw = str(response.get("status") or "submitted").lower()
        if venue_raw == "live":
            lifecycle_status = "live"
        elif venue_raw == "matched":
            lifecycle_status = "matched"
        elif venue_raw == "delayed":
            lifecycle_status = "delayed"
        else:
            lifecycle_status = "submitted_unconfirmed"
        emit_lifecycle_fn(
            req=req,
            side=side,
            order_id=order_id,
            worst_price=submitted_price,
            venue_status=venue_raw,
            lifecycle_status=lifecycle_status,
        )
        return ExecutionResult(
            accepted=True,
            order_id=order_id,
            status="submitted_unconfirmed",
            requested_amount=req.amount,
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=None,
            effective_price=submitted_price,
        )

    async def _project_immediate_fak_fill(
        self,
        *,
        req: ExecutionRequest,
        plan: PlannedExecutionOrder,
        lock: AcquiredSubmissionLock,
        response: dict[str, Any],
        order_id: Any,
        submitted_price: Decimal,
        ec: Any,
        market_data_service: MarketDataService | None,
        trading_permission: TradingPermissionService | None,
        save_submission_fn: Callable[..., Awaitable[None]],
        submit_gateway_fn: Callable[[MarketableOrderRequest], Awaitable[VenueSubmitResult]],
        emit_lifecycle_fn: Callable[..., None],
        logger: logging.Logger,
    ) -> ExecutionResult:
        side = plan.side
        raw_status = str(response.get("status") or "").lower()
        is_paper_closed = raw_status == "closed" and bool(response.get("paper"))
        if not is_paper_closed and raw_status not in {
            "matched",
            "filled",
            "cancelled",
            "canceled",
            "expired",
            "order_not_fillable",
            "not_fillable",
        }:
            return ExecutionResult(
                accepted=True,
                order_id=order_id,
                status="submitted_unconfirmed",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=None,
                effective_price=submitted_price,
            )

        matched_size = response_matched_size(response)
        avg_fill_price = response_avg_price(response) or Decimal("0")
        total_fill_cost = matched_size * avg_fill_price
        last_no_fill_error = response.get("error")
        order_ids = [str(order_id)] if order_id and matched_size > 0 else []
        max_fak_attempts = (
            positive_int(getattr(ec, "exit_fak_max_attempts", 8), 8)
            if plan.forced_sell_exit
            else 1
        )

        for fak_attempt in range(2, max_fak_attempts + 1):
            remaining_amount = (
                req.amount - total_fill_cost if side == "BUY" else req.amount - matched_size
            )
            if remaining_amount <= Decimal("0.0001"):
                break
            if trading_permission is not None:
                try:
                    await trading_permission.require_submission_allowed(
                        purpose=plan.submission_purpose
                    )
                except TradingPausedError as exc:
                    last_no_fill_error = str(exc)
                    logger.warning(
                        "copy_fak_repost_blocked_by_trading_control | token=%s attempt=%d",
                        req.token_id,
                        fak_attempt,
                    )
                    break
            if market_data_service is None:
                last_no_fill_error = "market_data_service not wired for protective FAK exit"
                break
            try:
                next_bundle = await fetch_fresh_market_bundle(
                    market_data_service,
                    req.token_id,
                    side=req.side,
                    condition_id=getattr(req, "condition_id", None),
                    policy=plan.market_data_policy,
                )
            except Exception as exc:
                last_no_fill_error = redact_error_detail(exc)
                logger.warning(
                    "copy_fak_repost_market_data_failed | token=%s attempt=%d error=%s",
                    req.token_id,
                    fak_attempt,
                    last_no_fill_error,
                )
                break
            next_quote_price = (
                next_bundle.quote.best_ask if side == "BUY" else next_bundle.quote.best_bid
            )
            if next_quote_price is None or next_quote_price <= 0:
                logger.info(
                    "copy_fak_repost_no_liquidity | token=%s attempt=%d side=%s",
                    req.token_id,
                    fak_attempt,
                    side,
                )
                break
            if side == "BUY" and (
                (plan.min_share_price > 0 and next_quote_price < plan.min_share_price)
                or (plan.max_share_price > 0 and next_quote_price > plan.max_share_price)
            ):
                logger.info(
                    "copy_fak_repost_price_gate | token=%s attempt=%d price=%s",
                    req.token_id,
                    fak_attempt,
                    next_quote_price,
                )
                break
            next_top_level_amount = top_level_marketable_amount(
                next_bundle,
                side,
                next_quote_price,
            )
            if next_top_level_amount <= 0:
                logger.info(
                    "copy_fak_repost_no_top_level_liquidity | token=%s attempt=%d side=%s price=%s",
                    req.token_id,
                    fak_attempt,
                    side,
                    next_quote_price,
                )
                break
            if plan.protective_band_limit is not None and quote_outside_band(
                next_quote_price,
                plan.protective_band_limit,
                side,
            ):
                logger.info(
                    "protective_exit_price_band_rejected | "
                    "token=%s side=%s quote=%s limit=%s "
                    "leader=%s drift_pct=%s attempt=%d",
                    req.token_id,
                    side,
                    next_quote_price,
                    plan.protective_band_limit,
                    plan.metadata.get("leader_price") or plan.protective_band_limit,
                    plan.band_drift_pct,
                    fak_attempt,
                    extra={
                        "type": "protective_exit_price_band_rejected",
                        "token_id": req.token_id,
                        "side": side,
                        "quote_price": str(next_quote_price),
                        "band_limit": str(plan.protective_band_limit),
                        "drift_pct": str(plan.band_drift_pct),
                        "fak_attempt": fak_attempt,
                    },
                )
                break

            next_order_amount = min(remaining_amount, next_top_level_amount)
            next_tick_size = next_bundle.tick.tick_size
            next_worst_price = (
                next_quote_price * (Decimal("1") + plan.slippage)
                if side == "BUY"
                else next_quote_price * (Decimal("1") - plan.slippage)
            )
            next_worst_price = quantize_marketable_price(
                next_worst_price,
                next_tick_size,
                side,
            )
            if plan.protective_band_limit is not None:
                next_worst_price = MARKETABLE_TICK_POLICY.apply_caller_protection(
                    calculated_price=next_worst_price,
                    caller_limit=plan.protective_band_limit,
                    tick_size=next_tick_size,
                    side=side,
                )
            next_request = MarketableOrderRequest(
                token_id=req.token_id,
                side=side,
                size=next_order_amount,
                price_limit=next_worst_price,
                condition_id=getattr(req, "condition_id", None),
                tick_size=next_tick_size,
                neg_risk=next_bundle.tick.neg_risk,
                order_type=ExecutionOrderType.FAK,
            )
            try:
                next_submit = await submit_gateway_fn(next_request)
                next_response = submit_result_to_response(next_submit)
                next_idem = f"{lock.idem_key}:fak:{fak_attempt}"
                next_doc = dict(lock.request_doc)
                next_doc["amount"] = str(next_order_amount)
                next_doc["remaining_amount"] = str(remaining_amount)
                next_doc["worst_price"] = str(next_worst_price)
                next_doc["fak_attempt"] = fak_attempt
                await save_submission_fn(
                    idempotency_key=next_idem,
                    request=next_doc,
                    response=next_response,
                )
            except asyncio.CancelledError:
                ambiguity_doc = dict(lock.request_doc)
                ambiguity_doc["amount"] = str(next_order_amount)
                ambiguity_doc["remaining_amount"] = str(remaining_amount)
                ambiguity_doc["worst_price"] = str(next_worst_price)
                ambiguity_doc["fak_attempt"] = fak_attempt
                with contextlib.suppress(Exception):
                    await asyncio.shield(
                        save_submission_fn(
                            idempotency_key=f"{lock.idem_key}:fak:{fak_attempt}",
                            request=ambiguity_doc,
                            response={
                                "success": False,
                                "status": "submitted_unconfirmed",
                                "error": "cancelled_during_submit",
                            },
                        )
                    )
                raise
            except Exception as exc:
                err_detail = redact_error_detail(exc)
                ambiguity_response = {
                    "success": False,
                    "status": "submitted_unconfirmed",
                    "error": err_detail,
                }
                ambiguity_doc = dict(lock.request_doc)
                ambiguity_doc["amount"] = str(next_order_amount)
                ambiguity_doc["remaining_amount"] = str(remaining_amount)
                ambiguity_doc["worst_price"] = str(next_worst_price)
                ambiguity_doc["fak_attempt"] = fak_attempt
                try:
                    await save_submission_fn(
                        idempotency_key=f"{lock.idem_key}:fak:{fak_attempt}",
                        request=ambiguity_doc,
                        response=ambiguity_response,
                    )
                except Exception:
                    logger.debug("copy_fak_repost_ambiguity_persist_failed", exc_info=True)
                logger.warning(
                    "copy_fak_repost_failed | token=%s attempt=%d remaining=%s error=%s",
                    req.token_id,
                    fak_attempt,
                    str(remaining_amount),
                    err_detail,
                )
                break

            next_order_id = response_order_id(next_response)
            next_matched = response_matched_size(next_response)
            if next_matched > 0:
                if next_order_id:
                    order_ids.append(str(next_order_id))
                next_avg = response_avg_price(next_response) or Decimal("0")
                matched_size += next_matched
                total_fill_cost += next_matched * next_avg
                avg_fill_price = (
                    total_fill_cost / matched_size if matched_size > 0 else avg_fill_price
                )
                continue
            last_no_fill_error = next_response.get("error") or last_no_fill_error
            if is_fak_repostable_no_fill(next_response):
                logger.info(
                    "copy_fak_repost_zero_fill_continue | token=%s attempt=%d side=%s status=%s error=%s",
                    req.token_id,
                    fak_attempt,
                    side,
                    str(next_response.get("status") or "cancelled").lower(),
                    next_response.get("error"),
                )
                continue
            break

        if matched_size > 0:
            if side == "BUY":
                fill_status = (
                    "filled"
                    if total_fill_cost + Decimal("0.0001") >= req.amount
                    else "partially_filled"
                )
            else:
                fill_status = (
                    "filled"
                    if matched_size + Decimal("0.00000001") >= req.amount
                    else "partially_filled"
                )
            resolved_order_id = ",".join(order_ids) if order_ids else order_id
            emit_lifecycle_fn(
                req=req,
                side=side,
                order_id=resolved_order_id,
                worst_price=submitted_price,
                venue_status=fill_status,
                lifecycle_status=fill_status,
            )
            return ExecutionResult(
                accepted=True,
                order_id=resolved_order_id,
                status=fill_status,
                requested_amount=req.amount,
                filled_amount=matched_size,
                avg_price=avg_fill_price,
                fees_paid=Decimal("0"),
                error=None,
                effective_price=submitted_price,
            )

        venue_no_fill = str(response.get("status") or "cancelled").lower()
        emit_lifecycle_fn(
            req=req,
            side=side,
            order_id=order_id,
            worst_price=submitted_price,
            venue_status=venue_no_fill,
            lifecycle_status=venue_no_fill,
        )
        return ExecutionResult(
            accepted=False,
            order_id=order_id,
            status=venue_no_fill if venue_no_fill else "cancelled",
            requested_amount=req.amount,
            filled_amount=Decimal("0"),
            avg_price=None,
            fees_paid=Decimal("0"),
            error=str(last_no_fill_error or "FAK order did not fill"),
            effective_price=submitted_price,
        )


__all__ = [
    "ExecutionSettlementProjector",
    "is_deferred_order_ack",
    "is_fak_repostable_no_fill",
    "level_size_at_price",
    "non_negative_int",
    "positive_int",
    "top_level_marketable_amount",
]
