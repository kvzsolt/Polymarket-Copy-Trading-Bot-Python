"""Deterministic idempotency key derivation and atomic submission-lock acquisition (WP-A)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Any

from pmcopy.services.execution.order_planner import PlannedExecutionOrder
from pmcopy.services.execution.submission import (
    is_pending_submission_expired,
    is_retry_blocking_existing,
    pending_submission_ttl_seconds,
)
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult


def derive_intent_id_from_request(req: ExecutionRequest) -> str:
    """Return a deterministic SHA-256 intent identifier when ``req.signal_id`` is absent."""
    payload = "|".join(
        [
            req.user_id,
            req.leader_id or "",
            getattr(req, "condition_id", "") or "",
            req.token_id,
            req.side.upper(),
            str(req.amount),
            str(req.max_slippage_bps),
            req.reason or "",
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class AcquiredSubmissionLock:
    """Durable idempotency lock ownership and serialized request payload."""

    idem_key: str
    request_doc: dict[str, Any]
    existing: dict[str, Any] | None


class SubmissionLockCoordinator:
    """Check prior idempotency state and acquire atomic repository submission locks."""

    async def acquire(
        self,
        req: ExecutionRequest,
        plan: PlannedExecutionOrder,
        *,
        config: Any,
        order_repo: Any,
        idempotency_key_fn: Callable[..., str],
        logger: logging.Logger,
    ) -> AcquiredSubmissionLock | ExecutionResult:
        """Acquire exclusive submission ownership or return a fail-closed/dedup result."""
        resolved_intent_id = req.signal_id or ""
        if not resolved_intent_id:
            resolved_intent_id = derive_intent_id_from_request(req)
            logger.debug(
                "execute: signal_id missing — derived deterministic intent_id=%s",
                resolved_intent_id,
            )
        idem_key = idempotency_key_fn(
            user=req.user_id,
            condition_id=getattr(req, "condition_id", None),
            token_id=req.token_id,
            side=plan.side,
            effective_size=plan.effective_order_amount,
            effective_price=plan.worst_price,
            tick_size=plan.tick_size,
            intent_id=resolved_intent_id,
            policy_version=getattr(config, "policy_version", "") or "",
            environment=str(
                getattr(getattr(config, "execution", None), "trading_mode", "") or ""
            ),
        )

        if order_repo is None:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="order_repository_unavailable",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="order repository not wired — refusing execution",
            )

        existing = await order_repo.get_by_idempotency_key(idem_key)
        pending_lock_expired = bool(
            existing
            and not existing.get("order_id")
            and is_pending_submission_expired(existing, config=config)
        )
        if pending_lock_expired and existing is not None:
            existing_status = str(
                existing.get("status") or existing.get("response", {}).get("status") or "blocking"
            ).lower()
            logger.warning(
                "expiring stale %s submission lock | idempotency_key=%s | ttl=%ss",
                existing_status,
                idem_key,
                int(pending_submission_ttl_seconds(config)),
            )
        existing = await order_repo.get_by_idempotency_key(idem_key)

        if is_retry_blocking_existing(existing, config=config) and existing is not None:
            return ExecutionResult(
                accepted=True,
                order_id=existing.get("order_id"),
                status="duplicate_ignored",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=None,
            )

        request_doc = asdict(req)
        request_doc["amount"] = str(plan.effective_order_amount)
        request_doc["requested_amount"] = str(req.amount)
        request_doc["worst_price"] = str(plan.worst_price)
        if req.price_limit is not None:
            request_doc["price_limit"] = str(req.price_limit)
        request_doc["tick_size"] = str(plan.tick_size)
        request_doc["min_order_size"] = str(plan.min_order_size)
        request_doc["neg_risk"] = plan.neg_risk
        request_doc["effective_idempotency_key"] = idem_key
        request_doc["effective_side"] = plan.side
        request_doc["sizing_method"] = "effective_post_quantize"
        request_doc["amount_unit"] = "quote" if plan.side == "BUY" else "shares"
        request_doc["order_kind"] = "market"
        if plan.requested_order_type or plan.forced_sell_exit:
            request_doc["order_type"] = plan.requested_order_type or "FAK"
        if hasattr(req, "condition_id") and req.condition_id:
            request_doc["condition_id"] = req.condition_id

        has_atomic_lock = hasattr(order_repo, "try_acquire_submission_lock")
        if not has_atomic_lock:
            logger.error(
                "Order repository lacks atomic submission lock — failing closed "
                "to prevent duplicate submissions | idem_key=%s user_id=%s leader_id=%s",
                idem_key,
                req.user_id,
                req.leader_id,
            )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="atomic_lock_unavailable",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    "order repository does not support atomic submission locking; "
                    "cannot safely submit orders without it"
                ),
            )

        acquire_kwargs: dict[str, Any] = {}
        if pending_lock_expired:
            acquire_kwargs["stale_takeover_seconds"] = int(pending_submission_ttl_seconds(config))
        lock_acquired = await order_repo.try_acquire_submission_lock(
            idem_key,
            req.user_id,
            req.leader_id,
            request_doc,
            **acquire_kwargs,
        )

        if not lock_acquired:
            existing_now = await order_repo.get_by_idempotency_key(idem_key)
            if existing_now and existing_now.get("order_id"):
                return ExecutionResult(
                    accepted=True,
                    order_id=existing_now.get("order_id"),
                    status="duplicate_ignored",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=None,
                )
            lock_held = await order_repo.is_submission_locked(idem_key)
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="inflight_lock",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    "another task is currently submitting this order"
                    if lock_held
                    else "submission lock not acquired — refusing to submit (fail-closed)"
                ),
            )

        return AcquiredSubmissionLock(
            idem_key=idem_key,
            request_doc=request_doc,
            existing=existing,
        )


__all__ = [
    "AcquiredSubmissionLock",
    "SubmissionLockCoordinator",
    "derive_intent_id_from_request",
]
