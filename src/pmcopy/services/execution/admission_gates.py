"""Pre-flight operational, compliance, and circuit-breaker admission gates (WP-A)."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pmcopy.domain.price_bound import OrderSide
from pmcopy.services.circuitBreaker import CircuitBreaker
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult
from pmcopy.services.trading_permission import (
    SubmissionPurpose,
    TradingPausedError,
    TradingPermissionService,
)

_FORCED_EXIT_SOURCES = frozenset(
    {
        "risk_exit",
        "protective_exit",
    }
)

_FORCED_EXIT_REASONS = frozenset(
    {
        "risk_exit",
        "protective_exit",
        "advanced_risk_exit",
        "stop_loss",
        "trailing_stop",
    }
)


def normalize_side(side: str) -> OrderSide:
    """Normalize ``side`` to ``"BUY"`` or ``"SELL"``, raising ``ValueError`` otherwise."""
    value = (side or "").strip().upper()
    if value == "BUY":
        return "BUY"
    if value == "SELL":
        return "SELL"
    raise ValueError(f"Unsupported side: {side}")


def is_forced_sell_exit_request(req: ExecutionRequest, side: str) -> bool:
    """Return True when ``req`` is a risk-reducing protective SELL exit."""
    if side != "SELL":
        return False
    metadata = req.recovery_metadata if isinstance(req.recovery_metadata, dict) else {}
    source = str(metadata.get("source") or "").strip().lower()
    reason = str(req.reason or metadata.get("reason") or "").strip().lower()
    return source in _FORCED_EXIT_SOURCES or reason in _FORCED_EXIT_REASONS


@dataclass(frozen=True, slots=True)
class ValidatedAdmission:
    """Normalized pre-flight context after operational and compliance gates pass."""

    side: OrderSide
    forced_sell_exit: bool
    submission_purpose: SubmissionPurpose


class ExecutionAdmissionEvaluator:
    """Evaluate side/amount, kill switches, trading permissions, breaker, and geoblock."""

    async def evaluate(
        self,
        req: ExecutionRequest,
        *,
        config: Any,
        trading_permission: TradingPermissionService | None,
        circuit_breaker: CircuitBreaker,
        geoblock_gate: Any,
    ) -> ValidatedAdmission | ExecutionResult:
        """Return :class:`ValidatedAdmission` or a terminal rejected :class:`ExecutionResult`."""
        side = normalize_side(req.side)
        if req.amount <= 0:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="amount must be > 0",
            )

        forced_sell_exit = is_forced_sell_exit_request(req, side)
        submission_purpose = (
            SubmissionPurpose.PROTECTIVE_EXIT if forced_sell_exit else SubmissionPurpose.STANDARD
        )

        global_cfg = getattr(config, "app", None)
        global_kill = bool(getattr(global_cfg, "kill_switch", False))
        exec_kill = bool(getattr(getattr(config, "execution", None), "kill_switch", False))
        if global_kill or (exec_kill and not forced_sell_exit):
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="kill_switch",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="kill_switch engaged — submission refused",
            )

        if trading_permission is not None:
            try:
                await trading_permission.require_submission_allowed(purpose=submission_purpose)
            except TradingPausedError as exc:
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

        if not circuit_breaker.allow_request():
            remaining = circuit_breaker.remaining_cooldown()
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="circuit_open",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"CLOB circuit breaker open — retry in {remaining:.0f}s",
            )

        if geoblock_gate is not None and side == "BUY":
            geoblock_decision = await geoblock_gate.ensure_allowed()
            if geoblock_decision is not None and not geoblock_decision.allowed:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="geoblocked",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=f"geoblocked: {geoblock_decision.reason}",
                )

        return ValidatedAdmission(
            side=side,
            forced_sell_exit=forced_sell_exit,
            submission_purpose=submission_purpose,
        )


__all__ = [
    "ExecutionAdmissionEvaluator",
    "ValidatedAdmission",
    "is_forced_sell_exit_request",
    "normalize_side",
]
