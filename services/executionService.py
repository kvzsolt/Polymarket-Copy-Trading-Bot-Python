from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from services.circuitBreaker import CircuitBreaker, CircuitOpen

from utils.logger import get_logger, log_api_error, log_api_response, redact_error_detail
from utils.createClobClient import _is_thin_or_empty_book

logger = get_logger("ExecutionService")

_CLOB_CIRCUIT = CircuitBreaker(
    name="clob_api",
    failure_threshold=5,
    cooldown_seconds=60,
)

_MAX_PERSISTED_FIELD_CHARS = 160
_MAX_PERSISTED_ERROR_CHARS = 512
_TRUNCATED_SUFFIX = "...[truncated]"

_PERSISTED_REQUEST_FIELDS = (
    "user_id",
    "leader_id",
    "token_id",
    "side",
    "amount",
    "max_slippage_bps",
    "reason",
    "signal_id",
    "worst_price",
    "tick_size",
    "neg_risk",
)

_PERSISTED_RESPONSE_FIELDS = (
    "success",
    "status",
    "orderID",
    "error",
    "pending_since",
    "pending_ttl_seconds",
    "pending_expires_at",
    "expired_at",
)


@dataclass(frozen=True)
class ExecutionRequest:
    user_id: str
    leader_id: str
    token_id: str
    side: str  # BUY or SELL
    amount: Decimal
    max_slippage_bps: int
    reason: str  # copy_trade, stop_loss, take_profit, rebalance
    signal_id: str | None = None


@dataclass
class ExecutionResult:
    accepted: bool
    order_id: str | None
    status: str
    requested_amount: Decimal
    filled_amount: Decimal
    avg_price: Decimal | None
    fees_paid: Decimal
    error: str | None = None


def _normalize_side(side: str) -> str:
    value = (side or "").strip().upper()
    if value not in ("BUY", "SELL"):
        raise ValueError(f"Unsupported side: {side}")
    return value


def _quantize_price(price: Decimal, tick_size: Decimal, side: str) -> Decimal:
    if tick_size <= 0:
        return price
    units = price / tick_size
    if side == "BUY":
        rounded_units = units.to_integral_value(rounding=ROUND_CEILING)
    else:
        rounded_units = units.to_integral_value(rounding=ROUND_FLOOR)
    return rounded_units * tick_size


_SUPPORTED_BOOK_SPREAD_MODES = {"absolute_bps", "relative_bps", "ticks"}


@dataclass(frozen=True)
class _BookSpreadMetrics:
    absolute: Decimal
    absolute_bps: int
    relative_bps: int | None
    ticks: int | None


def _resolve_book_spread_mode(raw_mode: Any) -> str:
    mode = str(raw_mode or "absolute_bps").strip().lower()
    if mode in _SUPPORTED_BOOK_SPREAD_MODES:
        return mode
    return "absolute_bps"


def _calculate_book_spread_metrics(
    *, best_bid: Decimal, best_ask: Decimal, tick_size: Decimal
) -> _BookSpreadMetrics:
    absolute = best_ask - best_bid
    absolute_bps = int(
        (absolute * Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING)
    )
    relative_bps: int | None = None
    if best_bid > 0:
        relative_bps = int(
            ((absolute / best_bid) * Decimal(10_000)).to_integral_value(
                rounding=ROUND_CEILING
            )
        )
    ticks: int | None = None
    if tick_size > 0:
        ticks = int((absolute / tick_size).to_integral_value(rounding=ROUND_CEILING))
    return _BookSpreadMetrics(
        absolute=absolute,
        absolute_bps=absolute_bps,
        relative_bps=relative_bps,
        ticks=ticks,
    )


def _idempotency_key(req: ExecutionRequest) -> str:
    payload = "|".join(
        [
            req.signal_id or "",
            req.user_id,
            req.leader_id,
            req.token_id,
            req.side.upper(),
            str(req.amount),
            str(req.max_slippage_bps),
            req.reason,
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _truncate_text(value: Any, *, max_chars: int) -> str:
    text = (
        str(value)
        .replace("\x00", " ")
        .replace("\r", " ")
        .replace("\n", " ")
        .replace("\t", " ")
    )
    if len(text) <= max_chars:
        return text
    keep = max(0, max_chars - len(_TRUNCATED_SUFFIX))
    return text[:keep] + _TRUNCATED_SUFFIX


def _sanitize_submission_value(
    value: Any, *, max_chars: int = _MAX_PERSISTED_FIELD_CHARS
) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    return _truncate_text(value, max_chars=max_chars)


def _sanitize_submission_request(request: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(request, dict):
        return {}
    sanitized: dict[str, Any] = {}
    for key in _PERSISTED_REQUEST_FIELDS:
        if key not in request:
            continue
        value = _sanitize_submission_value(request.get(key))
        if value is not None:
            sanitized[key] = value
    return sanitized


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _sanitize_submission_response(response: dict[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {
            "success": False,
            "status": "failed",
            "error": _truncate_text(response, max_chars=_MAX_PERSISTED_ERROR_CHARS),
        }

    sanitized: dict[str, Any] = {}
    for key in _PERSISTED_RESPONSE_FIELDS:
        if key not in response:
            continue
        raw_value = response.get(key)
        if key == "success":
            sanitized[key] = _coerce_bool(raw_value)
            continue
        if key in {"pending_since", "pending_expires_at", "expired_at"}:
            ts_value = _coerce_timestamp(raw_value)
            sanitized[key] = (
                ts_value
                if ts_value is not None
                else _sanitize_submission_value(raw_value)
            )
            continue
        if key == "error":
            error_value = _sanitize_submission_value(
                raw_value, max_chars=_MAX_PERSISTED_ERROR_CHARS
            )
            if error_value:
                sanitized[key] = error_value
            continue
        value = _sanitize_submission_value(raw_value)
        if value is not None:
            sanitized[key] = value

    if "success" not in sanitized:
        sanitized["success"] = _coerce_bool(response.get("success"))

    if "status" not in sanitized:
        sanitized["status"] = "submitted" if sanitized.get("success") else "failed"

    return sanitized


_RETRY_BLOCKING_STATUSES = {
    "pending",
    "submitted",
    "submitted_unconfirmed",
    "filled",
    "duplicate_ignored",
    "live",
    "matched",
}


def _pending_submission_ttl_seconds(config: AppConfig | None = None) -> float:
    if config is not None:
        return max(float(config.execution.pending_submission_ttl_seconds), 0.0)
    return 120.0


def _coerce_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            continue

    return None


def _pending_since(existing: dict[str, Any]) -> float | None:
    response = (
        existing.get("response") if isinstance(existing.get("response"), dict) else {}
    )
    for candidate in (
        existing.get("pending_since"),
        response.get("pending_since"),
        existing.get("updated_at"),
        existing.get("created_at"),
    ):
        ts = _coerce_timestamp(candidate)
        if ts is not None:
            return ts
    return None


def _pending_expires_at(existing: dict[str, Any]) -> float | None:
    response = (
        existing.get("response") if isinstance(existing.get("response"), dict) else {}
    )
    for candidate in (
        existing.get("pending_expires_at"),
        response.get("pending_expires_at"),
    ):
        ts = _coerce_timestamp(candidate)
        if ts is not None:
            return ts
    return None


def _build_pending_response(
    *, pending_since: float, ttl_seconds: float | None = None, config: AppConfig | None = None
) -> dict[str, Any]:
    ttl = (
        _pending_submission_ttl_seconds(config)
        if ttl_seconds is None
        else max(float(ttl_seconds), 0.0)
    )
    return {
        "success": False,
        "status": "pending",
        "pending_since": pending_since,
        "pending_ttl_seconds": ttl,
        "pending_expires_at": pending_since + ttl,
    }


def _is_pending_submission_expired(
    existing: dict[str, Any] | None,
    *,
    now_ts: float | None = None,
    pending_ttl_seconds: float | None = None,
    config: AppConfig | None = None,
) -> bool:
    if not existing:
        return False
    status = str(
        existing.get("status") or existing.get("response", {}).get("status") or ""
    ).lower()
    if existing.get("order_id"):
        return False
    if status not in _RETRY_BLOCKING_STATUSES:
        return False

    now_value = time.time() if now_ts is None else float(now_ts)
    explicit_expires_at = _pending_expires_at(existing)
    if explicit_expires_at is not None:
        return now_value >= explicit_expires_at

    ttl_seconds = (
        _pending_submission_ttl_seconds(config)
        if pending_ttl_seconds is None
        else float(pending_ttl_seconds)
    )
    if ttl_seconds <= 0:
        return True

    submitted_at = _pending_since(existing)
    if submitted_at is None:
        return True

    return (now_value - submitted_at) >= ttl_seconds


def _is_retry_blocking_existing(
    existing: dict[str, Any] | None,
    *,
    now_ts: float | None = None,
    pending_ttl_seconds: float | None = None,
    config: AppConfig | None = None,
) -> bool:
    if not existing:
        return False
    status = str(
        existing.get("status") or existing.get("response", {}).get("status") or ""
    ).lower()
    if existing.get("order_id"):
        return True
    if _is_pending_submission_expired(
        existing,
        now_ts=now_ts,
        pending_ttl_seconds=pending_ttl_seconds,
        config=config,
    ):
        return False
    return status in _RETRY_BLOCKING_STATUSES


class ExecutionService:
    def __init__(
        self,
        clob_client,
        market_meta_service,
        fee_service,
        order_repo,
        *,
        circuit_breaker: CircuitBreaker | None = None,
        config: AppConfig | None = None,
    ):
        self.clob_client = clob_client
        self.market_meta_service = market_meta_service
        self.fee_service = fee_service
        self.order_repo = order_repo
        self._config = config

        if circuit_breaker is not None:
            self.circuit_breaker = circuit_breaker
        elif config is not None:
            self.circuit_breaker = CircuitBreaker(
                name="clob_api",
                failure_threshold=config.execution.circuit_failure_threshold,
                cooldown_seconds=config.execution.circuit_cooldown_seconds,
            )
        else:
            self.circuit_breaker = _CLOB_CIRCUIT

    def _save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any] | Any,
    ) -> None:
        self.order_repo.save_submission(
            idempotency_key=idempotency_key,
            request=_sanitize_submission_request(request),
            response=_sanitize_submission_response(response),
        )

    async def _confirm_fill(self, order_id: str) -> dict[str, Any]:
        """Poll order status briefly and return best-effort fill details."""
        loop = asyncio.get_running_loop()
        last_error: Exception | None = None
        for _ in range(3):
            try:
                order = await loop.run_in_executor(
                    None,
                    lambda: self.clob_client.client.get_order(order_id),
                )
                if isinstance(order, dict):
                    status = str(order.get("status", "")).lower()
                    if status in {"filled", "matched"}:
                        return order
                    # Even canceled FOK may include matched/size.
                    if order.get("size_matched") or order.get("filled_size"):
                        return order
                await asyncio.sleep(0.15)
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.15)

        if last_error:
            logger.warning(
                "fill confirmation warning for %s (%s)",
                order_id,
                redact_error_detail(last_error),
            )
        return {}

    async def execute(self, req: ExecutionRequest) -> ExecutionResult:
        side = _normalize_side(req.side)
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

        if not self.circuit_breaker.allow_request():
            remaining = self.circuit_breaker.remaining_cooldown()
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

        ec = self._config.execution if self._config else None
        require_fresh_book = ec.execution_require_fresh_book if ec else True
        meta = await self.market_meta_service.get(
            req.token_id,
            refresh=require_fresh_book,
        )

        # --- Book quality gate: reject if spread is too wide ---
        spread_mode = _resolve_book_spread_mode(
            ec.book_spread_mode if ec else "absolute_bps"
        )
        max_spread_bps = int(ec.max_book_spread_bps if ec else 500)
        max_spread_ticks = int(ec.max_book_spread_ticks if ec else 0)
        spread_limit = max_spread_ticks if spread_mode == "ticks" else max_spread_bps
        spread_limit_unit = "ticks" if spread_mode == "ticks" else "bps"
        if (
            spread_limit > 0
            and meta.best_bid
            and meta.best_ask
            and meta.best_bid > 0
        ):
            # Detect garbage/placeholder orderbook (e.g. bid=0.01, ask=0.99)
            if _is_thin_or_empty_book(meta.best_bid, meta.best_ask):
                logger.info(
                    "book_quality_gate_thin_book",
                    extra={
                        "type": "book_quality_gate_thin_book",
                        "token_id": req.token_id,
                        "best_bid": str(meta.best_bid),
                        "best_ask": str(meta.best_ask),
                        "reason": "orderbook has no real liquidity (placeholder/dust orders only)",
                    },
                )
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="book_no_liquidity",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        f"no real liquidity — book appears empty or placeholder "
                        f"(bid={meta.best_bid} ask={meta.best_ask})"
                    ),
                )

            spread = _calculate_book_spread_metrics(
                best_bid=meta.best_bid,
                best_ask=meta.best_ask,
                tick_size=meta.tick_size,
            )
            if spread_mode == "relative_bps":
                measured_spread = spread.relative_bps
            elif spread_mode == "ticks":
                measured_spread = spread.ticks
            else:
                measured_spread = spread.absolute_bps

            if measured_spread is not None and measured_spread > spread_limit:
                logger.info(
                    "book_quality_gate_wide_spread",
                    extra={
                        "type": "book_quality_gate_wide_spread",
                        "token_id": req.token_id,
                        "spread_mode": spread_mode,
                        "spread_limit": spread_limit,
                        "spread_limit_unit": spread_limit_unit,
                        "measured_spread": measured_spread,
                        "absolute_spread": str(spread.absolute),
                        "absolute_bps": spread.absolute_bps,
                        "relative_bps": spread.relative_bps,
                        "spread_ticks": spread.ticks,
                        "best_bid": str(meta.best_bid),
                        "best_ask": str(meta.best_ask),
                    },
                )
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="book_spread_wide",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        "book spread too wide "
                        f"(mode={spread_mode}: {measured_spread} {spread_limit_unit} > "
                        f"{spread_limit} {spread_limit_unit} max; "
                        f"abs={spread.absolute} ({spread.absolute_bps} bps), "
                        f"rel={spread.relative_bps if spread.relative_bps is not None else 'n/a'} bps, "
                        f"ticks={spread.ticks if spread.ticks is not None else 'n/a'}; "
                        f"bid={meta.best_bid} ask={meta.best_ask})"
                    ),
                )
        elif spread_limit > 0 and (meta.best_bid is None or meta.best_ask is None):
            logger.info(
                "book_quality_gate_no_book",
                extra={
                    "type": "book_quality_gate_no_book",
                    "token_id": req.token_id,
                    "best_bid": str(meta.best_bid),
                    "best_ask": str(meta.best_ask),
                    "reason": "orderbook missing bid or ask — market may not exist on CLOB",
                },
            )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="book_no_liquidity",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    f"no orderbook data — bid={meta.best_bid} ask={meta.best_ask}"
                ),
            )

        quote = await self.clob_client.get_price(req.token_id, side)
        if not quote or quote.get("price") is None:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="unable to fetch live quote",
            )

        quote_price = Decimal(str(quote["price"]))
        min_share_price = Decimal(str(ec.min_share_price if ec else 0))
        if side == "BUY" and min_share_price > 0 and quote_price < min_share_price:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"price below min_share_price ({quote_price} < {min_share_price})",
            )

        slippage = Decimal(req.max_slippage_bps) / Decimal(10_000)
        if side == "BUY":
            worst_price = quote_price * (Decimal("1") + slippage)
        else:
            worst_price = quote_price * (Decimal("1") - slippage)
        worst_price = _quantize_price(worst_price, meta.tick_size, side)

        if side == "BUY" and meta.min_order_size > 0:
            requested_tokens = (
                req.amount / worst_price if worst_price > 0 else Decimal("0")
            )
            if requested_tokens + Decimal("1e-12") < meta.min_order_size:
                min_notional = meta.min_order_size * worst_price
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="rejected",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        "amount below min_order_size "
                        f"({meta.min_order_size} tokens ~= {min_notional:.4f} USDC at protected price {worst_price})"
                    ),
                )

        if (
            side == "SELL"
            and meta.min_order_size > 0
            and req.amount + Decimal("1e-12") < meta.min_order_size
        ):
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"amount below min_order_size ({meta.min_order_size})",
            )

        idem_key = _idempotency_key(req)
        existing = self.order_repo.get_by_idempotency_key(idem_key)
        if (
            existing
            and not existing.get("order_id")
            and _is_pending_submission_expired(existing, config=self._config)
        ):
            ttl_seconds = _pending_submission_ttl_seconds(self._config)
            ttl_seconds_log = int(ttl_seconds)
            existing_status = str(
                existing.get("status")
                or existing.get("response", {}).get("status")
                or "blocking"
            ).lower()
            logger.warning(
                "expiring stale %s submission lock | idempotency_key=%s | ttl=%ss",
                existing_status,
                idem_key,
                ttl_seconds_log,
            )
            stale_request = existing.get("request")
            if not isinstance(stale_request, dict):
                stale_request = {
                    "user_id": req.user_id,
                    "leader_id": req.leader_id,
                    "token_id": req.token_id,
                    "side": side,
                    "amount": str(req.amount),
                    "reason": req.reason,
                    "signal_id": req.signal_id,
                }
            pending_started_at = _pending_since(existing)
            if pending_started_at is None:
                pending_started_at = time.time()
            pending_expires_at = _pending_expires_at(existing)
            if pending_expires_at is None:
                pending_expires_at = pending_started_at + max(ttl_seconds, 0.0)
            self._save_submission(
                idempotency_key=idem_key,
                request=stale_request,
                response={
                    "success": False,
                    "status": "expired",
                    "error": f"stale {existing_status} submission expired after {ttl_seconds_log}s",
                    "expired_at": time.time(),
                    "pending_since": pending_started_at,
                    "pending_ttl_seconds": ttl_seconds,
                    "pending_expires_at": pending_expires_at,
                },
            )
            existing = self.order_repo.get_by_idempotency_key(idem_key)

        if _is_retry_blocking_existing(existing, config=self._config):
            existing_order_id = existing.get("order_id")
            return ExecutionResult(
                accepted=True,
                order_id=existing_order_id,
                status="duplicate_ignored",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=None,
            )

        side_const = BUY if side == "BUY" else SELL
        fee_bps = await self.fee_service.get_fee_bps(req.token_id)

        request_doc = asdict(req)
        request_doc["amount"] = str(req.amount)
        request_doc["worst_price"] = str(worst_price)
        request_doc["tick_size"] = str(meta.tick_size)
        request_doc["neg_risk"] = meta.neg_risk

        last_error: str | None = None
        response: dict[str, Any] = {}

        # Atomically acquire submission lock BEFORE any retry loop.
        # This prevents the race where two concurrent execute() calls both
        # pass the "existing?" check and both submit distinct signed orders.
        has_atomic_lock = hasattr(self.order_repo, 'try_acquire_submission_lock')
        if has_atomic_lock:
            lock_acquired = self.order_repo.try_acquire_submission_lock(
                idem_key, req.user_id, req.leader_id, request_doc
            )
        else:
            # Fallback for legacy/mock repos without atomic lock
            lock_acquired = existing is None or not _is_retry_blocking_existing(existing, config=self._config)

        if not lock_acquired:
            # Someone else holds the lock — check if it's a real submission or just a lock
            existing_now = self.order_repo.get_by_idempotency_key(idem_key)
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
            if has_atomic_lock and self.order_repo.is_submission_locked(idem_key):
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="inflight_lock",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error="another task is currently submitting this order",
                )

        pending_since = time.time()
        # Write pending row for crash visibility (even if we acquired the atomic lock above,
        # we still need the pending row so recovery knows about us).
        self._save_submission(
            idempotency_key=idem_key,
            request=request_doc,
            response=_build_pending_response(pending_since=pending_since, config=self._config),
        )

        for attempt in range(1, 4):
            signed_order = None
            post_called = False
            try:
                signed_order = await self.clob_client.create_market_order(
                    token_id=req.token_id,
                    side=side_const,
                    amount=float(req.amount),
                    max_price=float(worst_price),
                    tick_size=str(meta.tick_size),
                    neg_risk=meta.neg_risk,
                    order_type=OrderType.FOK,
                )
                post_called = True
                response = await self.clob_client.post_signed_order(
                    signed_order, OrderType.FOK
                )
                if response.get("success"):
                    self.circuit_breaker.record_success()
                    self._save_submission(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                    )
                    log_api_response(
                        logger,
                        method="POST",
                        url="post_signed_order",
                        status=200,
                        data=response,
                        meta={"side": side, "asset": req.token_id},
                    )
                    break
                # Check if the wrapper flagged this as an ambiguous post failure
                # (timeout/connection error after POST was sent to exchange).
                # In this case, the order MAY have been accepted — do NOT retry.
                if response.get("ambiguous_post_failure"):
                    response["status"] = "submitted_unconfirmed"
                    self._save_submission(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                    )
                    logger.warning(
                        "ambiguous post failure (timeout/connection after POST): %s",
                        response.get("error"),
                    )
                    break
                last_error = redact_error_detail(
                    response.get("error") or "unknown order submission error"
                )
            except Exception as exc:
                last_error = redact_error_detail(exc)
                # Log raw CLOB error body for diagnostics before redaction.
                raw_exc_body = ""
                if hasattr(exc, 'error_msg'):
                    raw_exc_body = str(exc.error_msg)
                elif hasattr(exc, '__cause__'):
                    raw_exc_body = str(exc.__cause__)
                if raw_exc_body:
                    logger.warning(
                        "raw_clob_error_on_%s | %s",
                        "create_order" if not post_called else "post_order",
                        raw_exc_body[:500],
                    )
                if post_called:
                    # post_signed_order was called but we got an exception —
                    # the order may have been accepted on the exchange.
                    # Do NOT retry; record as ambiguous.
                    response = {
                        "success": False,
                        "status": "submitted_unconfirmed",
                        "error": last_error,
                    }
                    self._save_submission(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                    )
                    log_api_response(
                        logger,
                        method="POST",
                        url="post_signed_order",
                        status=503,
                        error=last_error,
                        meta={"side": side, "asset": req.token_id, "state": "submitted_unconfirmed"},
                    )
                    logger.warning(
                        "ambiguous submission (post called but exception): %s",
                        last_error,
                    )
                    break
            await asyncio.sleep(min(0.15 * attempt, 0.30))
        else:
            # All retries exhausted without success
            self.circuit_breaker.record_failure()
            self._save_submission(
                idempotency_key=idem_key,
                request=request_doc,
                response=response
                or {"success": False, "error": last_error, "status": "failed"},
            )
            log_api_error(
                logger,
                side=req.side,
                error=last_error,
                asset=req.token_id,
                retry=3,
                max_retries=3,
            )

        if not response.get("success"):
            if str(response.get("status") or "").lower() == "submitted_unconfirmed":
                return ExecutionResult(
                    accepted=True,
                    order_id=response.get("orderID"),
                    status="submitted_unconfirmed",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=last_error,
                )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="failed",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=last_error or "order submission failed",
            )

        order_id = response.get("orderID")
        fill_info = await self._confirm_fill(order_id) if order_id else {}

        # Parse fill size — NEVER fallback to req.amount (that's notional for BUY, tokens for SELL)
        raw_fill = (
            fill_info.get("size_matched")
            or fill_info.get("filled_size")
            or fill_info.get("size")
        )
        fill_status = str(fill_info.get("status", "")).lower()

        if (
            raw_fill
            and Decimal(str(raw_fill)) > 0
        ):
            # Treat ANY positive fill as real, regardless of order status.
            # A canceled FOK with partial match is still a real fill.
            filled_amount = Decimal(str(raw_fill))
            confirmed = True
        else:
            # Order accepted but fill not confirmed — do NOT fabricate fill from request
            filled_amount = Decimal("0")
            confirmed = False

        avg_price = (
            Decimal(
                str(fill_info.get("avg_price") or fill_info.get("price") or worst_price)
            )
            if confirmed
            else None
        )

        if confirmed and avg_price and avg_price > 0:
            notional = filled_amount * avg_price
            fees_paid = (notional * Decimal(fee_bps) / Decimal(10_000)).quantize(
                Decimal("0.00000001")
            )
        else:
            notional = Decimal("0")
            fees_paid = Decimal("0")

        if order_id:
            self.order_repo.mark_fill(
                order_id=order_id,
                fill={
                    "filled_amount": str(filled_amount),
                    "avg_price": str(avg_price),
                    "fee_bps": fee_bps,
                    "fees_paid": str(fees_paid),
                },
            )

        result_status = "filled" if confirmed else "submitted_unconfirmed"
        if fill_status and fill_status not in {"", "filled", "matched"}:
            result_status = fill_status
        return ExecutionResult(
            accepted=True,
            order_id=order_id,
            status=result_status,
            requested_amount=req.amount,
            filled_amount=filled_amount,
            avg_price=avg_price,
            fees_paid=fees_paid,
            error=None,
        )
