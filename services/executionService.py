from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
from types import SimpleNamespace
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from interfaces.clob_gateway import ClobGateway

# CLOB venue access flows through the gateway; SDK-shaped types
# (``OrderType`` / ``OrderArgs`` / ``BUY`` / ``SELL`` constants) are not
# imported here. See ``interfaces/clob_gateway.py`` and issue #66.
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import MarketableOrderRequest
from services.circuitBreaker import CircuitBreaker, CircuitOpen
from utils.logger import get_logger, log_api_error, log_api_response, redact_error_detail
from utils.createClobClient import _is_thin_or_empty_book

# Re-exported from submodules for backward compatibility
from services.execution.types import (
    ExecutionRequest,
    ExecutionResult,
    BookSpreadMetrics as _BookSpreadMetrics,
    _SUPPORTED_BOOK_SPREAD_MODES,
)
from services.execution.book_spread import (
    resolve_book_spread_mode as _resolve_book_spread_mode,
    calculate_book_spread_metrics as _calculate_book_spread_metrics,
)
from services.execution.submission import (
    truncate_text as _truncate_text,
    sanitize_submission_value as _sanitize_submission_value,
    sanitize_submission_request as _sanitize_submission_request,
    sanitize_submission_response as _sanitize_submission_response,
    coerce_bool as _coerce_bool,
    coerce_timestamp as _coerce_timestamp,
    pending_submission_ttl_seconds as _pending_submission_ttl_seconds,
    pending_since as _pending_since,
    pending_expires_at as _pending_expires_at,
    build_pending_response as _build_pending_response,
    is_pending_submission_expired as _is_pending_submission_expired,
    is_retry_blocking_existing as _is_retry_blocking_existing,
)

logger = get_logger("ExecutionService")

_CLOB_CIRCUIT = CircuitBreaker(
    name="clob_api",
    failure_threshold=5,
    cooldown_seconds=60,
)


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(1, value)


def _non_negative_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(0, value)


def _parse_decimal_or_default(raw: Any, default: Decimal) -> Decimal:
    try:
        return Decimal(str(raw))
    except Exception:
        return default


def _default_execution_config() -> SimpleNamespace:
    return SimpleNamespace(
        execution_require_fresh_book=True,
        book_spread_mode="absolute_bps",
        max_book_spread_bps=2500,
        max_book_spread_ticks=0,
        min_share_price=0,
        max_share_price=0,
    )


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


def _submit_result_to_response(submit_result) -> dict[str, Any]:
    """Translate a ``VenueSubmitResult`` into the dict shape ExecutionService
    persists and returns. The dict view keeps downstream consumers
    (``order_repo``, fill confirmation, audit logs) working unchanged while
    SDK-shaped types stay behind the gateway boundary.
    """
    if submit_result.success:
        default_status = submit_result.status or "submitted"
    else:
        default_status = submit_result.status or "failed"
    return {
        "success": submit_result.success,
        "orderID": submit_result.order_id,
        "status": default_status,
        "error": submit_result.error,
        "ambiguous_post_failure": submit_result.ambiguous_post_failure,
        "non_retryable": submit_result.non_retryable,
        "raw": submit_result.raw,
    }


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
        clob_gateway: "ClobGateway | None" = None,
    ):
        self.clob_client = clob_client
        self.market_meta_service = market_meta_service
        self.fee_service = fee_service
        self.order_repo = order_repo
        self._config = config

        # Single CLOB venue boundary. ``runtime_bootstrap`` is expected to pass
        # a pre-constructed gateway so reconciliation, the execution
        # coordinator, and ExecutionService all share one instance. When no
        # gateway is supplied we build a default one over the legacy
        # ``clob_client`` wrapper to preserve backward-compatibility for
        # callers that have not yet been updated (the configless construction
        # itself is fail-closed in #73, not #66).
        if clob_gateway is None:
            from integrations.polymarket.clob_v2_gateway import ClobV2Gateway
            self.clob_gateway = ClobV2Gateway(clob_client, config)
        else:
            self.clob_gateway = clob_gateway

        # Always default to the shared module-level breaker so multi-user
        # runtimes get one venue-wide backoff window during a CLOB outage.
        # Only honor an explicit per-instance breaker passed via DI; do NOT
        # silently mint a fresh breaker just because typed config is present
        # (config is used for execution policy, not breaker isolation).
        if circuit_breaker is not None:
            self.circuit_breaker = circuit_breaker
        else:
            self.circuit_breaker = _CLOB_CIRCUIT

        self._submit_semaphore = asyncio.Semaphore(
            _positive_int(self._config.raw.get("EXECUTION_SUBMIT_CONCURRENCY", 2), 2) if self._config else 2
        )

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
        last_order: dict[str, Any] = {}
        terminal_no_fill_statuses = {"cancelled", "canceled", "expired", "rejected"}
        for _ in range(3):
            try:
                order = await loop.run_in_executor(
                    None,
                    lambda: self.clob_client.client.get_order(order_id),
                )
                if isinstance(order, dict):
                    last_order = order
                    status = str(order.get("status", "")).lower()
                    if status in {"filled", "matched"}:
                        return order
                    # Even canceled FOK may include matched/size.
                    if order.get("size_matched") or order.get("filled_size"):
                        return order
                    if status in terminal_no_fill_statuses:
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
        return last_order

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

        if self._config is None:
            logger.error(
                "execution_rejected_config_missing",
                extra={"type": "execution_rejected_config_missing", "token_id": req.token_id},
            )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="execution config not provided — refusing to execute (fail-closed)",
            )
        ec = self._config.execution
        require_fresh_book = ec.execution_require_fresh_book
        meta = await self.market_meta_service.get(
            req.token_id,
            refresh=require_fresh_book,
        )

        # --- Book quality gate: reject if spread is too wide ---
        spread_mode = _resolve_book_spread_mode(ec.book_spread_mode)
        try:
            max_spread_bps = int(ec.max_book_spread_bps)
        except (TypeError, ValueError):
            logger.warning(
                "invalid_max_book_spread_bps",
                extra={"value": getattr(ec, "max_book_spread_bps", None)},
            )
            max_spread_bps = 500
        try:
            max_spread_ticks = int(ec.max_book_spread_ticks)
        except (TypeError, ValueError):
            logger.warning(
                "invalid_max_book_spread_ticks",
                extra={"value": getattr(ec, "max_book_spread_ticks", None)},
            )
            max_spread_ticks = 0
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
        min_share_price = _parse_decimal_or_default(
            ec.min_share_price if ec else 0,
            Decimal("0"),
        )
        max_share_price = _parse_decimal_or_default(
            ec.max_share_price if ec else 0,
            Decimal("0"),
        )
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
        if side == "BUY" and max_share_price > 0 and quote_price > max_share_price:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"price above max_share_price ({quote_price} > {max_share_price})",
            )

        slippage = Decimal(req.max_slippage_bps) / Decimal(10_000)
        if side == "BUY":
            worst_price = quote_price * (Decimal("1") + slippage)
        else:
            worst_price = quote_price * (Decimal("1") - slippage)
        worst_price = _quantize_price(worst_price, meta.tick_size, side)

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

        # ``side`` is already the canonical "BUY"/"SELL" string (see
        # ``_normalize_side``). The gateway request consumes the same form;
        # SDK constants are an implementation detail of the gateway.
        fee_bps = await self.fee_service.get_fee_bps(req.token_id)

        request_doc = asdict(req)
        request_doc["amount"] = str(req.amount)
        request_doc["worst_price"] = str(worst_price)
        request_doc["tick_size"] = str(meta.tick_size)
        request_doc["neg_risk"] = meta.neg_risk

        last_error: str | None = None
        response: dict[str, Any] = {}

        # Atomically acquire submission lock BEFORE any retry loop to prevent the
        # race where two concurrent execute() calls both pass the "existing?" check
        # and both submit distinct signed orders.
        has_atomic_lock = hasattr(self.order_repo, "try_acquire_submission_lock")
        if has_atomic_lock:
            lock_acquired = self.order_repo.try_acquire_submission_lock(
                idem_key, req.user_id, req.leader_id, request_doc
            )
        else:
            lock_acquired = existing is None or not _is_retry_blocking_existing(
                existing, config=self._config
            )

        if not lock_acquired:
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

        if not hasattr(self, "_submit_semaphore"):
            self._submit_semaphore = asyncio.Semaphore(
                _positive_int(self._config.raw.get("EXECUTION_SUBMIT_CONCURRENCY"), 2) if self._config else 2
            )

        async with self._submit_semaphore:
            pending_since = time.time()
            self._save_submission(
                idempotency_key=idem_key,
                request=request_doc,
                response=_build_pending_response(
                    pending_since=pending_since, config=self._config
                ),
            )

            gateway_request = MarketableOrderRequest(
                token_id=req.token_id,
                side=side,
                size=req.amount,
                price_limit=worst_price,
                condition_id=getattr(req, "condition_id", None),
                tick_size=meta.tick_size,
                neg_risk=meta.neg_risk,
            )

            for attempt in range(1, 4):
                try:
                    submit_result = await self.clob_gateway.submit_marketable_order(
                        gateway_request
                    )
                    response = _submit_result_to_response(submit_result)
                    if submit_result.success:
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
                    if submit_result.ambiguous_post_failure:
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
                    last_error = (
                        submit_result.error or "unknown order submission error"
                    )
                    if submit_result.non_retryable:
                        logger.warning(
                            "non-retryable CLOB rejection, skipping retries: %s",
                            last_error,
                        )
                        break
                except VenueRateLimitError as exc:
                    last_error = redact_error_detail(exc)
                    logger.warning("clob rate limited (attempt %s): %s", attempt, last_error)
                except (VenueAuthError, VenueInvalidOrderError) as exc:
                    last_error = redact_error_detail(exc)
                    response = {
                        "success": False,
                        "status": "rejected",
                        "error": last_error,
                        "non_retryable": True,
                    }
                    self._save_submission(
                        idempotency_key=idem_key,
                        request=request_doc,
                        response=response,
                    )
                    logger.warning(
                        "non-retryable CLOB rejection, skipping retries: %s",
                        last_error,
                    )
                    break
                except VenueUnavailableError as exc:
                    last_error = redact_error_detail(exc)
                    # Treat as ambiguous: the POST may have hit the wire and
                    # we lost the response. A retry would risk duplicate fills.
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
                        meta={
                            "side": side,
                            "asset": req.token_id,
                            "state": "submitted_unconfirmed",
                        },
                    )
                    logger.warning(
                        "ambiguous submission (POST possibly delivered): %s",
                        last_error,
                    )
                    break
                except Exception as exc:
                    # Defensive: anything not classified by the gateway is
                    # treated as ambiguous to avoid duplicate-fill risk.
                    last_error = redact_error_detail(exc)
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
                    logger.warning(
                        "unclassified gateway exception (treating as ambiguous): %s",
                        last_error,
                    )
                    break
                await asyncio.sleep(min(0.15 * attempt, 0.30))
            else:
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

        # Parse fill size — never fall back to req.amount (BUY=notional, SELL=tokens).
        raw_fill = (
            fill_info.get("size_matched")
            or fill_info.get("filled_size")
            or fill_info.get("size")
        )
        fill_status = str(fill_info.get("status", "")).lower()

        if raw_fill and Decimal(str(raw_fill)) > 0:
            filled_amount = Decimal(str(raw_fill))
            confirmed = True
        else:
            filled_amount = Decimal("0")
            confirmed = False

        avg_price = (
            Decimal(
                str(
                    fill_info.get("avg_price")
                    or fill_info.get("price")
                    or worst_price
                )
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
