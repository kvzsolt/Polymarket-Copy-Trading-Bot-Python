from __future__ import annotations

import asyncio
import time
from dataclasses import asdict
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
from typing import Any, TYPE_CHECKING

from config.app_config import AppConfig

if TYPE_CHECKING:
    from interfaces.clob_gateway import ClobGateway
    from services.market_data_service import (
        MarketDataBundle,
        MarketDataService,
        MarketDataNotReady,
        StaleDataError,
    )

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
from services.idempotency import compute_effective_key as _compute_effective_key
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
    calculate_book_depth_metrics as _calculate_book_depth_metrics,
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


def _derive_intent_id_from_request(req: ExecutionRequest) -> str:
    """Stable hash of the request shape, used as intent_id when signal_id is absent.

    Two retries of the same logical request produce the same hash and collide
    on the idempotency table, blocking duplicate submission. Distinct trade
    intents must carry distinct ``signal_id`` (the canonical disambiguator).
    """
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
) -> str:
    """Compute idempotency key from effective post-sizing order parameters.

    Delegates to :func:`services.idempotency.compute_effective_key`.
    """
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
    )


class ExecutionService:
    def __init__(
        self,
        clob_client,
        fee_service=None,
        order_repo=None,
        *,
        circuit_breaker: CircuitBreaker | None = None,
        config: AppConfig,
        clob_gateway: "ClobGateway | None" = None,
        market_data_service: "MarketDataService | None" = None,
    ):
        self.clob_client = clob_client
        self.fee_service = fee_service
        self.order_repo = order_repo
        self._config = config
        self._market_data_service = market_data_service

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

        # Typed concurrency cap; raw env fallback survives only for legacy
        # callers that bypass AppConfig.execution.
        submit_concurrency = getattr(
            getattr(config, "execution", None),
            "execution_submit_concurrency",
            None,
        )
        if not isinstance(submit_concurrency, int) or submit_concurrency <= 0:
            submit_concurrency = _positive_int(
                config.raw.get("EXECUTION_SUBMIT_CONCURRENCY", 2), 2
            )
        self._submit_semaphore = asyncio.Semaphore(submit_concurrency)

    async def _save_submission(
        self,
        *,
        idempotency_key: str,
        request: dict[str, Any],
        response: dict[str, Any] | Any,
    ) -> None:
        from storage.postgres.orders_repo import OrderSubmissionRecord

        sanitized_request = _sanitize_submission_request(request)
        sanitized_response = _sanitize_submission_response(response)
        status = str(
            sanitized_response.get("status")
            or ("submitted" if sanitized_response.get("success") else "failed")
        )
        record = OrderSubmissionRecord(
            idempotency_key=idempotency_key,
            user_id=str(
                sanitized_request.get("user_id")
                or sanitized_request.get("userId")
                or ""
            ),
            leader_id=str(
                sanitized_request.get("leader_id")
                or sanitized_request.get("leaderWallet")
                or ""
            )
            or None,
            order_id=sanitized_response.get("orderID"),
            status=status,
            request_payload=sanitized_request,
            response_payload=sanitized_response,
            last_error=str(sanitized_response.get("error"))
            if sanitized_response.get("error")
            else None,
        )
        await self.order_repo.save_submission(record)

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

        ec = self._config.execution

        # --- Single fresh-data fetch — fail closed if market data not ready ---
        if self._market_data_service is None:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="market_data_service not wired — refusing execution",
            )

        try:
            from services.market_data_service import MarketDataNotReady, StaleDataError
            bundle = await self._market_data_service.require_fresh_data(
                req.token_id,
                side=req.side,
                condition_id=getattr(req, "condition_id", None),
            )
        except Exception as exc:
            from services.market_data_service import MarketDataNotReady, StaleDataError
            if isinstance(exc, MarketDataNotReady):
                status = "market_not_ready"
            else:
                status = "stale_market_data"
            logger.warning(
                "market_data_not_executable | token=%s reason=%s",
                req.token_id,
                exc,
            )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status=status,
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=str(exc),
            )

        best_bid = bundle.quote.best_bid
        best_ask = bundle.quote.best_ask
        tick_size = bundle.tick.tick_size
        neg_risk = bundle.tick.neg_risk
        min_order_size = bundle.tick.min_order_size

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
            and best_bid
            and best_ask
            and best_bid > 0
        ):
            # Detect garbage/placeholder orderbook (e.g. bid=0.01, ask=0.99)
            if _is_thin_or_empty_book(best_bid, best_ask):
                logger.info(
                    "book_quality_gate_thin_book",
                    extra={
                        "type": "book_quality_gate_thin_book",
                        "token_id": req.token_id,
                        "best_bid": str(best_bid),
                        "best_ask": str(best_ask),
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
                        f"(bid={best_bid} ask={best_ask})"
                    ),
                )

            spread = _calculate_book_spread_metrics(
                best_bid=best_bid,
                best_ask=best_ask,
                tick_size=tick_size,
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
                        "best_bid": str(best_bid),
                        "best_ask": str(best_ask),
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
                        f"bid={best_bid} ask={best_ask})"
                    ),
                )
        elif spread_limit > 0 and (best_bid is None or best_ask is None):
            # Side-aware: BUY needs best_ask, SELL needs best_bid.
            # The non-required side being None is a warning, not a rejection.
            if side == "BUY" and best_ask is None:
                logger.info(
                    "book_quality_gate_no_book",
                    extra={
                        "type": "book_quality_gate_no_book",
                        "token_id": req.token_id,
                        "best_bid": str(best_bid),
                        "best_ask": str(best_ask),
                        "side": side,
                        "reason": "BUY requires best_ask which is missing",
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
                        f"no orderbook data for required side — bid={best_bid} ask={best_ask}"
                    ),
                )
            if side == "SELL" and best_bid is None:
                logger.info(
                    "book_quality_gate_no_book",
                    extra={
                        "type": "book_quality_gate_no_book",
                        "token_id": req.token_id,
                        "best_bid": str(best_bid),
                        "best_ask": str(best_ask),
                        "side": side,
                        "reason": "SELL requires best_bid which is missing",
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
                        f"no orderbook data for required side — bid={best_bid} ask={best_ask}"
                    ),
                )
            # The required side is present but the other side is missing.
            # This is a one-sided book — log a warning but allow execution.
            logger.info(
                "book_one_sided",
                extra={
                    "type": "book_one_sided",
                    "token_id": req.token_id,
                    "best_bid": str(best_bid),
                    "best_ask": str(best_ask),
                    "side": side,
                    "reason": "one-sided book — required side present, other side missing",
                },
            )

        # --- Book depth gate (Phase 4): require minimum side liquidity ---
        min_book_depth_raw = getattr(ec, "min_book_depth_usdc", 0) or 0
        try:
            min_book_depth = Decimal(str(min_book_depth_raw))
        except Exception:
            min_book_depth = Decimal("0")
        if min_book_depth > 0:
            band_pct_raw = getattr(ec, "book_depth_band_pct", 5) or 5
            try:
                band_pct = Decimal(str(band_pct_raw))
            except Exception:
                band_pct = Decimal("5")
            depth = _calculate_book_depth_metrics(
                bids=getattr(bundle.book, "bids", {}),
                asks=getattr(bundle.book, "asks", {}),
                best_bid=best_bid,
                best_ask=best_ask,
                band_pct=band_pct,
            )
            side_notional = depth.side_notional_usdc(side)
            if side_notional < min_book_depth:
                logger.info(
                    "book_quality_gate_thin_depth",
                    extra={
                        "type": "book_quality_gate_thin_depth",
                        "token_id": req.token_id,
                        "side": side,
                        "band_pct": str(depth.band_pct),
                        "band_low": str(depth.band_low),
                        "band_high": str(depth.band_high),
                        "side_notional_usdc": str(side_notional),
                        "side_levels_in_band": depth.side_levels(side),
                        "min_book_depth_usdc": str(min_book_depth),
                        "best_bid": str(best_bid),
                        "best_ask": str(best_ask),
                    },
                )
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="book_thin_depth",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        f"book depth too thin "
                        f"(side={side} notional={side_notional} < {min_book_depth} "
                        f"required within {depth.band_pct}% of mid; "
                        f"levels_in_band={depth.side_levels(side)})"
                    ),
                )

        # Price from fresh quote bundle
        quote_price = best_ask if side == "BUY" else best_bid
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
        worst_price = _quantize_price(worst_price, tick_size, side)

        # Effective idempotency key computed after sizing/rounding so
        # duplicate suppression represents the final executable order
        # (condition_id, token_id, effective worst price, tick_size, side).
        # Issue #94 + PR #105 review: intent_id must be deterministic per
        # request shape. A per-call uuid4() broke retry idempotency — a
        # network retry generated a fresh key, missed the dedup row, and
        # could POST a duplicate signed order. Derive a stable hash from
        # the request fields when signal_id is absent so the second
        # attempt collides with the first.
        resolved_intent_id = req.signal_id or ""
        if not resolved_intent_id:
            resolved_intent_id = _derive_intent_id_from_request(req)
            logger.debug(
                "execute: signal_id missing — derived deterministic intent_id=%s",
                resolved_intent_id,
            )
        idem_key = _effective_idempotency_key(
            user=req.user_id,
            condition_id=getattr(req, "condition_id", None),
            token_id=req.token_id,
            side=side,
            effective_size=req.amount,
            effective_price=worst_price,
            tick_size=tick_size,
            intent_id=resolved_intent_id,
            policy_version=getattr(self._config, "policy_version", "") or "",
        )

        existing = await self.order_repo.get_by_idempotency_key(idem_key)
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
            await self._save_submission(
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
        existing = await self.order_repo.get_by_idempotency_key(idem_key)

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
        request_doc["tick_size"] = str(tick_size)
        request_doc["neg_risk"] = neg_risk
        request_doc["effective_idempotency_key"] = idem_key
        request_doc["effective_side"] = side
        request_doc["sizing_method"] = "effective_post_quantize"
        if hasattr(req, "condition_id") and req.condition_id:
            request_doc["condition_id"] = req.condition_id

        last_error: str | None = None
        response: dict[str, Any] = {}

        # Atomically acquire submission lock BEFORE any retry loop to prevent the
        # race where two concurrent execute() calls both pass the "existing?" check
        # and both submit distinct signed orders.
        has_atomic_lock = hasattr(self.order_repo, "try_acquire_submission_lock")
        if has_atomic_lock:
            lock_acquired = await self.order_repo.try_acquire_submission_lock(
                idem_key, req.user_id, req.leader_id, request_doc
            )
        else:
            # Non-atomic locking is unsafe for live trading: two concurrent
            # tasks can both observe no blocking row and both submit signed
            # orders, causing duplicate submissions and potential double-exposure.
            # Fail closed to prevent that silent data-corruption path (issue #95).
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

        if not lock_acquired:
            existing_now = await self.order_repo.get_by_idempotency_key(idem_key)
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
            if has_atomic_lock and await self.order_repo.is_submission_locked(idem_key):
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
            cap = getattr(
                getattr(self._config, "execution", None),
                "execution_submit_concurrency",
                None,
            )
            if not isinstance(cap, int) or cap <= 0:
                cap = _positive_int(
                    self._config.raw.get("EXECUTION_SUBMIT_CONCURRENCY"), 2
                )
            self._submit_semaphore = asyncio.Semaphore(cap)

        async with self._submit_semaphore:
            pending_since = time.time()
            await self._save_submission(
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
                tick_size=tick_size,
                neg_risk=neg_risk,
            )

            for attempt in range(1, 4):
                try:
                    submit_result = await self.clob_gateway.submit_marketable_order(
                        gateway_request
                    )
                    response = _submit_result_to_response(submit_result)
                    if submit_result.success:
                        self.circuit_breaker.record_success()
                        await self._save_submission(
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
                        last_error = submit_result.error or "ambiguous post failure"
                        response["status"] = "submitted_unconfirmed"
                        await self._save_submission(
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
                        response = {
                            "success": False,
                            "status": "rejected",
                            "error": last_error,
                            "non_retryable": True,
                        }
                        await self._save_submission(
                            idempotency_key=idem_key,
                            request=request_doc,
                            response=response,
                        )
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
                    await self._save_submission(
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
                    await self._save_submission(
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
                    await self._save_submission(
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
                await self._save_submission(
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
                    self._emit_post_lifecycle(
                        req=req,
                        side=side,
                        order_id=response.get("orderID"),
                        worst_price=worst_price,
                        venue_status=str(response.get("status") or "submitted_unconfirmed"),
                        lifecycle_status="submitted_unconfirmed",
                    )
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
                self._emit_post_lifecycle(
                    req=req,
                    side=side,
                    order_id=None,
                    worst_price=worst_price,
                    venue_status=str(response.get("status") or "failed"),
                    lifecycle_status="failed",
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
            # Settlement is owned by the canonical reconciler.
            # Execution never self-confirms fills — it returns
            # submitted_unconfirmed and lets the reconciler poll
            # venue state, apply incremental deltas, and finalise
            # terminal statuses. The lifecycle log emits the POST-side
            # status (live / matched / delayed / submitted_unconfirmed)
            # so the user sees order progress immediately.
            venue_raw = str(response.get("status") or "submitted").lower()
            if venue_raw == "live":
                lifecycle_status = "live"
            elif venue_raw == "matched":
                lifecycle_status = "matched"
            elif venue_raw == "delayed":
                lifecycle_status = "delayed"
            else:
                lifecycle_status = "submitted_unconfirmed"
            self._emit_post_lifecycle(
                req=req,
                side=side,
                order_id=order_id,
                worst_price=worst_price,
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
            from utils.logger import log_order_lifecycle

            usdc_value: float | None = None
            try:
                usdc_value = float(req.amount) * float(worst_price or 0)
            except Exception:
                usdc_value = None
            log_order_lifecycle(
                logger,
                status=lifecycle_status,
                side=side,
                condition_id=getattr(req, "condition_id", None),
                asset=req.token_id,
                order_id=order_id,
                size=float(req.amount) if req.amount is not None else None,
                price=float(worst_price) if worst_price is not None else None,
                usdc_value=usdc_value,
                venue_status=venue_status,
                submission_state=lifecycle_status,
                signal_id=getattr(req, "signal_id", None),
            )
        except Exception:
            # Logging must never break execution.
            pass
