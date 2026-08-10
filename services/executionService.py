from __future__ import annotations

import asyncio
import random
import time
from dataclasses import asdict, is_dataclass, replace
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
from typing import Any, TYPE_CHECKING

from config.app_config import AppConfig
from domain.units import BASIS_POINTS_PER_UNIT

if TYPE_CHECKING:
    from interfaces.clob_gateway import ClobGateway
    from services.market_data_service import MarketDataService

# CLOB venue access flows through the gateway; SDK-shaped types
# (``OrderType`` / ``OrderArgs`` / ``BUY`` / ``SELL`` constants) are not
# imported here. See ``interfaces/clob_gateway.py`` and issue #66.
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import MarketableOrderRequest
from services.circuitBreaker import CircuitBreaker
from services.idempotency import compute_effective_key as _compute_effective_key
from services.trading_permission import TradingPausedError, TradingPermissionService
from utils.logger import get_logger, log_api_error, log_api_response, redact_error_detail
from services.market_data_quality import is_thin_or_empty_book as _is_thin_or_empty_book

# Re-exported from submodules for backward compatibility
from services.execution.types import (
    ExecutionRequest,
    ExecutionResult,
)
from services.execution.book_spread import (
    resolve_book_spread_mode as _resolve_book_spread_mode,
    calculate_book_spread_metrics as _calculate_book_spread_metrics,
    calculate_book_depth_metrics as _calculate_book_depth_metrics,
)
from services.execution.submission import (
    truncate_text as _truncate_text,  # noqa: F401 - compatibility export
    sanitize_submission_request as _sanitize_submission_request,
    sanitize_submission_response as _sanitize_submission_response,
    pending_submission_ttl_seconds as _pending_submission_ttl_seconds,
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


_FORCED_EXIT_SOURCES = frozenset({
    "risk_exit",
    "protective_exit",
})

_FORCED_EXIT_REASONS = frozenset({
    "risk_exit",
    "protective_exit",
    "advanced_risk_exit",
    "stop_loss",
    "trailing_stop",
})


def _is_forced_sell_exit_request(req: ExecutionRequest, side: str) -> bool:
    if side != "SELL":
        return False
    metadata = req.recovery_metadata if isinstance(req.recovery_metadata, dict) else {}
    source = str(metadata.get("source") or "").strip().lower()
    reason = str(req.reason or metadata.get("reason") or "").strip().lower()
    return source in _FORCED_EXIT_SOURCES or reason in _FORCED_EXIT_REASONS


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
    raw = submit_result.raw if isinstance(submit_result.raw, dict) else {}
    raw_data = raw.get("data") if isinstance(raw.get("data"), dict) else {}

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
        "raw": submit_result.raw,
    }


def _response_order_id(response: dict[str, Any]) -> Any:
    return response.get("orderID") or response.get("order_id") or response.get("id")


def _response_decimal(response: dict[str, Any], *keys: str) -> Decimal:
    for key in keys:
        value = response.get(key)
        if value is None:
            continue
        try:
            return Decimal(str(value))
        except Exception:
            continue
    return Decimal("0")


def _response_matched_size(response: dict[str, Any]) -> Decimal:
    return _response_decimal(
        response,
        "size_matched",
        "filled_size",
        "matched_size",
        "matched_amount",
    )


def _response_avg_price(response: dict[str, Any]) -> Decimal | None:
    value = _response_decimal(response, "avg_price", "avg_fill_price")
    return value if value > 0 else None


def _response_error_text(response: dict[str, Any]) -> str:
    value = response.get("error")
    return str(value).strip().lower() if value is not None else ""


_DEFERRED_ORDER_ACK_STATUSES = {
    "live",
    "matched",
    "delayed",
    "submitted",
    "submitted_unconfirmed",
    "submitted_delayed",
    "order_status_live",
    "order_status_matched",
}


def _is_deferred_order_ack(response: dict[str, Any]) -> bool:
    """Whether the venue accepted an order whose fill is not known yet.

    A FAK POST can return ``delayed``/``matched`` with an order id before the
    matched amount is queryable. Treat that as reconciliation-owned, not as a
    zero-fill cancellation that should be immediately reposted.
    """
    if _response_matched_size(response) > 0:
        return False
    if not _response_order_id(response):
        return False
    status = str(response.get("status") or "").strip().lower()
    return status in _DEFERRED_ORDER_ACK_STATUSES


def _is_fak_repostable_no_fill(response: dict[str, Any]) -> bool:
    """Whether a zero-fill FAK response should refresh the book and retry."""
    if bool(response.get("ambiguous_post_failure")):
        return False
    if _is_deferred_order_ack(response):
        return False
    if _response_matched_size(response) > 0:
        return False
    status = str(response.get("status") or "").strip().lower()
    error = _response_error_text(response)
    if "fak_no_match" in error or "order_not_fully_fillable" in error:
        # Explicit no-match: nothing filled, safe to refresh the book and retry.
        return True
    # P1-p: a bare 'cancelled' carrying an order id but NO explicit no-match
    # signal may be a partial-fill-then-cancel whose filled amount the POST
    # response does not report (size_matched is GET-only; the venue returns
    # makingAmount/takingAmount, which this path does not parse). Reposting
    # would double-buy the already-filled portion, so defer to the reconciler —
    # it settles the real fill via the order id. A cancel with no order id has
    # nothing to reconcile and stays repostable.
    if _response_order_id(response):
        return False
    return status == "cancelled"


def _level_size_at_price(levels: Any, price: Decimal | None) -> Decimal:
    if price is None:
        return Decimal("0")
    if isinstance(levels, dict):
        iterable = levels.items()
    else:
        iterable = []
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


def _top_level_marketable_amount(bundle: Any, side: str, quote_price: Decimal) -> Decimal:
    """Return the request amount supported by the current best level.

    Polymarket market-order ``amount`` is USDC for BUY and shares for SELL, so
    BUY caps by ``best_ask_size * best_ask`` while SELL caps by bid size.
    """
    book = getattr(bundle, "book", None)
    if book is None:
        return Decimal("0")
    if side == "BUY":
        size = _level_size_at_price(getattr(book, "asks", {}), quote_price)
        return size * quote_price
    size = _level_size_at_price(getattr(book, "bids", {}), quote_price)
    return size


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
        geoblock_gate=None,
        trading_permission: TradingPermissionService | None = None,
    ):
        if config is None:
            raise TypeError("ExecutionService requires a typed AppConfig")
        self.clob_client = clob_client
        self.fee_service = fee_service
        self.order_repo = order_repo
        self._config = config
        self._market_data_service = market_data_service
        # P1-j: optional trade-path geoblock gate (services.compliance.GeoblockGate).
        # None = no enforcement (preserves the configless / test default).
        self._geoblock_gate = geoblock_gate
        self._trading_permission = trading_permission

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
            # P2-d: honor ``execution.circuit_failure_threshold`` from typed
            # config on the shared module breaker so the config value is not
            # dead (it previously hardcoded failure_threshold=5). We only
            # reconfigure the shared default — an explicitly injected breaker
            # above keeps its own threshold. The shared instance is preserved
            # (we mutate the threshold, not the object) so multi-user runtimes
            # still share one venue-wide backoff window.
            configured_threshold = getattr(
                getattr(config, "execution", None),
                "circuit_failure_threshold",
                None,
            )
            if isinstance(configured_threshold, int) and configured_threshold > 0:
                _CLOB_CIRCUIT.failure_threshold = configured_threshold
            self.circuit_breaker = _CLOB_CIRCUIT

        submit_concurrency = getattr(
            getattr(config, "execution", None),
            "execution_submit_concurrency",
            None,
        )
        if not isinstance(submit_concurrency, int) or submit_concurrency <= 0:
            submit_concurrency = 2
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
            order_id=_response_order_id(sanitized_response),
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

        # C1 — kill switches enforced at act-time, immediately before any
        # venue POST. The global master (config.global_kill_switch) supersedes
        # everything: the bot observes but submits nothing, including
        # protective SELL exits (a true full stop). The execution-level switch
        # (config.execution.kill_switch) refuses BUY entries but still allows
        # protective SELL exits — closing a position is risk-reducing and must
        # not be stranded by a hot-stop (mirrors geoblock semantics).
        forced_sell_exit = _is_forced_sell_exit_request(req, side)
        global_cfg = getattr(self._config, "app", None)
        global_kill = bool(getattr(global_cfg, "kill_switch", False))
        exec_kill = bool(getattr(self._config.execution, "kill_switch", False))
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

        if self._trading_permission is not None:
            try:
                await self._trading_permission.require_submission_allowed()
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

        # P1-j: consult the geoblock gate on the trade path (not just at boot)
        # and revalidate on expiry. Only BUY *entries* are blocked in a
        # geoblocked region — a protective SELL exit is risk-reducing and must
        # never be stranded by a compliance gate. The gate is fail-closed and
        # opt-in; ``None`` means enforcement is disabled.
        if self._geoblock_gate is not None and side == "BUY":
            geoblock_decision = await self._geoblock_gate.ensure_allowed()
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

        ec = self._config.execution
        metadata = req.recovery_metadata if isinstance(req.recovery_metadata, dict) else {}
        requested_order_type = str(
            req.order_type or metadata.get("order_type") or ""
        ).strip().upper() or None
        instant_fak = requested_order_type == "FAK" and req.price_limit is not None

        # --- Single fresh-data fetch — fail closed if market data not ready ---
        # Copy-trade FAK orders still need a current executable side of book,
        # but they intentionally do not use the REST-snapshot spread cap. FAK
        # is the fill-control mechanism here: buy the available best ask / sell
        # the available best bid, cancel the rest, then refresh before reposts.
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

        if instant_fak:
            caller_price_limit = Decimal(str(req.price_limit))
            if caller_price_limit <= 0:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="rejected",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error="price_limit must be > 0 for instant FAK execution",
                )

        try:
            from services.market_data_service import MarketDataNotReady
            market_data_policy = None
            if instant_fak or forced_sell_exit:
                base_policy = getattr(self._market_data_service, "_policy", None)
                if base_policy is not None and is_dataclass(base_policy):
                    market_data_policy = replace(
                        base_policy,
                        rest_snapshot_max_spread_bps=1_000_000,
                        require_book_depth=False,
                    )
            market_data_kwargs = {
                "side": req.side,
                "condition_id": getattr(req, "condition_id", None),
            }
            if market_data_policy is not None:
                market_data_kwargs["policy"] = market_data_policy
            bundle = await self._market_data_service.require_fresh_data(
                req.token_id,
                **market_data_kwargs,
            )
        except Exception as exc:
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
        quote_price = best_ask if side == "BUY" else best_bid

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
            not instant_fak
            and not forced_sell_exit
            and spread_limit > 0
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
        elif not instant_fak and spread_limit > 0 and (best_bid is None or best_ask is None):
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
        if min_book_depth > 0 and not forced_sell_exit and not instant_fak:
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

        if quote_price is None or quote_price <= 0:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="book_no_liquidity",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    f"no executable {'ask' if side == 'BUY' else 'bid'} "
                    f"for {side} — bid={best_bid} ask={best_ask}"
                ),
            )
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

        slippage = Decimal(req.max_slippage_bps) / BASIS_POINTS_PER_UNIT
        if side == "BUY":
            worst_price = quote_price * (Decimal("1") + slippage)
        else:
            worst_price = quote_price * (Decimal("1") - slippage)
        worst_price = _quantize_price(worst_price, tick_size, side)
        # H2 — honor the caller's explicit price_limit (the leader/signal
        # price) as a hard cap on the fill. Previously the price_limit was
        # validated for positivity then ignored; the gateway got the
        # quote-derived worst_price instead, so the leader price never capped
        # the fill. Cap worst_price at price_limit for BUY (never pay more
        # than the signal) and floor it at price_limit for SELL (never sell
        # below the signal).
        if instant_fak and req.price_limit is not None:
            try:
                caller_cap = Decimal(str(req.price_limit))
                if caller_cap > 0:
                    if side == "BUY":
                        worst_price = min(worst_price, caller_cap)
                    else:
                        worst_price = max(worst_price, caller_cap)
                    worst_price = _quantize_price(worst_price, tick_size, side)
            except Exception:
                logger.debug("price_limit_cap_apply_failed", exc_info=True)

        effective_order_amount = req.amount
        if instant_fak:
            top_level_amount = _top_level_marketable_amount(bundle, side, quote_price)
            if top_level_amount <= 0:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="book_no_liquidity",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        f"no executable top-level liquidity for {side} "
                        f"at {quote_price}"
                    ),
                )
            effective_order_amount = min(req.amount, top_level_amount)
            logger.info(
                "copy_fak_sized_from_fresh_book | token=%s side=%s requested=%s "
                "order_amount=%s quote=%s top_level_amount=%s spread_gate=disabled",
                req.token_id,
                side,
                req.amount,
                effective_order_amount,
                quote_price,
                top_level_amount,
            )

        # M5 — BUY min-size check is a unit mismatch: ``effective_order_amount``
        # is USDC spend for BUY but shares for SELL, while ``min_order_size`` is
        # always share-denominated. Compare in shares: for BUY, the share count
        # is ``effective_order_amount / quote_price``. Previously a 3 USDC BUY
        # at 0.10 (≈30 shares) was rejected against a 5-share minimum, while a
        # 6 USDC BUY at 0.95 (≈6.3 shares) passed — both wrong directions.
        if side == "BUY" and quote_price > 0:
            min_check_amount = effective_order_amount / quote_price
        else:
            min_check_amount = effective_order_amount
        if min_check_amount < min_order_size:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="size_below_min",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    f"effective size {min_check_amount} is below venue minimum "
                    f"{min_order_size}"
                ),
            )

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
            effective_size=effective_order_amount,
            effective_price=worst_price,
            tick_size=tick_size,
            intent_id=resolved_intent_id,
            policy_version=getattr(self._config, "policy_version", "") or "",
        )

        existing = await self.order_repo.get_by_idempotency_key(idem_key)
        pending_lock_expired = bool(
            existing
            and not existing.get("order_id")
            and _is_pending_submission_expired(existing, config=self._config)
        )
        if pending_lock_expired:
            # The prior submission lock for this key passed its pending TTL with
            # no order_id — a dead, never-submitted lock. Do NOT destructively
            # re-mark the row (e.g. status="expired") here: under concurrency a
            # late re-mark clobbers a peer's fresh takeover and lets multiple
            # retriers each submit (duplicate orders). It is also unnecessary —
            # is_retry_blocking_existing (below) already treats an expired
            # pending row as non-blocking, and try_acquire_submission_lock takes
            # the dead lock over atomically at the pending-TTL threshold (exactly
            # one winner; see the concurrent-post-expiry stress test). Record it
            # for observability only.
            existing_status = str(
                existing.get("status")
                or existing.get("response", {}).get("status")
                or "blocking"
            ).lower()
            logger.warning(
                "expiring stale %s submission lock | idempotency_key=%s | ttl=%ss",
                existing_status,
                idem_key,
                int(_pending_submission_ttl_seconds(self._config)),
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
        request_doc = asdict(req)
        request_doc["amount"] = str(effective_order_amount)
        request_doc["requested_amount"] = str(req.amount)
        request_doc["worst_price"] = str(worst_price)
        if req.price_limit is not None:
            request_doc["price_limit"] = str(req.price_limit)
        request_doc["tick_size"] = str(tick_size)
        request_doc["neg_risk"] = neg_risk
        request_doc["effective_idempotency_key"] = idem_key
        request_doc["effective_side"] = side
        request_doc["sizing_method"] = "effective_post_quantize"
        if requested_order_type or forced_sell_exit:
            request_doc["order_type"] = requested_order_type or "FAK"
        if hasattr(req, "condition_id") and req.condition_id:
            request_doc["condition_id"] = req.condition_id

        last_error: str | None = None
        response: dict[str, Any] = {}

        # Atomically acquire submission lock BEFORE any retry loop to prevent the
        # race where two concurrent execute() calls both pass the "existing?" check
        # and both submit distinct signed orders.
        has_atomic_lock = hasattr(self.order_repo, "try_acquire_submission_lock")
        if has_atomic_lock:
            # If THIS call confirmed the prior pending lock is expired (above),
            # authorise try_acquire to take the dead lock over at the same
            # pending-submission TTL threshold instead of the conservative
            # default — the now strictly fail-closed loser path (P1-n) would
            # otherwise block the legitimate post-expiry retry. Concurrent
            # post-expiry retriers still serialise to exactly one winner: the
            # takeover resets the row to a fresh ``pending_lock`` whose age is
            # < TTL, so the loser's takeover WHERE fails. Non-expiry concurrent
            # duplicates keep the conservative default and never steal an
            # actively-submitting lock.
            _acquire_kwargs: dict[str, Any] = {}
            if pending_lock_expired:
                _acquire_kwargs["stale_takeover_seconds"] = int(
                    _pending_submission_ttl_seconds(self._config)
                )
            lock_acquired = await self.order_repo.try_acquire_submission_lock(
                idem_key, req.user_id, req.leader_id, request_doc, **_acquire_kwargs
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
            # CRITICAL duplicate-order fail-closed (P1-n): we did NOT acquire the
            # submission lock and no durable order_id is visible yet. The lock
            # primitive serializes to exactly ONE acquirer, so reaching here
            # means another task is the winner (or the key is in a non-takeable
            # state). A task that does not own the lock MUST NEVER fall through
            # to the submit path below — that was the N-concurrent-identical-
            # intents -> N venue POSTs race: a loser whose is_submission_locked()
            # momentarily read False (the winner's row sitting between lock
            # states — pending_lock -> pending-response -> order_id — so its
            # status was transiently not in the locked set while order_id was
            # still NULL) would slip past the old guarded return and POST a
            # duplicate signed order. is_submission_locked is now advisory only
            # (it shapes the error string); it never gates this fail-closed
            # return. Proven by tests/test_execution_idempotency_extended.py::
            # TestAtomicLockConcurrencyStress and the release-gate twin.
            _lock_held = has_atomic_lock and await self.order_repo.is_submission_locked(idem_key)
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
                    if _lock_held
                    else "submission lock not acquired — refusing to submit (fail-closed)"
                ),
            )

        if not hasattr(self, "_submit_semaphore"):
            cap = getattr(
                getattr(self._config, "execution", None),
                "execution_submit_concurrency",
                None,
            )
            if not isinstance(cap, int) or cap <= 0:
                cap = 2
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
                size=effective_order_amount,
                price_limit=worst_price,
                condition_id=getattr(req, "condition_id", None),
                tick_size=tick_size,
                neg_risk=neg_risk,
                order_type=requested_order_type or ("FAK" if forced_sell_exit else None),
            )

            for attempt in range(1, 4):
                if self._trading_permission is not None:
                    try:
                        await self._trading_permission.require_submission_allowed()
                    except TradingPausedError as exc:
                        response = {
                            "success": False,
                            "status": "trading_paused",
                            "error": str(exc),
                            "non_retryable": True,
                        }
                        await self._save_submission(
                            idempotency_key=idem_key,
                            request=request_doc,
                            response=response,
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
                        terminal_status = submit_result.status or "rejected"
                        response = {
                            "success": False,
                            "status": terminal_status,
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
                    if isinstance(exc, VenueInsufficientBalanceError):
                        # Redaction strips the venue's balance text; restore the
                        # canonical marker so the risk monitor's drift detector
                        # (``_is_drift_error``) fires on this rejection shape,
                        # mirroring the SDK-wrapper dict path.
                        last_error = f"insufficient_balance | {last_error}"
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
                    # P1-m: a venue outage surfaces here as the dominant mode.
                    # Record it so the breaker can trip and stop POSTing into a
                    # dead venue — the order is already saved submitted_unconfirmed
                    # for the reconciler, so this only gates further submissions.
                    self.circuit_breaker.record_failure()
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
                    # P1-m: count unclassified gateway failures toward the
                    # breaker too (same venue-health signal as above).
                    self.circuit_breaker.record_failure()
                    break
                # Add jitter so concurrent runtimes retrying the same venue
                # outage do not re-submit in lockstep bursts. Base backoff is
                # unchanged; jitter only spreads attempts by up to 0.1s.
                await asyncio.sleep(min(0.15 * attempt, 0.30) + random.uniform(0, 0.1))
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

            if requested_order_type == "FAK" and _is_deferred_order_ack(response):
                venue_status = str(response.get("status") or "submitted_unconfirmed")
                self._emit_post_lifecycle(
                    req=req,
                    side=side,
                    order_id=_response_order_id(response),
                    worst_price=worst_price,
                    venue_status=venue_status,
                    lifecycle_status=venue_status,
                )
                return ExecutionResult(
                    accepted=True,
                    order_id=_response_order_id(response),
                    status="submitted_unconfirmed",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=None,
                )

            if not response.get("success"):
                if str(response.get("status") or "").lower() == "submitted_unconfirmed":
                    self._emit_post_lifecycle(
                        req=req,
                        side=side,
                        order_id=_response_order_id(response),
                        worst_price=worst_price,
                        venue_status=str(response.get("status") or "submitted_unconfirmed"),
                        lifecycle_status="submitted_unconfirmed",
                    )
                    return ExecutionResult(
                        accepted=True,
                        order_id=_response_order_id(response),
                        status="submitted_unconfirmed",
                        requested_amount=req.amount,
                        filled_amount=Decimal("0"),
                        avg_price=None,
                        fees_paid=Decimal("0"),
                        error=last_error,
                    )
                if not (
                    requested_order_type == "FAK"
                    and _is_fak_repostable_no_fill(response)
                ):
                    self._emit_post_lifecycle(
                        req=req,
                        side=side,
                        order_id=None,
                        worst_price=worst_price,
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
                    )
                logger.info(
                    "copy_fak_initial_zero_fill_retry | token=%s side=%s status=%s error=%s",
                    req.token_id,
                    side,
                    str(response.get("status") or "cancelled").lower(),
                    response.get("error"),
                )

            order_id = _response_order_id(response)
            immediate_fak_fill = requested_order_type == "FAK"
            if immediate_fak_fill:
                matched_size = _response_matched_size(response)
                # C5b — never substitute the slippage ceiling (worst_price)
                # for an unknown average fill price. CLAUDE.md requires unknown
                # averages to stay unknown (zero); fabricating the limit price
                # biases cost basis, under-buys across the repost loop, and
                # skews all PnL/exit math. Use zero so total_fill_cost stays
                # exact for the filled portion whose price IS known.
                avg_fill_price = _response_avg_price(response) or Decimal("0")
                total_fill_cost = matched_size * avg_fill_price
                last_no_fill_error = response.get("error")
                order_ids = [str(order_id)] if order_id and matched_size > 0 else []
                # Forced exits (near-max / trailing / stop-loss) must fully
                # offload the position across however many book levels it takes,
                # so they get a higher repost ceiling than ordinary copy fills.
                if forced_sell_exit:
                    max_fak_attempts = _positive_int(
                        self._config.execution.exit_fak_max_attempts,
                        8,
                    )
                else:
                    max_fak_attempts = _positive_int(
                        self._config.execution.copy_fak_max_attempts,
                        8,
                    )
                for fak_attempt in range(2, max_fak_attempts + 1):
                    if side == "BUY":
                        remaining_amount = req.amount - total_fill_cost
                    else:
                        remaining_amount = req.amount - matched_size
                    if remaining_amount <= Decimal("0.0001"):
                        break
                    if self._trading_permission is not None:
                        try:
                            await self._trading_permission.require_submission_allowed()
                        except TradingPausedError as exc:
                            last_no_fill_error = str(exc)
                            logger.warning(
                                "copy_fak_repost_blocked_by_trading_control | token=%s attempt=%d",
                                req.token_id,
                                fak_attempt,
                            )
                            break
                    try:
                        next_bundle = await self._market_data_service.require_fresh_data(
                            req.token_id,
                            **market_data_kwargs,
                        )
                        next_quote_price = (
                            next_bundle.quote.best_ask
                            if side == "BUY"
                            else next_bundle.quote.best_bid
                        )
                        if next_quote_price is None or next_quote_price <= 0:
                            logger.info(
                                "copy_fak_repost_no_liquidity | token=%s attempt=%d side=%s",
                                req.token_id,
                                fak_attempt,
                                side,
                            )
                            break
                        if (
                            side == "BUY"
                            and (
                                (min_share_price > 0 and next_quote_price < min_share_price)
                                or (max_share_price > 0 and next_quote_price > max_share_price)
                            )
                        ):
                            logger.info(
                                "copy_fak_repost_price_gate | token=%s attempt=%d price=%s",
                                req.token_id,
                                fak_attempt,
                                next_quote_price,
                            )
                            break
                        next_top_level_amount = _top_level_marketable_amount(
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
                        next_order_amount = min(remaining_amount, next_top_level_amount)
                        next_tick_size = next_bundle.tick.tick_size
                        next_worst_price = (
                            next_quote_price * (Decimal("1") + slippage)
                            if side == "BUY"
                            else next_quote_price * (Decimal("1") - slippage)
                        )
                        next_worst_price = _quantize_price(
                            next_worst_price,
                            next_tick_size,
                            side,
                        )
                        next_request = MarketableOrderRequest(
                            token_id=req.token_id,
                            side=side,
                            size=next_order_amount,
                            price_limit=next_worst_price,
                            condition_id=getattr(req, "condition_id", None),
                            tick_size=next_tick_size,
                            neg_risk=next_bundle.tick.neg_risk,
                            order_type="FAK",
                        )
                        next_submit = await self.clob_gateway.submit_marketable_order(
                            next_request
                        )
                        next_response = _submit_result_to_response(next_submit)
                        next_idem = f"{idem_key}:fak:{fak_attempt}"
                        next_doc = dict(request_doc)
                        next_doc["amount"] = str(next_order_amount)
                        next_doc["remaining_amount"] = str(remaining_amount)
                        next_doc["worst_price"] = str(next_worst_price)
                        next_doc["fak_attempt"] = fak_attempt
                        await self._save_submission(
                            idempotency_key=next_idem,
                            request=next_doc,
                            response=next_response,
                        )
                    except Exception as exc:
                        # C5 — a POST that hit the wire then raised (timeout /
                        # connection drop) is ambiguous: the order may have
                        # reached the venue. Persist a ``submitted_unconfirmed``
                        # row for the reconciler before breaking, mirroring the
                        # first-attempt ambiguity handling. Previously this only
                        # logged + ``break``ed, leaving a live venue order with
                        # no local record and no reconciliation path.
                        last_error = redact_error_detail(exc)
                        ambiguity_response = {
                            "success": False,
                            "status": "submitted_unconfirmed",
                            "error": last_error,
                        }
                        ambiguity_doc = dict(request_doc)
                        ambiguity_doc["amount"] = str(next_order_amount)
                        ambiguity_doc["remaining_amount"] = str(remaining_amount)
                        ambiguity_doc["worst_price"] = str(next_worst_price)
                        ambiguity_doc["fak_attempt"] = fak_attempt
                        try:
                            await self._save_submission(
                                idempotency_key=f"{idem_key}:fak:{fak_attempt}",
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
                            last_error,
                        )
                        break
                    next_order_id = _response_order_id(next_response)
                    next_matched = _response_matched_size(next_response)
                    if next_matched > 0:
                        if next_order_id:
                            order_ids.append(str(next_order_id))
                        # C5b — unknown avg fill stays unknown (zero), never
                        # the slippage ceiling. See the initial-fill note above.
                        next_avg = _response_avg_price(next_response) or Decimal("0")
                        matched_size += next_matched
                        total_fill_cost += next_matched * next_avg
                        avg_fill_price = (
                            total_fill_cost / matched_size
                            if matched_size > 0
                            else avg_fill_price
                        )
                        continue
                    last_no_fill_error = next_response.get("error") or last_no_fill_error
                    if _is_fak_repostable_no_fill(next_response):
                        logger.info(
                            "copy_fak_repost_zero_fill_continue | token=%s attempt=%d side=%s status=%s error=%s",
                            req.token_id,
                            fak_attempt,
                            side,
                            str(next_response.get("status") or "cancelled").lower(),
                            next_response.get("error"),
                        )
                        continue
                    if not next_response.get("success"):
                        break
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
                    self._emit_post_lifecycle(
                        req=req,
                        side=side,
                        order_id=",".join(order_ids) if order_ids else order_id,
                        worst_price=worst_price,
                        venue_status=fill_status,
                        lifecycle_status=fill_status,
                    )
                    return ExecutionResult(
                        accepted=True,
                        order_id=",".join(order_ids) if order_ids else order_id,
                        status=fill_status,
                        requested_amount=req.amount,
                        filled_amount=matched_size,
                        avg_price=avg_fill_price,
                        fees_paid=Decimal("0"),
                        error=None,
                    )

                venue_no_fill = str(response.get("status") or "cancelled").lower()
                self._emit_post_lifecycle(
                    req=req,
                    side=side,
                    order_id=order_id,
                    worst_price=worst_price,
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
                )
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
        except Exception as exc:
            # SHP-08: logging must never break execution, but it shouldn't
            # vanish either. Keep at debug so the failure is observable
            # under ``--log-level=DEBUG`` without spamming production
            # warn/error.
            logger.debug("lifecycle_log_failed | %r", exc)
