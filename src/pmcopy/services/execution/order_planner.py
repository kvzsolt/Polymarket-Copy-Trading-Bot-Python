"""Order planning, book-quality gating, price-bound quantization, and min-size checks (WP-A)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.config.app_config import ExecutionConfig
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.domain.price_bound import (
    HARD_BOUND_TICK_POLICY,
    MARKETABLE_TICK_POLICY,
    OrderSide,
)
from pmcopy.domain.units import BASIS_POINTS_PER_UNIT
from pmcopy.services.execution.admission_gates import ValidatedAdmission
from pmcopy.services.execution.book_spread import (
    calculate_book_depth_metrics,
    calculate_book_spread_metrics,
    resolve_book_spread_mode,
)
from pmcopy.services.execution.types import ExecutionRequest, ExecutionResult
from pmcopy.services.market_data.market_data_quality import is_thin_or_empty_book
from pmcopy.services.market_data.market_data_service import (
    MarketDataBundle,
    MarketDataNotReady,
    MarketDataPolicy,
)
from pmcopy.services.trading.order_sizing import MinimumOrderPolicy
from pmcopy.services.trading.trading_permission import SubmissionPurpose

if TYPE_CHECKING:
    from pmcopy.services.market_data.market_data_service import MarketDataService

_COPY_MINIMUM_ORDER_POLICY = MinimumOrderPolicy()
_DEFAULT_ENTRY_PRICE_DRIFT_PCT = ExecutionConfig.max_entry_price_drift_pct


def parse_decimal_or_default(raw: Any, default: Decimal) -> Decimal:
    """Parse ``raw`` into ``Decimal`` or return ``default`` on invalid input."""
    try:
        return Decimal(str(raw))
    except Exception:
        return default


def quote_outside_band(quote_price: Decimal, band_limit: Decimal, side: str) -> bool:
    """Return True when ``quote_price`` breaches caller BUY ceiling or SELL floor."""
    if side == "BUY":
        return quote_price > band_limit
    return quote_price < band_limit


def quantize_marketable_price(price: Decimal, tick_size: Decimal, side: OrderSide) -> Decimal:
    """Snap a marketable quote toward crossing via :data:`MARKETABLE_TICK_POLICY`."""
    return MARKETABLE_TICK_POLICY.quantize(price=price, tick_size=tick_size, side=side)


def copy_fak_limit_on_tick(
    *,
    price_limit: Decimal,
    tick_size: Decimal,
    side: str,
    max_share_price: Decimal,
) -> Decimal:
    """Snap a hard copied FAK bound conservatively via :data:`HARD_BOUND_TICK_POLICY`."""
    return HARD_BOUND_TICK_POLICY.snap_copy_fak_limit(
        price_limit=price_limit,
        tick_size=tick_size,
        side=side,
        max_share_price=max_share_price,
    )


async def fetch_fresh_market_bundle(
    market_data_service: MarketDataService,
    token_id: str,
    *,
    side: str,
    condition_id: str | None,
    policy: MarketDataPolicy | None = None,
) -> MarketDataBundle:
    """Fetch fresh market bundle with exact typed keyword arguments."""
    if policy is not None:
        return await market_data_service.require_fresh_data(
            token_id,
            side=side,
            condition_id=condition_id,
            policy=policy,
        )
    return await market_data_service.require_fresh_data(
        token_id,
        side=side,
        condition_id=condition_id,
    )


@dataclass(frozen=True, slots=True)
class PlannedExecutionOrder:
    """Validated, tick-quantized execution plan ready for idempotency locking."""

    side: OrderSide
    forced_sell_exit: bool
    submission_purpose: SubmissionPurpose
    requested_order_type: ExecutionOrderType | None
    instant_fak: bool
    effective_order_amount: Decimal
    worst_price: Decimal
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    slippage: Decimal
    min_share_price: Decimal
    max_share_price: Decimal
    band_drift_pct: Decimal
    protective_band_limit: Decimal | None
    market_data_policy: MarketDataPolicy | None
    metadata: dict[str, Any]


class ExecutionOrderPlanner:
    """Resolve market metadata/books, enforce quality/price bands, and quantize limits."""

    async def plan(
        self,
        req: ExecutionRequest,
        admission: ValidatedAdmission,
        *,
        config: Any,
        market_data_service: MarketDataService | None,
        logger: logging.Logger,
    ) -> PlannedExecutionOrder | ExecutionResult:
        """Return :class:`PlannedExecutionOrder` or a rejected :class:`ExecutionResult`."""
        side = admission.side
        forced_sell_exit = admission.forced_sell_exit
        ec = getattr(config, "execution", None)
        metadata = req.recovery_metadata if isinstance(req.recovery_metadata, dict) else {}
        raw_order_type = req.order_type or metadata.get("order_type")
        requested_order_type = (
            ExecutionOrderType(str(raw_order_type).strip().upper()) if raw_order_type else None
        )
        copy_source = str(metadata.get("source") or req.reason or "").strip().lower()
        is_copy_intent = copy_source in {"copy_trade", "leader_sell"}
        instant_fak = (
            requested_order_type is ExecutionOrderType.FAK
            and req.price_limit is not None
            and is_copy_intent
            and not forced_sell_exit
        )
        caller_price_limit = Decimal(str(req.price_limit)) if req.price_limit is not None else None

        bundle: MarketDataBundle | None = None
        best_bid: Decimal | None = None
        best_ask: Decimal | None = None
        quote_price: Decimal | None = None
        market_data_policy: MarketDataPolicy | None = None

        if instant_fak:
            if caller_price_limit is None or caller_price_limit <= 0:
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
            missing_metadata = [
                field
                for field, value in (
                    ("tick_size", req.tick_size),
                    ("min_order_size", req.min_order_size),
                    ("neg_risk", req.neg_risk),
                )
                if value is None
            ]
            if missing_metadata:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="market_metadata_unavailable",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=("missing copy execution metadata: " + ", ".join(missing_metadata)),
                )
            tick_size = Decimal(str(req.tick_size))
            min_order_size = Decimal(str(req.min_order_size))
            neg_risk = req.neg_risk
            if tick_size <= 0 or min_order_size <= 0 or not isinstance(neg_risk, bool):
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="market_metadata_unavailable",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error="copy execution metadata must contain positive tick/min and bool neg_risk",
                )
            quote_price = caller_price_limit
        else:
            if market_data_service is None:
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
            if forced_sell_exit:
                base_policy = getattr(market_data_service, "_policy", None)
                if isinstance(base_policy, MarketDataPolicy):
                    market_data_policy = replace(
                        base_policy,
                        rest_snapshot_max_spread_bps=1_000_000,
                        require_book_depth=False,
                    )
            try:
                bundle = await fetch_fresh_market_bundle(
                    market_data_service,
                    req.token_id,
                    side=req.side,
                    condition_id=getattr(req, "condition_id", None),
                    policy=market_data_policy,
                )
            except Exception as exc:
                status = (
                    "market_not_ready" if isinstance(exc, MarketDataNotReady) else "stale_market_data"
                )
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

        book_rejection = self._evaluate_book_gates(
            req=req,
            ec=ec,
            side=side,
            instant_fak=instant_fak,
            forced_sell_exit=forced_sell_exit,
            bundle=bundle,
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=tick_size,
            logger=logger,
        )
        if book_rejection is not None:
            return book_rejection

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

        min_share_price = parse_decimal_or_default(
            getattr(ec, "min_share_price", 0) if ec else 0,
            Decimal("0"),
        )
        max_share_price = parse_decimal_or_default(
            getattr(ec, "max_share_price", 0) if ec else 0,
            Decimal("0"),
        )
        band_rejection = self._evaluate_share_price_bounds(
            req=req,
            side=side,
            instant_fak=instant_fak,
            quote_price=quote_price,
            min_share_price=min_share_price,
            max_share_price=max_share_price,
            metadata=metadata,
        )
        if band_rejection is not None:
            return band_rejection

        band_drift_pct = parse_decimal_or_default(
            getattr(ec, "max_entry_price_drift_pct", _DEFAULT_ENTRY_PRICE_DRIFT_PCT) if ec else 0,
            Decimal("0"),
        )
        protective_band_limit = (
            caller_price_limit
            if forced_sell_exit and caller_price_limit is not None and band_drift_pct > 0
            else None
        )
        if protective_band_limit is not None and quote_outside_band(
            quote_price,
            protective_band_limit,
            side,
        ):
            leader_price = metadata.get("leader_price") or protective_band_limit
            logger.info(
                "protective_exit_price_band_rejected | token=%s side=%s quote=%s limit=%s "
                "leader=%s drift_pct=%s",
                req.token_id,
                side,
                quote_price,
                protective_band_limit,
                leader_price,
                band_drift_pct,
                extra={
                    "type": "protective_exit_price_band_rejected",
                    "token_id": req.token_id,
                    "side": side,
                    "quote_price": str(quote_price),
                    "band_limit": str(protective_band_limit),
                    "leader_price": str(leader_price),
                    "drift_pct": str(band_drift_pct),
                },
            )
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="price_drift_exceeded",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    f"protective {side} quote {quote_price} is outside limit "
                    f"{protective_band_limit} (leader {leader_price}, drift {band_drift_pct}%)"
                ),
            )

        slippage = Decimal(req.max_slippage_bps) / BASIS_POINTS_PER_UNIT
        if instant_fak:
            copied_price_limit = Decimal(str(req.price_limit))
            try:
                worst_price = copy_fak_limit_on_tick(
                    price_limit=copied_price_limit,
                    tick_size=tick_size,
                    side=side,
                    max_share_price=max_share_price,
                )
            except ValueError:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="price_limit_unexecutable",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error="No executable tick satisfies the caller price bound",
                )
        else:
            raw_worst = (
                quote_price * (Decimal("1") + slippage)
                if side == "BUY"
                else quote_price * (Decimal("1") - slippage)
            )
            worst_price = quantize_marketable_price(raw_worst, tick_size, side)
            if caller_price_limit is not None:
                worst_price = MARKETABLE_TICK_POLICY.apply_caller_protection(
                    calculated_price=worst_price,
                    caller_limit=caller_price_limit,
                    tick_size=tick_size,
                    side=side,
                )

        effective_order_amount = req.amount
        min_check_amount = (
            effective_order_amount / worst_price
            if side == "BUY" and worst_price > 0
            else effective_order_amount
        )
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
                    f"effective size {min_check_amount} is below venue minimum {min_order_size}"
                ),
            )
        execution_notional = (
            effective_order_amount if side == "BUY" else effective_order_amount * worst_price
        )
        if instant_fak and execution_notional < _COPY_MINIMUM_ORDER_POLICY.minimum_notional_usdc:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="size_below_min",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"effective notional {execution_notional} is below the $1 CLOB minimum",
            )

        return PlannedExecutionOrder(
            side=side,
            forced_sell_exit=forced_sell_exit,
            submission_purpose=admission.submission_purpose,
            requested_order_type=requested_order_type,
            instant_fak=instant_fak,
            effective_order_amount=effective_order_amount,
            worst_price=worst_price,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            slippage=slippage,
            min_share_price=min_share_price,
            max_share_price=max_share_price,
            band_drift_pct=band_drift_pct,
            protective_band_limit=protective_band_limit,
            market_data_policy=market_data_policy,
            metadata=metadata,
        )

    @staticmethod
    def _evaluate_book_gates(
        *,
        req: ExecutionRequest,
        ec: Any,
        side: OrderSide,
        instant_fak: bool,
        forced_sell_exit: bool,
        bundle: MarketDataBundle | None,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
        tick_size: Decimal,
        logger: logging.Logger,
    ) -> ExecutionResult | None:
        spread_mode = resolve_book_spread_mode(getattr(ec, "book_spread_mode", None))
        try:
            max_spread_bps = int(ec.max_book_spread_bps)
        except (AttributeError, TypeError, ValueError):
            logger.warning(
                "invalid_max_book_spread_bps",
                extra={"value": getattr(ec, "max_book_spread_bps", None)},
            )
            max_spread_bps = 500
        try:
            max_spread_ticks = int(ec.max_book_spread_ticks)
        except (AttributeError, TypeError, ValueError):
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
            if is_thin_or_empty_book(best_bid, best_ask):
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

            spread = calculate_book_spread_metrics(
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
                    error=f"no orderbook data for required side — bid={best_bid} ask={best_ask}",
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
                    error=f"no orderbook data for required side — bid={best_bid} ask={best_ask}",
                )
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

        min_book_depth_raw = getattr(ec, "min_book_depth_usdc", 0) or 0
        try:
            min_book_depth = Decimal(str(min_book_depth_raw))
        except Exception:
            min_book_depth = Decimal("0")
        if min_book_depth > 0 and not forced_sell_exit and not instant_fak:
            if bundle is None:
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="stale_market_data",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error="book data unavailable for depth validation",
                )
            band_pct_raw = getattr(ec, "book_depth_band_pct", 5) or 5
            try:
                band_pct = Decimal(str(band_pct_raw))
            except Exception:
                band_pct = Decimal("5")
            depth = calculate_book_depth_metrics(
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
        return None

    @staticmethod
    def _evaluate_share_price_bounds(
        *,
        req: ExecutionRequest,
        side: OrderSide,
        instant_fak: bool,
        quote_price: Decimal,
        min_share_price: Decimal,
        max_share_price: Decimal,
        metadata: dict[str, Any],
    ) -> ExecutionResult | None:
        leader_reference_price = quote_price
        if instant_fak and side == "BUY":
            leader_reference_price = parse_decimal_or_default(
                metadata.get("leader_price"),
                quote_price,
            )
        if (
            instant_fak
            and side == "BUY"
            and min_share_price > 0
            and leader_reference_price < min_share_price
        ):
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    "leader price below min_share_price "
                    f"({leader_reference_price} < {min_share_price})"
                ),
            )
        if (
            instant_fak
            and side == "BUY"
            and max_share_price > 0
            and leader_reference_price > max_share_price
        ):
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=(
                    "leader price above max_share_price "
                    f"({leader_reference_price} > {max_share_price})"
                ),
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
        return None


__all__ = [
    "ExecutionOrderPlanner",
    "PlannedExecutionOrder",
    "copy_fak_limit_on_tick",
    "fetch_fresh_market_bundle",
    "parse_decimal_or_default",
    "quantize_marketable_price",
    "quote_outside_band",
]
