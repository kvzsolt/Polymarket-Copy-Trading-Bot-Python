"""SELL-side holdings lookup, live-share sync, leader-fraction derivation, and request stage."""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from typing import Any

from pmcopy.application.runtime_state import resolve_slippage_tolerance_bps
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.cost_basis import PositionCostBasis
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.domain.models import TradeSignal
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _copy_entry_price_plan,
    _fetch_live_position_size as _default_fetch_live_position_size,
    _pipeline_facade_attr,
)
from pmcopy.orchestrators.pipeline.pipeline_sizing_and_caps import (
    TradePipelineResult,
    _derive_sell_tokens,
    _extract_leader_sell_fraction,
    _resolve_leader_sell_tokens,
    _trade_config,
)
from pmcopy.services.execution.execution_service import ExecutionRequest
from pmcopy.services.observability.metrics import metrics
from pmcopy.storage.postgres.orders_repo import _RETRYABLE_FAILED_STATUS
from pmcopy.utils.logger import (
    get_logger,
    log_trade_skip as _default_log_trade_skip,
    print_skip_reason as _default_print_skip_reason,
    shorten_addr,
)

logger = get_logger("TradePipeline")


async def _prepare_sell_execution_request(
    *,
    context: TradingContext,
    signal: TradeSignal,
    raw: dict[str, Any],
    leader_id: str,
    trade_key: str,
    labels: dict[str, str],
    results: list[TradePipelineResult],
    live_price: float,
    market_min_size: float,
    market_min_size_decimal: Decimal,
    market_tick_size_decimal: Decimal,
    neg_risk_by_token: dict[str, bool],
) -> tuple[ExecutionRequest | None, Decimal | None]:
    """Evaluate SELL inventory, live holdings, leader sell fraction, and build ExecutionRequest."""
    print_skip_reason = _pipeline_facade_attr("print_skip_reason", _default_print_skip_reason)
    log_trade_skip = _pipeline_facade_attr("log_trade_skip", _default_log_trade_skip)
    _fetch_live_position_size = _pipeline_facade_attr(
        "_fetch_live_position_size", _default_fetch_live_position_size
    )
    sell_entry_price: Decimal | None = None
    pos = await context.positions_repo.get_position(
        context.user_id, signal.condition_id, token_id=signal.token_id
    )
    if pos is None:
        # No exact (condition_id, token_id) match — check if we hold the *other* outcome token.
        candidates = await context.positions_repo.list_positions_for_condition(
            context.user_id, signal.condition_id
        )
        wrong_token_held = any(p.token_id != signal.token_id for p in candidates)
        if wrong_token_held:
            skip_detail = (
                f"wrong token held — leader sells "
                f"{shorten_addr(signal.token_id, 8, 6)} but we hold a different outcome"
            )
            skip_code = "wrong_token_held"
        else:
            skip_detail = "never held — we never entered this market"
            skip_code = "never_held"
        print_skip_reason("SELL skipped", skip_detail)
        logger.debug(
            "sell_skip",
            extra={
                "reason": skip_code,
                "condition_id": signal.condition_id,
                "token_id": signal.token_id,
                "leader_id": leader_id,
            },
        )
        metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(signal.size) if signal.size else None,
            price=float(signal.price) if signal.price else None,
            usdc_value=float(signal.usdc_size),
            skip_code=skip_code,
            reason=skip_detail,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{skip_code}",
        )
        results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
        return None, None

    if pos.size_tokens <= Decimal("0"):
        skip_detail = "already closed — position size is zero"
        skip_code = "already_closed"
        print_skip_reason("SELL skipped", skip_detail)
        logger.debug(
            "sell_skip",
            extra={
                "reason": skip_code,
                "condition_id": signal.condition_id,
                "leader_id": leader_id,
            },
        )
        metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(signal.size) if signal.size else None,
            price=float(signal.price) if signal.price else None,
            usdc_value=float(signal.usdc_size),
            skip_code=skip_code,
            reason=skip_detail,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{skip_code}",
        )
        results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
        return None, None

    # Capture entry price before apply_fill mutates or closes/deletes the position.
    sell_entry_price = PositionCostBasis.from_value(
        getattr(pos, "avg_entry_price", None)
    ).entry_price

    sell_missing_policy = (
        context.config.execution.sell_fraction_missing_policy
        if context.config
        else "skip"
    )
    requested_sell_tokens = _derive_sell_tokens(
        signal, pos, raw, missing_policy=sell_missing_policy
    )
    if requested_sell_tokens is None:
        # Fraction unknown (positions API outage) and policy=skip.
        # Retryable, not terminal: the next monitor cycle re-fetches
        # and re-annotates the leader trade once the data API
        # recovers, so the exit is copied late rather than never.
        reason = "leader sell fraction unknown — cannot size copy sell"
        skip_code = "sell_fraction_missing_skipped"
        print_skip_reason("SELL skipped", reason)
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=0.0,
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=skip_code,
            reason=reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=_RETRYABLE_FAILED_STATUS,
        )
        results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
        return None, None
    max_sell_tokens = pos.size_tokens

    live_sellable_tokens = await _fetch_live_position_size(
        context,
        token_id=signal.token_id,
    )
    if live_sellable_tokens is not None:
        venue_sell_cap = live_sellable_tokens.quantize(
            Decimal("0.01"), rounding=ROUND_DOWN
        )
        if venue_sell_cap <= Decimal("0") and live_sellable_tokens > Decimal("0"):
            venue_sell_cap = live_sellable_tokens
        if venue_sell_cap < requested_sell_tokens:
            logger.info(
                "sell_size_clamped_to_live_balance | cond=%s leader=%s "
                "requested=%.4f live_balance=%.4f clamped=%.4f",
                signal.condition_id[:12],
                leader_id[:12],
                float(requested_sell_tokens),
                float(live_sellable_tokens),
                float(venue_sell_cap),
                extra={
                    "type": "sell_size_clamped_to_live_balance",
                    "condition_id": signal.condition_id,
                    "leader_id": leader_id,
                    "requested_sell_tokens": float(requested_sell_tokens),
                    "live_sellable_tokens": float(live_sellable_tokens),
                    "clamped_sell_tokens": float(venue_sell_cap),
                },
            )
            if venue_sell_cap <= Decimal("0") and live_sellable_tokens <= Decimal("0"):
                await context.positions_repo.delete_position(
                    context.user_id,
                    signal.condition_id,
                    token_id=signal.token_id,
                )
            max_sell_tokens = venue_sell_cap
    else:
        # P2-m: live-holdings fetch returned None (transport error /
        # unknown). We cannot determine the on-chain sellable size,
        # so selling against the (possibly stale) DB position size is
        # unsafe — it can oversell or sell shares we no longer hold.
        # Fail closed: skip this SELL. ``None`` (unknown) is distinct
        # from ``Decimal("0")`` (known-zero), which the clamp above
        # already handles as a close/delete.
        reason = "live holdings unavailable — cannot confirm sellable size"
        skip_code = "sell_clamp_live_holdings_unavailable"
        print_skip_reason("SELL skipped", reason)
        logger.warning(
            "sell_clamp_live_holdings_unavailable | cond=%s leader=%s token=%s db_size=%.4f",
            signal.condition_id[:12],
            leader_id[:12],
            shorten_addr(signal.token_id, 8, 6),
            float(max_sell_tokens),
            extra={
                "type": skip_code,
                "user_id": context.user_id,
                "condition_id": signal.condition_id,
                "leader_id": leader_id,
                "token_id": signal.token_id,
                "db_position_size_tokens": float(max_sell_tokens),
            },
        )
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(requested_sell_tokens),
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=skip_code,
            reason=reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
        # Transient/unknown — persist a RETRYABLE status (not a
        # terminal one). A missing live-holdings read is not a final
        # decision; the atomic claim above already inserted an
        # ``in_progress`` row, so we must downgrade it to
        # ``retryable_failed`` to release the claim and let the next
        # poll retry the protective SELL once the venue/data-API
        # recovers (same retryable status the transient venue-failure
        # path uses).
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=_RETRYABLE_FAILED_STATUS,
        )
        results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
        return None, None

    leader_sell_fraction = _extract_leader_sell_fraction(raw) or Decimal("1")
    sub_min_sell_policy = str(
        getattr(_trade_config(context).execution, "sub_min_sell_policy", "round_up")
    )
    sell_tokens = _resolve_leader_sell_tokens(
        requested_sell_tokens=requested_sell_tokens,
        max_sell_tokens=max_sell_tokens,
        market_min_size=market_min_size_decimal,
        live_price=Decimal(str(signal.price or 0)),
        sub_min_sell_policy=sub_min_sell_policy,
    )

    if sell_tokens <= Decimal("0"):
        below_minimum = requested_sell_tokens > 0 and max_sell_tokens > 0
        skip_code = "sell_below_exchange_minimum" if below_minimum else "sell_size_zero"
        reason = (
            "Sellable shares do not meet the exchange minimum under the configured policy"
            if below_minimum
            else "SELL size is zero"
        )
        print_skip_reason("SELL skipped", reason)
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(sell_tokens),
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=skip_code,
            reason=reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc(
            "trades_skipped_total",
            labels={**labels, "reason": skip_code},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{skip_code}",
        )
        results.append(
            TradePipelineResult(
                trade_key, "skipped", skip_code if below_minimum else reason, None
            )
        )
        return None, None

    if requested_sell_tokens + Decimal("1e-12") < sell_tokens:
        logger.info(
            "sell_size_bumped_to_min | requested=%.4f bumped_to=%.4f held=%.4f",
            float(requested_sell_tokens),
            float(sell_tokens),
            float(pos.size_tokens),
        )

    logger.info(
        "sell_ratio_applied | cond=%s leader=%s held=%.4f leader_fraction=%.6f sell_tokens=%.4f",
        signal.condition_id[:12],
        leader_id[:12],
        float(pos.size_tokens),
        float(leader_sell_fraction),
        float(sell_tokens),
        extra={
            "type": "sell_ratio_applied",
            "condition_id": signal.condition_id,
            "leader_id": leader_id,
            "position_size": float(pos.size_tokens),
            "leader_sell_fraction": float(leader_sell_fraction),
            "requested_sell_tokens": float(requested_sell_tokens),
            "max_sell_tokens": float(max_sell_tokens),
            "market_min_size": float(market_min_size),
            "sell_tokens": float(sell_tokens),
        },
    )

    sell_price_limit, sell_recovery_metadata = _copy_entry_price_plan(
        context, signal, "SELL", leader_id=leader_id
    )
    sell_recovery_metadata.update(
        {
            "leader_sell_fraction": str(leader_sell_fraction),
            "leader_sell_fraction_resolution": str(
                raw.get("_leader_sell_fraction_resolution") or "polling_snapshot"
            ),
            "live_position_size": str(live_sellable_tokens),
        }
    )
    exec_req = ExecutionRequest(
        user_id=context.user_id,
        leader_id=leader_id,
        token_id=signal.token_id,
        side="SELL",
        amount=sell_tokens,
        max_slippage_bps=resolve_slippage_tolerance_bps(context, 200),
        reason="leader_sell",
        signal_id=signal.tx_hash,
        condition_id=signal.condition_id,
        recovery_metadata=sell_recovery_metadata,
        order_type=ExecutionOrderType.FAK,
        price_limit=sell_price_limit,
        tick_size=market_tick_size_decimal,
        min_order_size=market_min_size_decimal,
        neg_risk=neg_risk_by_token.get(signal.token_id),
        leader_sell_fraction=leader_sell_fraction,
        live_position_size=live_sellable_tokens,
        sub_min_sell_policy=sub_min_sell_policy,
    )

    return exec_req, sell_entry_price
