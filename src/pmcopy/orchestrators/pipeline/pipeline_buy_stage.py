"""BUY-side copy sizing, decision scoring, risk cap, affordability, and balance-regime stage."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pmcopy.application.runtime_state import (
    resolve_reservations_repo,
    resolve_slippage_tolerance_bps,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.domain.models import TradeSignal
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _copy_entry_price_plan,
    _pipeline_facade_attr,
)
from pmcopy.orchestrators.pipeline.pipeline_sizing_and_caps import (
    TradePipelineResult,
    _account_cap_policy_enabled,
    _buy_fee_bps_or_none,
    _buy_required_cash_with_fees,
    _explicit_copier_exposure_cap,
    _fetch_balance_snapshot,
    _resolve_buy_risk_cap,
    _resolve_position_pct_cap,
    copier_open_exposure_headroom,
)
from pmcopy.services.execution.execution_service import ExecutionRequest
from pmcopy.services.observability.metrics import metrics
from pmcopy.utils.logger import (
    get_logger,
    log_trade_decision,
    log_trade_skip as _default_log_trade_skip,
    print_skip_reason as _default_print_skip_reason,
)

logger = get_logger("TradePipeline")


async def _prepare_buy_execution_request(
    *,
    context: TradingContext,
    signal: TradeSignal,
    raw: dict[str, Any],
    leader_id: str,
    trade_key: str,
    labels: dict[str, str],
    results: list[TradePipelineResult],
    copy_decision_svc: Any,
    leader_score: Any,
    live_price: float,
    _bundle_for_pair: Any,
    market_min_size: float,
    market_min_size_decimal: Decimal,
    market_tick_size: float,
    market_tick_size_decimal: Decimal,
    neg_risk_by_token: dict[str, bool],
) -> ExecutionRequest | None:
    """Evaluate BUY sizing, decision, risk caps, fee affordability, and balance regime."""
    print_skip_reason = _pipeline_facade_attr("print_skip_reason", _default_print_skip_reason)
    log_trade_skip = _pipeline_facade_attr("log_trade_skip", _default_log_trade_skip)
    sizing_result = context.copy_notional(signal.usdc_size)
    target_notional = sizing_result.final_notional_usdc
    configured_buy_max = Decimal(
        str(getattr(context, "buy_max_amount", target_notional) or target_notional)
    )
    # BUY_MAX_AMOUNT is the hard ceiling for decision-layer upsizing.
    # Keep base_notional_usdc as the strategy target below, but let
    # CopyDecisionService apply trust sizing up to this ceiling before
    # we further clamp by always-on cash/position caps.
    max_notional_for_decision = configured_buy_max

    snapshot = await _fetch_balance_snapshot(
        context,
        signal.condition_id,
        leader_id,
    )
    if snapshot is None:
        reason = "balance_unavailable"
        print_skip_reason("Balance unavailable", reason)
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(signal.size) if signal.size else None,
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=reason,
            reason=reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc(
            "trades_skipped_total",
            labels={**labels, "reason": reason},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{reason}",
        )
        results.append(TradePipelineResult(trade_key, "skipped", reason, None))
        return None

    position_pct_cap = await _resolve_position_pct_cap(context, signal, snapshot)
    if not position_pct_cap.allow:
        reason = (
            f"position cap [{position_pct_cap.policy_name}] blocked BUY: "
            f"{position_pct_cap.reason}"
        )
        print_skip_reason(
            f"Position cap [{position_pct_cap.policy_name}] blocked BUY",
            position_pct_cap.reason,
        )
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(signal.size) if signal.size else None,
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=position_pct_cap.reason,
            reason=reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc(
            "trades_skipped_total",
            labels={**labels, "reason": position_pct_cap.reason},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{position_pct_cap.reason}",
        )
        results.append(
            TradePipelineResult(
                trade_key,
                "skipped",
                position_pct_cap.reason,
                None,
            )
        )
        return None

    max_notional_for_decision = min(
        max_notional_for_decision,
        position_pct_cap.cap_notional_usdc,
    )

    buy_risk_cap = None
    if _account_cap_policy_enabled(context):
        buy_risk_cap = await _resolve_buy_risk_cap(context, leader_id, signal, snapshot)
        if not buy_risk_cap.allow:
            reason = (
                f"account-cap policy [{buy_risk_cap.policy_name}] headroom: "
                f"{buy_risk_cap.reason}"
            )
            print_skip_reason(
                f"Account-cap policy [{buy_risk_cap.policy_name}] headroom blocked BUY",
                buy_risk_cap.reason,
            )
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=live_price,
                usdc_value=float(signal.usdc_size),
                skip_code=buy_risk_cap.reason,
                reason=reason,
                event_slug=signal.event_slug,
                meta={"user_id": context.user_id, "leader_id": leader_id},
            )
            metrics.inc(
                "trades_skipped_total",
                labels={**labels, "reason": buy_risk_cap.reason},
            )
            await context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=f"skipped_{buy_risk_cap.reason}",
            )
            results.append(
                TradePipelineResult(
                    trade_key,
                    "skipped",
                    buy_risk_cap.reason,
                    None,
                )
            )
            return None

        max_notional_for_decision = min(
            max_notional_for_decision,
            buy_risk_cap.cap_notional_usdc,
        )

    # Task 24 fix — schema-v2 copier open-exposure cap
    # (``limits.max_open_exposure_quote``). Total per-copier open
    # exposure = CONFIRMED open exposure (the snapshot's existing
    # cost-basis accounting) + IN-FLIGHT reserved quote (this
    # account's ACTIVE BUY reservations). Enforced for copied
    # ENTRY admission only — this whole block sits in the BUY
    # branch, so protective SELL exits and reductions can never
    # be blocked by an exposure cap. Independently of (and in
    # addition to) the account-level risk policy above.
    copier_exposure_cap = _explicit_copier_exposure_cap(context)
    if copier_exposure_cap is not None:
        reserved_quote = Decimal("0")
        cap_evidence_failed = snapshot.total_exposure_usdc is None
        _reservations_repo = resolve_reservations_repo(context)
        if _reservations_repo is not None:
            try:
                reserved_quote = await _reservations_repo.sum_active_quote_for_user(
                    context.user_id
                )
            except Exception as exc:
                logger.warning(
                    "copier_exposure_cap_reserved_lookup_failed | "
                    "user=%s error_type=%s",
                    context.user_id,
                    type(exc).__name__,
                )
                cap_evidence_failed = True
        headroom = copier_open_exposure_headroom(
            cap=copier_exposure_cap,
            open_exposure=snapshot.total_exposure_usdc,
            reserved_quote=reserved_quote,
        )
        cap_reason: str | None = None
        detail = ""
        if cap_evidence_failed or headroom is None:
            cap_reason = "copier_open_exposure_unavailable"
            detail = (
                "open exposure or reserved quote could not be "
                "established — failing closed under the configured cap"
            )
        elif headroom <= 0:
            cap_reason = "copier_open_exposure_cap"
            detail = (
                f"open {snapshot.total_exposure_usdc} + reserved "
                f"{reserved_quote} leaves no room under cap "
                f"{copier_exposure_cap}"
            )
        if cap_reason is not None:
            print_skip_reason("Copier open-exposure cap reached", detail)
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=live_price,
                usdc_value=float(signal.usdc_size),
                skip_code=cap_reason,
                reason=detail,
                event_slug=signal.event_slug,
                meta={"user_id": context.user_id, "leader_id": leader_id},
            )
            metrics.inc(
                "trades_skipped_total",
                labels={**labels, "reason": cap_reason},
            )
            await context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=f"skipped_{cap_reason}",
            )
            results.append(TradePipelineResult(trade_key, "skipped", cap_reason, None))
            return None
        assert headroom is not None
        max_notional_for_decision = min(max_notional_for_decision, headroom)

    policy_enabled = _account_cap_policy_enabled(context)

    # --- Sizing audit: unified log showing both sizing sources ---
    policy_binding = (
        buy_risk_cap is not None and buy_risk_cap.cap_notional_usdc < target_notional
    )
    logger.info(
        "sizing_audit | cond=%s leader=%s "
        "leader_notional=%.2f strategy=%s sizing_target=%.4f "
        "policy_enabled=%s policy_cap=%s policy_binding=%s "
        "max_for_decision=%.4f | %s",
        signal.condition_id[:12],
        leader_id[:12],
        float(signal.usdc_size),
        sizing_result.strategy,
        float(target_notional),
        policy_enabled,
        f"{float(buy_risk_cap.cap_notional_usdc):.4f}[{buy_risk_cap.policy_name}]"
        if buy_risk_cap
        else "n/a",
        policy_binding,
        float(max_notional_for_decision),
        sizing_result.reasoning,
        extra={
            "type": "sizing_audit",
            "condition_id": signal.condition_id,
            "leader_id": leader_id,
            "leader_notional": float(signal.usdc_size),
            "strategy": sizing_result.strategy,
            "sizing_target": float(target_notional),
            "policy_enabled": policy_enabled,
            "policy_cap": float(buy_risk_cap.cap_notional_usdc)
            if buy_risk_cap
            else None,
            "policy_binding": policy_binding,
            "max_for_decision": float(max_notional_for_decision),
        },
    )

    # Run the copy decision gate
    decision = copy_decision_svc.decide_buy(
        trade=raw,
        leader_score=leader_score,
        live_price=live_price,
        base_notional_usdc=float(target_notional),
        max_notional_usdc=float(max_notional_for_decision),
        market_min_size=market_min_size,
        tick_size=market_tick_size,
    )

    _decision_meta: dict[str, Any] = {
        "user_id": context.user_id,
        "leader_id": leader_id,
        "leader_score": leader_score.score,
        "trade_score": decision.trade_score,
        "price_drift_pct": decision.price_drift_pct,
        "edge_decay_pct": decision.edge_decay_pct,
        "leader_notional": float(signal.usdc_size),
        "tx_hash": signal.tx_hash,
    }
    if _bundle_for_pair is not None:
        _bsrc = getattr(_bundle_for_pair, "source", None)
        _decision_meta["market_data_source"] = getattr(_bsrc, "value", None) or str(
            _bsrc or ""
        )
        _q = getattr(_bundle_for_pair, "quote", None)
        _b = getattr(_bundle_for_pair, "book", None)
        if _q is not None:
            _decision_meta["quote_source"] = getattr(_q, "source", None)
        if _b is not None:
            _decision_meta["book_source"] = getattr(_b, "source", None)
    log_trade_decision(
        logger,
        side=signal.side,
        condition_id=signal.condition_id,
        asset=signal.token_id,
        decision_score=decision.decision_score,
        allow=decision.allow,
        reason=decision.reason,
        event_slug=signal.event_slug,
        meta=_decision_meta,
    )

    if not decision.allow:
        if decision.skip_code == "signal_too_old":
            skip_title = "Signal too old"
        elif decision.skip_code in {
            "risk_cap_too_low_for_min_order",
            "buy_below_minimum_order_requirements",
            "buy_cap_blocks_upsize_to_venue_min",
        }:
            skip_title = "BUY blocked by execution minimums/risk cap"
        else:
            skip_title = "Decision gate rejected BUY"
        print_skip_reason(skip_title, decision.reason)
        log_trade_skip(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(signal.size) if signal.size else None,
            price=live_price,
            usdc_value=float(signal.usdc_size),
            skip_code=decision.skip_code,
            reason=decision.reason,
            event_slug=signal.event_slug,
            meta={"user_id": context.user_id, "leader_id": leader_id},
        )
        metrics.inc(
            "trades_skipped_total",
            labels={**labels, "reason": decision.skip_code},
        )
        await context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status=f"skipped_{decision.skip_code}",
        )
        results.append(TradePipelineResult(trade_key, "skipped", decision.reason, None))
        return None

    # Use decision-gated notional (may be adjusted by scoring), clamped in Decimal
    gated_notional = (
        min(Decimal(str(decision.target_notional_usdc)), max_notional_for_decision)
        if decision.target_notional_usdc > 0
        else min(target_notional, max_notional_for_decision)
    )

    # --- P2-e: fee-aware affordability gate ---
    # The upstream cash gates (headroom_for_buy / balance regime)
    # compare against the BARE notional and ignore venue fees, so a
    # BUY affordable on notional alone but NOT once fees are added
    # could slip through. Compare required_cash = notional*(1+fee)
    # against live free cash and fail closed if it does not fit. This
    # runs BEFORE the regime reservation so a rejection never leaks a
    # held reservation, and it does NOT alter sizing — the order
    # amount stays exactly ``gated_notional``.
    buy_fee_bps = await _buy_fee_bps_or_none(context, signal.token_id)
    if buy_fee_bps is not None and buy_fee_bps > 0:
        required_cash_with_fees = _buy_required_cash_with_fees(
            gated_notional, buy_fee_bps
        )
        if required_cash_with_fees > snapshot.free_cash_usdc:
            reason = (
                f"insufficient cash for BUY net of fees: "
                f"need {required_cash_with_fees} "
                f"(notional {gated_notional} + {buy_fee_bps}bps), "
                f"free {snapshot.free_cash_usdc}"
            )
            skip_code = "insufficient_cash_with_fees"
            print_skip_reason("Insufficient cash (fees)", reason)
            logger.warning(
                "buy_insufficient_cash_with_fees | cond=%s leader=%s "
                "notional=%s fee_bps=%d required=%s free_cash=%s",
                signal.condition_id[:12],
                leader_id[:12],
                str(gated_notional),
                buy_fee_bps,
                str(required_cash_with_fees),
                str(snapshot.free_cash_usdc),
                extra={
                    "type": skip_code,
                    "user_id": context.user_id,
                    "condition_id": signal.condition_id,
                    "leader_id": leader_id,
                    "notional_usdc": str(gated_notional),
                    "fee_bps": buy_fee_bps,
                    "required_cash_usdc": str(required_cash_with_fees),
                    "free_cash_usdc": str(snapshot.free_cash_usdc),
                },
            )
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=live_price,
                usdc_value=float(gated_notional),
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
            results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
            return None

    # --- Phase 4 balance regime gate ---
    regime = getattr(context, "balance_regime", None)
    if regime is not None:
        # Greptile P1 (PR #106 review): replace the non-atomic
        # ``check_buy → submit → record_buy`` triple with the
        # atomic ``await regime.reserve(...)`` so two concurrent
        # BUYs cannot both pass the cap against the same
        # ``daily_spent`` snapshot. ``reserve`` atomically checks
        # the cap AND records the hold under ``order_id=trade_key``;
        # the post-submit branches below either ``commit`` (on
        # accepted submission) or ``release`` (on
        # duplicate_ignored / not accepted / exception paths) the
        # held reservation. The proven atomic semantics live in
        # ``tests/test_balance_regime_atomic_reserve.py``.
        regime_decision = await regime.reserve(
            order_id=trade_key,
            notional_usdc=gated_notional,
            free_cash_usdc=snapshot.free_cash_usdc,
        )
        if not regime_decision.allow:
            reason = (
                f"balance regime [{regime_decision.policy_name}] blocked BUY: "
                f"{regime_decision.reason}"
            )
            print_skip_reason(
                f"Balance regime [{regime_decision.policy_name}] blocked BUY",
                regime_decision.reason,
            )
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=live_price,
                usdc_value=float(gated_notional),
                skip_code=regime_decision.reason,
                reason=reason,
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "requested_usdc": str(regime_decision.requested_usdc),
                    "daily_spent_usdc": str(regime_decision.daily_spent_usdc),
                    "daily_remaining_usdc": str(regime_decision.daily_remaining_usdc),
                    "headroom_usdc": str(regime_decision.headroom_usdc),
                },
            )
            metrics.inc(
                "trades_skipped_total",
                labels={**labels, "reason": regime_decision.reason},
            )
            await context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=f"skipped_{regime_decision.reason}",
            )
            results.append(
                TradePipelineResult(trade_key, "skipped", regime_decision.reason, None)
            )
            return None

    # Log final execution sizing if decision gate changed the amount
    if float(gated_notional) != float(target_notional):
        logger.info(
            "sizing_final | cond=%s decision_gate adjusted %.4f → %.4f "
            "(trust_mult=%.3f)",
            signal.condition_id[:12],
            float(target_notional),
            float(gated_notional),
            decision.details.get("trust_multiplier", 0),
            extra={
                "type": "sizing_final",
                "condition_id": signal.condition_id,
                "leader_id": leader_id,
                "original_notional": float(target_notional),
                "gated_notional": float(gated_notional),
            },
        )

    buy_price_limit, buy_recovery_metadata = _copy_entry_price_plan(
        context, signal, "BUY", leader_id=leader_id
    )
    exec_req = ExecutionRequest(
        user_id=context.user_id,
        leader_id=leader_id,
        token_id=signal.token_id,
        side="BUY",
        amount=gated_notional,
        max_slippage_bps=resolve_slippage_tolerance_bps(context, 200),
        reason="copy_trade",
        signal_id=signal.tx_hash,
        condition_id=signal.condition_id,
        recovery_metadata=buy_recovery_metadata,
        order_type=ExecutionOrderType.FAK,
        price_limit=buy_price_limit,
        tick_size=market_tick_size_decimal,
        min_order_size=market_min_size_decimal,
        neg_risk=neg_risk_by_token.get(signal.token_id),
    )
    return exec_req
