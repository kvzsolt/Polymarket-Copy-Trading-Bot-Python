from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from config.env import ENV
from domain.models import TradeSignal
from domain.trading_context import TradingContext
from services.executionService import ExecutionRequest
from services.copyDecisionService import CopyDecisionService
from services.leaderScoringService import LeaderScoringService
from services.healthServer import update_readiness
from services.metrics import metrics
from services.riskEngine import headroom_for_buy
from storage.sqlite.fills_repo import FillRecord
from utils.dayEquity import get_day_start_equity
from utils.getMyBalance import get_my_balance
from utils.logger import (
    get_logger,
    log_trade_execution,
    log_trade_submitted,
    log_trade_decision,
    log_trade_skip,
)

logger = get_logger("TradePipeline")

# Module-level singletons (initialized lazily).
_leader_scorer: LeaderScoringService | None = None
_copy_decision: CopyDecisionService | None = None


def _get_leader_scorer() -> LeaderScoringService:
    global _leader_scorer
    if _leader_scorer is None:
        _leader_scorer = LeaderScoringService()
    return _leader_scorer


def _get_copy_decision() -> CopyDecisionService:
    global _copy_decision
    if _copy_decision is None:
        _copy_decision = CopyDecisionService(leader_scorer=_get_leader_scorer())
    return _copy_decision


@dataclass
class TradePipelineResult:
    trade_key: str
    status: str
    reason: str | None
    order_id: str | None


@dataclass(frozen=True)
class _BuyRiskCap:
    allow: bool
    reason: str
    regime: str
    cap_notional_usdc: Decimal



def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _resolve_leader_tier(context: TradingContext, leader_id: str) -> str:
    """Look up the follow tier for a leader. Defaults to live_follow if no repo/record."""
    if not hasattr(context, "leaders_repo") or context.leaders_repo is None:
        return "live_follow"
    try:
        rec = context.leaders_repo.get_tier(leader_id)
        tier = rec.follow_tier if rec and rec.follow_tier else None
    except Exception:
        return "live_follow"
    if not tier or tier not in ("watchlist", "paper_follow", "live_follow"):
        return "live_follow"
    return tier



def build_trade_key(user_id: str, leader_id: str, trade: dict[str, Any]) -> str:
    tx_hash = trade.get("transactionHash") or trade.get("txHash") or ""
    side = str(trade.get("side") or "").strip().upper()
    asset = trade.get("asset") or trade.get("tokenID") or trade.get("token_id") or ""
    return f"{_normalize(user_id)}|{_normalize(leader_id)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}"



def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _max_non_terminal_failure_attempts() -> int:
    raw = ENV.get("NON_TERMINAL_FAILURE_MAX_ATTEMPTS", 3)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


def _retry_attempt_from_status(status: str | None) -> int:
    text = str(status or "").strip().lower()
    if not text.startswith("retry_pending_"):
        return 0
    suffix = text.replace("retry_pending_", "", 1)
    try:
        return int(suffix)
    except (TypeError, ValueError):
        return 0


def _balance_tiered_policy_enabled() -> bool:
    return bool(ENV.get("ENABLE_BALANCE_TIERED_POLICY", False))


def _position_notional_usdc(position: Any) -> Decimal:
    size_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    avg_entry_price = _to_decimal(getattr(position, "avg_entry_price", 0))
    notional = size_tokens * avg_entry_price
    return notional if notional > 0 else Decimal("0")


async def _resolve_buy_risk_cap(
    context: TradingContext,
    leader_id: str,
    signal: TradeSignal,
) -> _BuyRiskCap:
    balance_usdc = await get_my_balance(context.wallet_address)
    if balance_usdc is None:
        return _BuyRiskCap(
            allow=False,
            reason="balance_unavailable",
            regime="unknown",
            cap_notional_usdc=Decimal("0"),
        )

    free_cash_usdc = Decimal(str(balance_usdc))
    open_positions = context.positions_repo.list_open_positions(context.user_id)

    total_exposure_usdc = Decimal("0")
    market_exposure_usdc = Decimal("0")
    leader_exposure_usdc = Decimal("0")
    for position in open_positions:
        position_notional = _position_notional_usdc(position)
        total_exposure_usdc += position_notional
        if str(getattr(position, "condition_id", "") or "") == signal.condition_id:
            market_exposure_usdc += position_notional
        if str(getattr(position, "leader_id", "") or "") == leader_id:
            leader_exposure_usdc += position_notional

    equity_usdc = free_cash_usdc + total_exposure_usdc

    day_start_equity = Decimal(str(ENV.get("DAY_START_EQUITY_USDC", 0) or 0))
    if day_start_equity <= 0:
        day_start_equity = get_day_start_equity(equity_usdc)
    daily_loss_usdc = max(Decimal("0"), day_start_equity - equity_usdc)

    # The active pipeline does not yet persist per-position categories, so
    # category headroom cannot be reconstructed reliably here.
    headroom = headroom_for_buy(
        equity_usdc=equity_usdc,
        free_cash_usdc=free_cash_usdc,
        open_positions_count=len(open_positions),
        market_exposure_usdc=market_exposure_usdc,
        category_exposure_usdc=Decimal("0"),
        leader_exposure_usdc=leader_exposure_usdc,
        daily_loss_usdc=daily_loss_usdc,
        day_start_equity_usdc=day_start_equity,
    )
    return _BuyRiskCap(
        allow=headroom.allow,
        reason=headroom.reason,
        regime=headroom.regime,
        cap_notional_usdc=headroom.cap_notional_usdc,
    )



def normalize_signals(raw_signals: list[dict[str, Any]], leader_id: str) -> list[tuple[TradeSignal, dict[str, Any]]]:
    out: list[tuple[TradeSignal, dict[str, Any]]] = []
    for raw in raw_signals:
        side = str(raw.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            continue

        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        token_id = str(raw.get("asset") or raw.get("tokenID") or raw.get("token_id") or "")
        tx_hash = str(raw.get("transactionHash") or raw.get("txHash") or "")
        if not condition_id or not token_id or not tx_hash:
            continue

        size = _to_decimal(raw.get("size") or 0)
        price = _to_decimal(raw.get("price") or 0)
        usdc_size = _to_decimal(raw.get("usdcSize") or 0)
        if usdc_size <= 0 and size > 0 and price > 0:
            usdc_size = size * price

        ts_raw = raw.get("timestamp") or int(time.time())
        ts = int(float(ts_raw) / 1000) if float(ts_raw) > 1e12 else int(float(ts_raw))

        signal = TradeSignal(
            leader_id=leader_id,
            condition_id=condition_id,
            token_id=token_id,
            side=side,
            size=size,
            price=price,
            usdc_size=usdc_size,
            tx_hash=tx_hash,
            event_ts=ts,
            title=str(raw.get("title") or ""),
            event_slug=str(raw.get("eventSlug") or raw.get("slug") or ""),
        )
        out.append((signal, raw))
    return out


async def execute_trades(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    now_ts: float,
) -> list[TradePipelineResult]:
    if context.execution_service is None:
        raise RuntimeError("TradingContext.execution_service is required")

    normalized = normalize_signals(signals, leader_id)
    results: list[TradePipelineResult] = []

    for signal, raw in normalized:
        labels = {"user_id": context.user_id, "leader_id": leader_id}
        await metrics.inc("trades_detected_total", labels=labels)

        trade_key = build_trade_key(context.user_id, leader_id, raw)
        if context.orders_repo.has_processed_trade(trade_key):
            await metrics.inc("duplicate_order_prevented_total", labels=labels)
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason="already_processed",
                    order_id=None,
                )
            )
            continue

        # --- Tier gate: check follow tier before costly decision/execution ---
        leader_tier = _resolve_leader_tier(context, leader_id)
        if leader_tier == "watchlist":
            logger.info(
                "tier_gate_watchlist",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "trade_key": trade_key,
                    "side": signal.side,
                    "condition_id": signal.condition_id,
                },
            )
            await metrics.inc("trades_skipped_total", labels={**labels, "reason": "tier_watchlist"})
            results.append(TradePipelineResult(trade_key, "skipped", "tier_watchlist", None))
            continue

        if leader_tier == "paper_follow":
            logger.info(
                "tier_gate_paper",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "trade_key": trade_key,
                    "side": signal.side,
                    "condition_id": signal.condition_id,
                    "usdc_size": float(signal.usdc_size),
                },
            )
            await metrics.inc("trades_paper_total", labels=labels)
            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status="paper_follow",
            )
            results.append(TradePipelineResult(trade_key, "paper", "tier_paper_follow", None))
            continue

        # --- Decision gate: score the leader and this specific trade ---
        copy_decision_svc = _get_copy_decision()
        leader_scorer = _get_leader_scorer()

        # Score the leader using available context
        pnl_metrics = context.pnl_tracker.metrics() if hasattr(context.pnl_tracker, 'metrics') else {}
        leader_score = leader_scorer.score_leader(
            leader_id,
            analytics={
                "net_pnl": float(pnl_metrics.get("net_pnl", 0.0) or 0.0),
                "win_rate": float(pnl_metrics.get("win_rate", 50.0) or 50.0),
            },
            recent_trade_count=len(signals),
        )

        # Fetch live price for decision scoring
        live_price = float(signal.price or 0)
        market_min_size = float(ENV.get("MIN_ORDER_SIZE", 0.00001) or 0.00001)
        try:
            if hasattr(context, 'execution_service') and context.execution_service:
                meta = await context.execution_service.market_meta_service.get(signal.token_id)
                market_min_size = max(market_min_size, float(getattr(meta, "min_order_size", 0) or 0))
        except Exception:
            pass

        if signal.side == "BUY":
            target_notional = context.copy_notional(signal.usdc_size)
            max_notional_for_decision = target_notional
            if _balance_tiered_policy_enabled():
                buy_risk_cap = await _resolve_buy_risk_cap(context, leader_id, signal)
                if not buy_risk_cap.allow:
                    reason = (
                        f"balance regime [{buy_risk_cap.regime}] headroom: "
                        f"{buy_risk_cap.reason}"
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
                    await metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": buy_risk_cap.reason},
                    )
                    context.orders_repo.mark_processed_trade(
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
                    continue

                max_notional_for_decision = min(
                    target_notional,
                    buy_risk_cap.cap_notional_usdc,
                )

            # Run the copy decision gate
            decision = copy_decision_svc.decide_buy(
                trade=raw,
                leader_score=leader_score,
                live_price=live_price,
                base_notional_usdc=float(target_notional),
                max_notional_usdc=float(max_notional_for_decision),
                market_min_size=market_min_size,
            )

            log_trade_decision(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                decision_score=decision.decision_score,
                allow=decision.allow,
                reason=decision.reason,
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "leader_score": leader_score.score,
                    "trade_score": decision.trade_score,
                    "price_drift_pct": decision.price_drift_pct,
                    "edge_decay_pct": decision.edge_decay_pct,
                    "leader_notional": float(signal.usdc_size),
                    "tx_hash": signal.tx_hash,
                },
            )

            if not decision.allow:
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
                await metrics.inc("trades_skipped_total", labels={**labels, "reason": decision.skip_code})
                context.orders_repo.mark_processed_trade(
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
                continue

            # Use decision-gated notional (may be adjusted by scoring)
            gated_notional = Decimal(str(decision.target_notional_usdc)) if decision.target_notional_usdc > 0 else target_notional

            exec_req = ExecutionRequest(
                user_id=context.user_id,
                leader_id=leader_id,
                token_id=signal.token_id,
                side="BUY",
                amount=gated_notional,
                max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                reason="copy_trade",
                signal_id=signal.tx_hash,
            )
        else:
            pos = context.positions_repo.get_position(context.user_id, signal.condition_id)
            if pos is None or pos.size_tokens <= Decimal("0"):
                await metrics.inc("trades_skipped_total", labels={**labels, "reason": "no_position"})
                log_trade_skip(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(signal.size) if signal.size else None,
                    price=float(signal.price) if signal.price else None,
                    usdc_value=float(signal.usdc_size),
                    skip_code="no_position",
                    reason="no open position to sell",
                    event_slug=signal.event_slug,
                    meta={"user_id": context.user_id, "leader_id": leader_id},
                )
                context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="skipped_no_position",
                )
                results.append(TradePipelineResult(trade_key, "skipped", "no_position", None))
                continue

            sell_tokens = signal.size if signal.size > 0 else pos.size_tokens
            if sell_tokens > pos.size_tokens:
                sell_tokens = pos.size_tokens

            # Run the copy decision gate for SELL
            decision = copy_decision_svc.decide_sell(
                trade=raw,
                sell_tokens=float(sell_tokens),
                live_price=live_price,
                market_min_size=market_min_size,
                max_sell_tokens=float(pos.size_tokens),
            )

            log_trade_decision(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                decision_score=decision.decision_score,
                allow=decision.allow,
                reason=decision.reason,
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "sell_tokens": float(sell_tokens),
                    "position_size": float(pos.size_tokens),
                },
            )

            if not decision.allow:
                log_trade_skip(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(sell_tokens),
                    price=live_price,
                    usdc_value=float(signal.usdc_size),
                    skip_code=decision.skip_code,
                    reason=decision.reason,
                    event_slug=signal.event_slug,
                    meta={"user_id": context.user_id, "leader_id": leader_id},
                )
                await metrics.inc("trades_skipped_total", labels={**labels, "reason": decision.skip_code})
                context.orders_repo.mark_processed_trade(
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
                continue

            # Use decision-gated sell size
            gated_sell = Decimal(str(decision.target_size_tokens)) if decision.target_size_tokens > 0 else sell_tokens

            exec_req = ExecutionRequest(
                user_id=context.user_id,
                leader_id=leader_id,
                token_id=signal.token_id,
                side="SELL",
                amount=gated_sell,
                max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                reason="leader_sell",
                signal_id=signal.tx_hash,
            )

        exec_result = await context.execution_service.execute(exec_req)

        if exec_result.status == "duplicate_ignored":
            await metrics.inc("duplicate_order_prevented_total", labels=labels)
            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status="duplicate_ignored",
            )
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason="duplicate_ignored",
                    order_id=exec_result.order_id,
                )
            )
            continue

        if exec_result.accepted and exec_result.filled_amount <= 0:
            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status="submitted_unconfirmed",
            )
            logger.info(
                "trade_copy_submitted_unconfirmed",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "condition_id": signal.condition_id,
                    "side": signal.side,
                    "order_id": exec_result.order_id,
                },
            )
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="submitted_unconfirmed",
                    reason="accepted_no_fill_confirmed",
                    order_id=exec_result.order_id,
                )
            )
            continue

        if not exec_result.accepted:
            await metrics.inc("execution_failures_total", labels={**labels, "reason": exec_result.status})
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=float(signal.price) if signal.price else None,
                usdc_value=float(signal.usdc_size),
                skip_code=exec_result.status,
                reason=exec_result.error or exec_result.status,
                status="failed",
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "order_id": exec_result.order_id,
                },
            )
            is_terminal = exec_result.status == "rejected"
            max_non_terminal_attempts = _max_non_terminal_failure_attempts()
            prev_trade = context.orders_repo.get_processed_trade(trade_key) or {}
            prev_retry_attempt = _retry_attempt_from_status(prev_trade.get("status"))
            next_retry_attempt = prev_retry_attempt + 1
            non_terminal_retry_exhausted = (
                not is_terminal and next_retry_attempt >= max_non_terminal_attempts
            )
            status_to_persist = (
                f"failed_{exec_result.status}"
                if is_terminal
                else (
                    "failed_non_terminal_retry_exhausted"
                    if non_terminal_retry_exhausted
                    else f"retry_pending_{next_retry_attempt}"
                )
            )
            context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=status_to_persist,
            )
            fail_reason = exec_result.error or exec_result.status
            if non_terminal_retry_exhausted:
                fail_reason = (
                    f"{fail_reason} "
                    f"(retry budget exhausted after {next_retry_attempt} attempts)"
                )
            logger.warning(
                "trade_copy_failed",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "condition_id": signal.condition_id,
                    "side": signal.side,
                    "status": exec_result.status,
                    "error": exec_result.error,
                    "terminal": is_terminal,
                    "retry_exhausted": non_terminal_retry_exhausted,
                    "retry_attempt": next_retry_attempt,
                    "max_non_terminal_attempts": max_non_terminal_attempts,
                    "persisted_status": status_to_persist,
                },
            )
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="failed",
                    reason=fail_reason,
                    order_id=exec_result.order_id,
                )
            )
            continue

        fill_price = exec_result.avg_price or signal.price
        fill_size = Decimal(str(exec_result.filled_amount))
        fee_paid = Decimal(str(exec_result.fees_paid or 0))
        context.fills_repo.append_fill(
            FillRecord(
                user_id=context.user_id,
                leader_id=leader_id,
                order_id=exec_result.order_id,
                condition_id=signal.condition_id,
                token_id=signal.token_id,
                side=signal.side,
                size_tokens=fill_size,
                price=Decimal(str(fill_price or 0)),
                fee=fee_paid,
                fill_ts=int(now_ts),
            ),
            raw_payload={
                "signal_tx_hash": signal.tx_hash,
                "copy_order_id": exec_result.order_id,
                "status": exec_result.status,
            },
        )
        updated_position = context.positions_repo.apply_fill(
            user_id=context.user_id,
            leader_id=leader_id,
            condition_id=signal.condition_id,
            token_id=signal.token_id,
            side=signal.side,
            fill_size=fill_size,
            fill_price=Decimal(str(fill_price or 0)),
        )

        if signal.side == "BUY":
            context.pnl_tracker.record_buy(
                condition_id=signal.condition_id,
                price=float(fill_price or 0),
                size_tokens=float(fill_size),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                leader_wallet=leader_id,
                event_slug=signal.event_slug,
                title=signal.title,
            )
        else:
            entry_price = float(updated_position.avg_entry_price) if updated_position else float(signal.price or 0)
            realized = (float(fill_price or 0) - entry_price) * float(fill_size)
            context.pnl_tracker.record_sell(
                condition_id=signal.condition_id,
                price=float(fill_price or 0),
                size_tokens=float(fill_size),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                entry_price=entry_price,
                realized_pnl=realized,
                exit_reason="leader_sell",
                leader_wallet=leader_id,
                event_slug=signal.event_slug,
                title=signal.title,
            )

        await metrics.inc("trades_copied_total", labels=labels)

        log_trade_submitted(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(fill_size),
            price=float(fill_price or 0),
            usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
            event_slug=signal.event_slug,
            meta={
                "user_id": context.user_id,
                "leader_id": leader_id,
                "order_id": exec_result.order_id,
                "fee_paid": float(fee_paid),
            },
        )
        log_trade_execution(
            logger,
            side=signal.side,
            condition_id=signal.condition_id,
            asset=signal.token_id,
            size=float(fill_size),
            price=float(fill_price or 0),
            usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
            tx_hash=signal.tx_hash,
            event_slug=signal.event_slug,
            meta={
                "user_id": context.user_id,
                "leader_id": leader_id,
                "order_id": exec_result.order_id,
                "fee_paid": float(fee_paid),
                "status": exec_result.status,
            },
        )

        context.orders_repo.mark_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=signal.tx_hash,
            side=signal.side,
            asset=signal.token_id,
            condition_id=signal.condition_id,
            status="executed",
        )

        tx_hashes = raw.get("_txHashes") or raw.get("txHashes") or []
        if isinstance(tx_hashes, list):
            for txh in tx_hashes:
                tx_key = build_trade_key(
                    context.user_id,
                    leader_id,
                    {
                        "transactionHash": txh,
                        "side": signal.side,
                        "asset": signal.token_id,
                    },
                )
                context.orders_repo.mark_processed_trade(
                    trade_key=tx_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=str(txh),
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="executed",
                )

        update_readiness(last_successful_order_ts=int(time.time()))
        results.append(
            TradePipelineResult(
                trade_key=trade_key,
                status="executed",
                reason=None,
                order_id=exec_result.order_id,
            )
        )

    return results
