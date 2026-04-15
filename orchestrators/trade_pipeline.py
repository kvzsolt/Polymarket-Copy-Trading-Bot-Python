from __future__ import annotations

import sqlite3
import time
import traceback as _tb
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from config.env import ENV
from domain.models import TradeSignal
from domain.trading_context import TradingContext
from services.executionService import ExecutionRequest
from services.metrics import metrics
from services.riskEngine import headroom_for_buy
from storage.sqlite.fills_repo import FillRecord
from utils import is_valid_clob_token_id
from utils.dayEquity import get_day_start_equity
from utils.getMyBalance import get_my_balance
from utils.logger import (
    get_logger,
    log_trade_execution,
    log_trade_submitted,
    log_trade_decision,
    log_trade_skip,
    print_executing_strategy,
    print_skip_reason,
    print_trade_result,
)

logger = get_logger("TradePipeline")


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
    policy_name: str
    cap_notional_usdc: Decimal



def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _first_text(raw: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_trade_outcome(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "outcome",
        "outcomeName",
        "outcome_name",
        "tokenOutcome",
        "token_outcome",
        "selection",
        "selectionName",
        "choice",
    )


def _extract_trade_submarket(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "submarket",
        "subMarket",
        "sub_market",
        "marketSubtype",
        "market_subtype",
        "marketType",
        "market_type",
        "lineType",
        "line_type",
        "betType",
        "bet_type",
        "subMarketName",
        "groupItemTitle",
        "groupTitle",
    )


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
    asset = trade.get("tokenID") or trade.get("token_id") or trade.get("asset") or ""
    return f"{_normalize(user_id)}|{_normalize(leader_id)}|{_normalize(tx_hash)}|{side}|{_normalize(asset)}"



def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _max_non_terminal_failure_attempts() -> int:
    raw = ENV.get("NON_TERMINAL_FAILURE_MAX_ATTEMPTS", 3)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


_RETRYABLE_FAILED_STATUS_PREFIX = "retryable_failed_"
_RETRYABLE_FAILED_STATUS = "retryable_failed"
_LEGACY_RETRY_PREFIX = "retry_pending_"


def _retry_attempt_from_status(status: str | None, retry_attempt: int | None = None) -> int:
    """Return the retry attempt number.

    Prefers the explicit ``retry_attempt`` column value when available.
    Falls back to parsing legacy status strings (``retryable_failed_<n>``
    or ``retry_pending_<n>``) for rows written before the column existed.
    """
    if retry_attempt is not None and retry_attempt > 0:
        return retry_attempt
    text = str(status or "").strip().lower()
    for prefix in (_RETRYABLE_FAILED_STATUS_PREFIX, _LEGACY_RETRY_PREFIX):
        if text.startswith(prefix):
            suffix = text.replace(prefix, "", 1)
            try:
                return int(suffix)
            except (TypeError, ValueError):
                return 0
    return 0


def _account_cap_policy_enabled() -> bool:
    return bool(ENV.get("ENABLE_BALANCE_TIERED_POLICY", False))


def _position_notional_usdc(position: Any) -> Decimal:
    size_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    avg_entry_price = _to_decimal(getattr(position, "avg_entry_price", 0))
    notional = size_tokens * avg_entry_price
    return notional if notional > 0 else Decimal("0")


def _derive_sell_tokens(signal: TradeSignal, position: Any) -> Decimal:
    """Derive the copied sell size from the signal payload.

    Some leader payloads carry SELL notional but leave `size` empty. In that
    case, use `usdc_size / price` instead of dumping the full position.
    """
    if signal.size > 0:
        return signal.size

    if signal.usdc_size > 0 and signal.price > 0:
        derived = signal.usdc_size / signal.price
        if derived > 0:
            return derived

    return _to_decimal(getattr(position, "size_tokens", 0))


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
            policy_name="unknown",
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
        policy_name=headroom.policy_name,
        cap_notional_usdc=headroom.cap_notional_usdc,
    )



def normalize_signals(raw_signals: list[dict[str, Any]], leader_id: str) -> list[tuple[TradeSignal, dict[str, Any]]]:
    out: list[tuple[TradeSignal, dict[str, Any]]] = []
    for raw in raw_signals:
        side = str(raw.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            continue

        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        token_id = str(raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or "")
        tx_hash = str(raw.get("transactionHash") or raw.get("txHash") or "")
        if not condition_id or not token_id or not tx_hash:
            continue
        if not is_valid_clob_token_id(token_id):
            logger.warning("normalize_signals: skipping signal with invalid token_id=%r", token_id)
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
            outcome=_extract_trade_outcome(raw),
            submarket=_extract_trade_submarket(raw),
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
        sell_entry_price: float | None = None
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

        # print_new_trade_detected removed: tradeMonitor already prints on detection

        # --- Tier gate: check follow tier before costly decision/execution ---
        leader_tier = _resolve_leader_tier(context, leader_id)

        # Global paper-trade-only override: if PAPER_TRADE_ONLY=true, treat every
        # signal as paper_follow regardless of the leader's actual tier. This allows
        # safe dry-run verification without touching live order submission.
        if ENV.get("PAPER_TRADE_ONLY", False) and leader_tier == "live_follow":
            logger.info(
                "paper_trade_only_override",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "trade_key": trade_key,
                    "side": signal.side,
                    "condition_id": signal.condition_id,
                    "original_tier": leader_tier,
                },
            )
            leader_tier = "paper_follow"

        if leader_tier == "watchlist" and ENV.get("ENABLE_TIER_GATE_WATCHLIST", True):
            if ENV.get("ENABLE_TIER_GATE_WATCHLIST_LOG", True):
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
                print_skip_reason("Tier gate: watchlist", "Leader is on watchlist — observing only")
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
            print_skip_reason("Tier gate: paper follow", "Paper-tracking only — no live order submitted")
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
        copy_decision_svc = context.copy_decision_service
        leader_scorer = context.leader_scorer

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
            sizing_result = context.copy_notional(signal.usdc_size)
            target_notional = sizing_result.final_notional_usdc
            max_notional_for_decision = target_notional

            buy_risk_cap = None
            if _account_cap_policy_enabled():
                buy_risk_cap = await _resolve_buy_risk_cap(context, leader_id, signal)
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

            # --- Sizing audit: unified log showing both sizing sources ---
            policy_binding = (
                buy_risk_cap is not None
                and buy_risk_cap.cap_notional_usdc < target_notional
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
                _account_cap_policy_enabled(),
                f"{float(buy_risk_cap.cap_notional_usdc):.4f}[{buy_risk_cap.policy_name}]"
                if buy_risk_cap else "n/a",
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
                    "policy_enabled": _account_cap_policy_enabled(),
                    "policy_cap": float(buy_risk_cap.cap_notional_usdc) if buy_risk_cap else None,
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
                if decision.skip_code != "signal_too_old":
                    skip_title = (
                        "BUY blocked by execution minimums/risk cap"
                        if decision.skip_code == "risk_cap_too_low_for_min_order"
                        else "Decision gate rejected BUY"
                    )
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

            # Log final execution sizing if decision gate changed the amount
            if float(gated_notional) != float(target_notional):
                logger.info(
                    "sizing_final | cond=%s decision_gate adjusted %.4f → %.4f "
                    "(trust_mult=%.3f)",
                    signal.condition_id[:12],
                    float(target_notional),
                    float(gated_notional),
                    decision.trust_multiplier if hasattr(decision, "trust_multiplier") else 0,
                    extra={
                        "type": "sizing_final",
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                        "original_notional": float(target_notional),
                        "gated_notional": float(gated_notional),
                    },
                )

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
                # Distinguish "never bought this market" from "position already closed"
                if pos is None:
                    skip_detail = "never held — we never entered this market"
                    skip_code = "never_held"
                else:
                    skip_detail = "already closed — position size is zero"
                    skip_code = "already_closed"
                logger.debug(
                    "sell_skip",
                    extra={
                        "reason": skip_code,
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                    },
                )
                await metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
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
                context.orders_repo.mark_processed_trade(
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
                continue

            # Capture entry price before apply_fill mutates or closes/deletes the position.
            sell_entry_price = _to_optional_float(getattr(pos, "avg_entry_price", None))

            sell_tokens = _derive_sell_tokens(signal, pos)
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
                print_skip_reason("Decision gate rejected SELL", decision.reason)
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

        print_executing_strategy(signal.side)
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
            prev_retry_attempt = _retry_attempt_from_status(
                prev_trade.get("status"), prev_trade.get("retry_attempt")
            )
            next_retry_attempt = prev_retry_attempt + 1
            non_terminal_retry_exhausted = (
                not is_terminal and next_retry_attempt >= max_non_terminal_attempts
            )
            status_to_persist = (
                "terminal_failed"
                if is_terminal
                else (
                    "terminal_failed"
                    if non_terminal_retry_exhausted
                    else _RETRYABLE_FAILED_STATUS
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
                retry_attempt=next_retry_attempt,
            )
            fail_reason = exec_result.error or exec_result.status
            if non_terminal_retry_exhausted:
                fail_reason = (
                    f"{fail_reason} "
                    f"(retry budget exhausted after {next_retry_attempt} attempts)"
                )
            print_trade_result(
                success=False,
                side=signal.side,
                reason=fail_reason,
                title=signal.title,
                submarket=signal.submarket,
                outcome=signal.outcome,
            )
            logger.warning(
                "trade_copy_failed",
                extra={
                    "type": "trade_copy_failed",
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "condition_id": signal.condition_id,
                    "side": signal.side,
                    "status": exec_result.status,
                    "error": exec_result.error,
                    "title": signal.title,
                    "submarket": signal.submarket,
                    "outcome": signal.outcome,
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
        try:
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
        except sqlite3.IntegrityError as ie:
            error_msg = str(ie)
            if "idx_fills_unique" in error_msg or "UNIQUE constraint failed: fills" in error_msg:
                # Duplicate fill — idempotent, skip gracefully
                logger.info(
                    "trade_pipeline_duplicate_fill_ignored",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "order_id": exec_result.order_id,
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
            # Non-duplicate integrity error — log and re-raise
            logger.error(
                "trade_pipeline_integrity_error",
                extra={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "condition_id": signal.condition_id,
                    "signal_tx_hash": signal.tx_hash,
                    "order_id": exec_result.order_id,
                    "traceback": _tb.format_exc(),
                },
            )
            raise

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
            updated_entry_price = _to_optional_float(
                getattr(updated_position, "avg_entry_price", None) if updated_position is not None else None
            )
            signal_entry_price = _to_optional_float(signal.price)
            entry_price = next(
                (
                    candidate
                    for candidate in (sell_entry_price, updated_entry_price, signal_entry_price)
                    if candidate is not None
                ),
                0.0,
            )
            if entry_price == 0.0 and all(
                candidate is None for candidate in (sell_entry_price, updated_entry_price, signal_entry_price)
            ):
                logger.warning(
                    "trade_sell_entry_price_missing",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "order_id": exec_result.order_id,
                    },
                )
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
                "copyOrderId": exec_result.order_id,
                "order_id": exec_result.order_id,
                "fee_paid": float(fee_paid),
                "title": signal.title,
                "submarket": signal.submarket,
                "outcome": signal.outcome,
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
                "copyOrderId": exec_result.order_id,
                "order_id": exec_result.order_id,
                "fee_paid": float(fee_paid),
                "status": exec_result.status,
                "title": signal.title,
                "submarket": signal.submarket,
                "outcome": signal.outcome,
            },
        )
        print_trade_result(
            success=True,
            side=signal.side,
            leader_tx=signal.tx_hash,
            copy_tx=exec_result.order_id,
            amount=float(fill_size * Decimal(str(fill_price or 0))),
            price=float(fill_price or 0),
            title=signal.title,
            submarket=signal.submarket,
            outcome=signal.outcome,
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

        context.readiness.update(last_successful_order_ts=int(time.time()))
        results.append(
            TradePipelineResult(
                trade_key=trade_key,
                status="executed",
                reason=None,
                order_id=exec_result.order_id,
            )
        )

    return results
