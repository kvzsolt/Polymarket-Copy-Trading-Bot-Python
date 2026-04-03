from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from config.env import ENV
from domain.models import TradeSignal
from domain.trading_context import TradingContext
from services.executionService import ExecutionRequest
from services.healthServer import update_readiness
from services.metrics import metrics
from storage.sqlite.fills_repo import FillRecord
from utils.logger import get_logger

logger = get_logger("TradePipeline")


@dataclass
class TradePipelineResult:
    trade_key: str
    status: str
    reason: str | None
    order_id: str | None



def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()



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

        logger.info(
            "trade_copy_decision",
            extra={
                "user_id": context.user_id,
                "leader_id": leader_id,
                "condition_id": signal.condition_id,
                "token_id": signal.token_id,
                "side": signal.side,
                "tx_hash": signal.tx_hash,
                "leader_notional": float(signal.usdc_size),
            },
        )

        if signal.side == "BUY":
            target_notional = context.copy_notional(signal.usdc_size)
            exec_req = ExecutionRequest(
                user_id=context.user_id,
                leader_id=leader_id,
                token_id=signal.token_id,
                side="BUY",
                amount=target_notional,
                max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                reason="copy_trade",
                signal_id=signal.tx_hash,
            )
        else:
            pos = context.positions_repo.get_position(context.user_id, signal.condition_id)
            if pos is None or pos.size_tokens <= Decimal("0"):
                await metrics.inc("trades_skipped_total", labels={**labels, "reason": "no_position"})
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

            exec_req = ExecutionRequest(
                user_id=context.user_id,
                leader_id=leader_id,
                token_id=signal.token_id,
                side="SELL",
                amount=sell_tokens,
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
