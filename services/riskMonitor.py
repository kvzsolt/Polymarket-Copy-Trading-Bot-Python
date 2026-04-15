"""Risk monitor — background task that checks open positions for SL/TP triggers."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from domain.trading_context import TradingContext
from services.accountingService import AccountingService, PositionLot
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.healthServer import ReadinessTracker
from services.marketMetaService import MarketMetaService
from services.metrics import metrics
from services.orderRepository import OrderRepository
from services.riskEngine import active_policy
from storage.sqlite.fills_repo import FillRecord
from utils import is_valid_clob_token_id
from utils.logger import get_logger, log_risk_exit

logger = get_logger("RiskMonitor")

# Track token_ids we've already warned about to avoid log spam every cycle.
_warned_invalid_tokens: set[str] = set()


async def risk_monitor_loop(
    clob_client,
    bot_params: dict,
    position_manager,
    pnl_tracker,
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig | None = None,
):
    """Continuously check open positions against stop-loss / take-profit levels.

    Runs as a background asyncio task alongside the trade monitor.

    Legacy compatibility path: active runtime uses
    `risk_monitor_loop_for_contexts` with persisted position repositories.
    """
    pm = position_manager

    if config is not None:
        interval = config.polling.risk_check_interval
        enabled = config.risk.enable_risk_mgmt
        slippage_bps = int(config.execution.slippage_tolerance * 100)
        funder_address = config.wallet.funder_address
        enable_tiered = config.risk.enable_balance_tiered_policy
        sl_pct = config.risk.stop_loss_pct
        tp_pct = config.risk.take_profit_pct
        fee_bps = int(config.execution.transaction_fee_bps)
    else:
        interval = 15
        enabled = True
        slippage_bps = 200
        funder_address = ""
        enable_tiered = False
        sl_pct = 20
        tp_pct = 30
        fee_bps = 20

    execution_service = ExecutionService(
        clob_client=clob_client,
        market_meta_service=MarketMetaService(clob_client),
        fee_service=FeeService(clob_client, default_fee_bps=fee_bps),
        order_repo=OrderRepository(),
    )
    accounting = AccountingService()

    if not enabled:
        logger.info("Risk management disabled — monitor will not run")
        return

    logger.info(
        f"Risk monitor started: SL={sl_pct}% "
        f"TP={tp_pct}% interval={interval}s"
    )

    while True:
        try:
            readiness_tracker.update(last_risk_check_ts=int(time.time()))
            positions = pm.open_positions
            for cid, pos in positions.items():
                if not is_valid_clob_token_id(pos.asset):
                    logger.warning("Legacy risk monitor: skipping invalid asset=%r cid=%s", pos.asset, cid)
                    continue
                try:
                    price_data = await clob_client.get_price(pos.asset, 'SELL')
                    if not price_data or price_data.get('price') is None:
                        continue
                    current_price = float(str(price_data['price']).strip())
                    if current_price <= 0:
                        continue

                    action = pm.evaluate_risk_action(cid, current_price)
                    if action:
                        trigger = str(action.get("reason") or "risk_exit")
                        fraction = float(action.get("size_fraction", 1.0) or 1.0)

                        # Simplified account-cap policy: partial exits are a single global choice.
                        if enable_tiered and fraction < 1.0:
                            policy = active_policy()
                            if not policy.allow_partial_exits:
                                logger.info(
                                    "Balance policy [%s] upgrades partial exit to full for %s…",
                                    policy.name,
                                    cid[:12],
                                )
                                fraction = 1.0

                        sell_size = pos.size_tokens * max(0.0, min(1.0, fraction))
                        if sell_size <= 0:
                            continue
                        logger.info(
                            f"Executing risk {trigger} for {cid[:12]}… "
                            f"tokens={sell_size:.4f}"
                        )
                        exec_req = ExecutionRequest(
                            user_id=funder_address,
                            leader_id=pos.leader_wallet or "risk-monitor",
                            token_id=pos.asset,
                            side="SELL",
                            amount=Decimal(str(sell_size)),
                            max_slippage_bps=slippage_bps,
                            reason=trigger,
                            signal_id=f"risk:{trigger}:{cid}:{int(time.time())}",
                        )
                        result = await execution_service.execute(exec_req)
                        if result.accepted and result.filled_amount > 0:
                            fill_tokens = float(result.filled_amount)
                            fill_price = float(result.avg_price or current_price)
                            usdc_value = fill_price * fill_tokens
                            tracker = pnl_tracker
                            tracked = pm.get_position(cid)
                            entry_price = tracked.entry_price if tracked else pos.entry_price
                            pnl = (fill_price - entry_price) * fill_tokens
                            log_risk_exit(
                                logger,
                                condition_id=cid,
                                asset=pos.asset,
                                size=fill_tokens,
                                price=fill_price,
                                usdc_value=usdc_value,
                                reason=trigger,
                                event_slug=pos.event_slug,
                                meta={
                                    "copyOrderId": result.order_id,
                                    "leaderWallet": pos.leader_wallet,
                                },
                            )
                            pm.track_sell(cid, fill_price, fill_tokens, reason=trigger)
                            tracker.record_sell(
                                condition_id=cid,
                                price=fill_price,
                                size_tokens=fill_tokens,
                                usdc_value=usdc_value,
                                entry_price=entry_price,
                                realized_pnl=pnl,
                                exit_reason=trigger,
                                leader_wallet=pos.leader_wallet,
                                event_slug=pos.event_slug,
                                title=pos.title,
                            )
                            user_id = str(
                                bot_params.get("proxyWallet")
                                or funder_address
                            )
                            accounting.book_fill(
                                user_id=user_id,
                                fill_doc={
                                    "order_id": result.order_id,
                                    "condition_id": cid,
                                    "token_id": pos.asset,
                                    "side": "SELL",
                                    "size_tokens": str(fill_tokens),
                                    "price": str(fill_price),
                                    "usdc_value": str(usdc_value),
                                    "leader_id": pos.leader_wallet,
                                    "category": str(pos.category or "unknown"),
                                    "reason": trigger,
                                },
                            )
                            remaining = pm.get_position(cid)
                            if remaining is None:
                                accounting.close_position(user_id=user_id, condition_id=str(cid or ""))
                            else:
                                accounting.upsert_position(
                                    PositionLot(
                                        user_id=user_id,
                                        condition_id=str(cid or ""),
                                        token_id=str(remaining.asset or pos.asset),
                                        size_tokens=Decimal(str(remaining.size_tokens)),
                                        avg_entry_price=Decimal(str(remaining.entry_price)),
                                        leader_id=remaining.leader_wallet or pos.leader_wallet,
                                        category=str(remaining.category or pos.category or "unknown"),
                                    )
                                )
                except Exception as e:
                    logger.error(f"Risk check error for {cid[:12]}…: {e}")

        except Exception as e:
            logger.error(f"Risk monitor loop error: {e}")

        await asyncio.sleep(interval)


def rehydrate_position_manager(context: TradingContext) -> None:
    """Load persisted open positions into the context position manager."""
    pm = context.position_manager
    if not hasattr(pm, "track_buy"):
        return

    for pos in context.positions_repo.list_open_positions(context.user_id):
        existing = None
        if hasattr(pm, "get_position"):
            try:
                existing = pm.get_position(pos.condition_id)
            except Exception:
                existing = None
        if existing is not None:
            continue
        try:
            pm.track_buy(
                condition_id=pos.condition_id,
                asset=pos.token_id,
                entry_price=float(pos.avg_entry_price),
                size_tokens=float(pos.size_tokens),
                usdc_cost=float(pos.size_tokens * pos.avg_entry_price),
                leader_wallet=pos.leader_id or "",
            )
        except Exception:
            continue


def _fallback_risk_decision(
    entry_price: Decimal,
    current_price: Decimal,
    *,
    config: AppConfig | None = None,
) -> dict | None:
    """Simple fallback SL/TP decision when position manager is unavailable."""
    if entry_price <= 0:
        return None
    if config is not None:
        sl_pct = Decimal(str(config.risk.stop_loss_pct)) / Decimal("100")
        tp_pct = Decimal(str(config.risk.take_profit_pct)) / Decimal("100")
    else:
        sl_pct = Decimal("0.20")
        tp_pct = Decimal("0.30")
    pct_change = (current_price - entry_price) / entry_price
    if pct_change <= -sl_pct:
        return {"reason": "stop_loss", "size_fraction": 1.0}
    if pct_change >= tp_pct:
        return {"reason": "take_profit", "size_fraction": 1.0}
    return None


async def run_risk_check_for_context(context: TradingContext) -> None:
    if not context.execution_service:
        return

    rehydrate_position_manager(context)
    slippage_bps = int(float(context.extra.get("slippage_tolerance_bps", 200)))

    # Read enable_tiered from context extra (set by bootstrap from config)
    enable_tiered = str(context.extra.get("enable_balance_tiered_policy", "false")).lower() in ("1", "true", "yes")

    for pos in context.positions_repo.list_open_positions(context.user_id):
        if not is_valid_clob_token_id(pos.token_id):
            if pos.token_id not in _warned_invalid_tokens:
                _warned_invalid_tokens.add(pos.token_id)
                logger.warning(
                    "Skipping position with invalid token_id=%r (condition=%s) — "
                    "likely stale placeholder; close or delete manually",
                    pos.token_id, pos.condition_id,
                )
            continue
        try:
            price_data = await context.client.get_price(pos.token_id, "SELL")
            if not price_data or price_data.get("price") is None:
                continue
            current_price = Decimal(str(price_data["price"]))
            if current_price <= 0:
                continue

            action = None
            if hasattr(context.position_manager, "evaluate_risk_action"):
                action = context.position_manager.evaluate_risk_action(pos.condition_id, float(current_price))
            if not action:
                action = _fallback_risk_decision(pos.avg_entry_price, current_price)
            if not action:
                continue

            fraction = max(0.0, min(1.0, float(action.get("size_fraction", 1.0) or 1.0)))
            reason = str(action.get("reason") or "risk_exit")

            # Simplified account-cap policy
            if fraction < 1.0 and enable_tiered:
                policy = active_policy()
                if not policy.allow_partial_exits:
                    logger.info(
                        "Balance policy [%s] upgrades partial exit → full for %s (was %.0f%%)",
                        policy.name, pos.condition_id[:12], fraction * 100,
                    )
                    fraction = 1.0

            sell_size = (pos.size_tokens * Decimal(str(fraction))).quantize(Decimal("0.00000001"))
            if sell_size <= 0:
                continue

            req = ExecutionRequest(
                user_id=context.user_id,
                leader_id=pos.leader_id or "risk-monitor",
                token_id=pos.token_id,
                side="SELL",
                amount=sell_size,
                max_slippage_bps=slippage_bps,
                reason=reason,
                signal_id=f"risk:{reason}:{pos.condition_id}:{int(time.time())}",
            )
            result = await context.execution_service.execute(req)
            if not result.accepted or result.filled_amount <= 0:
                continue

            fill_size = Decimal(str(result.filled_amount))
            fill_price = Decimal(str(result.avg_price or current_price))
            context.fills_repo.append_fill(
                FillRecord(
                    user_id=context.user_id,
                    leader_id=pos.leader_id,
                    order_id=result.order_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                    side="SELL",
                    size_tokens=fill_size,
                    price=fill_price,
                    fee=Decimal(str(result.fees_paid or 0)),
                    fill_ts=int(time.time()),
                ),
            )
            updated_pos = context.positions_repo.apply_fill(
                user_id=context.user_id,
                leader_id=pos.leader_id,
                condition_id=pos.condition_id,
                token_id=pos.token_id,
                side="SELL",
                fill_size=fill_size,
                fill_price=fill_price,
            )
            if hasattr(context.pnl_tracker, "record_sell"):
                entry_price = float(pos.avg_entry_price)
                realized = (float(fill_price) - entry_price) * float(fill_size)
                context.pnl_tracker.record_sell(
                    condition_id=pos.condition_id,
                    price=float(fill_price),
                    size_tokens=float(fill_size),
                    usdc_value=float(fill_size * fill_price),
                    entry_price=entry_price,
                    realized_pnl=realized,
                    exit_reason=reason,
                    leader_wallet=pos.leader_id or "",
                )
            await metrics.inc(
                "risk_exits_total",
                labels={
                    "user_id": context.user_id,
                    "leader_id": str(pos.leader_id or ""),
                    "reason": reason,
                },
            )
            if hasattr(context.position_manager, "track_sell"):
                context.position_manager.track_sell(
                    pos.condition_id,
                    float(fill_price),
                    float(fill_size),
                    reason=reason,
                )
        except Exception as exc:
            logger.error(f"Risk context check failed user={context.user_id} condition={pos.condition_id}: {exc}")


async def risk_monitor_loop_for_contexts(
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig | None = None,
    interval: int | None = None,
) -> None:
    """Restart-safe risk monitor using persisted positions as the source of truth."""
    if not contexts:
        logger.warning("Risk monitor started with no contexts")
        return

    if config is not None:
        enabled = config.risk.enable_risk_mgmt
        poll_interval = int(interval or config.polling.risk_check_interval)
    else:
        enabled = True
        poll_interval = int(interval or 15)

    if not enabled:
        logger.info("Risk management disabled — monitor will not run")
        return

    while True:
        readiness_tracker.update(last_risk_check_ts=int(time.time()))
        for context in contexts:
            try:
                await run_risk_check_for_context(context)
            except Exception as exc:
                logger.error(f"Risk loop error user={context.user_id}: {exc}")
        await asyncio.sleep(poll_interval)
