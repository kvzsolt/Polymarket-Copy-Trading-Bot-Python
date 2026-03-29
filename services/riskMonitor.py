"""Risk monitor — background task that checks open positions for SL/TP triggers."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

from config.env import ENV
from services.accountingService import AccountingService, PositionLot
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.marketMetaService import MarketMetaService
from services.orderRepository import OrderRepository
from services.positionManager import get_position_manager
from services.pnlTracker import get_pnl_tracker
from services.riskEngine import regime_for_equity, get_regime
from utils.logger import get_logger, log_risk_exit

logger = get_logger("RiskMonitor")


async def risk_monitor_loop(clob_client, bot_params: dict):
    """Continuously check open positions against stop-loss / take-profit levels.

    Runs as a background asyncio task alongside the trade monitor.
    """
    pm = get_position_manager()
    interval = ENV.get('RISK_CHECK_INTERVAL', 15)
    enabled = ENV.get('ENABLE_RISK_MGMT', True)
    slippage_bps = int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100)
    execution_service = ExecutionService(
        clob_client=clob_client,
        market_meta_service=MarketMetaService(clob_client),
        fee_service=FeeService(clob_client),
        order_repo=OrderRepository(),
    )
    accounting = AccountingService()

    if not enabled:
        logger.info("Risk management disabled — monitor will not run")
        return

    logger.info(
        f"Risk monitor started: SL={ENV.get('STOP_LOSS_PCT', 20)}% "
        f"TP={ENV.get('TAKE_PROFIT_PCT', 30)}% interval={interval}s"
    )

    while True:
        try:
            positions = pm.open_positions
            for cid, pos in positions.items():
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

                        # Regime-aware partial exit gating
                        enable_tiered = str(ENV.get("ENABLE_BALANCE_TIERED_POLICY", "false")).lower() in ("1", "true", "yes")
                        if enable_tiered and fraction < 1.0:
                            # Estimate equity from balance (simplified — no full portfolio snapshot)
                            try:
                                from utils.getMyBalance import get_my_balance
                                proxy_wallet = str(
                                    bot_params.get("proxyWallet")
                                    or ENV.get("FUNDER_ADDRESS")
                                    or ENV.get("PROXY_WALLET")
                                    or ""
                                )
                                bal = await get_my_balance(proxy_wallet)
                                equity = Decimal(str(bal or 0))
                                rname = regime_for_equity(equity)
                                regime = get_regime(rname)
                                if not regime.allow_partial_exits:
                                    # Micro regime: upgrade partial to full exit
                                    logger.info(
                                        f"Regime [{rname}] upgrades partial exit to full for {cid[:12]}…"
                                    )
                                    fraction = 1.0
                            except Exception as e:
                                logger.warning(f"Regime check failed, proceeding with original fraction: {e}")

                        sell_size = pos.size_tokens * max(0.0, min(1.0, fraction))
                        if sell_size <= 0:
                            continue
                        logger.info(
                            f"Executing risk {trigger} for {cid[:12]}… "
                            f"tokens={sell_size:.4f}"
                        )
                        exec_req = ExecutionRequest(
                            user_id=str(
                                ENV.get("FUNDER_ADDRESS")
                                or ENV.get("PROXY_WALLET")
                                or ENV.get("USER_ADDRESS")
                                or ""
                            ),
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
                            tracker = get_pnl_tracker()
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
                                or ENV.get("FUNDER_ADDRESS")
                                or ENV.get("PROXY_WALLET")
                                or ENV.get("USER_ADDRESS")
                                or ""
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
