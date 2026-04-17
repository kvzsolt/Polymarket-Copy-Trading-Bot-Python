"""Auto-redeem — claims USDC from winning positions after market resolution."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from services.redemptionService import RedemptionService
from utils.logger import get_logger

logger = get_logger("AutoRedeem")

async def auto_redeem_loop(clob_client, config: AppConfig):
    """Background task that periodically checks for resolved markets and redeems winnings.

    Runs as an asyncio task alongside the trade monitor and risk monitor.
    Uses the logic ported from the TypeScript reference implementation.
    """
    enabled = config.redemption.enable_auto_redeem
    interval = config.redemption.redeem_check_interval

    if not enabled:
        logger.info("Auto-redeem disabled — will not check for resolved markets")
        return

    rpc_url = config.network.rpc_url
    if not rpc_url:
        logger.error("Auto-redeem disabled: RPC_URL not configured")
        return

    logger.info(f"Auto-redeem started: check every {interval}s")
    redemption_service = RedemptionService(clob_client, config)
    user_id = config.wallet.funder_address
    if not user_id:
        logger.error("Auto-redeem disabled: missing FUNDER_ADDRESS")
        return

    while True:
        try:
            # 1. Load and filter positions using the TS logic criteria
            redeemables = await redemption_service.scan_redeemables(user_id)
            
            if redeemables:
                # 2. Group positions by conditionId to avoid duplicate redemptions
                positions_by_condition: dict[str, list] = {}
                for pos in redeemables:
                    positions_by_condition.setdefault(pos.condition_id, []).append(pos)
                
                logger.info(f"Found {len(redeemables)} redeemable positions grouped into {len(positions_by_condition)} conditions.")

                # 3. Redeem once per condition
                for condition_id, pos_group in positions_by_condition.items():
                    total_value = sum(p.current_value for p in pos_group)
                    logger.info(f"Redeeming condition {condition_id} (value: ${total_value:.2f})")
                    
                    for p in pos_group:
                        status = "WIN" if p.cur_price >= 0.99 else "LOSS"
                        logger.info(f"   -> {status} | {p.title} | {p.outcome} | {p.size:.2f} tokens")
                    
                    result = await redemption_service.redeem_condition(condition_id)
                    if result.submitted and result.status == "confirmed":
                        logger.info(f"✅ Successfully redeemed condition {condition_id}")
                    elif result.error:
                        logger.error(f"❌ Failed to redeem condition {condition_id}: {result.error}")
                        
                    # Small delay between transactions
                    await asyncio.sleep(2)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Auto-redeem loop error: {e}")

        await asyncio.sleep(interval)
