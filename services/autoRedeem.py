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
            redeemables = await redemption_service.scan_redeemables(user_id)
            for pos in redeemables:
                result = await redemption_service.redeem(pos)
                if result.submitted:
                    logger.info(
                        "Redeem submitted condition=%s size=%s status=%s tx=%s",
                        pos.condition_id[:12] + "…",
                        pos.size,
                        result.status,
                        result.tx_hash,
                    )
                elif result.error:
                    logger.warning(
                        "Redeem skipped/failed condition=%s reason=%s",
                        pos.condition_id[:12] + "…",
                        result.error,
                    )
        except Exception as e:
            logger.error(f"Auto-redeem loop error: {e}")

        await asyncio.sleep(interval)
