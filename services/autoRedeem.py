"""Auto-redeem — claims USDC from winning positions after market resolution."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from services.redemptionService import RedemptionService
from storage.sqlite.positions_repo import PositionsRepo
from utils.logger import get_logger

logger = get_logger("AutoRedeem")


def _prune_redeemed_positions(positions_repo: PositionsRepo, user_id: str, positions: list) -> int:
    pruned = 0
    for pos in positions:
        condition_id = str(getattr(pos, "condition_id", "") or "").strip()
        token_id = str(getattr(pos, "token_id", "") or "").strip()
        if not condition_id:
            continue
        positions_repo.delete_position(user_id, condition_id, token_id=token_id or None)
        pruned += 1
    return pruned


async def auto_redeem_loop(clob_client, config: AppConfig):
    """Background task that periodically checks for resolved markets and redeems winnings.

    Runs as an asyncio task alongside the trade monitor and risk monitor.
    Uses the logic ported from the TypeScript reference implementation.
    """
    enabled = config.redemption.enable_auto_redeem
    interval = config.polling.redeem_check_interval

    if not enabled:
        logger.info("Auto-redeem disabled — will not check for resolved markets")
        return

    rpc_url = config.network.rpc_url
    if not rpc_url:
        logger.error("Auto-redeem disabled: RPC_URL not configured")
        return

    logger.info(f"Auto-redeem started: check every {interval}s")
    redemption_service = RedemptionService(clob_client, config)
    positions_repo = PositionsRepo(config.database.db_path)
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

                estimated_value = sum(float(pos.current_value or 0.0) for pos in redeemables)
                logger.info(
                    "redemption_scan",
                    extra={
                        "type": "redemption_scan",
                        "intervalSeconds": interval,
                        "redeemableCount": len(redeemables),
                        "conditionCount": len(positions_by_condition),
                        "estimatedValue": estimated_value,
                    },
                )

                # 3. Redeem once per condition
                for condition_id, pos_group in positions_by_condition.items():
                    total_value = sum(float(p.current_value or 0.0) for p in pos_group)
                    logger.info(
                        "Redeeming condition %s | positions=%s | value=$%.2f",
                        condition_id,
                        len(pos_group),
                        total_value,
                    )

                    for p in pos_group:
                        status = "WIN" if float(p.cur_price or 0.0) >= 0.99 else "LOSS"
                        logger.info(
                            "   -> %s | %s | %s | size=%.2f | value=$%.2f | price=%.4f",
                            status,
                            p.title,
                            p.outcome,
                            float(p.size),
                            float(p.current_value or 0.0),
                            float(p.cur_price or 0.0),
                        )

                    result = await redemption_service.redeem_condition(condition_id)
                    logger.info(
                        "redemption_result",
                        extra={
                            "type": "redemption_result",
                            "conditionId": condition_id,
                            "status": result.status,
                            "positionCount": len(pos_group),
                            "estimatedValue": total_value,
                            "txHash": result.tx_hash,
                            "error": result.error,
                        },
                    )
                    if result.error:
                        logger.error(f"❌ Failed to redeem condition {condition_id}: {result.error}")
                    elif result.submitted and result.status in {"confirmed", "reverted"}:
                        pruned = _prune_redeemed_positions(positions_repo, user_id, pos_group)
                        logger.info(
                            "redeemed_positions_pruned",
                            extra={
                                "type": "redeemed_positions_pruned",
                                "conditionId": condition_id,
                                "positionCount": len(pos_group),
                                "prunedCount": pruned,
                                "txHash": result.tx_hash,
                                "status": result.status,
                            },
                        )
            
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Auto-redeem loop error: {e}")
            await asyncio.sleep(interval)
