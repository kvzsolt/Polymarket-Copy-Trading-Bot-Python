from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from orchestrators.leader_registry import LeaderRegistry
from services.metrics import metrics
from utils.logger import get_logger, redact_error_detail

if TYPE_CHECKING:
    from config.app_config import AppConfig

logger = get_logger("RuntimeDiscovery")


async def wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    config: AppConfig,
) -> list[str]:
    state = await registry.refresh(configured_leaders)
    if state.active_monitored:
        return state.active_monitored

    if not configured_leaders:
        logger.error(
            "No active leaders after initial refresh. No LEADER_WALLETS configured. "
            "Discovery may score DB candidates, but live copying requires "
            "python bot.py rotate-traders or manual LEADER_WALLETS."
        )
    else:
        logger.warning(
            "No active leaders available; entering discovery-only mode and continuing refresh."
        )
    while True:
        metrics.inc("no_leaders_monitored_total")
        await asyncio.sleep(refresh_interval)
        state = await registry.refresh(configured_leaders)
        if state.active_monitored:
            logger.info("Configured active leaders available; resuming live monitoring")
            return state.active_monitored


async def discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    leaders_repo=None,
    on_new_leaders: Callable[[list[str]], Awaitable[None]] | None = None,
    already_monitored: set[str] | None = None,
) -> None:
    while True:
        await asyncio.sleep(refresh_interval)
        try:
            await registry.refresh(configured_leaders)
        except Exception as exc:
            logger.warning(
                "Background discovery refresh failed",
                extra={"error": redact_error_detail(exc)},
            )
