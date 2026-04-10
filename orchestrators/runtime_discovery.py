from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from config.env import ENV
from orchestrators.leader_registry import LeaderRegistry
from services.metrics import metrics
from utils.logger import get_logger, redact_error_detail

logger = get_logger("RuntimeDiscovery")


async def wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
) -> list[str]:
    state = await registry.refresh(configured_leaders)
    if state.active_monitored:
        return state.active_monitored

    if not configured_leaders:
        if ENV.get("LEADER_AUTO", False):
            logger.error(
                "LEADER_AUTO is enabled, but discovery did not find any eligible leaders "
                "to promote. Relax MIN_LEADER_* or AUTO_DISCOVER_MIN_SCORE, or add "
                "manual LEADER_WALLETS."
            )
        else:
            logger.error(
                "No active leaders after initial refresh. No LEADER_WALLETS configured and "
                "discovery did not promote any leaders. Set LEADER_WALLETS=0x... in your .env, "
                "or enable LEADER_AUTO=true to auto-discover and promote leaders."
            )
    else:
        logger.warning(
            "No active leaders available; entering discovery-only mode and continuing refresh."
        )
    while True:
        await metrics.inc("no_leaders_monitored_total")
        await asyncio.sleep(refresh_interval)
        state = await registry.refresh(configured_leaders)
        if state.active_monitored:
            logger.info(
                "Discovery produced active leaders; resuming live monitoring",
                extra={"leaders": state.active_monitored},
            )
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
    monitored: set[str] = set(already_monitored) if already_monitored else set()
    while True:
        await asyncio.sleep(refresh_interval)
        try:
            await registry.refresh(configured_leaders)
            if leaders_repo is not None and on_new_leaders is not None:
                live = set(leaders_repo.get_leaders_by_tier("live_follow"))
                paper = set(leaders_repo.get_leaders_by_tier("paper_follow"))
                monitorable = live | paper
                new_leaders = sorted(monitorable - monitored)
                if new_leaders:
                    logger.info(
                        "discovery_new_monitorable_leaders",
                        extra={"count": len(new_leaders), "wallets": new_leaders},
                    )
                    monitored.update(new_leaders)
                    await on_new_leaders(new_leaders)
        except Exception as exc:
            logger.warning(
                "Background discovery refresh failed",
                extra={"error": redact_error_detail(exc)},
            )
