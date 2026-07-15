from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable

from orchestrators.leader_registry import LeaderRegistry, LeaderRegistryState
from services.metrics import metrics
from utils.logger import get_logger, redact_error_detail

logger = get_logger("RuntimeDiscovery")


async def wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    config: object,
    initial_state: LeaderRegistryState | None = None,
) -> list[str]:
    state = initial_state or await registry.refresh(configured_leaders)
    if state.active_monitored:
        return state.active_monitored
    scheduler = getattr(getattr(config, "discovery", None), "scheduler", None)
    timeout_seconds = max(
        1,
        int(getattr(scheduler, "startup_wait_timeout_seconds", 3900)),
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        metrics.inc("no_leaders_monitored_total")
        remaining = deadline - loop.time()
        if remaining <= 0:
            logger.error(
                "startup_discovery_wait_timed_out",
                extra={"timeout_seconds": timeout_seconds},
            )
            metrics.inc("startup_discovery_wait_timeout_total")
            return []
        await asyncio.sleep(min(max(1, int(refresh_interval)), remaining))
        if loop.time() >= deadline:
            continue
        state = await registry.refresh(configured_leaders)
        if state.active_monitored:
            return state.active_monitored


async def discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int | None = None,
    *,
    on_new_leaders: Callable[[list[str]], Awaitable[Iterable[str] | None]]
    | None = None,
    already_monitored: set[str] | None = None,
) -> None:
    """Run non-overlapping rediscovery and cached-rescore schedules."""
    scheduler = registry.config.discovery.scheduler
    rediscover_seconds = max(
        1,
        int(
            refresh_interval
            if refresh_interval is not None
            else scheduler.rediscover_seconds
        ),
    )
    rescore_seconds = max(1, int(scheduler.rescore_seconds))
    monitored = {
        wallet.lower() for wallet in (already_monitored or set()) if wallet
    }
    loop = asyncio.get_running_loop()
    next_discovery = loop.time() + rediscover_seconds
    next_rescore = loop.time() + rescore_seconds

    while True:
        await asyncio.sleep(
            max(0.05, min(next_discovery, next_rescore) - loop.time())
        )
        try:
            now = loop.time()
            if now >= next_discovery:
                state = await registry.refresh(configured_leaders)
                next_discovery = loop.time() + rediscover_seconds
            else:
                state = await registry.rescore(configured_leaders)
                next_rescore = loop.time() + rescore_seconds
            await _start_new_monitors(state.active_monitored, monitored, on_new_leaders)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "background_discovery_cycle_failed",
                extra={"error": redact_error_detail(exc)},
            )
            # Advance both due deadlines after failure. This is the loop's
            # misfire guard: a slow or failing cycle never overlaps or spins.
            now = loop.time()
            if now >= next_discovery:
                next_discovery = now + rediscover_seconds
            if now >= next_rescore:
                next_rescore = now + rescore_seconds


async def _start_new_monitors(
    active_wallets: list[str],
    monitored: set[str],
    callback: Callable[[list[str]], Awaitable[Iterable[str] | None]] | None,
) -> None:
    if callback is None:
        return
    new_wallets = [
        wallet for wallet in active_wallets if wallet.lower() not in monitored
    ]
    if not new_wallets:
        return
    started = _normalized_wallet_set(await callback(new_wallets))
    if not started:
        logger.warning(
            "dynamic_leader_callback_started_no_wallets",
            extra={"wallets": new_wallets},
        )
        return
    monitored.update(started)


def _normalized_wallet_set(
    wallets: Iterable[str] | str | None,
) -> set[str]:
    if wallets is None:
        return set()
    if isinstance(wallets, str):
        wallets = (wallets,)
    return {
        str(wallet).strip().lower()
        for wallet in wallets
        if str(wallet).strip()
    }


__all__ = ["discovery_refresh_loop", "wait_for_active_leaders"]
