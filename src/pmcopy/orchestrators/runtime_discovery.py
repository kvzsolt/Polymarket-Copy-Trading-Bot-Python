"""Run bounded startup and background leader-discovery refresh cycles."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

from pmcopy.orchestrators.leader_registry import LeaderRegistry, LeaderRegistryState
from pmcopy.services.metrics import metrics
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("RuntimeDiscovery")


async def startup_refresh_targets(
    registry: LeaderRegistry,
    *,
    force_discovery: bool,
) -> list[str] | None:
    """Return the persisted wallets the startup refresh should re-check.

    Only an ``--auto-trader`` start consults the database: a manual roster
    never runs discovery, so there is nothing to target. A read failure is
    logged and degrades to ``None`` (a full sweep) so a database problem
    cannot block startup.

    Args:
        registry: Leader registry exposing ``persisted_wallets``.
        force_discovery: ``True`` when the operator passed ``--auto-trader``.

    Returns:
        The persisted wallets to target, or ``None`` to run the full sweep.
    """
    if not force_discovery:
        return None
    try:
        targets = [str(wallet) for wallet in await registry.persisted_wallets()]
    except Exception as exc:
        logger.warning(
            "auto_trader_persisted_wallets_unavailable",
            extra={"error": redact_error_detail(exc)},
        )
        return None
    return targets or None


@dataclass(frozen=True, slots=True)
class AutoTraderStartupBurst:
    """Run only the fresh discovery cycles required for startup promotion."""

    _registry: LeaderRegistry
    _configured_leaders: tuple[str, ...]
    _required_cycles: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "_required_cycles", max(1, int(self._required_cycles)))

    async def run(
        self,
        initial_state: LeaderRegistryState | None = None,
    ) -> list[str]:
        """Return a qualified roster without any scheduling delay.

        The database is consulted first: when the last persisted cycle accepted
        wallets, those are re-checked against fresh evidence instead of paying
        for a full leaderboard sweep. The sweep is the fallback, used when
        nothing is persisted or when the re-check produces neither an active
        roster nor a promotable candidate. Both modes consume the same
        ``min_tenure_cycles`` bound; the remaining cycles continue in whichever
        mode produced candidates.

        Args:
            initial_state: Optional result of the startup refresh already run by
                the composition root. The root targets that refresh with the
                same persisted roster (``startup_refresh_targets``), so it
                stands in for the first cycle of whichever mode applies.

        Returns:
            Active wallets, or an empty list after a provider failure, a
            zero-safe-wallet result, or exhaustion of the tenure-cycle bound.
        """
        targets = await self._persisted_targets()
        cycles_used = 1
        if targets:
            logger.info("auto_trader_targeted_recheck", extra={"wallets": len(targets)})
            state = initial_state or await self._targeted_refresh(targets)
            if not state.active_monitored and not state.discovered:
                logger.info(
                    "auto_trader_startup_sweep_fallback",
                    extra={"reason": "targeted_recheck_empty"},
                )
                targets = []
                state = await self._sweep_refresh()
                cycles_used = 2
        else:
            logger.info(
                "auto_trader_startup_sweep_fallback",
                extra={"reason": "no_persisted_candidates"},
            )
            state = initial_state or await self._sweep_refresh()

        if state.active_monitored:
            return state.active_monitored
        if not state.discovered:
            logger.info(
                "auto_trader_startup_burst_finished",
                extra={"reason": "no_viable_candidates", "refresh_count": cycles_used},
            )
            return []

        remaining_refreshes = self._required_cycles - cycles_used
        metrics.inc("auto_trader_startup_burst_started_total")
        for refresh_index in range(remaining_refreshes):
            state = (
                await self._targeted_refresh(targets) if targets else await self._sweep_refresh()
            )
            refresh_count = cycles_used + refresh_index + 1
            if state.active_monitored:
                logger.info(
                    "auto_trader_startup_burst_finished",
                    extra={"reason": "roster_promoted", "refresh_count": refresh_count},
                )
                metrics.inc("auto_trader_startup_burst_succeeded_total")
                return state.active_monitored
            if not state.discovered:
                logger.info(
                    "auto_trader_startup_burst_finished",
                    extra={
                        "reason": "no_viable_candidates",
                        "refresh_count": refresh_count,
                    },
                )
                return []

        logger.info(
            "auto_trader_startup_burst_finished",
            extra={"reason": "tenure_cycles_exhausted", "refresh_count": self._required_cycles},
        )
        metrics.inc("auto_trader_startup_burst_exhausted_total")
        return []

    async def _persisted_targets(self) -> list[str]:
        """Return the stored accepted roster this burst should re-check.

        A read failure is not fatal: the burst degrades to the full sweep it
        performed before, so a database problem cannot block startup.

        Returns:
            Persisted wallets, or an empty list when none are readable.
        """
        return await startup_refresh_targets(self._registry, force_discovery=True) or []

    async def _targeted_refresh(self, targets: list[str]) -> LeaderRegistryState:
        """Run one discovery cycle restricted to the supplied wallets.

        Args:
            targets: Wallets to re-check against fresh provider evidence.

        Returns:
            The registry state produced by that cycle.
        """
        return await self._registry.refresh(
            list(self._configured_leaders),
            force_discovery=True,
            target_wallets=list(targets),
        )

    async def _sweep_refresh(self) -> LeaderRegistryState:
        """Run one full provider-shortlist discovery cycle.

        Returns:
            The registry state produced by that cycle.
        """
        return await self._registry.refresh(
            list(self._configured_leaders),
            force_discovery=True,
        )


async def wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    config: object,
    initial_state: LeaderRegistryState | None = None,
    force_discovery: bool = False,
) -> list[str]:
    """Wait within the configured bound for one usable leader roster.

    Args:
        registry: Discovery and durable-roster coordinator.
        configured_leaders: Effective manual roster for this process.
        refresh_interval: Seconds between provider refresh attempts.
        config: Runtime configuration containing the startup timeout.
        initial_state: Optional already-refreshed registry state.
        force_discovery: Run provider discovery even when its scheduler is off.

    Returns:
        Active monitored wallets, or an empty list when the wait expires.
    """
    if force_discovery:
        hysteresis = getattr(getattr(config, "discovery", None), "hysteresis", None)
        return await AutoTraderStartupBurst(
            registry,
            tuple(configured_leaders),
            int(getattr(hysteresis, "min_tenure_cycles", 1)),
        ).run(initial_state)

    state = initial_state or await registry.refresh(
        configured_leaders,
        force_discovery=force_discovery,
    )
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
        state = await registry.refresh(
            configured_leaders,
            force_discovery=force_discovery,
        )
        if state.active_monitored:
            return state.active_monitored


async def discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int | None = None,
    *,
    on_new_leaders: Callable[[list[str]], Awaitable[Iterable[str] | None]] | None = None,
    already_monitored: set[str] | None = None,
    force_discovery: bool = False,
) -> None:
    """Run non-overlapping rediscovery and cached-rescore schedules.

    Args:
        registry: Discovery and durable-roster coordinator.
        configured_leaders: Effective manual roster for this process.
        refresh_interval: Optional provider refresh interval override.
        on_new_leaders: Callback that starts newly promoted leader monitors.
        already_monitored: Mutable process-local monitor identity set.
        force_discovery: Run provider discovery even when its scheduler is off.
    """
    scheduler = registry.config.discovery.scheduler
    rediscover_seconds = max(
        1,
        int(refresh_interval if refresh_interval is not None else scheduler.rediscover_seconds),
    )
    rescore_seconds = max(1, int(scheduler.rescore_seconds))
    # Keep the caller-owned set as the durable in-process monitor registry.
    # The supervisor may recreate this coroutine after a crash; mutating the
    # shared set prevents the replacement loop from hot-adding the same wallet
    # a second time.
    monitored = already_monitored if already_monitored is not None else set()
    normalized_monitored = _normalized_wallet_set(monitored)
    monitored.clear()
    monitored.update(normalized_monitored)
    loop = asyncio.get_running_loop()
    next_discovery = loop.time() + rediscover_seconds
    next_rescore = loop.time() + rescore_seconds

    while True:
        await asyncio.sleep(max(0.05, min(next_discovery, next_rescore) - loop.time()))
        try:
            now = loop.time()
            if now >= next_discovery:
                state = await registry.refresh(
                    configured_leaders,
                    force_discovery=force_discovery,
                )
                completed_at = loop.time()
                next_discovery = completed_at + rediscover_seconds
                # A full discovery also computes current scores. Move the
                # cached-rescore deadline forward so coincident deadlines do
                # not trigger a redundant rescore immediately afterward.
                next_rescore = completed_at + rescore_seconds
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
    new_wallets = [wallet for wallet in active_wallets if wallet.lower() not in monitored]
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
    return {str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}


__all__ = [
    "AutoTraderStartupBurst",
    "discovery_refresh_loop",
    "wait_for_active_leaders",
]
