"""Compose and supervise persistent automatic trader management."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

import asyncpg

from domain.auto_trader import (
    AutoRoster,
    AutoTraderPolicy,
    AutoTraderScope,
    FalconObservation,
    TraderStatus,
)
from services.auto_trader import AutoTraderService
from services.discovery.models import Candidate
from storage.postgres.auto_trader_repository import PostgresAutoTraderRepository
from storage.postgres.leaders_repo import LeaderRosterRecord, LeaderTierRecord
from utils.logger import get_logger

if TYPE_CHECKING:
    from bootstrap import Application
    from services.multiUserConfig import RuntimeUser

logger = get_logger("AutoTraderRuntime")


class FalconEvaluationSource(Protocol):
    """Successful provider-cycle evidence without a cached-score fallback."""

    last_cycle_degraded: bool
    last_cycle_id: str | None
    last_evaluations: tuple[Candidate, ...]

    async def run(self, *, wallets: Sequence[str] | None = None) -> list[Candidate]:
        """Evaluate exactly the supplied identities or explicitly sweep."""
        ...


class FalconEvidenceProvider:
    """Translate accepted and rejected provider observations into domain evidence."""

    def __init__(self, *, source: FalconEvaluationSource, clock: Callable[[], datetime]) -> None:
        """Inject the already configured Falcon adapter and clock."""
        self._source = source
        self._clock = clock

    async def evaluate(
        self, wallets: tuple[str, ...] | None = None
    ) -> tuple[FalconObservation, ...]:
        """Never interpret an empty target set or provider failure as a sweep."""
        if wallets == ():
            return ()
        await self._source.run(wallets=wallets)
        if self._source.last_cycle_degraded or not self._source.last_cycle_id:
            raise RuntimeError("Falcon evaluation unavailable")
        now = self._clock()
        return tuple(
            FalconObservation(
                row.wallet,
                Decimal(str(row.score)),
                now,
                f"{self._source.last_cycle_id}:{row.wallet}",
                accepted=row.rejected_reason is None,
            )
            for row in self._source.last_evaluations
        )


class TraderExitOwnership(Protocol):
    """Read authoritative outstanding inventory and in-flight execution."""

    async def retained_wallets(self, scope: AutoTraderScope) -> set[str]:
        """Return all identities whose exits still require observation."""
        ...


class AutoTraderRuntime:
    """Keep per-account lifecycle services behind shared runtime admission."""

    def __init__(
        self,
        *,
        services: Mapping[AutoTraderScope, AutoTraderService],
        ownership: TraderExitOwnership | None = None,
    ) -> None:
        """Bind exactly one execution mode for each follower in this process."""
        self.services = dict(services)
        self._ownership = ownership
        self._scopes = {scope.follower_account: scope for scope in services}
        if len(self._scopes) != len(self.services):
            raise ValueError("a follower cannot compose two execution modes")

    @classmethod
    def create(
        cls,
        *,
        application: "Application",
        pool: asyncpg.Pool,
        users: Sequence["RuntimeUser"],
        clock: Callable[[], datetime] | None = None,
    ) -> "AutoTraderRuntime":
        """Compose durable services using each follower's own public balance."""
        from orchestrators.discovery_funds import build_discovery_funds_callable
        from utils.getMyBalance import get_my_balance

        now = clock or cls.now
        config = application.config
        scheduler = config.discovery.scheduler
        hysteresis = config.discovery.hysteresis
        evidence = config.discovery.auto_follow
        policy = AutoTraderPolicy(
            target_count=scheduler.max_active_leaders,
            promote_score=Decimal(str(hysteresis.promote_score)),
            demote_score=Decimal(str(hysteresis.demote_score)),
            required_cycles=hysteresis.min_tenure_cycles,
            minimum_tenure_seconds=hysteresis.min_dwell_seconds,
            evidence_max_age_seconds=scheduler.rediscover_seconds * 2,
            paper_days=evidence.paper_days,
            minimum_episodes=evidence.minimum_episodes,
            max_paper_drawdown=evidence.max_paper_drawdown,
        )
        services = {}
        repository = PostgresAutoTraderRepository(pool)
        for user in users:
            if not user.wallet.funder_address:
                raise ValueError(
                    "automatic traders require an explicit follower funder_address for durable account identity"
                )
            scope = AutoTraderScope(user.user_id, config.execution.trading_mode)
            source = application.discovery_service.with_funds(
                build_discovery_funds_callable(
                    user.wallet.funder_address, get_balance=get_my_balance
                )
            )
            services[scope] = AutoTraderService(
                repository=repository,
                provider=FalconEvidenceProvider(source=source, clock=now),
                policy=policy,
                clock=now,
            )
        from storage.postgres.auto_trader_ownership import PostgresAutoTraderOwnership

        return cls(services=services, ownership=PostgresAutoTraderOwnership(pool))

    @staticmethod
    def now() -> datetime:
        """Supply the production clock only at the composition boundary."""
        return datetime.now(UTC)

    async def startup(self, *, legacy_wallets: Sequence[str] = ()) -> list[str]:
        """Restore all follower scopes and their count-derived paper cohorts."""
        for scope, service in self.services.items():
            await service.startup(scope, legacy_wallets=legacy_wallets)
        return await self.monitored_wallets()

    async def monitored_wallets(self) -> list[str]:
        """Include selected incumbents, exit owners, and bounded paper cohorts."""
        wallets: dict[str, None] = {}
        for scope, service in self.services.items():
            roster = await service.repository.load(scope)
            retained = {row.wallet for row in roster.traders if row.selected_at is not None}
            if self._ownership is not None:
                try:
                    retained = await self._ownership.retained_wallets(scope)
                except Exception as exc:
                    retained = {row.wallet for row in roster.traders}
                    logger.warning(
                        "auto_trader_exit_ownership_unavailable",
                        extra={"error_type": type(exc).__name__},
                    )
            for row in roster.traders:
                if row.status is TraderStatus.ACTIVE or row.wallet in retained:
                    wallets[row.wallet] = None
            for wallet in self._paper_wallets(roster, service.policy.target_count):
                wallets[wallet] = None
        return list(wallets)

    async def paper_wallets(self, follower_account: str) -> tuple[str, ...]:
        """Return the non-live paper cohort for one follower."""
        scope = self._scopes.get(follower_account)
        if scope is None:
            return ()
        service = self.services[scope]
        roster = await service.repository.load(scope)
        active = (
            tuple(row.wallet for row in roster.traders if row.status is TraderStatus.ACTIVE)
            if scope.trading_mode == "paper"
            else ()
        )
        return active + self._paper_wallets(roster, service.policy.target_count)

    async def admit(self, follower_account: str, wallet: str, side: str) -> bool:
        """Fail closed for entries while retaining owned exits after demotion."""
        scope = self._scopes.get(follower_account)
        if scope is None:
            return False
        if scope.trading_mode == "paper":
            return False
        try:
            roster = await self.services[scope].repository.load(scope)
        except Exception as exc:
            logger.warning(
                "auto_trader_admission_unavailable", extra={"error_type": type(exc).__name__}
            )
            return False
        if side.upper() not in {"BUY", "SELL"} or (
            side.upper() == "BUY" and roster.entry_block_reason
        ):
            return False
        row = next((row for row in roster.traders if row.wallet == wallet.lower()), None)
        if row is None:
            return False
        return row.status is TraderStatus.ACTIVE or (
            side.upper() == "SELL"
            and (row.selected_at is not None or row.status is TraderStatus.DRAINING)
        )

    async def evaluate_known(self) -> None:
        """Run targeted checks off the trading path."""
        failure: Exception | None = None
        for scope, service in self.services.items():
            try:
                await service.evaluate_known(scope)
            except Exception as exc:
                failure = failure or exc
        if failure is not None:
            raise failure

    def for_account(self, follower_account: str) -> "AutomaticLeaderView":
        """Bind legacy tier reads to the exact follower instead of a global roster."""
        return AutomaticLeaderView(runtime=self, scope=self._scopes[follower_account])

    async def refresh_monitors(
        self,
        *,
        monitored: set[str],
        on_new_leaders: Callable[[list[str]], Awaitable[object]],
        on_retired_leaders: Callable[[list[str]], Awaitable[None]] | None = None,
    ) -> None:
        """Keep only selected cohorts and authoritative outstanding exit owners."""
        wallets = await self.monitored_wallets()
        additions = [wallet for wallet in wallets if wallet not in monitored]
        if additions:
            started = await on_new_leaders(additions)
            if isinstance(started, (list, tuple, set)):
                monitored.update(started)
        removals = sorted(monitored - set(wallets))
        if removals and on_retired_leaders is not None:
            await on_retired_leaders(removals)
            monitored.difference_update(removals)

    async def run(
        self,
        *,
        evaluation_seconds: int,
        falcon_seconds: int,
        on_new_leaders: Callable[[list[str]], Awaitable[object]],
        monitored: set[str],
        on_retired_leaders: Callable[[list[str]], Awaitable[None]] | None = None,
        evaluate_results: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Supervise non-overlapping local and known-wallet evaluation schedules."""
        loop = asyncio.get_running_loop()
        next_falcon = loop.time() + falcon_seconds
        while True:
            await asyncio.sleep(evaluation_seconds)
            try:
                if evaluate_results is not None:
                    await evaluate_results()
                if loop.time() >= next_falcon:
                    await self.evaluate_known()
                    next_falcon = loop.time() + falcon_seconds
                else:
                    for scope, service in self.services.items():
                        await service.rotate(scope)
                await self.refresh_monitors(
                    monitored=monitored,
                    on_new_leaders=on_new_leaders,
                    on_retired_leaders=on_retired_leaders,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                next_falcon = max(next_falcon, loop.time() + falcon_seconds)
                logger.warning(
                    "auto_trader_evaluation_failed", extra={"error_type": type(exc).__name__}
                )

    @staticmethod
    def _paper_wallets(roster: AutoRoster, limit: int) -> tuple[str, ...]:
        reserves = sorted(
            (
                row
                for row in roster.traders
                if row.status in {TraderStatus.RESERVE, TraderStatus.PAPER}
            ),
            key=AutoTraderPolicy.rank,
        )
        return tuple(row.wallet for row in reserves[:limit])


class AutomaticLeaderView:
    """Project scoped membership into the existing trade-pipeline tier interface."""

    def __init__(self, *, runtime: AutoTraderRuntime, scope: AutoTraderScope) -> None:
        """Retain only the selected follower's automatic service."""
        self.runtime = runtime
        self.scope = scope

    async def get_tier(self, wallet: str) -> LeaderTierRecord:
        """Keep exit-capable contexts routable; the coordinator gates each entry."""
        service = self.runtime.services[self.scope]
        roster = await service.repository.load(self.scope)
        row = next((row for row in roster.traders if row.wallet == wallet.lower()), None)
        tier = "watchlist"
        if row is not None:
            tier = (
                "live_follow"
                if row.status is TraderStatus.ACTIVE or row.selected_at is not None
                else "paper_follow"
            )
        return LeaderTierRecord(wallet.lower(), tier, None)

    async def get_roster(self, wallet: str) -> LeaderRosterRecord | None:
        """Expose the saved rank without calling the analytics provider."""
        service = self.runtime.services[self.scope]
        roster = await service.repository.load(self.scope)
        row = next((row for row in roster.traders if row.wallet == wallet.lower()), None)
        if row is None:
            return None
        return LeaderRosterRecord(
            row.wallet,
            row.status.value,
            float(row.score),
            float(row.score),
            None,
            0,
            row.qualification_streak,
            row.falcon_strikes,
            row.selected_at.timestamp() if row.selected_at else 0,
            row.selected_at.timestamp() if row.selected_at else None,
            row.observed_at.timestamp() if row.observed_at else None,
            "automatic",
        )


class AutomaticDiscoveryRunner:
    """Route explicit control-plane discovery into the durable automatic roster."""

    def __init__(self, *, runtime: AutoTraderRuntime, clock: Callable[[], datetime]) -> None:
        """Inject the running scoped services and manual-job identity clock."""
        self._runtime = runtime
        self._clock = clock

    async def run(self, *, force: bool) -> int:
        """Run an explicitly requested sweep without replacing healthy incumbents."""
        del force
        event = "manual:" + self._clock().isoformat()
        count = 0
        for scope, service in self._runtime.services.items():
            roster = await service.discover(scope, event=event)
            count += len(roster.traders)
        return count
