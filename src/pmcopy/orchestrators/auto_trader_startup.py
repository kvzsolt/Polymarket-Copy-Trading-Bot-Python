"""Read-only readiness of durable automatic membership before venue startup."""

from collections.abc import Awaitable, Callable

from pmcopy.domain.auto_trader import AutoTraderScope
from pmcopy.services.auto_trader import AutoTraderRepository


class AutoTraderStartupProbe:
    """Distinguish initialized empty rosters from missing bootstrap evidence."""

    def __init__(
        self,
        *,
        repository: AutoTraderRepository,
        scopes: tuple[AutoTraderScope, ...],
        legacy_wallets: Callable[[], Awaitable[list[str]]],
    ) -> None:
        """Inject only local durable state and legacy migration evidence."""
        self._repository = repository
        self._scopes = scopes
        self._legacy_wallets = legacy_wallets

    async def restorable(self) -> bool:
        """Return whether every follower can restore without analytics requests."""
        if not self._scopes:
            return False
        for scope in self._scopes:
            if not (await self._repository.load(scope)).initialized:
                return bool(await self._legacy_wallets())
        return True
