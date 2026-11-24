"""Aggregate configured market runtimes into the existing process health fields."""

from collections.abc import Mapping
from dataclasses import dataclass
from pmcopy.interfaces.readiness import ReadinessWriter


@dataclass(frozen=True, slots=True)
class _MarketState:
    connected: bool = False
    connecting: bool = False
    ready: bool = False
    subscribed: int = 0
    stale: int = 0
    dirty: int = 0


class MarketReadinessGroup:
    """Keep every configured account non-ready until its own runtime reports.

    Counts describe account subscriptions, so a shared token on two accounts
    counts twice. Mutations are synchronous on the application's event loop.
    """

    def __init__(self, readiness: ReadinessWriter, accounts: tuple[str, ...]) -> None:
        if any(not account for account in accounts) or len(set(accounts)) != len(accounts):
            raise ValueError("Market readiness accounts must be unique and non-empty")
        self._readiness = readiness
        self._states = dict.fromkeys(accounts, _MarketState())

    def scope(self, account: str) -> "MarketReadinessScope":
        """Return a writer restricted to one configured account."""
        if account not in self._states:
            raise ValueError("Market readiness account is not configured")
        return MarketReadinessScope(self, account)

    def publish(self, account: str, fields: Mapping[str, object]) -> None:
        """Apply one account's observation and publish the aggregate.

        Aggregates stay the fail-closed ``all(...)`` projections the strict
        readiness gate reads; the per-account maps (multi-copier addon §7)
        ride alongside so a degraded account's runtime is *named* in the
        snapshot rather than concealed by one global boolean. Account ids
        are the configured follower roster, so both maps are bounded.
        """
        previous = self._states[account]
        state = _MarketState(
            connected=self._boolean(fields, "wss_market_ok", previous.connected),
            connecting=self._boolean(fields, "wss_market_connecting", previous.connecting),
            ready=self._boolean(fields, "market_data_ready", previous.ready),
            subscribed=self._count(fields, "subscribed_token_count", previous.subscribed),
            stale=self._count(fields, "stale_token_count", previous.stale),
            dirty=self._count(fields, "dirty_token_count", previous.dirty),
        )
        error = fields.get("last_error")
        if error is not None and not isinstance(error, str):
            raise ValueError("Market readiness error must be sanitized text")
        self._states[account] = state
        rows = tuple(self._states.values())
        aggregate: dict[str, object] = {
            "wss_market_ok": bool(rows) and all(row.connected for row in rows),
            "wss_market_connecting": any(row.connecting for row in rows),
            "market_data_ready": bool(rows) and all(row.connected and row.ready for row in rows),
            "subscribed_token_count": sum(row.subscribed for row in rows),
            "stale_token_count": sum(row.stale for row in rows),
            "dirty_token_count": sum(row.dirty for row in rows),
            "wss_market_accounts": {name: row.connected for name, row in self._states.items()},
            "market_data_accounts": {
                name: (row.connected and row.ready) for name, row in self._states.items()
            },
        }
        if "last_error" in fields:
            aggregate["last_error"] = error
        self._readiness.update(**aggregate)

    @staticmethod
    def _boolean(fields: Mapping[str, object], name: str, default: bool) -> bool:
        value = fields.get(name, default)
        if not isinstance(value, bool):
            raise ValueError("Market readiness flag must be boolean")
        return value

    @staticmethod
    def _count(fields: Mapping[str, object], name: str, default: int) -> int:
        value = fields.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError("Market readiness count must be a non-negative integer")
        return value


class MarketReadinessScope:
    """Publish one account's market health without overriding peer accounts."""

    def __init__(self, owner: MarketReadinessGroup, account: str) -> None:
        self._owner = owner
        self._account = account

    def update(self, **fields: object) -> None:
        """Forward this runtime's health observation to its aggregate owner."""
        self._owner.publish(self._account, fields)
