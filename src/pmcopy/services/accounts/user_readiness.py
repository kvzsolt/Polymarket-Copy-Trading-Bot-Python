"""Aggregate authenticated user streams without letting peers overwrite failure."""

from collections.abc import Mapping
from dataclasses import dataclass

from pmcopy.interfaces.readiness import ReadinessWriter


@dataclass(frozen=True, slots=True)
class _UserState:
    authenticated: bool = False
    connecting: bool = False


class UserReadinessGroup:
    """Own one bounded authentication state for each configured follower."""

    def __init__(self, readiness: ReadinessWriter, accounts: tuple[str, ...]) -> None:
        if any(not account for account in accounts) or len(set(accounts)) != len(accounts):
            raise ValueError("User readiness accounts must be unique and non-empty")
        self._readiness = readiness
        self._states = dict.fromkeys(accounts, _UserState())

    def scope(self, account: str) -> "UserReadinessScope":
        """Return a writer restricted to one configured follower."""
        if account not in self._states:
            raise ValueError("User readiness account is not configured")
        return UserReadinessScope(self, account)

    def publish(self, account: str, fields: Mapping[str, object]) -> None:
        """Apply one authentication observation and publish the aggregate.

        The aggregate booleans stay the fail-closed ``all(...)`` / ``any(...)``
        projections the strict readiness gate reads. The per-account map
        (multi-copier addon §7) rides alongside them so a degraded follower is
        *named* in the readiness snapshot instead of being concealed by one
        global boolean. Account ids are the configured follower roster, so the
        map is bounded by startup configuration.
        """
        previous = self._states[account]
        authenticated = fields.get("wss_user_ok", previous.authenticated)
        connecting = fields.get("wss_user_connecting", previous.connecting)
        if not isinstance(authenticated, bool) or not isinstance(connecting, bool):
            raise ValueError("User readiness flags must be boolean")
        error = fields.get("last_error")
        if error is not None and not isinstance(error, str):
            raise ValueError("User readiness error must be sanitized text")
        self._states[account] = _UserState(authenticated, connecting)
        rows = tuple(self._states.values())
        aggregate: dict[str, object] = {
            "wss_user_ok": bool(rows) and all(row.authenticated for row in rows),
            "wss_user_connecting": any(row.connecting for row in rows),
            "wss_user_accounts": {
                name: state.authenticated for name, state in self._states.items()
            },
        }
        if "last_error" in fields:
            aggregate["last_error"] = error
        self._readiness.update(**aggregate)


class UserReadinessScope:
    """Publish one user's stream health without overriding peer accounts."""

    def __init__(self, owner: UserReadinessGroup, account: str) -> None:
        self._owner = owner
        self._account = account

    def update(self, **fields: object) -> None:
        """Forward this runtime's health observation to its aggregate owner."""
        self._owner.publish(self._account, fields)
