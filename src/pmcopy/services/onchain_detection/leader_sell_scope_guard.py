"""Fail-closed BUY guard for degraded leader SELL-fraction scopes.

The service owns in-process safety state and delegates durability to a narrow
repository port. Detection and polling paths share this service without
depending on PostgreSQL types.
"""

from __future__ import annotations

import logging
from typing import Protocol


class LeaderSellScopeStore(Protocol):
    """Persist leader/token scopes whose BUY side is paused."""

    async def set_paused(self, leader_wallet: str, token_id: str, reason: str) -> None:
        """Persist a BUY pause for one normalized scope."""

        ...

    async def clear_paused(self, leader_wallet: str, token_id: str) -> None:
        """Remove a recovered BUY pause for one normalized scope."""

        ...

    async def is_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Return whether one normalized scope is durably paused."""

        ...


class LeaderSellBuyScopeGuard(Protocol):
    """Expose fail-closed BUY-pause decisions to detection and copy paths."""

    async def pause_buys(
        self,
        leader_wallet: str,
        token_id: str,
        *,
        reason: str,
    ) -> None:
        """Pause BUYs after degraded emergency SELL sizing."""

        ...

    async def clear_buys_pause(self, leader_wallet: str, token_id: str) -> bool:
        """Clear the pause after authoritative sizing evidence recovers."""

        ...

    async def is_buy_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Return whether BUYs must remain blocked for one scope."""

        ...


class LeaderSellScopeGuard:
    """Combine durable scope state with a fail-closed process-local mirror."""

    def __init__(
        self,
        *,
        store: LeaderSellScopeStore | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind optional durable storage and structured diagnostics.

        Args:
            store: Durable scope repository. ``None`` retains process-local
                behavior for isolated tests.
            logger: Diagnostic logger; defaults to this module's logger.
        """

        self._store = store
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._paused: set[tuple[str, str]] = set()
        self._unpersisted_pauses: set[tuple[str, str]] = set()

    async def pause_buys(
        self,
        leader_wallet: str,
        token_id: str,
        *,
        reason: str,
    ) -> None:
        """Pause BUYs immediately and persist the pause when possible.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw outcome-token identifier.
            reason: Bounded machine-readable pause reason.
        """

        leader, token = self._scope(leader_wallet, token_id)
        self._paused.add((leader, token))
        if self._store is None:
            return
        try:
            await self._store.set_paused(leader, token, str(reason).strip())
        except Exception as exc:
            self._unpersisted_pauses.add((leader, token))
            self._logger.error(
                "leader_sell_scope_pause_persist_failed | leader=%s token=%s error_type=%s",
                leader,
                token,
                type(exc).__name__,
            )
        else:
            self._unpersisted_pauses.discard((leader, token))

    async def clear_buys_pause(self, leader_wallet: str, token_id: str) -> bool:
        """Clear a pause only after its durable record is removed.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw outcome-token identifier.

        Returns:
            ``True`` when the scope is safely clear. A storage failure returns
            ``False`` and retains the process-local pause.
        """

        leader, token = self._scope(leader_wallet, token_id)
        if self._store is not None:
            try:
                await self._store.clear_paused(leader, token)
            except Exception as exc:
                self._paused.add((leader, token))
                self._unpersisted_pauses.add((leader, token))
                self._logger.error(
                    "leader_sell_scope_clear_failed | leader=%s token=%s error_type=%s",
                    leader,
                    token,
                    type(exc).__name__,
                )
                return False
        self._paused.discard((leader, token))
        self._unpersisted_pauses.discard((leader, token))
        return True

    async def is_buy_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Return the shared pause state, treating read failures as paused.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw outcome-token identifier.

        Returns:
            ``True`` for a known pause or when durable state is unavailable.
        """

        leader, token = self._scope(leader_wallet, token_id)
        if (leader, token) in self._unpersisted_pauses:
            return True
        if self._store is None:
            return (leader, token) in self._paused
        try:
            paused = await self._store.is_paused(leader, token)
        except Exception as exc:
            self._logger.error(
                "leader_sell_scope_read_failed | leader=%s token=%s error_type=%s",
                leader,
                token,
                type(exc).__name__,
            )
            return True
        if paused:
            self._paused.add((leader, token))
        else:
            self._paused.discard((leader, token))
        return paused

    @staticmethod
    def _scope(leader_wallet: str, token_id: str) -> tuple[str, str]:
        leader = str(leader_wallet or "").strip().lower()
        token = str(token_id or "").strip()
        if not leader:
            raise ValueError("leader_wallet is required")
        if not token:
            raise ValueError("token_id is required")
        return leader, token


__all__ = ["LeaderSellBuyScopeGuard", "LeaderSellScopeGuard", "LeaderSellScopeStore"]
