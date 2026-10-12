"""PostgreSQL adapter for degraded leader SELL-fraction BUY pauses."""

from __future__ import annotations

import asyncpg


class LeaderSellSafetyScopeRepo:
    """Persist leader/token BUY pauses across source paths and restarts."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Bind the shared PostgreSQL pool.

        Args:
            pool: Migrated application database pool.
        """

        self._pool = pool

    async def set_paused(self, leader_wallet: str, token_id: str, reason: str) -> None:
        """Upsert a durable BUY pause for one normalized scope.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw Polymarket outcome-token identifier.
            reason: Machine-readable safety reason.

        Raises:
            ValueError: If an identifier or reason is blank.
        """

        leader, token = self._scope(leader_wallet, token_id)
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("leader SELL scope pause reason is required")
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO leader_sell_safety_scopes (
                    leader_wallet, token_id, reason, updated_at
                ) VALUES ($1, $2, $3, now())
                ON CONFLICT (leader_wallet, token_id) DO UPDATE
                SET reason = EXCLUDED.reason,
                    updated_at = now()
                """,
                leader,
                token,
                normalized_reason,
            )

    async def clear_paused(self, leader_wallet: str, token_id: str) -> None:
        """Delete a recovered BUY pause idempotently.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw Polymarket outcome-token identifier.
        """

        leader, token = self._scope(leader_wallet, token_id)
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM leader_sell_safety_scopes
                WHERE leader_wallet = $1 AND token_id = $2
                """,
                leader,
                token,
            )

    async def is_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Return whether one scope has a durable BUY pause.

        Args:
            leader_wallet: Copied leader wallet.
            token_id: Raw Polymarket outcome-token identifier.

        Returns:
            ``True`` when a pause row exists.
        """

        leader, token = self._scope(leader_wallet, token_id)
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM leader_sell_safety_scopes
                    WHERE leader_wallet = $1 AND token_id = $2
                )
                """,
                leader,
                token,
            )
        return bool(value)

    @staticmethod
    def _scope(leader_wallet: str, token_id: str) -> tuple[str, str]:
        leader = str(leader_wallet or "").strip().lower()
        token = str(token_id or "").strip()
        if not leader:
            raise ValueError("leader_wallet is required")
        if not token:
            raise ValueError("token_id is required")
        return leader, token


__all__ = ["LeaderSellSafetyScopeRepo"]
