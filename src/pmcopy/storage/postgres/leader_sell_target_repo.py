"""PostgreSQL adapter for leader SELL target generations and re-drive claims."""

from __future__ import annotations

import json
from typing import Mapping

import asyncpg

from pmcopy.domain.leader_sell_target import LeaderSellTarget


class LeaderSellTargetRepo:
    """Persist latest SELL intent state and one-shot terminal-release claims."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Bind the migrated application database pool.

        Args:
            pool: Shared asyncpg pool.
        """

        self._pool = pool

    async def upsert_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        source_signal_id: str,
        intent_payload: dict[str, object],
        task_id: str | None = None,
    ) -> LeaderSellTarget:
        """Persist a fresh target and atomically increment its generation.

        Args:
            user_id: Follower account identity.
            leader_wallet: Copied leader wallet.
            condition_id: Market condition identifier.
            token_id: Raw outcome-token identifier.
            source_signal_id: Fresh leader fill identity.
            intent_payload: Secret-safe protected execution intent.

        Returns:
            The newly persisted target generation.
        """

        scope = self._scope(user_id, leader_wallet, condition_id, token_id)
        signal = str(source_signal_id or "").strip()
        if not signal:
            raise ValueError("source_signal_id is required")
        encoded = json.dumps(intent_payload, sort_keys=True, separators=(",", ":"))
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO leader_sell_targets (
                    user_id, leader_wallet, condition_id, token_id,
                    generation, source_signal_id, intent_json, updated_at, task_id
                ) VALUES ($1, $2, $3, $4, 1, $5, $6::jsonb, now(), $7)
                ON CONFLICT (user_id, leader_wallet, condition_id, token_id, task_id)
                DO UPDATE SET
                    generation = leader_sell_targets.generation + 1,
                    source_signal_id = EXCLUDED.source_signal_id,
                    intent_json = EXCLUDED.intent_json,
                    updated_at = now()
                RETURNING user_id, leader_wallet, condition_id, token_id,
                          generation, source_signal_id, intent_json, task_id
                """,
                *scope,
                signal,
                encoded,
                task_id or "",
            )
        if row is None:
            raise RuntimeError("leader SELL target upsert returned no row")
        return self._target(row)

    async def get_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        task_id: str | None = None,
    ) -> LeaderSellTarget | None:
        """Return the latest target for one normalized scope.

        Args:
            user_id: Follower account identity.
            leader_wallet: Copied leader wallet.
            condition_id: Market condition identifier.
            token_id: Raw outcome-token identifier.

        Returns:
            The latest target, or ``None`` when no SELL was recorded.
        """

        scope = self._scope(user_id, leader_wallet, condition_id, token_id)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT user_id, leader_wallet, condition_id, token_id,
                       generation, source_signal_id, intent_json, task_id
                FROM leader_sell_targets
                WHERE user_id = $1 AND leader_wallet = $2
                  AND condition_id = $3 AND token_id = $4 AND task_id = $5
                """,
                *scope,
                task_id or "",
            )
        return self._target(row) if row is not None else None

    async def claim_redrive(
        self,
        *,
        target: LeaderSellTarget,
        released_reservation_id: str,
    ) -> bool:
        """Claim one target generation/released-reservation pair once.

        Args:
            target: Latest persisted target generation.
            released_reservation_id: Older reservation proven terminal no-fill.

        Returns:
            ``True`` only for the first durable claim.
        """

        reservation_id = str(released_reservation_id or "").strip()
        if not reservation_id:
            raise ValueError("released_reservation_id is required")
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                """
                INSERT INTO leader_sell_target_redrives (
                    user_id, leader_wallet, condition_id, token_id,
                    generation, released_reservation_id, claimed_at, task_id
                ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7)
                ON CONFLICT DO NOTHING
                RETURNING 1
                """,
                target.user_id,
                target.leader_wallet.lower(),
                target.condition_id,
                target.token_id,
                int(target.generation),
                reservation_id,
                target.task_id or "",
            )
        return value == 1

    @staticmethod
    def _target(row: Mapping[str, object]) -> LeaderSellTarget:
        raw_payload = row["intent_json"]
        if isinstance(raw_payload, str):
            decoded = json.loads(raw_payload)
        else:
            decoded = raw_payload
        if not isinstance(decoded, Mapping):
            raise ValueError("leader SELL target intent_json must be an object")
        return LeaderSellTarget(
            user_id=str(row["user_id"]),
            leader_wallet=str(row["leader_wallet"]),
            condition_id=str(row["condition_id"]),
            token_id=str(row["token_id"]),
            generation=int(str(row["generation"])),
            source_signal_id=str(row["source_signal_id"]),
            intent_payload=dict(decoded),
            task_id=str(row["task_id"]) or None,
        )

    @staticmethod
    def _scope(
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
    ) -> tuple[str, str, str, str]:
        values = (
            str(user_id or "").strip(),
            str(leader_wallet or "").strip().lower(),
            str(condition_id or "").strip(),
            str(token_id or "").strip(),
        )
        if not all(values):
            raise ValueError("leader SELL target scope fields are required")
        return values


__all__ = ["LeaderSellTargetRepo"]
