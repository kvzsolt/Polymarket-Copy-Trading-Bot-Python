"""Durable nonsecret settlement attempts and session-scoped wallet ownership."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
import json
import re

import asyncpg

from domain.redemption import (
    RedemptionAttempt,
    RedemptionAuthorization,
    RedemptionMarket,
    RedemptionSnapshot,
    RedemptionState,
    SettlementStorageError,
)
from storage.postgres.v1_migration_runner import (
    MigrationConnection,
    PostgresV1MigrationRunner,
    V1Migration,
    V1MigrationCatalog,
)


class RedemptionMigrationCatalog(V1MigrationCatalog):
    """Allow the standalone command to install only its independent additive table."""

    def migrations(self) -> tuple[V1Migration, ...]:
        """Return the shared journal migration without running unrelated migrations."""
        selected = tuple(
            item
            for item in super().migrations()
            if item.migration_id == "20260916_09_redemption_attempts"
        )
        if len(selected) != 1:
            raise SettlementStorageError("redemption_migration_missing")
        return selected


class PostgresRedemptionJournal:
    """Keep one pooled connection and advisory lock for an entire wallet run.

    Each write commits before network submission. The session lock spans those
    commits, without holding a transaction open during remote confirmation.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Inject the caller-owned pool; this repository never reads credentials."""
        self._pool = pool
        self._connection: MigrationConnection | None = None
        self._wallet = ""

    async def initialize(self) -> None:
        """Idempotently install the journal through the canonical migration runner."""
        try:
            async with self._pool.acquire() as connection:
                await PostgresV1MigrationRunner(catalog=RedemptionMigrationCatalog()).apply(
                    connection
                )
        except Exception:
            raise SettlementStorageError("redemption_journal_initialization_failed") from None

    @asynccontextmanager
    async def lock(self, wallet: str) -> AsyncIterator[None]:
        """Acquire exclusive wallet ownership or fail immediately.

        Args:
            wallet: Explicit Deposit Wallet address, normalized for lock identity.

        Raises:
            SettlementStorageError: If another process owns the wallet or storage fails.
        """
        normalized = self._normalize(wallet)
        if self._connection is not None:
            raise SettlementStorageError("redemption_already_running")
        try:
            connection = await self._pool.acquire()
        except Exception:
            raise SettlementStorageError("redemption_storage_unavailable") from None
        acquired = False
        try:
            try:
                acquired = (
                    await connection.fetchval(
                        "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                        "pmcopy:redemption:" + normalized,
                    )
                    is True
                )
            except Exception:
                raise SettlementStorageError("redemption_storage_unavailable") from None
            if not acquired:
                raise SettlementStorageError("redemption_already_running")
            self._connection, self._wallet = connection, normalized
            yield
        finally:
            self._connection, self._wallet = None, ""
            try:
                if acquired:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                        "pmcopy:redemption:" + normalized,
                    )
            except Exception:
                raise SettlementStorageError("redemption_lock_release_failed") from None
            finally:
                await self._pool.release(connection)

    async def pending(self, wallet: str) -> tuple[RedemptionAttempt, ...]:
        """Reload unresolved authorizations only while holding the same wallet lock."""
        connection = self._require_lock(wallet)
        try:
            rows = await connection.fetch(
                "SELECT authorization_digest, payload FROM redemption_attempts "
                "WHERE wallet = $1 AND state = 'pending' ORDER BY created_at",
                self._wallet,
            )
        except Exception:
            raise SettlementStorageError("redemption_storage_unavailable") from None
        attempts: list[RedemptionAttempt] = []
        try:
            for row in rows:
                data = json.loads(row["payload"])
                snapshot = data.pop("snapshot")
                market = snapshot.pop("market")
                market["token_ids"] = tuple(market["token_ids"])
                snapshot["market"] = RedemptionMarket(**market)
                snapshot["balances"] = tuple(snapshot["balances"])
                snapshot["payout_numerators"] = tuple(snapshot["payout_numerators"])
                data["snapshot"] = RedemptionSnapshot(**snapshot)
                data["authorization"] = RedemptionAuthorization(**data["authorization"])
                data["state"] = RedemptionState(data["state"])
                attempt = RedemptionAttempt(**data)
                if (
                    attempt.wallet != self._wallet
                    or attempt.state is not RedemptionState.PENDING
                    or attempt.authorization.digest != row["authorization_digest"]
                    or not isinstance(attempt.transaction_id, str)
                ):
                    raise ValueError
                attempts.append(attempt)
        except Exception:
            raise SettlementStorageError("redemption_journal_evidence_invalid") from None
        return tuple(attempts)

    async def save(self, attempt: RedemptionAttempt) -> None:
        """Commit before POST and prevent terminal evidence from becoming pending again."""
        connection = self._require_lock(attempt.wallet)
        attempt = replace(attempt, wallet=self._wallet)
        try:
            payload = json.dumps(asdict(attempt), separators=(",", ":"))
            saved = await connection.fetchval(
                """
                INSERT INTO redemption_attempts (authorization_digest, wallet, state, payload)
                VALUES ($1, $2, $3, $4::jsonb)
                ON CONFLICT (authorization_digest) DO UPDATE
                SET state = EXCLUDED.state, payload = EXCLUDED.payload, updated_at = now()
                WHERE redemption_attempts.wallet = EXCLUDED.wallet
                  AND redemption_attempts.payload->'authorization' = EXCLUDED.payload->'authorization'
                  AND redemption_attempts.payload->'snapshot' = EXCLUDED.payload->'snapshot'
                  AND (redemption_attempts.state = 'pending'
                       OR redemption_attempts.payload = EXCLUDED.payload)
                RETURNING authorization_digest
                """,
                attempt.authorization.digest,
                self._wallet,
                attempt.state.value,
                payload,
            )
        except Exception:
            raise SettlementStorageError("redemption_storage_write_failed") from None
        if saved != attempt.authorization.digest:
            raise SettlementStorageError("redemption_journal_transition_rejected")

    def _require_lock(self, wallet: str) -> MigrationConnection:
        if self._connection is None or self._wallet != self._normalize(wallet):
            raise SettlementStorageError("redemption_wallet_lock_required")
        return self._connection

    @staticmethod
    def _normalize(wallet: str) -> str:
        if not isinstance(wallet, str) or re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet) is None:
            raise SettlementStorageError("redemption_wallet_identity_invalid")
        return wallet.lower()
