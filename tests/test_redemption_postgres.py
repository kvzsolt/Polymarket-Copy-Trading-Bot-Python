"""Durable settlement ownership against isolated PostgreSQL, never the live database."""

from dataclasses import replace

import pytest

from domain.redemption import (
    RedemptionAttempt,
    RedemptionAuthorization,
    RedemptionMarket,
    RedemptionSnapshot,
    RedemptionState,
    SettlementStorageError,
)
from storage.postgres.redemption_journal import PostgresRedemptionJournal
from tests.test_fills_repo_dedup_key import _pg_pool

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]
WALLET = "0x" + "1" * 40


class TestPostgresRedemptionJournal:
    """Prove persistence, exclusive ownership, and terminal state protection."""

    @staticmethod
    def attempt() -> RedemptionAttempt:
        """Return an exact, nonsecret authorization record."""
        market = RedemptionMarket("0x" + "ab" * 32, ("123", "456"), False)
        snapshot = RedemptionSnapshot(market, (0, 1234567890123456789012345678901), (1, 0), 1, True)
        return RedemptionAttempt(
            WALLET, snapshot, RedemptionAuthorization(7, 1700000600, "0x" + "c" * 64)
        )

    async def test_restart_round_trips_exact_evidence_and_terminal_receipt(self) -> None:
        """Verify restart round trips exact evidence and terminal receipt."""
        async with _pg_pool() as pool:
            first = PostgresRedemptionJournal(pool)
            await first.initialize()
            async with first.lock(WALLET):
                await first.save(self.attempt())
            second = PostgresRedemptionJournal(pool)
            async with second.lock(WALLET):
                assert await second.pending(WALLET) == (self.attempt(),)
                confirmed = replace(
                    self.attempt(),
                    state=RedemptionState.CONFIRMED,
                    transaction_id="tx-1",
                    transaction_hash="0x" + "f" * 64,
                    received_payout_units=0,
                )
                await second.save(confirmed)
                await second.save(confirmed)
                assert await second.pending(WALLET) == ()
                with pytest.raises(SettlementStorageError):
                    await second.save(self.attempt())
            async with pool.acquire() as conn:
                payload = await conn.fetchval("SELECT payload::text FROM redemption_attempts")
                assert "private_key" not in payload and "signature" not in payload
                assert "1234567890123456789012345678901" in payload

    async def test_session_lock_rejects_other_process_and_releases_after_error(self) -> None:
        """Verify session lock rejects other process and releases after error."""
        async with _pg_pool() as pool:
            first, second = PostgresRedemptionJournal(pool), PostgresRedemptionJournal(pool)
            with pytest.raises(RuntimeError, match="controlled"):
                async with first.lock(WALLET):
                    with pytest.raises(SettlementStorageError, match="already_running"):
                        async with second.lock(WALLET.upper().replace("0X", "0x")):
                            pytest.fail("second wallet lock must not be acquired")
                    raise RuntimeError("controlled")
            async with second.lock(WALLET):
                await second.save(self.attempt())

    async def test_unlocked_or_wrong_wallet_write_fails_closed(self) -> None:
        """Verify unlocked or wrong wallet write fails closed."""
        async with _pg_pool() as pool:
            journal = PostgresRedemptionJournal(pool)
            with pytest.raises(SettlementStorageError):
                await journal.save(self.attempt())
            async with journal.lock("0x" + "2" * 40):
                with pytest.raises(SettlementStorageError):
                    await journal.save(self.attempt())
                with pytest.raises(SettlementStorageError):
                    await journal.pending(WALLET)

    async def test_only_one_unresolved_authorization_can_own_a_wallet(self) -> None:
        """Verify only one unresolved authorization can own a wallet."""
        async with _pg_pool() as pool:
            journal = PostgresRedemptionJournal(pool)
            attempt = self.attempt()
            replacement = replace(
                attempt, authorization=RedemptionAuthorization(7, 1700001200, "0x" + "d" * 64)
            )
            async with journal.lock(WALLET):
                await journal.save(attempt)
                with pytest.raises(SettlementStorageError):
                    await journal.save(replacement)
                await journal.save(replace(attempt, state=RedemptionState.EXPIRED))
                await journal.save(replacement)
                assert await journal.pending(WALLET) == (replacement,)

    async def test_corrupt_journal_stops_before_authorizing_another_submission(self) -> None:
        """Verify corrupt journal stops before authorizing another submission."""
        async with _pg_pool() as pool:
            journal = PostgresRedemptionJournal(pool)
            async with journal.lock(WALLET):
                await journal.save(self.attempt())
            async with pool.acquire() as conn:
                await conn.execute("UPDATE redemption_attempts SET payload = '{}'::jsonb")
            async with journal.lock(WALLET):
                with pytest.raises(SettlementStorageError, match="invalid"):
                    await journal.pending(WALLET)

    async def test_standalone_initialization_installs_only_its_independent_tables(self) -> None:
        """A fresh schema needs no copy-trader tables to host redemption ownership."""
        from unittest.mock import MagicMock

        async with _pg_pool() as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("CREATE SCHEMA standalone_redemption")
                    await connection.execute("SET LOCAL search_path TO standalone_redemption")
                    owned_pool = MagicMock()
                    owned_pool.acquire.return_value.__aenter__.return_value = connection
                    journal = PostgresRedemptionJournal(owned_pool)
                    await journal.initialize()
                    await journal.initialize()
                    tables = await connection.fetch(
                        "SELECT tablename FROM pg_tables WHERE schemaname = 'standalone_redemption'"
                    )
                    assert {row["tablename"] for row in tables} == {
                        "app_schema_migrations",
                        "redemption_attempts",
                    }
                    assert (
                        await connection.fetchval("SELECT count(*) FROM app_schema_migrations") == 1
                    )
