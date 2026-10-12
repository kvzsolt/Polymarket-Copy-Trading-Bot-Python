"""Disposable-Postgres gate for account-scoped ambiguous submission recovery.

Audit A01 final gate: the fakes in ``test_ambiguous_recovery_account_isolation``
pin the contracts, but cross-account attachment must be STRUCTURALLY
impossible in the real SQL.  These tests run the actual statements against a
throwaway Postgres (testcontainers) under two accounts copying the same
source trade — identical token/side/size/price/close timestamps — plus a
legacy blank-user row:

* the batch predicate scopes by account BEFORE ``LIMIT`` (an older foreign
  row cannot crowd out the account's own row within the bound);
* attach / absence-evidence / confirmed-absent are compare-and-set on the
  account identity;
* a foreign account's scan can never reset the owning account's proof;
* ``get_unconfirmed_buys`` stops unioning blank/NULL-user rows;
* the one-shot provenance mapping helper reports then maps unambiguous
  legacy rows without touching ambiguous ones.

The repo's conftest auto-marks this file ``integration`` (it mentions
PostgresContainer), so run it explicitly:

    .venv/bin/python -m pytest -m integration \
        tests/test_ambiguous_recovery_account_isolation_postgres.py -q
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousSubmission,
    AmbiguousSubmissionRecoveryService,
    AuthoritativeRecoveryScan,
    RecoveryMatch,
    RecoveryMatchSource,
    SubmissionSide,
)
from storage.postgres.orders_repo import OrdersRepo

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
_TOKEN = "71321045679252212594626385532706912750332728571942532289631379312455583992563"
_CONDITION = "0x" + "ab" * 32
_USER_A = "user-a"
_USER_B = "user-b"
_WALLET_A = "0x" + "a1" * 20
_WALLET_B = "0x" + "b2" * 20


@asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer and apply the full schema."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:  # pragma: no cover - dependency guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:  # pragma: no cover - environment guard
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _request_payload(user_id: str) -> dict:
    # Identical venue identity for both accounts: the same copied source
    # trade with the same close timestamp and request fields.
    return {
        "user_id": user_id,
        "leader_id": "leader-1",
        "token_id": _TOKEN,
        "condition_id": _CONDITION,
        "side": "BUY",
        "amount": "12.50",
        "price_limit": "0.625",
    }


def _response_payload() -> dict:
    return {
        "status": "submitted_unconfirmed",
        "pending_since": 1_760_160_400.0,
    }


async def _insert_order(
    conn,
    *,
    key: str,
    user_id: str,
    created_at: datetime,
    request: dict | None = None,
    order_id: str | None = None,
    status: str = "submitted_unconfirmed",
) -> None:
    await conn.execute(
        """
        INSERT INTO orders (
            idempotency_key, user_id, leader_id, order_id, status,
            request_json, response_json, attempt_count, last_error,
            created_at, updated_at
        ) VALUES ($1, $2, 'leader-1', $3, $4, $5::jsonb, $6::jsonb, 1, NULL, $7, $7)
        ON CONFLICT (idempotency_key) DO NOTHING
        """,
        key,
        user_id,
        order_id,
        status,
        json.dumps(request if request is not None else _request_payload(user_id)),
        json.dumps(_response_payload()),
        created_at,
    )


def _submission(user_id: str, key: str) -> AmbiguousSubmission:
    return AmbiguousSubmission(
        idempotency_key=key,
        user_id=user_id,
        leader_id="leader-1",
        token_id=_TOKEN,
        condition_id=_CONDITION,
        side=SubmissionSide.BUY,
        amount=Decimal("12.50"),
        price_limit=Decimal("0.625"),
        submitted_at=_NOW - timedelta(seconds=600),
    )


def _match_scan(venue_order_id: str) -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=False,
        private_fills_complete=True,
        position_check=AccountCheckResult.NOT_CHECKED,
        balance_check=AccountCheckResult.NOT_CHECKED,
        match=RecoveryMatch(
            venue_order_id=venue_order_id,
            source=RecoveryMatchSource.PRIVATE_FILLS,
        ),
    )


def _clear_absence_scan() -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )


def _incomplete_scan() -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=False,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )


class _FixedEvidence:
    """Per-account evidence fake bound to one wallet."""

    def __init__(self, user_id: str, wallet: str, scans: list[AuthoritativeRecoveryScan]):
        self._user_id = user_id
        self._wallet = wallet
        self._scans = list(scans)
        self.scanned_users: list[str] = []

    def wallet_for_user(self, user_id: str) -> str | None:
        return self._wallet if user_id == self._user_id else None

    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        self.scanned_users.append(submission.user_id)
        return self._scans.pop(0)


class _FixedClock:
    def __init__(self) -> None:
        self._now = _NOW

    def now(self) -> datetime:
        return self._now


def _service(
    repo: OrdersRepo, evidence: _FixedEvidence, user_id: str
) -> AmbiguousSubmissionRecoveryService:
    return AmbiguousSubmissionRecoveryService(
        repository=repo,
        evidence_source=evidence,
        account_user_id=user_id,
        account_wallet=_WALLET_A if user_id == _USER_A else _WALLET_B,
        clock=_FixedClock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=3,
    )


async def test_real_sql_scopes_batches_before_limit_and_quarantines_legacy_rows():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            # B's row is OLDEST: without a pre-LIMIT account predicate a
            # bounded batch would be crowded out by the foreign row.
            await _insert_order(
                conn,
                key="copy:leader:event:user-b",
                user_id=_USER_B,
                created_at=_NOW - timedelta(minutes=10),
            )
            await _insert_order(
                conn,
                key="copy:leader:event:user-a",
                user_id=_USER_A,
                created_at=_NOW - timedelta(minutes=9),
            )
            await _insert_order(
                conn,
                key="legacy-blank",
                user_id="",
                created_at=_NOW - timedelta(minutes=30),
            )

        for_a = await repo.list_no_id_ambiguous_submissions(user_id=_USER_A, limit=1)
        assert [s.idempotency_key for s in for_a] == ["copy:leader:event:user-a"]
        assert all(s.user_id == _USER_A for s in for_a)

        for_b = await repo.list_no_id_ambiguous_submissions(user_id=_USER_B, limit=1)
        assert [s.idempotency_key for s in for_b] == ["copy:leader:event:user-b"]

        # The legacy blank-user row joins neither batch and never crashes one;
        # a blank scope itself fails closed instead of returning everything.
        assert [s.idempotency_key for s in for_b] == ["copy:leader:event:user-b"]
        with pytest.raises(ValueError, match="user_id"):
            await repo.list_no_id_ambiguous_submissions(user_id="", limit=5)


async def test_attach_is_a_real_account_compare_and_set():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            await _insert_order(
                conn,
                key="copy:leader:event:user-a",
                user_id=_USER_A,
                created_at=_NOW - timedelta(minutes=5),
            )

        # B recovered an identity from ITS private history but points at A's
        # row: the compare-and-set must refuse.
        assert (
            await repo.attach_recovered_venue_order_id(
                user_id=_USER_B,
                idempotency_key="copy:leader:event:user-a",
                venue_order_id="venue-order-b",
                source=RecoveryMatchSource.PRIVATE_FILLS,
                recovered_at=_NOW,
            )
            is False
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT order_id, response_json FROM orders "
                "WHERE idempotency_key = 'copy:leader:event:user-a'"
            )
        assert row["order_id"] is None

        assert (
            await repo.attach_recovered_venue_order_id(
                user_id=_USER_A,
                idempotency_key="copy:leader:event:user-a",
                venue_order_id="venue-order-a",
                source=RecoveryMatchSource.PRIVATE_FILLS,
                recovered_at=_NOW,
            )
            is True
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT order_id, status FROM orders "
                "WHERE idempotency_key = 'copy:leader:event:user-a'"
            )
        assert row["order_id"] == "venue-order-a"
        assert row["status"] == "submitted_unconfirmed"


async def test_absence_proof_is_owned_by_the_account_in_real_sql():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            await _insert_order(
                conn,
                key="copy:leader:event:user-a",
                user_id=_USER_A,
                created_at=_NOW - timedelta(minutes=5),
            )

        for offset in range(2):
            progress = await repo.record_ambiguous_scan_evidence(
                user_id=_USER_A,
                idempotency_key="copy:leader:event:user-a",
                scan=_clear_absence_scan(),
                observed_at=_NOW.replace(second=offset),
                required_scan_count=3,
                evidence_history_limit=4,
            )
            assert progress is not None
        assert progress.consecutive_complete_absence_scans == 2

        # B's non-qualifying scan against A's row: no-op, no reset.
        assert (
            await repo.record_ambiguous_scan_evidence(
                user_id=_USER_B,
                idempotency_key="copy:leader:event:user-a",
                scan=_incomplete_scan(),
                observed_at=_NOW.replace(second=30),
                required_scan_count=3,
                evidence_history_limit=4,
            )
            is None
        )

        # A's next qualifying scan still sees the full 2-scan proof.
        progress = await repo.record_ambiguous_scan_evidence(
            user_id=_USER_A,
            idempotency_key="copy:leader:event:user-a",
            scan=_clear_absence_scan(),
            observed_at=_NOW.replace(second=31),
            required_scan_count=3,
            evidence_history_limit=4,
        )
        assert progress.consecutive_complete_absence_scans == 3

        # B cannot resolve A's row even at threshold.
        assert (
            await repo.mark_ambiguous_submission_confirmed_absent(
                user_id=_USER_B,
                idempotency_key="copy:leader:event:user-a",
                required_scan_count=3,
                confirmed_at=_NOW,
            )
            is False
        )
        assert (
            await repo.mark_ambiguous_submission_confirmed_absent(
                user_id=_USER_A,
                idempotency_key="copy:leader:event:user-a",
                required_scan_count=3,
                confirmed_at=_NOW,
            )
            is True
        )


async def test_two_services_recover_the_same_trade_on_real_sql_without_cross_attach():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            await _insert_order(
                conn,
                key="copy:leader:event:user-a",
                user_id=_USER_A,
                created_at=_NOW - timedelta(minutes=5),
            )
            await _insert_order(
                conn,
                key="copy:leader:event:user-b",
                user_id=_USER_B,
                created_at=_NOW - timedelta(minutes=5),
            )

        evidence_a = _FixedEvidence(_USER_A, _WALLET_A, [_match_scan("venue-order-a")])
        evidence_b = _FixedEvidence(_USER_B, _WALLET_B, [_match_scan("venue-order-b")])
        service_a = _service(repo, evidence_a, _USER_A)
        service_b = _service(repo, evidence_b, _USER_B)

        results_a = await service_a.recover_once()
        results_b = await service_b.recover_once()

        assert [r.venue_order_id for r in results_a] == ["venue-order-a"]
        assert [r.venue_order_id for r in results_b] == ["venue-order-b"]
        assert evidence_a.scanned_users == [_USER_A]
        assert evidence_b.scanned_users == [_USER_B]

        async with pool.acquire() as conn:
            rows = {
                r["user_id"]: r
                for r in await conn.fetch(
                    "SELECT user_id, order_id FROM orders "
                    "WHERE idempotency_key LIKE 'copy:leader:event:%'"
                )
            }
        assert rows[_USER_A]["order_id"] == "venue-order-a"
        assert rows[_USER_B]["order_id"] == "venue-order-b"


async def test_unconfirmed_buys_stop_unioning_legacy_rows_in_real_sql():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            # ``processed_trades.user_id`` is NOT NULL DEFAULT '' in this
            # schema, so the legacy unowned shape is the blank string.
            for trade_key, user_id in (
                ("trade-a", _USER_A),
                ("trade-b", _USER_B),
                ("trade-legacy-blank", ""),
            ):
                await conn.execute(
                    """
                    INSERT INTO processed_trades (
                        trade_key, user_id, leader_id, tx_hash, side, asset,
                        condition_id, status, retry_attempt, order_id, processed_at
                    ) VALUES ($1, $2, 'leader-1', NULL, 'BUY', $3, $4,
                              'submitted_unconfirmed', 0, NULL, $5)
                    ON CONFLICT (trade_key) DO NOTHING
                    """,
                    trade_key,
                    user_id,
                    _TOKEN,
                    _CONDITION,
                    _NOW.timestamp() - 600,
                )

        buys_for_a = await repo.get_unconfirmed_buys(max_age_seconds=3600.0, user_id=_USER_A)
        buys_for_b = await repo.get_unconfirmed_buys(max_age_seconds=3600.0, user_id=_USER_B)
        assert [row["trade_key"] for row in buys_for_a] == ["trade-a"]
        assert [row["trade_key"] for row in buys_for_b] == ["trade-b"]


async def test_provenance_mapping_helper_is_explicit_and_unambiguous():
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        async with pool.acquire() as conn:
            # Unambiguous: request payload user_id and the reservation's
            # owner agree on user-a.
            await conn.execute(
                """
                INSERT INTO order_reservations (
                    id, user_id, order_intent_key, condition_id, token_id, side,
                    reason, reserved_usdc, reserved_size, status, created_at,
                    updated_at, expires_at
                ) VALUES ('res-1', $1, 'intent-1', $2, $3, 'BUY', 'copy',
                          '0', '0', 'released', 0, 0, 0)
                ON CONFLICT (id) DO NOTHING
                """,
                _USER_A,
                _CONDITION,
                _TOKEN,
            )
            await _insert_order(
                conn,
                key="legacy-map-me",
                user_id="",
                created_at=_NOW - timedelta(minutes=30),
                request={
                    **_request_payload(_USER_A),
                    "recovery_metadata": {"reservation_id": "res-1"},
                },
            )
            # Ambiguous: request payload says user-b, reservation says user-a.
            await _insert_order(
                conn,
                key="legacy-conflict",
                user_id="",
                created_at=_NOW - timedelta(minutes=29),
                request={
                    **_request_payload(_USER_B),
                    "recovery_metadata": {"reservation_id": "res-1"},
                },
            )
            # Unresolved: no provenance at all.
            await _insert_order(
                conn,
                key="legacy-no-evidence",
                user_id="",
                created_at=_NOW - timedelta(minutes=28),
                request={"token_id": _TOKEN, "condition_id": _CONDITION, "side": "BUY"},
            )

        report = await repo.map_legacy_unowned_rows_to_provenance_owners(apply=False)
        assert report.orders_candidates == 3
        assert report.orders_mapped == 1
        assert report.orders_ambiguous == 1
        assert report.orders_unresolved == 1

        # Dry run wrote nothing: the quarantined rows are still unmapped and
        # excluded from user-a's recovery batch.
        assert [
            s.idempotency_key
            for s in await repo.list_no_id_ambiguous_submissions(user_id=_USER_A, limit=10)
        ] == []

        applied = await repo.map_legacy_unowned_rows_to_provenance_owners(apply=True)
        assert applied.orders_mapped == 1

        # The mapped row now joins its owner's batch; ambiguous and
        # unresolved rows stay quarantined.
        scoped = await repo.list_no_id_ambiguous_submissions(user_id=_USER_A, limit=10)
        assert [s.idempotency_key for s in scoped] == ["legacy-map-me"]
        assert [
            s.idempotency_key
            for s in await repo.list_no_id_ambiguous_submissions(user_id=_USER_B, limit=10)
        ] == []
