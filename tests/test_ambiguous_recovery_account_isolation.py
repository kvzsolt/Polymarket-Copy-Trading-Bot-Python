"""Account-scoped isolation contracts for no-ID ambiguous submission recovery.

Audit A01: with multiple follower accounts, one account's recovery service
must never treat another account's venue evidence as a match, never attach
another account's venue order id, and never reset another account's absence
proof.  These contracts pin that isolation at every layer:

* the service owns exactly one account identity and rejects foreign
  submissions before any private read or persistence mutation;
* the evidence source declares which wallet it serves so an unknown account
  cannot be scanned against the wrong credentials;
* the repository scopes its batch SQL by account BEFORE ``LIMIT`` and guards
  every compare-and-set mutation with the account identity;
* legacy blank/NULL-user rows are quarantined (excluded and logged) instead
  of crashing or joining any account's batch.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousRecoveryProgress,
    AmbiguousSubmission,
    AmbiguousSubmissionRecoveryService,
    AuthoritativeRecoveryScan,
    RecoveryDisposition,
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


class _Clock:
    def __init__(self, now: datetime = _NOW) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _Row:
    """One persisted ambiguous order row inside a repository fake."""

    def __init__(self, submission: AmbiguousSubmission, *, created_at: datetime) -> None:
        self.submission = submission
        self.created_at = created_at
        self.venue_order_id: str | None = None
        self.absent = False
        self.consecutive = 0
        self.attached: list[tuple[str, str, str]] = []  # (user, key, venue id)
        self.recorded: list[tuple[str, bool]] = []  # (user, qualifies)
        self.marked_absent: list[str] = []  # users that resolved the row


class _ScopedRepository:
    """Repository fake honoring the account-scoped contract.

    Every mutation carries the caller's ``user_id`` and is a no-op when it
    does not match the row's owner — mirroring the SQL compare-and-set.
    """

    def __init__(self) -> None:
        self.rows: dict[str, _Row] = {}
        self.list_calls: list[tuple[str, int]] = []

    def add(self, submission: AmbiguousSubmission, *, created_at: datetime) -> _Row:
        row = _Row(submission, created_at=created_at)
        self.rows[submission.idempotency_key] = row
        return row

    async def list_no_id_ambiguous_submissions(
        self, *, user_id: str, limit: int
    ) -> tuple[AmbiguousSubmission, ...]:
        self.list_calls.append((user_id, limit))
        owned = [
            row
            for row in self.rows.values()
            if row.submission.user_id == user_id and row.venue_order_id is None and not row.absent
        ]
        owned.sort(key=lambda row: (row.created_at, row.submission.idempotency_key))
        return tuple(row.submission for row in owned[:limit])

    async def record_ambiguous_scan_evidence(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        scan: AuthoritativeRecoveryScan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ) -> AmbiguousRecoveryProgress | None:
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return None
        if row.venue_order_id is not None or row.absent:
            return None
        qualifies = scan.qualifies_as_complete_absence
        row.consecutive = min(row.consecutive + 1, required_scan_count) if qualifies else 0
        row.recorded.append((user_id, qualifies))
        return AmbiguousRecoveryProgress(
            consecutive_complete_absence_scans=row.consecutive,
            retained_evidence_entries=min(len(row.recorded), evidence_history_limit),
        )

    async def attach_recovered_venue_order_id(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        venue_order_id: str,
        source: RecoveryMatchSource,
        recovered_at: datetime,
    ) -> bool:
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return False
        if row.venue_order_id is not None or row.absent:
            return False
        row.venue_order_id = venue_order_id
        row.attached.append((user_id, idempotency_key, venue_order_id))
        return True

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return False
        if row.consecutive < required_scan_count:
            return False
        row.absent = True
        row.marked_absent.append(user_id)
        return True


class _GlobalRepository(_ScopedRepository):
    """Legacy repository defect: the batch ignores the account scope.

    Used to prove the service-level guard keeps isolation even when the
    persistence layer misbehaves (the pre-fix production behaviour: every
    account's loop raced over the same global batch).
    """

    async def list_no_id_ambiguous_submissions(
        self, *, user_id: str, limit: int
    ) -> tuple[AmbiguousSubmission, ...]:
        self.list_calls.append((user_id, limit))
        pending = [
            row for row in self.rows.values() if row.venue_order_id is None and not row.absent
        ]
        pending.sort(key=lambda row: (row.created_at, row.submission.idempotency_key))
        return tuple(row.submission for row in pending[:limit])


class _AccountEvidenceSource:
    """Evidence fake that only knows the wallets it serves."""

    def __init__(
        self,
        wallet_by_user: dict[str, str | None],
        scans_by_user: dict[str, list[AuthoritativeRecoveryScan]],
    ) -> None:
        self._wallet_by_user = dict(wallet_by_user)
        self._scans_by_user = {user: list(scans) for user, scans in scans_by_user.items()}
        self.calls: list[AmbiguousSubmission] = []

    def wallet_for_user(self, user_id: str) -> str | None:
        return self._wallet_by_user.get(user_id)

    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        self.calls.append(submission)
        queue = self._scans_by_user.setdefault(submission.user_id, [])
        if not queue:
            raise AssertionError(f"unexpected scan for {submission.user_id}")
        return queue.pop(0)


def _copied_submission(
    *,
    user_id: str,
    submitted_at: datetime = _NOW - timedelta(seconds=600),
) -> AmbiguousSubmission:
    """Both accounts copied the SAME source trade: identical venue identity."""

    return AmbiguousSubmission(
        idempotency_key=f"copy:leader:event:{user_id}",
        user_id=user_id,
        leader_id="leader-1",
        token_id=_TOKEN,
        condition_id=_CONDITION,
        side=SubmissionSide.BUY,
        amount=Decimal("12.50"),
        price_limit=Decimal("0.625"),
        submitted_at=submitted_at,
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


def _service(
    *,
    account_user_id: str,
    repository,
    evidence,
    account_wallet: str | None = None,
    required_absence_scans: int = 2,
) -> AmbiguousSubmissionRecoveryService:
    return AmbiguousSubmissionRecoveryService(
        repository=repository,
        evidence_source=evidence,
        account_user_id=account_user_id,
        account_wallet=account_wallet,
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=required_absence_scans,
    )


# ---------------------------------------------------------------------------
# Service-level isolation
# ---------------------------------------------------------------------------


async def test_two_accounts_recover_the_same_source_trade_without_cross_attach() -> None:
    """Identical token/side/size/price/close-time rows attach their OWN ids."""
    repo = _ScopedRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    submission_b = _copied_submission(user_id=_USER_B)
    repo.add(submission_a, created_at=_NOW - timedelta(seconds=2))
    repo.add(submission_b, created_at=_NOW - timedelta(seconds=1))

    evidence_a = _AccountEvidenceSource(
        {_USER_A: _WALLET_A}, {_USER_A: [_match_scan("venue-order-a")]}
    )
    evidence_b = _AccountEvidenceSource(
        {_USER_B: _WALLET_B}, {_USER_B: [_match_scan("venue-order-b")]}
    )
    service_a = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=evidence_a,
        account_wallet=_WALLET_A,
    )
    service_b = _service(
        account_user_id=_USER_B,
        repository=repo,
        evidence=evidence_b,
        account_wallet=_WALLET_B,
    )

    results_a = await service_a.recover_once()
    results_b = await service_b.recover_once()

    assert [result.disposition for result in results_a] == [RecoveryDisposition.RECOVERED_ORDER_ID]
    assert [result.disposition for result in results_b] == [RecoveryDisposition.RECOVERED_ORDER_ID]
    assert results_a[0].venue_order_id == "venue-order-a"
    assert results_b[0].venue_order_id == "venue-order-b"

    # Each evidence source scanned only its own account's submission.
    assert [submission.user_id for submission in evidence_a.calls] == [_USER_A]
    assert [submission.user_id for submission in evidence_b.calls] == [_USER_B]

    # Attachments landed on the owning rows only, carrying the owner identity.
    row_a = repo.rows[submission_a.idempotency_key]
    row_b = repo.rows[submission_b.idempotency_key]
    assert row_a.venue_order_id == "venue-order-a"
    assert row_b.venue_order_id == "venue-order-b"
    assert row_a.attached == [(_USER_A, submission_a.idempotency_key, "venue-order-a")]
    assert row_b.attached == [(_USER_B, submission_b.idempotency_key, "venue-order-b")]

    # No cross-account evidence, absence, or proof mutation occurred.
    assert row_a.recorded == [] and row_b.recorded == []
    assert row_a.marked_absent == [] and row_b.marked_absent == []
    assert repo.list_calls == [(_USER_A, 100), (_USER_B, 100)]


async def test_foreign_submission_is_rejected_before_any_private_read_or_attach() -> None:
    """Even a global (unscoped) batch can never leak rows across accounts."""
    repo = _GlobalRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    submission_b = _copied_submission(user_id=_USER_B)
    repo.add(submission_a, created_at=_NOW - timedelta(seconds=1))
    repo.add(submission_b, created_at=_NOW - timedelta(seconds=2))

    # Account A's evidence knows ONLY A's wallet: B's account is unserved.
    evidence_a = _AccountEvidenceSource(
        {_USER_A: _WALLET_A},
        {_USER_A: [_match_scan("venue-order-a")]},
    )
    service_a = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=evidence_a,
        account_wallet=_WALLET_A,
    )

    results = await service_a.recover_once()

    # B's earlier unresolved row is processed first and must be a no-op.
    assert results[0].idempotency_key == submission_b.idempotency_key
    assert results[0].disposition is RecoveryDisposition.ACCOUNT_MISMATCH
    assert results[1].disposition is RecoveryDisposition.RECOVERED_ORDER_ID

    # Only A's submission reached the private evidence scan.
    assert [submission.user_id for submission in evidence_a.calls] == [_USER_A]

    row_b = repo.rows[submission_b.idempotency_key]
    assert row_b.venue_order_id is None
    assert row_b.recorded == []
    assert row_b.attached == []
    assert row_b.marked_absent == []


async def test_account_wallet_mismatch_is_rejected_without_private_reads() -> None:
    """A resolved wallet that is not the service's own account wallet is a mismatch."""
    repo = _ScopedRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    repo.add(submission_a, created_at=_NOW)

    # Mis-wired map: A's submission resolves to B's wallet.
    evidence = _AccountEvidenceSource(
        {_USER_A: _WALLET_B}, {_USER_A: [_match_scan("venue-order-b")]}
    )
    service = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=evidence,
        account_wallet=_WALLET_A,
    )

    results = await service.recover_once()

    assert results[0].disposition is RecoveryDisposition.ACCOUNT_MISMATCH
    assert evidence.calls == []
    assert repo.rows[submission_a.idempotency_key].venue_order_id is None


async def test_unresolvable_evidence_wallet_is_an_account_mismatch() -> None:
    """An evidence source that cannot declare the wallet fails closed."""
    repo = _ScopedRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    repo.add(submission_a, created_at=_NOW)

    class _NoWalletDeclaration:
        async def scan(self, submission):  # pragma: no cover - must not run
            raise AssertionError("scan must not run for an undeclared wallet")

    service = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=_NoWalletDeclaration(),
        account_wallet=_WALLET_A,
    )

    results = await service.recover_once()

    assert results[0].disposition is RecoveryDisposition.ACCOUNT_MISMATCH
    assert repo.rows[submission_a.idempotency_key].venue_order_id is None


async def test_blank_account_identity_is_rejected_at_construction() -> None:
    with pytest.raises(ValueError, match="account_user_id"):
        _service(account_user_id="  ", repository=_ScopedRepository(), evidence=object())


async def test_restarted_services_preserve_account_isolation() -> None:
    """Recreated services (process restart) keep each account on its own rows."""
    repo = _ScopedRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    submission_b = _copied_submission(user_id=_USER_B)
    repo.add(submission_a, created_at=_NOW - timedelta(seconds=2))
    repo.add(submission_b, created_at=_NOW - timedelta(seconds=1))

    evidence_specs = {
        _USER_A: (_WALLET_A, "venue-order-a"),
        _USER_B: (_WALLET_B, "venue-order-b"),
    }
    for user, (wallet, venue_id) in evidence_specs.items():
        evidence = _AccountEvidenceSource({user: wallet}, {user: [_match_scan(venue_id)]})
        service = _service(
            account_user_id=user,
            repository=repo,
            evidence=evidence,
            account_wallet=wallet,
        )
        await service.recover_once()

    # Restart: fresh service objects over the same persisted rows.
    restarted_repo = _ScopedRepository()
    restarted_repo.rows = repo.rows
    for user, (wallet, _venue_id) in evidence_specs.items():
        evidence = _AccountEvidenceSource({user: wallet}, {user: []})
        service = _service(
            account_user_id=user,
            repository=restarted_repo,
            evidence=evidence,
            account_wallet=wallet,
        )
        results = await service.recover_once()
        # Every row is already resolved for its own account: nothing pending,
        # nothing foreign, no cross-attach could or did occur.
        assert results == ()

    row_a = repo.rows[submission_a.idempotency_key]
    row_b = repo.rows[submission_b.idempotency_key]
    assert row_a.venue_order_id == "venue-order-a"
    assert row_b.venue_order_id == "venue-order-b"


async def test_concurrent_recovery_workers_converge_without_cross_attach() -> None:
    """Two accounts' loops racing over one shared batch stay isolated."""
    repo = _GlobalRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    submission_b = _copied_submission(user_id=_USER_B)
    repo.add(submission_a, created_at=_NOW - timedelta(seconds=2))
    repo.add(submission_b, created_at=_NOW - timedelta(seconds=1))

    evidence_a = _AccountEvidenceSource(
        {_USER_A: _WALLET_A},
        {_USER_A: [_match_scan("venue-order-a"), _incomplete_scan()]},
    )
    evidence_b = _AccountEvidenceSource(
        {_USER_B: _WALLET_B},
        {_USER_B: [_match_scan("venue-order-b"), _incomplete_scan()]},
    )
    service_a = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=evidence_a,
        account_wallet=_WALLET_A,
    )
    service_b = _service(
        account_user_id=_USER_B,
        repository=repo,
        evidence=evidence_b,
        account_wallet=_WALLET_B,
    )

    await asyncio.gather(service_a.recover_once(), service_b.recover_once())

    assert [submission.user_id for submission in evidence_a.calls] == [_USER_A]
    assert [submission.user_id for submission in evidence_b.calls] == [_USER_B]
    assert repo.rows[submission_a.idempotency_key].venue_order_id == "venue-order-a"
    assert repo.rows[submission_b.idempotency_key].venue_order_id == "venue-order-b"
    # A second concurrent sweep after resolution stays a no-op for both.
    await asyncio.gather(service_a.recover_once(), service_b.recover_once())
    assert repo.rows[submission_a.idempotency_key].attached == [
        (_USER_A, submission_a.idempotency_key, "venue-order-a")
    ]
    assert repo.rows[submission_b.idempotency_key].attached == [
        (_USER_B, submission_b.idempotency_key, "venue-order-b")
    ]


# ---------------------------------------------------------------------------
# Absence-proof ownership
# ---------------------------------------------------------------------------


async def test_foreign_scan_cannot_reset_another_accounts_absence_proof() -> None:
    repo = _ScopedRepository()
    submission_a = _copied_submission(user_id=_USER_A)
    row = repo.add(submission_a, created_at=_NOW)

    evidence_a = _AccountEvidenceSource(
        {_USER_A: _WALLET_A},
        {_USER_A: [_clear_absence_scan(), _clear_absence_scan()]},
    )
    service_a = _service(
        account_user_id=_USER_A,
        repository=repo,
        evidence=evidence_a,
        account_wallet=_WALLET_A,
        required_absence_scans=3,
    )
    await service_a.recover_once()
    await service_a.recover_once()
    assert row.consecutive == 2

    # A non-qualifying scan recorded under the WRONG account must be a no-op.
    progress = await repo.record_ambiguous_scan_evidence(
        user_id=_USER_B,
        idempotency_key=submission_a.idempotency_key,
        scan=_incomplete_scan(),
        observed_at=_NOW,
        required_scan_count=3,
        evidence_history_limit=4,
    )
    assert progress is None
    assert row.consecutive == 2
    assert row.recorded == [(_USER_A, True), (_USER_A, True)]

    # The owning account's own non-qualifying scan still resets its proof.
    progress = await repo.record_ambiguous_scan_evidence(
        user_id=_USER_A,
        idempotency_key=submission_a.idempotency_key,
        scan=_incomplete_scan(),
        observed_at=_NOW,
        required_scan_count=3,
        evidence_history_limit=4,
    )
    assert progress is not None
    assert progress.consecutive_complete_absence_scans == 0
    assert row.consecutive == 0


# ---------------------------------------------------------------------------
# Repository SQL contracts
# ---------------------------------------------------------------------------


class _Txn:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _Acquire:
    def __init__(self, conn: "_SqlConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_SqlConn":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _Pool:
    def __init__(self, conn: "_SqlConn") -> None:
        self._conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self._conn)


class _SqlConn:
    """Connection fake applying the account predicates like Postgres would."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows = [dict(row) for row in rows]
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self) -> _Txn:
        return _Txn()

    async def fetch(self, query: str, *params: object) -> list[dict]:
        self.calls.append((query, params))
        if "user_id = $1" in query:
            return [dict(r) for r in self.rows if r.get("user_id") == params[0]]
        return [dict(r) for r in self.rows]

    async def fetchval(self, query: str, *params: object):
        self.calls.append((query, params))
        if "count(*)" in query:
            return sum(
                1
                for r in self.rows
                if r.get("status") == "submitted_unconfirmed"
                and r.get("order_id") is None
                and not str(r.get("user_id") or "")
            )
        if "SET order_id" in query:
            key, user, order_id, response = params[0], params[1], params[2], params[3]
            for row in self.rows:
                if (
                    row["idempotency_key"] == key
                    and row.get("user_id") == user
                    and row.get("status") == "submitted_unconfirmed"
                    and row.get("order_id") is None
                ):
                    row["order_id"] = order_id
                    row["response_json"] = response
                    return key
            return None
        if "SET status = 'confirmed_absent'" in query:
            key, user, response = params[0], params[1], params[2]
            for row in self.rows:
                if (
                    row["idempotency_key"] == key
                    and row.get("user_id") == user
                    and row.get("status") == "submitted_unconfirmed"
                    and row.get("order_id") is None
                ):
                    row["status"] = "confirmed_absent"
                    row["response_json"] = response
                    return key
            return None
        return None

    async def fetchrow(self, query: str, *params: object) -> dict | None:
        self.calls.append((query, params))
        if "FOR UPDATE" in query:
            key, user = params[0], params[1]
            for row in self.rows:
                if (
                    row["idempotency_key"] == key
                    and row.get("user_id") == user
                    and row.get("status") == "submitted_unconfirmed"
                    and row.get("order_id") is None
                ):
                    return dict(row)
            return None
        return dict(self.rows[0]) if self.rows else None

    async def execute(self, query: str, *params: object) -> str:
        self.calls.append((query, params))
        if "UPDATE orders" in query:
            key, user, response = params[0], params[1], params[2]
            for row in self.rows:
                if (
                    row["idempotency_key"] == key
                    and row.get("user_id") == user
                    and row.get("status") == "submitted_unconfirmed"
                    and row.get("order_id") is None
                ):
                    row["response_json"] = response
                    return "UPDATE 1"
            return "UPDATE 0"
        return "SELECT 1"


def _order_row(
    *,
    key: str,
    user_id: str,
    response: dict | None = None,
) -> dict:
    return {
        "idempotency_key": key,
        "user_id": user_id,
        "leader_id": "leader-1",
        "order_id": None,
        "status": "submitted_unconfirmed",
        "request_json": {
            "user_id": user_id,
            "token_id": _TOKEN,
            "condition_id": _CONDITION,
            "side": "BUY",
            "amount": "12.50",
            "price_limit": "0.625",
        },
        "response_json": response
        or {"status": "submitted_unconfirmed", "pending_since": 1_760_160_400.0},
        "attempt_count": 1,
        "last_error": "venue response unavailable",
        "created_at": _NOW - timedelta(minutes=30),
        "updated_at": _NOW - timedelta(minutes=30),
        "matched_total": "0",
    }


async def test_list_sql_scopes_the_account_before_the_limit() -> None:
    conn = _SqlConn(
        [
            _order_row(key="copy:leader:event:user-a", user_id=_USER_A),
            _order_row(key="copy:leader:event:user-b", user_id=_USER_B),
            _order_row(key="legacy-blank", user_id=""),
        ]
    )
    repo = OrdersRepo(_Pool(conn))

    submissions = await repo.list_no_id_ambiguous_submissions(user_id=_USER_A, limit=10)

    assert [submission.idempotency_key for submission in submissions] == [
        "copy:leader:event:user-a"
    ]
    query, params = conn.calls[0]
    assert "status = 'submitted_unconfirmed'" in query
    assert "order_id IS NULL" in query
    assert "user_id = $1" in query
    # The account predicate is applied BEFORE the bound is taken.
    assert query.index("user_id = $1") < query.index("LIMIT $2")
    assert params == (_USER_A, 10)


async def test_list_rejects_a_blank_account_scope_fail_closed() -> None:
    repo = OrdersRepo(_Pool(_SqlConn([])))

    with pytest.raises(ValueError, match="user_id"):
        await repo.list_no_id_ambiguous_submissions(user_id="  ", limit=10)


async def test_legacy_blank_user_rows_neither_crash_nor_join_any_account() -> None:
    class _LeakyConn(_SqlConn):
        """Simulate a row set that leaks a blank-user row past the SQL scope."""

        async def fetch(self, query: str, *params: object) -> list[dict]:
            self.calls.append((query, params))
            return [dict(r) for r in self.rows]

    conn = _LeakyConn(
        [
            _order_row(key="copy:leader:event:user-a", user_id=_USER_A),
            # A legacy blank-user row leaking through despite the SQL scope
            # must be quarantined (skipped, logged) instead of poisoning the
            # batch: AmbiguousSubmission(user_id="") would raise.
            _order_row(key="legacy-blank", user_id=""),
        ]
    )
    repo = OrdersRepo(_Pool(conn))

    submissions = await repo.list_no_id_ambiguous_submissions(user_id=_USER_A, limit=10)

    assert [submission.user_id for submission in submissions] == [_USER_A]


async def test_attach_is_compare_and_set_on_account_identity() -> None:
    conn = _SqlConn([_order_row(key="copy:leader:event:user-a", user_id=_USER_A)])
    repo = OrdersRepo(_Pool(conn))

    foreign = await repo.attach_recovered_venue_order_id(
        user_id=_USER_B,
        idempotency_key="copy:leader:event:user-a",
        venue_order_id="venue-order-b",
        source=RecoveryMatchSource.PRIVATE_FILLS,
        recovered_at=_NOW,
    )
    assert foreign is False
    assert conn.rows[0]["order_id"] is None

    own = await repo.attach_recovered_venue_order_id(
        user_id=_USER_A,
        idempotency_key="copy:leader:event:user-a",
        venue_order_id="venue-order-a",
        source=RecoveryMatchSource.PRIVATE_FILLS,
        recovered_at=_NOW,
    )
    assert own is True
    assert conn.rows[0]["order_id"] == "venue-order-a"

    for query, _params in conn.calls:
        if "FOR UPDATE" in query or "SET order_id" in query:
            assert "user_id = $2" in query


async def test_absence_evidence_and_resolution_are_account_scoped() -> None:
    conn = _SqlConn([_order_row(key="copy:leader:event:user-a", user_id=_USER_A)])
    repo = OrdersRepo(_Pool(conn))

    foreign_progress = await repo.record_ambiguous_scan_evidence(
        user_id=_USER_B,
        idempotency_key="copy:leader:event:user-a",
        scan=_clear_absence_scan(),
        observed_at=_NOW,
        required_scan_count=2,
        evidence_history_limit=4,
    )
    assert foreign_progress is None

    for offset in range(2):
        progress = await repo.record_ambiguous_scan_evidence(
            user_id=_USER_A,
            idempotency_key="copy:leader:event:user-a",
            scan=_clear_absence_scan(),
            observed_at=_NOW.replace(second=offset),
            required_scan_count=2,
            evidence_history_limit=4,
        )
        assert progress is not None
    for query, _params in conn.calls:
        if "FOR UPDATE" in query or "UPDATE orders" in query:
            assert "user_id = $2" in query

    absent = await repo.mark_ambiguous_submission_confirmed_absent(
        user_id=_USER_A,
        idempotency_key="copy:leader:event:user-a",
        required_scan_count=2,
        confirmed_at=_NOW,
    )
    assert absent is True

    # A foreign resolution attempt on an already-owned row is a no-op.
    conn.rows[0]["status"] = "submitted_unconfirmed"
    conn.rows[0]["order_id"] = None
    assert (
        await repo.mark_ambiguous_submission_confirmed_absent(
            user_id=_USER_B,
            idempotency_key="copy:leader:event:user-a",
            required_scan_count=1,
            confirmed_at=_NOW,
        )
        is False
    )
