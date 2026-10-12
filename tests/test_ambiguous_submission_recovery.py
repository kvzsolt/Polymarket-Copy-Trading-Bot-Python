"""Focused safety contracts for no-order-id ambiguous submission recovery."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from pmcopy.services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousRecoveryProgress,
    AmbiguousRecoveryResult,
    AmbiguousSubmission,
    AmbiguousSubmissionRecoveryService,
    AuthoritativeRecoveryScan,
    EvidenceCollectionOutcome,
    RecoveryDisposition,
    RecoveryMatch,
    RecoveryMatchSource,
    SubmissionSide,
)

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


class _Clock:
    def __init__(self, now: datetime = _NOW) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value


class _EvidenceSource:
    def __init__(self, scans: list[AuthoritativeRecoveryScan]) -> None:
        self._scans = list(scans)
        self.calls: list[AmbiguousSubmission] = []

    def wallet_for_user(self, user_id: str) -> str | None:
        return "0x" + "11" * 20 if user_id == "user-1" else None

    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        self.calls.append(submission)
        return self._scans.pop(0)


class _Repository:
    def __init__(self, submission: AmbiguousSubmission) -> None:
        self.submission = submission
        self.consecutive = 0
        self.list_scopes: list[tuple[str, int]] = []
        self.recorded: list[AuthoritativeRecoveryScan] = []
        self.attached: list[tuple[str, str, str, RecoveryMatchSource, datetime]] = []
        self.marked_absent: list[tuple[str, str, int, datetime]] = []

    async def list_no_id_ambiguous_submissions(
        self, *, user_id: str, limit: int
    ) -> tuple[AmbiguousSubmission, ...]:
        self.list_scopes.append((user_id, limit))
        return (self.submission,) if self.submission.user_id == user_id else ()

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
        del observed_at, evidence_history_limit
        assert idempotency_key == self.submission.idempotency_key
        assert user_id == self.submission.user_id
        self.recorded.append(scan)
        self.consecutive = min(
            self.consecutive + 1 if scan.qualifies_as_complete_absence else 0,
            required_scan_count,
        )
        return AmbiguousRecoveryProgress(
            consecutive_complete_absence_scans=self.consecutive,
            retained_evidence_entries=min(len(self.recorded), required_scan_count),
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
        self.attached.append((user_id, idempotency_key, venue_order_id, source, recovered_at))
        return True

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        self.marked_absent.append((user_id, idempotency_key, required_scan_count, confirmed_at))
        return self.consecutive >= required_scan_count


def _submission(*, age_seconds: int = 600) -> AmbiguousSubmission:
    return AmbiguousSubmission(
        idempotency_key="copy:leader:event",
        user_id="user-1",
        leader_id="leader-1",
        token_id="token-1",
        condition_id="condition-1",
        side=SubmissionSide.BUY,
        amount=Decimal("12.50"),
        price_limit=Decimal("0.625"),
        submitted_at=_NOW - timedelta(seconds=age_seconds),
    )


def _clear_absence_scan() -> AuthoritativeRecoveryScan:
    return AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_orders_candidates_empty=True,
        private_fills_candidates_empty=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )


async def test_before_grace_remains_blocking_without_querying_private_evidence() -> None:
    submission = _submission(age_seconds=29)
    repo = _Repository(submission)
    evidence = _EvidenceSource([_clear_absence_scan()])
    service = AmbiguousSubmissionRecoveryService(
        repository=repo,
        evidence_source=evidence,
        account_user_id="user-1",
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=2,
    )

    results = await service.recover_once()

    assert results == (
        AmbiguousRecoveryResult(
            idempotency_key=submission.idempotency_key,
            disposition=RecoveryDisposition.WAITING_FOR_GRACE,
            consecutive_complete_absence_scans=0,
            venue_order_id=None,
        ),
    )
    assert evidence.calls == []
    assert repo.recorded == []
    assert repo.attached == []
    assert repo.marked_absent == []


async def test_complete_fill_history_match_attaches_order_id_without_resubmitting() -> None:
    submission = _submission()
    repo = _Repository(submission)
    evidence = _EvidenceSource(
        [
            AuthoritativeRecoveryScan(
                private_orders_complete=False,
                private_fills_complete=True,
                position_check=AccountCheckResult.NOT_CHECKED,
                balance_check=AccountCheckResult.NOT_CHECKED,
                match=RecoveryMatch(
                    venue_order_id="venue-order-7",
                    source=RecoveryMatchSource.PRIVATE_FILLS,
                ),
            )
        ]
    )
    service = AmbiguousSubmissionRecoveryService(
        repository=repo,
        evidence_source=evidence,
        account_user_id="user-1",
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=2,
    )

    results = await service.recover_once()

    assert results[0].disposition is RecoveryDisposition.RECOVERED_ORDER_ID
    assert results[0].venue_order_id == "venue-order-7"
    assert repo.attached == [
        (
            "user-1",
            submission.idempotency_key,
            "venue-order-7",
            RecoveryMatchSource.PRIVATE_FILLS,
            _NOW,
        )
    ]
    assert repo.recorded == []
    assert repo.marked_absent == []
    # The injected recovery surface is deliberately read-only: there is no
    # submit/retry collaborator for the service to invoke.
    assert not hasattr(evidence, "submit_order")


async def test_confirmed_absent_requires_repeated_complete_scans_and_account_checks() -> None:
    submission = _submission()
    repo = _Repository(submission)
    evidence = _EvidenceSource([_clear_absence_scan(), _clear_absence_scan()])
    service = AmbiguousSubmissionRecoveryService(
        repository=repo,
        evidence_source=evidence,
        account_user_id="user-1",
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=2,
        evidence_history_limit=2,
    )

    first = await service.recover_once()
    second = await service.recover_once()

    assert first[0].disposition is RecoveryDisposition.EVIDENCE_RECORDED
    assert first[0].consecutive_complete_absence_scans == 1
    assert second[0].disposition is RecoveryDisposition.CONFIRMED_ABSENT
    assert second[0].consecutive_complete_absence_scans == 2
    assert repo.marked_absent == [("user-1", submission.idempotency_key, 2, _NOW)]


async def test_incomplete_or_conflicting_account_scan_resets_absence_progress() -> None:
    submission = _submission()
    repo = _Repository(submission)
    incomplete = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        position_check=AccountCheckResult.EVIDENCE_OF_EXECUTION,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )
    evidence = _EvidenceSource([_clear_absence_scan(), incomplete, _clear_absence_scan()])
    service = AmbiguousSubmissionRecoveryService(
        repository=repo,
        evidence_source=evidence,
        account_user_id="user-1",
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=2,
    )

    first = await service.recover_once()
    second = await service.recover_once()
    third = await service.recover_once()

    assert first[0].consecutive_complete_absence_scans == 1
    assert second[0].disposition is RecoveryDisposition.INCONCLUSIVE
    assert second[0].consecutive_complete_absence_scans == 0
    assert third[0].disposition is RecoveryDisposition.EVIDENCE_RECORDED
    assert third[0].consecutive_complete_absence_scans == 1
    assert repo.marked_absent == []


async def test_result_and_evidence_dtos_are_frozen() -> None:
    scan = _clear_absence_scan()
    with pytest.raises(FrozenInstanceError):
        scan.private_orders_complete = False  # type: ignore[misc]


async def test_match_requires_the_corresponding_private_history_to_be_complete() -> None:
    with pytest.raises(ValueError, match="complete private order history"):
        AuthoritativeRecoveryScan(
            private_orders_complete=False,
            private_fills_complete=True,
            position_check=AccountCheckResult.NOT_CHECKED,
            balance_check=AccountCheckResult.NOT_CHECKED,
            match=RecoveryMatch(
                venue_order_id="venue-order-7",
                source=RecoveryMatchSource.PRIVATE_ORDERS,
            ),
        )


# ---------------------------------------------------------------------------
# Absence qualification is computed over the submission candidate set (A03)
# ---------------------------------------------------------------------------


async def test_undeclared_collection_outcome_never_qualifies_as_absence() -> None:
    """A scan that does not declare its collection outcomes cannot prove absence.

    ``private_*_complete`` alone was enough pre-A03: a misread collection that
    silently returned ``()`` looked complete.  Qualification requires the
    evidence source to declare BOTH collection outcomes.
    """

    undeclared = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )

    assert undeclared.qualifies_as_complete_absence is False


async def test_undeclared_candidate_set_never_qualifies_as_absence() -> None:
    """Without a declared candidate verdict, emptiness proves nothing."""

    undeclared = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )

    assert undeclared.qualifies_as_complete_absence is False


async def test_complete_nonempty_history_with_zero_candidates_qualifies_as_absence() -> None:
    """Unrelated venue activity on the condition must not block the proof.

    The market-scoped collections carry the account's ENTIRE history on the
    condition; qualifying absence over raw collection emptiness made the proof
    permanently unreachable for any previously active condition.  A COMPLETE
    read (either emptiness) whose candidate set for THIS submission is empty
    qualifies.
    """

    nonempty_unrelated = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        private_orders_candidates_empty=True,
        private_fills_candidates_empty=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )
    mixed_unrelated = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        private_orders_candidates_empty=True,
        private_fills_candidates_empty=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )

    assert nonempty_unrelated.qualifies_as_complete_absence is True
    assert mixed_unrelated.qualifies_as_complete_absence is True


async def test_candidate_activity_in_either_collection_never_qualifies_as_absence() -> None:
    fills_candidate = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        private_orders_candidates_empty=True,
        private_fills_candidates_empty=False,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )
    orders_candidate = AuthoritativeRecoveryScan(
        private_orders_complete=True,
        private_fills_complete=True,
        private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
        private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
        private_orders_candidates_empty=False,
        private_fills_candidates_empty=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )

    assert fills_candidate.qualifies_as_complete_absence is False
    assert orders_candidate.qualifies_as_complete_absence is False


@pytest.mark.parametrize(
    ("orders_outcome", "fills_outcome"),
    [
        (EvidenceCollectionOutcome.PARTIAL, EvidenceCollectionOutcome.PARTIAL),
        (EvidenceCollectionOutcome.UNAVAILABLE, EvidenceCollectionOutcome.COMPLETE_EMPTY),
        (EvidenceCollectionOutcome.INVALID, EvidenceCollectionOutcome.COMPLETE_EMPTY),
        (EvidenceCollectionOutcome.PARTIAL, EvidenceCollectionOutcome.INVALID),
    ],
)
async def test_failed_collection_outcomes_never_qualify_as_absence(
    orders_outcome: EvidenceCollectionOutcome,
    fills_outcome: EvidenceCollectionOutcome,
) -> None:
    """Zero candidates among whatever was read is not a proof: the read failed."""

    scan = AuthoritativeRecoveryScan(
        private_orders_complete=orders_outcome.is_complete,
        private_fills_complete=fills_outcome.is_complete,
        private_orders_outcome=orders_outcome,
        private_fills_outcome=fills_outcome,
        private_orders_candidates_empty=True,
        private_fills_candidates_empty=True,
        position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
    )

    assert scan.qualifies_as_complete_absence is False


async def test_outcome_must_agree_with_the_completeness_flag() -> None:
    with pytest.raises(ValueError, match="private_orders_outcome"):
        AuthoritativeRecoveryScan(
            private_orders_complete=False,
            private_fills_complete=True,
            private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
            position_check=AccountCheckResult.NOT_CHECKED,
            balance_check=AccountCheckResult.NOT_CHECKED,
        )


async def test_outcome_must_be_an_evidence_collection_outcome() -> None:
    with pytest.raises(TypeError):
        AuthoritativeRecoveryScan(
            private_orders_complete=True,
            private_fills_complete=True,
            private_orders_outcome="complete_empty",  # type: ignore[arg-type]
            position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
            balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        )


async def test_candidate_verdict_must_be_a_bool() -> None:
    with pytest.raises(TypeError):
        AuthoritativeRecoveryScan(
            private_orders_complete=True,
            private_fills_complete=True,
            private_orders_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
            private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_EMPTY,
            private_orders_candidates_empty="yes",  # type: ignore[arg-type]
            position_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
            balance_check=AccountCheckResult.CONSISTENT_WITH_ABSENCE,
        )


async def test_a_match_requires_candidates_in_its_source_collection() -> None:
    with pytest.raises(ValueError, match="fill candidates"):
        AuthoritativeRecoveryScan(
            private_orders_complete=False,
            private_fills_complete=True,
            private_fills_outcome=EvidenceCollectionOutcome.COMPLETE_NONEMPTY,
            private_fills_candidates_empty=True,
            position_check=AccountCheckResult.NOT_CHECKED,
            balance_check=AccountCheckResult.NOT_CHECKED,
            match=RecoveryMatch(
                venue_order_id="venue-order-7",
                source=RecoveryMatchSource.PRIVATE_FILLS,
            ),
        )
