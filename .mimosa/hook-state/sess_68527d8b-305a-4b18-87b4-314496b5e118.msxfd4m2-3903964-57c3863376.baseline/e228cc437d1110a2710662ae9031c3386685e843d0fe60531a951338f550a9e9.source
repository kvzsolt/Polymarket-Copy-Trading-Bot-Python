"""Focused safety contracts for no-order-id ambiguous submission recovery."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousRecoveryProgress,
    AmbiguousRecoveryResult,
    AmbiguousSubmission,
    AmbiguousSubmissionRecoveryService,
    AuthoritativeRecoveryScan,
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

    async def scan(self, submission: AmbiguousSubmission) -> AuthoritativeRecoveryScan:
        self.calls.append(submission)
        return self._scans.pop(0)


class _Repository:
    def __init__(self, submission: AmbiguousSubmission) -> None:
        self.submission = submission
        self.consecutive = 0
        self.list_limits: list[int] = []
        self.recorded: list[AuthoritativeRecoveryScan] = []
        self.attached: list[tuple[str, str, RecoveryMatchSource, datetime]] = []
        self.marked_absent: list[tuple[str, int, datetime]] = []

    async def list_no_id_ambiguous_submissions(
        self, *, limit: int
    ) -> tuple[AmbiguousSubmission, ...]:
        self.list_limits.append(limit)
        return (self.submission,)

    async def record_ambiguous_scan_evidence(
        self,
        *,
        idempotency_key: str,
        scan: AuthoritativeRecoveryScan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ) -> AmbiguousRecoveryProgress | None:
        del observed_at, evidence_history_limit
        assert idempotency_key == self.submission.idempotency_key
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
        idempotency_key: str,
        venue_order_id: str,
        source: RecoveryMatchSource,
        recovered_at: datetime,
    ) -> bool:
        self.attached.append((idempotency_key, venue_order_id, source, recovered_at))
        return True

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        self.marked_absent.append((idempotency_key, required_scan_count, confirmed_at))
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
        clock=_Clock(),
        grace_period=timedelta(seconds=30),
        required_absence_scans=2,
    )

    results = await service.recover_once()

    assert results[0].disposition is RecoveryDisposition.RECOVERED_ORDER_ID
    assert results[0].venue_order_id == "venue-order-7"
    assert repo.attached == [
        (
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
    assert repo.marked_absent == [(submission.idempotency_key, 2, _NOW)]


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
