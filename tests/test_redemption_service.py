"""One-shot redemption, durable ambiguity, and restart recovery without network I/O."""

from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal

from pmcopy.domain.redemption import (
    RedemptionAuthorization,
    RedemptionAttempt,
    RedemptionConfirmation,
    RedemptionMarket,
    RedemptionSnapshot,
    RedemptionState,
    SettlementAmbiguous,
    SettlementAuthError,
    SettlementMarketError,
    SettlementStorageError,
)
from pmcopy.services.accounting.settlement import RedemptionService

WALLET = "0x" + "1" * 40


class FakeClock:
    """Advance only when the service asks to wait."""

    def __init__(self) -> None:
        self.elapsed = 0.0

    def now(self) -> int:
        """Return a fixed epoch plus controlled elapsed seconds."""
        return 1700000000 + int(self.elapsed)

    def monotonic(self) -> float:
        """Return controlled monotonic seconds."""
        return self.elapsed

    async def sleep(self, seconds: float) -> None:
        """Simulate bounded waiting without wall-clock sleeps."""
        self.elapsed += seconds


class FakeJournal:
    """Retain immutable attempts across service instances."""

    def __init__(self) -> None:
        self.attempts: dict[str, RedemptionAttempt] = {}
        self.locked = False
        self.fail_save = False

    @asynccontextmanager
    async def lock(self, wallet: str):
        """Serialize wallet execution and release on every exit path."""
        if self.locked:
            raise SettlementStorageError("redemption_already_running")
        self.locked = True
        try:
            yield
        finally:
            self.locked = False

    async def pending(self, wallet: str) -> tuple[RedemptionAttempt, ...]:
        """Return only attempts that still own reconciliation."""
        return tuple(a for a in self.attempts.values() if a.state is RedemptionState.PENDING)

    async def save(self, attempt: RedemptionAttempt) -> None:
        """Persist exactly the state the service hands off."""
        if self.fail_save:
            raise SettlementStorageError("redemption_storage_unavailable")
        self.attempts[attempt.authorization.digest] = attempt


class FakeGateway:
    """Model settled inventory and submission ambiguity at the actual port."""

    def __init__(self, journal: FakeJournal) -> None:
        market = RedemptionMarket("0x" + "ab" * 32, ("123", "456"), False, "Example")
        self.snapshots = [RedemptionSnapshot(market, (0, 5), (1, 0), 1, True)]
        self.journal = journal
        self.prepared = 0
        self.submitted = 0
        self.polls = 0
        self.submit_error: Exception | None = None
        self.inspect_errors: set[str] = set()
        self.confirmation = RedemptionConfirmation(RedemptionState.CONFIRMED, "0x" + "f" * 64, 0)
        self.finalized = 1700000000

    async def inventory(self, wallet: str) -> tuple[RedemptionMarket, ...]:
        """Return all held conditions, including unresolved holdings."""
        return tuple(snapshot.market for snapshot in self.snapshots)

    async def inspect(self, wallet: str, market: RedemptionMarket) -> RedemptionSnapshot:
        """Return a fresh authoritative balance and payout snapshot."""
        if market.condition_id in self.inspect_errors:
            raise SettlementMarketError("market_metadata_unavailable")
        return next(s for s in self.snapshots if s.market.condition_id == market.condition_id)

    async def prepare(self, wallet: str, snapshot: RedemptionSnapshot) -> RedemptionAuthorization:
        """Return a different authorization for every intentional fresh submission."""
        self.prepared += 1
        return RedemptionAuthorization(
            self.prepared, self.finalized + 600, "0x" + f"{self.prepared:064x}"
        )

    async def submit(self, authorization: RedemptionAuthorization) -> str:
        """Assert durable ownership already exists at the dangerous boundary."""
        assert self.journal.locked
        assert authorization.digest in self.journal.attempts
        assert self.journal.attempts[authorization.digest].state is RedemptionState.PENDING
        self.submitted += 1
        if self.submit_error:
            raise self.submit_error
        return "tx-1"

    async def confirm(self, wallet: str, attempt: RedemptionAttempt) -> RedemptionConfirmation:
        """Return controlled terminal or in-flight evidence."""
        self.polls += 1
        return self.confirmation

    async def finalized_timestamp(self) -> int:
        """Use chain time, not the caller's clock, for expiry recovery."""
        return self.finalized


class TestRedemptionService:
    """Prove that execution never outruns permission, persistence, or confirmation."""

    @staticmethod
    def scenario():
        """Construct independently injected fakes and the real service."""
        journal = FakeJournal()
        gateway = FakeGateway(journal)
        clock = FakeClock()
        service = RedemptionService(gateway, journal, clock, confirmation_timeout=4)
        return service, gateway, journal, clock

    async def test_preview_losing_tokens_does_not_sign_submit_or_persist(self) -> None:
        """Verify preview losing tokens does not sign submit or persist."""
        service, gateway, journal, _ = self.scenario()
        report = await service.run(WALLET, execute=False)
        assert report.success
        assert report.results[0].state is RedemptionState.PLANNED
        assert report.results[0].snapshot.expected_payout == Decimal(0)
        assert gateway.prepared == gateway.submitted == 0
        assert not journal.attempts

    async def test_confirmation_is_required_before_success_and_receipt_is_persisted(self) -> None:
        """Verify confirmation is required before success and receipt is persisted."""
        service, gateway, journal, _ = self.scenario()
        report = await service.run(WALLET, execute=True)
        assert report.success
        assert gateway.submitted == 1
        assert report.results[0].state is RedemptionState.CONFIRMED
        assert report.results[0].received_payout == Decimal(0)
        assert next(iter(journal.attempts.values())).transaction_id == "tx-1"
        assert next(iter(journal.attempts.values())).state is RedemptionState.CONFIRMED
        assert not journal.locked

    async def test_unresolved_and_empty_positions_are_skipped(self) -> None:
        """Verify unresolved and empty positions are skipped."""
        service, gateway, _, _ = self.scenario()
        gateway.snapshots[0] = replace(
            gateway.snapshots[0], payout_numerators=(0, 0), payout_denominator=0
        )
        report = await service.run(WALLET, execute=True)
        assert report.success
        assert report.results[0].reason == "unresolved"
        assert gateway.prepared == gateway.submitted == 0

    async def test_ambiguous_post_is_never_retried_even_on_restart(self) -> None:
        """Verify ambiguous post is never retried even on restart."""
        service, gateway, journal, _ = self.scenario()
        gateway.submit_error = SettlementAmbiguous("submission_unconfirmed")
        first = await service.run(WALLET, execute=True)
        second = await service.run(WALLET, execute=True)
        assert not first.success and not second.success
        assert gateway.submitted == gateway.prepared == 1
        assert next(iter(journal.attempts.values())).state is RedemptionState.PENDING

    async def test_expired_unknown_submission_requires_finalized_chain_recheck(self) -> None:
        """Verify expired unknown submission requires finalized chain recheck."""
        service, gateway, journal, clock = self.scenario()
        gateway.submit_error = SettlementAmbiguous("submission_unconfirmed")
        await service.run(WALLET, execute=True)
        clock.elapsed = 10000
        assert not (await service.run(WALLET, execute=True)).success
        assert gateway.submitted == 1
        gateway.finalized += 601
        gateway.submit_error = None
        assert (await service.run(WALLET, execute=True)).success
        assert gateway.submitted == 2
        assert len(journal.attempts) == 2

    async def test_restart_polls_existing_transaction_without_new_post(self) -> None:
        """Verify restart polls existing transaction without new post."""
        service, gateway, journal, _ = self.scenario()
        gateway.confirmation = RedemptionConfirmation(RedemptionState.PENDING)
        assert not (await service.run(WALLET, execute=True)).success
        gateway.confirmation = RedemptionConfirmation(RedemptionState.CONFIRMED, "0x" + "f" * 64, 0)
        assert (await service.run(WALLET, execute=True)).success
        assert gateway.submitted == 1
        assert next(iter(journal.attempts.values())).state is RedemptionState.CONFIRMED

    async def test_database_failure_prevents_submission(self) -> None:
        """Verify database failure prevents submission."""
        service, gateway, journal, _ = self.scenario()
        journal.fail_save = True
        report = await service.run(WALLET, execute=True)
        assert not report.success
        assert gateway.submitted == 0

    async def test_empty_inventory_does_not_release_a_still_valid_unknown_authorization(
        self,
    ) -> None:
        """A balance change cannot invalidate a signed batch still held by the relay."""
        service, gateway, journal, _ = self.scenario()
        gateway.submit_error = SettlementAmbiguous("submission_unconfirmed")
        await service.run(WALLET, execute=True)
        gateway.snapshots[0] = replace(gateway.snapshots[0], balances=(0, 0))
        assert not (await service.run(WALLET, execute=True)).success
        assert next(iter(journal.attempts.values())).state is RedemptionState.PENDING
        gateway.finalized += 601
        assert (await service.run(WALLET, execute=True)).success
        assert next(iter(journal.attempts.values())).state is RedemptionState.CLEARED
        assert gateway.submitted == 1

    async def test_authentication_failure_stops_the_wallet(self) -> None:
        """Verify authentication failure stops the wallet."""
        service, gateway, _, _ = self.scenario()
        gateway.submit_error = SettlementAuthError("relayer_authentication_failed")
        assert not (await service.run(WALLET, execute=True)).success
        assert gateway.submitted == 1

    async def test_bad_market_is_reported_while_other_conditions_are_processed(self) -> None:
        """Verify bad market is reported while other conditions are processed."""
        service, gateway, _, _ = self.scenario()
        bad = replace(
            gateway.snapshots[0], market=RedemptionMarket("0x" + "cd" * 32, ("321", "654"), True)
        )
        gateway.snapshots.insert(0, bad)
        gateway.inspect_errors.add(bad.market.condition_id)
        report = await service.run(WALLET, execute=True)
        assert not report.success
        assert gateway.submitted == 1
        assert [result.state for result in report.results] == [
            RedemptionState.FAILED,
            RedemptionState.CONFIRMED,
        ]

    async def test_concurrent_command_cannot_submit(self) -> None:
        """Verify concurrent command cannot submit."""
        service, gateway, journal, _ = self.scenario()
        journal.locked = True
        assert not (await service.run(WALLET, execute=True)).success
        assert gateway.submitted == 0

    async def test_ambiguous_submission_is_logged_as_a_pending_decision(self, caplog) -> None:
        """The stop path remains visible without printing signatures or credentials."""
        service, gateway, _, _ = self.scenario()
        gateway.submit_error = SettlementAmbiguous("submission_unconfirmed")
        with caplog.at_level("INFO", logger="Redemption"):
            await service.run(WALLET, execute=True)
        decisions = [record for record in caplog.records if record.message == "redemption_decision"]
        assert len(decisions) == 1
        assert decisions[0].state == "pending"

    async def test_terminal_rejection_does_not_report_awaiting_confirmation(self) -> None:
        """Verify terminal rejection does not report awaiting confirmation."""
        service, gateway, _, _ = self.scenario()
        gateway.confirmation = RedemptionConfirmation(RedemptionState.FAILED)
        report = await service.run(WALLET, execute=True)
        assert not report.success
        assert report.results[0].reason == "settlement_transaction_failed"
