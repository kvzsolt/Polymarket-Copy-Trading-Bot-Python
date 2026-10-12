"""One-shot resolved-position settlement with durable submission ownership."""

from dataclasses import replace
import logging

from pmcopy.domain.redemption import (
    RedemptionAttempt,
    RedemptionReport,
    RedemptionResult,
    RedemptionSnapshot,
    RedemptionState,
    SettlementAmbiguous,
    SettlementAuthError,
    SettlementError,
    SettlementMarketError,
    SettlementStorageError,
    SettlementUnavailable,
)
from pmcopy.interfaces.settlement import RedemptionJournal, SettlementClock, SettlementGateway

logger = logging.getLogger("Redemption")


class RedemptionService:
    """Run explicit settlement without interacting with order execution or strategy."""

    def __init__(
        self,
        gateway: SettlementGateway,
        journal: RedemptionJournal | None,
        clock: SettlementClock,
        *,
        confirmation_timeout: float = 120,
    ) -> None:
        """Inject all I/O and clocks; previews do not require persistence."""
        if confirmation_timeout <= 0:
            raise ValueError("Confirmation timeout must be positive")
        self._gateway = gateway
        self._journal = journal
        self._clock = clock
        self._confirmation_timeout = confirmation_timeout

    async def run(self, wallet: str, *, execute: bool) -> RedemptionReport:
        """Preview or execute all supported resolved holdings for one wallet.

        Args:
            wallet: Explicitly selected and validated follower Deposit Wallet.
            execute: Caller-validated live execution opt-in.

        Returns:
            Partial results remain visible on failures or ambiguous submissions.
        """
        results: list[RedemptionResult] = []
        report = RedemptionReport(wallet, execute)
        try:
            if execute:
                if self._journal is None:
                    raise SettlementStorageError("redemption_journal_required")
                async with self._journal.lock(wallet):
                    recovered, complete = await self._recover(wallet, results)
                    if complete:
                        report = await self._process(wallet, True, results, recovered)
            else:
                report = await self._process(wallet, False, results, set())
        except SettlementError as error:
            return replace(report, results=tuple(results), error=str(error))
        return replace(report, results=tuple(results))

    async def _process(
        self, wallet: str, execute: bool, results: list[RedemptionResult], excluded: set[str]
    ) -> RedemptionReport:
        markets = await self._gateway.inventory(wallet)
        discovered = tuple(
            dict.fromkeys(
                [result.market.condition_id for result in results]
                + [market.condition_id for market in markets]
            )
        )
        inspected = list(dict.fromkeys(result.market.condition_id for result in results))
        failure = ""
        for market in markets:
            if market.condition_id in excluded:
                continue
            inspected.append(market.condition_id)
            index = len(results)
            try:
                snapshot = await self._gateway.inspect(wallet, market)
                if not snapshot.eligible:
                    reason = "unresolved" if snapshot.payout_denominator == 0 else "already_empty"
                    results.append(
                        RedemptionResult(market, RedemptionState.SKIPPED, snapshot, reason)
                    )
                elif not execute:
                    results.append(RedemptionResult(market, RedemptionState.PLANNED, snapshot))
                elif not await self._execute(wallet, snapshot, results):
                    self._log_decision(results[-1])
                    break
            except SettlementMarketError as error:
                results.append(RedemptionResult(market, RedemptionState.FAILED, reason=str(error)))
            except SettlementError as error:
                failure = str(error)
                if len(results) == index:
                    results.append(RedemptionResult(market, RedemptionState.FAILED, reason=failure))
                self._log_decision(results[-1])
                break
            self._log_decision(results[-1])
        return RedemptionReport(
            wallet, execute, tuple(results), failure, discovered, tuple(dict.fromkeys(inspected))
        )

    async def _execute(
        self, wallet: str, snapshot: RedemptionSnapshot, results: list[RedemptionResult]
    ) -> bool:
        authorization = await self._gateway.prepare(wallet, snapshot)
        attempt = RedemptionAttempt(wallet, snapshot, authorization)
        await self._save(attempt)
        index = len(results)
        results.append(self._result(attempt, "submission_in_progress"))
        try:
            transaction_id = await self._gateway.submit(authorization)
        except SettlementAmbiguous:
            results[index] = self._result(attempt, "submission_unconfirmed")
            return False
        except (SettlementAuthError, SettlementMarketError) as error:
            attempt = replace(attempt, state=RedemptionState.FAILED)
            await self._save(attempt)
            results[index] = self._result(attempt, str(error))
            if isinstance(error, SettlementAuthError):
                raise
            return True
        attempt = replace(attempt, transaction_id=transaction_id)
        results[index] = self._result(attempt, "awaiting_confirmation")
        await self._save(attempt)
        return await self._finish(attempt, results, index)

    async def _finish(
        self, attempt: RedemptionAttempt, results: list[RedemptionResult], index: int
    ) -> bool:
        until = self._clock.monotonic() + self._confirmation_timeout
        while True:
            try:
                evidence = await self._gateway.confirm(attempt.wallet, attempt)
            except SettlementUnavailable:
                results[index] = self._result(attempt, "confirmation_unavailable")
                return False
            attempt = replace(
                attempt,
                state=evidence.state,
                transaction_hash=evidence.transaction_hash,
                received_payout_units=evidence.received_payout_units,
            )
            reason = {
                RedemptionState.CONFIRMED: "",
                RedemptionState.FAILED: "settlement_transaction_failed",
                RedemptionState.PENDING: "awaiting_confirmation",
            }[evidence.state]
            results[index] = self._result(attempt, reason)
            await self._save(attempt)
            if evidence.state is not RedemptionState.PENDING:
                return True
            remaining = until - self._clock.monotonic()
            if remaining <= 0:
                results[index] = self._result(attempt, "confirmation_timeout")
                return False
            await self._clock.sleep(min(2, remaining))

    async def _recover(self, wallet: str, results: list[RedemptionResult]) -> tuple[set[str], bool]:
        if self._journal is None:
            raise SettlementStorageError("redemption_journal_required")
        recovered: set[str] = set()
        for attempt in await self._journal.pending(wallet):
            index = len(results)
            results.append(self._result(attempt, "reconciling_previous_submission"))
            if attempt.transaction_id:
                if not await self._finish(attempt, results, index):
                    return recovered, False
                recovered.add(attempt.snapshot.market.condition_id)
                continue
            snapshot = await self._gateway.inspect(wallet, attempt.snapshot.market)
            if await self._gateway.finalized_timestamp() <= attempt.authorization.deadline:
                results[index] = self._result(attempt, "submission_unconfirmed_until_expiry")
                return recovered, False
            if not any(snapshot.balances):
                await self._save(replace(attempt, state=RedemptionState.CLEARED))
                results[index] = RedemptionResult(
                    snapshot.market,
                    RedemptionState.SKIPPED,
                    snapshot,
                    "already_empty_payout_unattributed",
                )
                recovered.add(snapshot.market.condition_id)
            else:
                await self._save(replace(attempt, state=RedemptionState.EXPIRED))
                results[index] = RedemptionResult(
                    snapshot.market,
                    RedemptionState.SKIPPED,
                    snapshot,
                    "expired_authorization_reconciled",
                )
        return recovered, True

    async def _save(self, attempt: RedemptionAttempt) -> None:
        if self._journal is None:
            raise SettlementStorageError("redemption_journal_required")
        await self._journal.save(attempt)

    @staticmethod
    def _log_decision(result: RedemptionResult) -> None:
        logger.info(
            "redemption_decision",
            extra={
                "condition_id": result.market.condition_id,
                "state": result.state.value,
                "reason": result.reason,
            },
        )

    @staticmethod
    def _result(attempt: RedemptionAttempt, reason: str) -> RedemptionResult:
        return RedemptionResult(
            attempt.snapshot.market,
            attempt.state,
            attempt.snapshot,
            reason,
            attempt.transaction_id,
            attempt.transaction_hash,
            attempt.received_payout_units,
        )
