"""Durable automatic selection with injected provider, clock, and persistence.

All capacity decisions use optimistic database revisions. Provider work never
holds a database transaction or runs on the copy-order hot path.
"""

import asyncio
import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Protocol

from pmcopy.domain.auto_trader import (
    AutoRoster,
    AutoTraderPolicy,
    AutoTraderScope,
    CopyPerformance,
    FalconObservation,
    Trader,
    TraderStatus,
)


class AutoTraderConflict(RuntimeError):
    """The requested revision changed before an atomic roster mutation."""


class PromotionRejected(ValueError):
    """Paper qualification or the requested evaluation is unavailable."""


class AutoTraderRepository(Protocol):
    """Atomic durable membership and discovery-job admission."""

    async def load(self, scope: AutoTraderScope) -> AutoRoster:
        """Read durable state; database errors must propagate."""
        ...

    async def save(
        self,
        scope: AutoTraderScope,
        expected_revision: int,
        roster: AutoRoster,
        *,
        event: str,
        actor: str = "auto-trader",
        receipt_key: str | None = None,
    ) -> bool:
        """Compare and swap the roster, audit event, and optional command receipt."""
        ...

    async def history(self, scope: AutoTraderScope, *, limit: int = 50) -> tuple[AutoRoster, ...]:
        """Read bounded immutable evaluation snapshots for operator review."""
        ...

    async def receipt(self, scope: AutoTraderScope, key: str) -> AutoRoster | None:
        """Read an already committed command result."""
        ...

    async def claim_discovery(self, scope: AutoTraderScope, event: str, now: datetime) -> bool:
        """Lease one provider job for a specific initialization or degradation event."""
        ...

    async def finish_discovery(
        self, scope: AutoTraderScope, event: str, *, succeeded: bool
    ) -> None:
        """Record a bounded discovery job's outcome."""
        ...


class TraderEvidenceProvider(Protocol):
    """Evaluate specified identities or perform an explicitly requested sweep."""

    async def evaluate(
        self, wallets: tuple[str, ...] | None = None
    ) -> tuple[FalconObservation, ...]:
        """Return successful accepted/rejected observations; raise on provider failure."""
        ...


class AutoTraderService:
    """Coordinate stable membership and automatic or operator promotion."""

    def __init__(
        self,
        *,
        repository: AutoTraderRepository,
        provider: TraderEvidenceProvider,
        policy: AutoTraderPolicy,
        clock: Callable[[], datetime],
    ) -> None:
        """Inject every I/O and time dependency."""
        self.repository = repository
        self.policy = policy
        self._provider = provider
        self._clock = clock
        self._provider_lock = asyncio.Lock()

    async def startup(
        self, scope: AutoTraderScope, *, legacy_wallets: Sequence[str] = ()
    ) -> AutoRoster:
        """Restore membership without provider calls, or initialize it once."""
        roster = await self.repository.load(scope)
        if roster.initialized:
            return await self._change(scope, self._select, event="roster_restored")
        if legacy_wallets:
            now = self._clock()
            rows = tuple(
                Trader(
                    wallet, status=TraderStatus.ACTIVE, selected_at=now, activation_source="legacy"
                )
                for wallet in dict.fromkeys(legacy_wallets)
            )
            return await self._change(
                scope,
                lambda old: (
                    old
                    if old.initialized
                    else replace(old, initialized=True, traders=self.policy.select(rows, now))
                ),
                event="legacy_roster_imported",
            )
        return await self.discover(scope, event="initialization")

    async def discover(self, scope: AutoTraderScope, *, event: str) -> AutoRoster:
        """Run one leased bounded sweep, then target only its accepted identities."""
        async with self._provider_lock:
            if not await self.repository.claim_discovery(scope, event, self._clock()):
                return await self.repository.load(scope)
            try:
                observations = await self._provider.evaluate()
                await self._apply_observations(scope, observations)
                targets = tuple(
                    row.wallet
                    for row in observations
                    if row.accepted and row.score >= self.policy.promote_score
                )
                for _ in range(self.policy.required_cycles - 1):
                    if not targets:
                        break
                    observations = await self._provider.evaluate(targets)
                    self._validate_targets(targets, observations)
                    await self._apply_observations(scope, observations)
                    targets = tuple(
                        row.wallet
                        for row in observations
                        if row.accepted and row.score >= self.policy.promote_score
                    )
                result = await self._change(
                    scope,
                    lambda old: self._select(replace(old, initialized=True, entry_block_reason="")),
                    event="discovery_completed",
                )
            except BaseException:
                await self.repository.finish_discovery(scope, event, succeeded=False)
                raise
            await self.repository.finish_discovery(scope, event, succeeded=True)
            return result

    async def evaluate_known(self, scope: AutoTraderScope) -> AutoRoster:
        """Refresh known monitored candidates without requesting a shortlist."""
        roster = await self.repository.load(scope)
        targets = self.evaluation_wallets(roster)
        if not targets:
            return roster
        try:
            async with self._provider_lock:
                observations = await self._provider.evaluate(targets)
            self._validate_targets(targets, observations)
        except Exception:
            await self._change(
                scope,
                lambda old: replace(old, entry_block_reason="falcon_unavailable"),
                event="falcon_unavailable",
            )
            raise
        await self._apply_observations(scope, observations)
        await self._change(
            scope, lambda old: replace(old, entry_block_reason=""), event="falcon_available"
        )
        return await self.rotate(scope)

    async def rotate(self, scope: AutoTraderScope) -> AutoRoster:
        """Fill vacancies from storage before considering a degradation-triggered sweep."""
        roster = await self._change(scope, self._select, event="roster_evaluated")
        if (
            roster.entry_block_reason
            or sum(row.status is TraderStatus.ACTIVE for row in roster.traders)
            >= self.policy.target_count
        ):
            return roster
        degraded = [
            row
            for row in roster.traders
            if row.status is TraderStatus.DEGRADED and row.selected_at is not None
        ]
        if not degraded:
            return roster
        identity = "|".join(
            sorted(f"{row.wallet}:{row.selected_at}:{row.degradation_reason}" for row in degraded)
        )
        event = "degradation:" + hashlib.sha256(identity.encode()).hexdigest()
        return await self.discover(scope, event=event)

    async def record_performance(
        self, scope: AutoTraderScope, wallet: str, results: CopyPerformance, *, source: str
    ) -> AutoRoster:
        """Persist source-separated performance before applying loss strikes."""
        if source not in {"paper", "live"}:
            raise ValueError("invalid performance source")

        def update(old: AutoRoster) -> AutoRoster:
            rows = []
            for row in old.traders:
                if row.wallet == wallet:
                    prior = row.paper if source == "paper" else row.live
                    if prior is not None and prior.observed_at > results.observed_at:
                        rows.append(row)
                        continue
                    row = replace(
                        row, revision=row.revision + int(prior != results), **{source: results}
                    )
                    authoritative = (
                        "live"
                        if scope.trading_mode == "live" and row.status is TraderStatus.ACTIVE
                        else "paper"
                    )
                    if source == authoritative:
                        if row.result_source != source or (
                            prior is not None and prior.started_at != results.started_at
                        ):
                            row = replace(
                                row,
                                result_source=source,
                                result_episode_count=0,
                                result_watermark="",
                                result_strikes=0,
                            )
                        row = self.policy.observe_results(row, results, self._clock())
                rows.append(row)
            return replace(old, traders=tuple(rows))

        return await self._change(scope, update, event=f"{source}_evaluated")

    async def promote(
        self,
        scope: AutoTraderScope,
        wallet: str,
        *,
        expected_revision: int,
        evaluation_id: str,
        actor: str,
        request_id: str,
    ) -> AutoRoster:
        """Approve qualified paper evidence, activating only an available slot."""
        if not actor.strip() or not request_id.strip() or len(request_id) > 128:
            raise PromotionRejected("invalid_request")
        fingerprint = hashlib.sha256(
            json.dumps([wallet, expected_revision, evaluation_id]).encode()
        ).hexdigest()
        receipt_key = hashlib.sha256(json.dumps([actor, request_id]).encode()).hexdigest()
        prior = await self.repository.receipt(scope, receipt_key)
        if prior is not None:
            if prior.last_command_fingerprint != fingerprint:
                raise PromotionRejected("request_payload_mismatch")
            return prior

        def update(old: AutoRoster) -> AutoRoster:
            if old.entry_block_reason:
                raise PromotionRejected(old.entry_block_reason)
            candidate = next((row for row in old.traders if row.wallet == wallet), None)
            if candidate is None:
                raise PromotionRejected("not_found")
            if candidate.revision != expected_revision:
                raise AutoTraderConflict("revision_mismatch")
            if candidate.paper is None or candidate.paper.evaluation_id != evaluation_id:
                raise PromotionRejected("evaluation_mismatch")
            reasons = self.policy.paper_qualification(candidate, candidate.paper, self._clock())
            if reasons:
                raise PromotionRejected(",".join(reasons))
            active_count = sum(row.status is TraderStatus.ACTIVE for row in old.traders)
            status = (
                TraderStatus.ACTIVE
                if active_count < self.policy.target_count
                or candidate.status is TraderStatus.ACTIVE
                else TraderStatus.RESERVE
            )
            approved = replace(
                candidate,
                status=status,
                activation_source="paper_operator",
                selected_at=(
                    self._clock()
                    if status is TraderStatus.ACTIVE and candidate.status is not TraderStatus.ACTIVE
                    else candidate.selected_at
                ),
                revision=candidate.revision + 1,
            )
            return replace(
                old,
                traders=tuple(approved if row.wallet == wallet else row for row in old.traders),
                last_command_fingerprint=fingerprint,
            )

        try:
            result = await self._change(
                scope, update, event="paper_promoted", actor=actor, receipt_key=receipt_key
            )
        except (AutoTraderConflict, PromotionRejected):
            committed = await self.repository.receipt(scope, receipt_key)
            if committed is None:
                raise
            result = committed
        if result.last_command_fingerprint != fingerprint:
            raise PromotionRejected("request_payload_mismatch")
        return result

    def evaluation_wallets(self, roster: AutoRoster) -> tuple[str, ...]:
        """Bound provider work to incumbents and one count-derived reserve cohort."""
        active = [row for row in roster.traders if row.status is TraderStatus.ACTIVE]
        reserves = sorted(
            (
                row
                for row in roster.traders
                if row.status in {TraderStatus.RESERVE, TraderStatus.PAPER}
            ),
            key=self.policy.rank,
        )[: self.policy.target_count]
        return tuple(row.wallet for row in (*active, *reserves))

    @staticmethod
    def _validate_targets(
        targets: tuple[str, ...], observations: tuple[FalconObservation, ...]
    ) -> None:
        if {row.wallet for row in observations} != set(targets) or len(observations) != len(
            targets
        ):
            raise ValueError("incomplete or unexpected targeted provider evaluation")

    async def _apply_observations(
        self, scope: AutoTraderScope, observations: tuple[FalconObservation, ...]
    ) -> AutoRoster:
        def update(old: AutoRoster) -> AutoRoster:
            rows = {row.wallet: row for row in old.traders}
            for observation in observations:
                rows[observation.wallet] = self.policy.observe_falcon(
                    rows.get(observation.wallet, Trader(observation.wallet)), observation
                )
            return replace(old, traders=tuple(rows.values()))

        return await self._change(scope, update, event="falcon_evaluated")

    def _select(self, roster: AutoRoster) -> AutoRoster:
        if roster.entry_block_reason:
            return roster
        return replace(roster, traders=self.policy.select(roster.traders, self._clock()))

    async def _change(
        self,
        scope: AutoTraderScope,
        change: Callable[[AutoRoster], AutoRoster],
        *,
        event: str,
        actor: str = "auto-trader",
        receipt_key: str | None = None,
    ) -> AutoRoster:
        for _ in range(5):
            old = await self.repository.load(scope)
            updated = change(old)
            if updated == old and receipt_key is None:
                return old
            if await self.repository.save(
                scope, old.revision, updated, event=event, actor=actor, receipt_key=receipt_key
            ):
                return replace(updated, revision=old.revision + 1)
            if receipt_key is not None:
                receipt = await self.repository.receipt(scope, receipt_key)
                if receipt is not None:
                    return receipt
        raise AutoTraderConflict("automatic roster changed repeatedly")
