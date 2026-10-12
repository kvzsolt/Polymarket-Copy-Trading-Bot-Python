"""Durable startup, targeted evaluation, and promotion with deterministic ports."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pmcopy.domain.auto_trader import (
    AutoRoster,
    AutoTraderPolicy,
    AutoTraderScope,
    FalconObservation,
    Trader,
    TraderStatus,
)
from pmcopy.services.auto_trader import AutoTraderService, PromotionRejected
from tests.test_auto_trader_lifecycle import performance

NOW = datetime(2026, 9, 16, tzinfo=UTC)
A = "0x" + "a" * 40
B = "0x" + "b" * 40
SCOPE = AutoTraderScope("follower", "live")


class MemoryRepository:
    def __init__(self, roster=None):
        self.roster = roster or AutoRoster()
        self.snapshots = []
        self.events = []
        self.jobs = set()
        self.receipts = {}

    async def load(self, scope):
        return self.roster

    async def save(
        self, scope, expected_revision, roster, *, event, actor="auto-trader", receipt_key=None
    ):
        if self.roster.revision != expected_revision:
            return False
        self.roster = replace(roster, revision=expected_revision + 1)
        self.events.append((event, actor))
        self.snapshots.append(self.roster)
        if receipt_key:
            self.receipts[receipt_key] = self.roster
        return True

    async def history(self, scope, *, limit=50):
        return tuple(reversed(self.snapshots[-limit:]))

    async def receipt(self, scope, key):
        return self.receipts.get(key)

    async def claim_discovery(self, scope, event, now):
        if event in self.jobs:
            return False
        self.jobs.add(event)
        return True

    async def finish_discovery(self, scope, event, *, succeeded):
        pass


class Provider:
    def __init__(self):
        self.calls = []
        self.score = Decimal("0.8")
        self.wallets = [A, B]

    async def evaluate(self, wallets=None):
        self.calls.append(wallets)
        return tuple(
            FalconObservation(wallet, self.score, NOW, f"{len(self.calls)}:{wallet}")
            for wallet in (self.wallets if wallets is None else wallets)
        )


def service(repo, provider=None):
    return AutoTraderService(
        repository=repo,
        provider=provider or Provider(),
        policy=AutoTraderPolicy(target_count=1),
        clock=lambda: NOW,
    )


async def test_restart_restores_incumbents_without_any_provider_call():
    active = Trader(A, status=TraderStatus.ACTIVE, activation_source="legacy")
    repo = MemoryRepository(AutoRoster(initialized=True, traders=(active,)))
    provider = Provider()
    result = await service(repo, provider).startup(SCOPE)
    assert result.traders[0].status is TraderStatus.ACTIVE
    assert provider.calls == []


async def test_initial_discovery_promotes_and_second_start_reads_database_only():
    repo = MemoryRepository()
    provider = Provider()
    first = await service(repo, provider).startup(SCOPE)
    assert sum(row.status is TraderStatus.ACTIVE for row in first.traders) == 1
    assert len(provider.calls) == 3
    assert provider.calls[0] is None
    assert all(call == (A, B) for call in provider.calls[1:])
    second = await service(repo, provider).startup(SCOPE)
    assert second == first
    assert len(provider.calls) == 3


async def test_successful_empty_discovery_is_not_repeated_on_restart():
    repo = MemoryRepository()
    provider = Provider()
    provider.wallets = []
    await service(repo, provider).startup(SCOPE)
    await service(repo, provider).startup(SCOPE)
    assert provider.calls == [None]
    assert repo.roster.initialized


async def test_database_failure_does_not_fall_back_to_discovery():
    class Broken(MemoryRepository):
        async def load(self, scope):
            raise ConnectionError("database unavailable")

    provider = Provider()
    with pytest.raises(ConnectionError):
        await service(Broken(), provider).startup(SCOPE)
    assert provider.calls == []


async def test_known_wallet_evaluation_never_sweeps_for_healthy_traders():
    repo = MemoryRepository()
    provider = Provider()
    manager = service(repo, provider)
    await manager.startup(SCOPE)
    provider.calls.clear()
    await manager.evaluate_known(SCOPE)
    assert provider.calls == [(A, B)]


async def test_concurrent_promotion_cannot_exceed_target_and_retry_is_idempotent():
    paper = performance()
    traders = tuple(
        Trader(wallet, score=Decimal("0.8"), observed_at=NOW, qualification_streak=3, paper=paper)
        for wallet in (A, B)
    )
    repo = MemoryRepository(AutoRoster(initialized=True, traders=traders))
    manager = service(repo)
    await asyncio.gather(
        *(
            manager.promote(
                SCOPE,
                wallet,
                expected_revision=1,
                evaluation_id=paper.evaluation_id,
                actor="operator",
                request_id=wallet,
            )
            for wallet in (A, B)
        )
    )
    assert sum(row.status is TraderStatus.ACTIVE for row in repo.roster.traders) == 1
    await manager.promote(
        SCOPE,
        A,
        expected_revision=1,
        evaluation_id=paper.evaluation_id,
        actor="operator",
        request_id=A,
    )
    assert len(repo.events) == 2


async def test_promotion_rejects_wrong_or_failing_evaluation():
    paper = performance(realized_pnl=Decimal("-1"))
    candidate = Trader(
        A, score=Decimal("0.8"), observed_at=NOW, qualification_streak=3, paper=paper
    )
    manager = service(MemoryRepository(AutoRoster(initialized=True, traders=(candidate,))))
    with pytest.raises(PromotionRejected, match="not_profitable"):
        await manager.promote(
            SCOPE,
            A,
            expected_revision=1,
            evaluation_id=paper.evaluation_id,
            actor="operator",
            request_id="one",
        )


async def test_legacy_active_rows_are_preserved_without_falcon():
    provider = Provider()
    result = await service(MemoryRepository(), provider).startup(SCOPE, legacy_wallets=(A,))
    assert result.traders[0].activation_source == "legacy"
    assert provider.calls == []


async def test_outage_does_not_demote_or_launch_discovery():
    class Broken(Provider):
        async def evaluate(self, wallets=None):
            assert wallets is not None
            raise RuntimeError("provider unavailable")

    row = Trader(A, status=TraderStatus.ACTIVE, selected_at=NOW - timedelta(days=1))
    repo = MemoryRepository(AutoRoster(initialized=True, traders=(row,)))
    with pytest.raises(RuntimeError):
        await service(repo, Broken()).evaluate_known(SCOPE)
    assert repo.roster.traders == (row,)
    assert repo.jobs == set()


async def test_provider_failure_persists_entry_block_without_demoting_exit_owner():
    class Failed(Provider):
        async def evaluate(self, wallets=None):
            raise RuntimeError("unavailable")

    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE),))
    )
    with pytest.raises(RuntimeError):
        await service(repo, Failed()).evaluate_known(SCOPE)
    assert repo.roster.entry_block_reason == "falcon_unavailable"
    assert repo.roster.traders[0].status is TraderStatus.ACTIVE
    await service(repo).evaluate_known(SCOPE)
    assert repo.roster.entry_block_reason == ""


async def test_promotion_request_id_is_bound_to_its_reviewed_payload():
    paper = performance()
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=tuple(
                Trader(
                    wallet,
                    score=Decimal(".8"),
                    observed_at=NOW,
                    qualification_streak=3,
                    paper=paper,
                )
                for wallet in (A, B)
            ),
        )
    )
    manager = service(repo)
    await manager.promote(
        SCOPE,
        A,
        expected_revision=1,
        evaluation_id=paper.evaluation_id,
        actor="operator",
        request_id="same",
    )
    with pytest.raises(PromotionRejected, match="request_payload_mismatch"):
        await manager.promote(
            SCOPE,
            B,
            expected_revision=1,
            evaluation_id=paper.evaluation_id,
            actor="operator",
            request_id="same",
        )


async def test_partial_targeted_response_is_unavailable_not_negative_evidence():
    class Partial(Provider):
        async def evaluate(self, wallets=None):
            return ()

    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE),))
    )
    with pytest.raises(ValueError, match="incomplete"):
        await service(repo, Partial()).evaluate_known(SCOPE)
    assert repo.roster.entry_block_reason


async def test_unqualified_reserve_does_not_authorize_a_runtime_discovery_sweep():
    provider = Provider()
    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.DEGRADED),))
    )
    await service(repo, provider).rotate(SCOPE)
    assert provider.calls == []


async def test_reapproving_active_trader_preserves_original_tenure():
    from tests.test_auto_trader_lifecycle import performance
    from datetime import timedelta

    selected = NOW - timedelta(days=1)
    paper = performance()
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(
                    A,
                    status=TraderStatus.ACTIVE,
                    selected_at=selected,
                    observed_at=NOW,
                    score=Decimal(".8"),
                    qualification_streak=3,
                    paper=paper,
                ),
            ),
        )
    )
    result = await service(repo).promote(
        SCOPE,
        A,
        expected_revision=1,
        evaluation_id=paper.evaluation_id,
        actor="operator",
        request_id="keep-tenure",
    )
    assert result.traders[0].selected_at == selected


async def test_new_paper_observation_window_resets_previous_episode_strikes():
    old = performance(closed_episodes=40, recent_pnl=Decimal("-5"))
    candidate = Trader(
        A,
        paper=old,
        result_source="paper",
        result_episode_count=40,
        result_watermark="paper:old",
        result_strikes=2,
    )
    repo = MemoryRepository(AutoRoster(initialized=True, traders=(candidate,)))
    fresh = performance(
        started_at=NOW - timedelta(days=1),
        closed_episodes=20,
        recent_pnl=Decimal("-1"),
        watermark="paper:new",
    )
    result = await service(repo).record_performance(SCOPE, A, fresh, source="paper")
    assert result.traders[0].result_episode_count == 20
    assert result.traders[0].result_strikes == 1


async def test_concurrent_command_commit_between_receipt_lookup_and_roster_read_is_idempotent():
    from unittest.mock import AsyncMock

    paper = performance()
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(
                    A, score=Decimal(".8"), observed_at=NOW, qualification_streak=3, paper=paper
                ),
            ),
        )
    )
    manager = service(repo)
    command = dict(
        expected_revision=1,
        evaluation_id=paper.evaluation_id,
        actor="operator",
        request_id="concurrent-command",
    )
    committed = await manager.promote(SCOPE, A, **command)
    repo.receipt = AsyncMock(side_effect=[None, committed])
    assert await manager.promote(SCOPE, A, **command) == committed
    assert len(repo.events) == 1
