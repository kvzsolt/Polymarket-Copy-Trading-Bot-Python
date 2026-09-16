"""Bounded paper observations reuse runtime sizing without private execution."""

from datetime import UTC, datetime
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import AsyncMock

from domain.auto_trader import AutoRoster, AutoTraderScope, Trader
from orchestrators.auto_trader_runtime import AutoTraderRuntime
from orchestrators.auto_trader_paper_runtime import AutoTraderPaperRuntime
from tests.test_auto_trader_service import MemoryRepository, service, A
from tests.test_auto_trader_paper_service import MemoryLedger, evaluator
from tests.config_helpers import load_test_config

NOW = datetime(2026, 9, 16, tzinfo=UTC)
SCOPE = AutoTraderScope("follower", "paper")


def worker(*, capacity=2):
    manager = service(MemoryRepository(AutoRoster(initialized=True, traders=(Trader(A),))))
    runtime = AutoTraderRuntime(services={SCOPE: manager})
    repo = MemoryLedger()
    context = SimpleNamespace(
        user_id="follower",
        config=load_test_config(),
        copy_notional=lambda _: SimpleNamespace(final_notional_usdc=D("10")),
    )
    return (
        AutoTraderPaperRuntime(
            lifecycle=runtime,
            evaluators={SCOPE: evaluator(repo)},
            contexts={A: [context]},
            seed_funds={"follower": D("100")},
            clock=lambda: NOW,
            capacity=capacity,
        ),
        repo,
        manager,
    )


def signal():
    return {
        "side": "BUY",
        "asset": "1234567890123456789012345678901234567890",
        "conditionId": "condition",
        "transactionHash": "tx",
        "size": "100",
        "price": ".5",
        "timestamp": int(NOW.timestamp()),
    }


async def test_paper_worker_records_results_and_never_sweeps():
    runtime, repo, manager = worker()
    await runtime.process(A, signal())
    assert repo.ledger.positions[0].shares > 0
    assert (await manager.repository.load(SCOPE)).traders[0].paper is not None


async def test_queue_overflow_invalidates_evidence_instead_of_biasing_profit():
    runtime, repo, manager = worker(capacity=1)
    runtime.offer(A, [signal(), signal()])
    assert runtime.pending_count == 1
    await runtime.process(A, signal())
    assert not repo.ledger.evidence_complete


async def test_future_or_stale_entries_do_not_create_virtual_positions():
    runtime, repo, _ = worker()
    row = signal()
    row["timestamp"] = int(NOW.timestamp()) - 86400
    await runtime.process(A, row)
    assert repo.ledger is None


async def test_evaluation_records_live_fills_separately_from_paper():
    from domain.auto_trader import TraderStatus
    from tests.test_auto_trader_lifecycle import performance

    live_scope = AutoTraderScope("follower", "live")
    manager = service(
        MemoryRepository(
            AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE),))
        )
    )
    lifecycle = AutoTraderRuntime(services={live_scope: manager})
    evidence = performance(evaluation_id="live:1", watermark="live:1", drawdown=None)
    reader = SimpleNamespace(read=AsyncMock(return_value=evidence))
    runtime = AutoTraderPaperRuntime(
        lifecycle=lifecycle,
        evaluators={live_scope: evaluator(MemoryLedger())},
        contexts={},
        seed_funds={},
        clock=lambda: NOW,
        live_results=reader,
    )
    await runtime.evaluate_results()
    row = (await manager.repository.load(live_scope)).traders[0]
    assert row.live == evidence and row.paper is None


async def test_restart_preserves_inventory_but_invalidates_unobserved_interval():
    runtime, repo, manager = worker()
    await runtime.process(A, signal())
    before = repo.ledger
    await runtime.prepare()
    assert repo.ledger.positions == before.positions
    assert repo.ledger.cash == before.cash
    assert not repo.ledger.evidence_complete
    assert not (await manager.repository.load(SCOPE)).traders[0].paper.complete


async def test_flat_portfolio_starts_new_evidence_window_after_gap():
    from dataclasses import replace
    from domain.paper_evaluation import PaperLedger
    from datetime import timedelta

    runtime, repo, _ = worker()
    repo.ledger = replace(
        PaperLedger.start(D("100"), NOW - timedelta(days=8)),
        cash=D("110"),
        evidence_complete=False,
        closed_episodes=30,
        realized_pnl=D("10"),
        revision=5,
    )
    await runtime.prepare()
    assert repo.ledger.evidence_complete
    assert repo.ledger.started_at == NOW
    assert repo.ledger.closed_episodes == 0
    assert repo.ledger.cash == D("100")
    assert repo.ledger.revision > 5


async def test_periodic_gap_is_consumed_and_does_not_poison_future_flat_windows():
    from dataclasses import replace
    from domain.paper_evaluation import PaperLedger

    runtime, repo, _ = worker(capacity=1)
    repo.ledger = PaperLedger.start(D("100"), NOW)
    runtime.offer(A, [signal(), signal()])
    await runtime.evaluate_results()
    assert not repo.ledger.evidence_complete
    repo.ledger = replace(repo.ledger, revision=repo.ledger.revision + 1)
    await runtime.evaluate_results()
    assert repo.ledger.evidence_complete


async def test_live_results_failure_does_not_block_paper_or_other_accounts():
    from domain.auto_trader import TraderStatus
    from tests.test_auto_trader_lifecycle import performance

    scopes = (AutoTraderScope("first", "live"), AutoTraderScope("second", "live"))
    managers = {
        scope: service(
            MemoryRepository(
                AutoRoster(
                    initialized=True,
                    traders=(Trader(A, status=TraderStatus.ACTIVE, live=performance()),),
                )
            )
        )
        for scope in scopes
    }
    evidence = performance(evaluation_id="second:live", watermark="second:live")
    reader = SimpleNamespace(read=AsyncMock(side_effect=[ValueError("invalid evidence"), evidence]))
    runtime = AutoTraderPaperRuntime(
        lifecycle=AutoTraderRuntime(services=managers),
        evaluators={scope: evaluator(MemoryLedger()) for scope in scopes},
        contexts={},
        seed_funds={},
        clock=lambda: NOW,
        live_results=reader,
    )
    await runtime.evaluate_results()
    assert not (await managers[scopes[0]].repository.load(scopes[0])).traders[0].live.complete
    assert (await managers[scopes[1]].repository.load(scopes[1])).traders[0].live == evidence


async def test_runtime_uses_durable_cross_source_claim_without_collapsing_chain_logs():
    runtime, repo, _ = worker()
    chain = {**signal(), "_source_receipt": "receipt", "log_index": 1}
    await runtime.process(A, chain)
    first = repo.ledger.cash
    await runtime.process(A, signal())
    assert repo.ledger.cash == first
    await runtime.process(A, {**chain, "log_index": 2})
    assert repo.ledger.cash < first
