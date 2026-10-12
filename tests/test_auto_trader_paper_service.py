"""Paper evaluation deduplicates signals and honors shared risk limits."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal as D

from pmcopy.domain.auto_trader import AutoTraderScope
from pmcopy.domain.paper_evaluation import PaperBook, PaperSimulation
from pmcopy.services.paper_evaluation import PaperCopyOrder, PaperCopyEvaluator
from pmcopy.services.risk.risk_engine import RiskEngine, policy_from_config
from tests.config_helpers import load_test_config

NOW = datetime(2026, 9, 16, tzinfo=UTC)
SCOPE = AutoTraderScope("follower", "live")


class MemoryLedger:
    def __init__(self):
        self.ledger = None
        self.events = set()
        self.sources = {}

    async def load(self, scope, wallet):
        return self.ledger

    async def claim_source(self, scope, wallet, event, *, source, now):
        key = (scope, wallet, event)
        self.sources.setdefault(key, source)
        return self.sources[key] == source

    async def seen(self, scope, wallet, event):
        return event in self.events

    async def save(self, scope, wallet, ledger, *, event, now):
        if event in self.events:
            return False
        self.ledger = replace(ledger, revision=ledger.revision + 1)
        self.events.add(event)
        return True


class Market:
    async def book(self, token, condition):
        return PaperBook(
            ((D(".5"), D("1000")),), ((D(".49"), D("1000")),), D(".005"), D("1"), D(".05")
        )


class Execution:
    async def fill(self, book, *, token_id, side, size, limit, event, now):
        return PaperSimulation().fill(book, side=side, size=size, limit=limit)


def evaluator(repo, *, kill=False):
    policy = replace(
        policy_from_config(load_test_config()),
        kill_switch=kill,
        max_trade_usdc=D("100"),
        max_portfolio_pct_per_trade=D("1"),
    )
    return PaperCopyEvaluator(
        repository=repo,
        market=Market(),
        execution=Execution(),
        risk=RiskEngine(),
        policy=policy,
        clock=lambda: NOW,
    )


def order(side="BUY", event="buy"):
    return PaperCopyOrder(
        SCOPE, "leader", "token", "condition", side, D("10"), D(".5"), D(".5"), event, D("100")
    )


async def test_paper_copy_uses_depth_costs_and_survives_duplicate_restart():
    repo = MemoryLedger()
    await evaluator(repo).evaluate(order())
    cash = repo.ledger.cash
    assert cash < D("100")
    assert repo.ledger.positions[0].net_cost == D("100") - cash
    await evaluator(repo).evaluate(order())
    assert repo.ledger.cash == cash
    await evaluator(repo).evaluate(order(side="SELL", event="sell"))
    assert repo.ledger.positions[0].shares > 0
    assert repo.ledger.closed_episodes == 0


async def test_risk_kill_prevents_paper_entry_and_records_decision():
    repo = MemoryLedger()
    await evaluator(repo, kill=True).evaluate(order())
    assert repo.ledger.cash == D("100")
    assert not repo.ledger.positions
    assert "buy" in repo.events


async def test_production_paper_policy_applies_existing_cash_reserve_and_trade_cap():
    config = load_test_config()
    repo = MemoryLedger()
    instance = PaperCopyEvaluator(
        repository=repo,
        market=Market(),
        execution=Execution(),
        risk=RiskEngine(),
        policy=policy_from_config(config),
        clock=lambda: NOW,
        config=config,
    )
    await instance.evaluate(order())
    assert D("100") - repo.ledger.cash <= D(str(config.risk.balance_policy_hard_cap_ceiling))


async def test_paper_quality_gate_rejects_low_score_with_exact_price_inputs():
    config = load_test_config(execution={"enable_copy_decision_gate": True})
    repo = MemoryLedger()
    instance = PaperCopyEvaluator(
        repository=repo,
        market=Market(),
        execution=Execution(),
        risk=RiskEngine(),
        policy=policy_from_config(config),
        clock=lambda: NOW,
        config=config,
    )
    await instance.evaluate(replace(order(), trader_score=D(".01"), event_ts=int(NOW.timestamp())))
    assert not repo.ledger.positions


async def test_paper_source_claim_preserves_sibling_fills_without_recopying_polling():
    repo = MemoryLedger()
    first = replace(order(event="chain:1"), transaction_event="tx", observation_source="on_chain")
    await evaluator(repo).evaluate(first)
    after_first = repo.ledger.cash
    await evaluator(repo).evaluate(replace(first, event="chain:2"))
    after_sibling = repo.ledger.cash
    assert after_sibling < after_first
    await evaluator(repo).evaluate(replace(first, event="polling", observation_source="polling"))
    assert repo.ledger.cash == after_sibling


async def test_paper_polling_winner_survives_restart_and_blocks_later_chain_copies():
    repo = MemoryLedger()
    first = replace(order(event="polling"), transaction_event="tx", observation_source="polling")
    await evaluator(repo).evaluate(first)
    before = repo.ledger.cash
    await evaluator(repo).evaluate(replace(first, event="chain:1", observation_source="on_chain"))
    assert repo.ledger.cash == before
