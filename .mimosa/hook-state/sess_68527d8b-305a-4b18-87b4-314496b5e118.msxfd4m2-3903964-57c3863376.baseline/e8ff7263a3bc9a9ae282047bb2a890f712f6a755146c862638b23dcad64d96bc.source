"""Production adapters for canonical copy-task leader events."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from domain.detection import DetectionFinality, DetectionMode, TradeSide
from orchestrators.v1_event_adapters import (
    CopyTaskRuntimeIngress,
    DeferredCopyTaskEventHandler,
    LeaderEventNormalizer,
    TaskSnapshotUnavailable,
    TradingContextTaskExecutionAdapter,
    TradingContextTaskSnapshotProvider,
)
from services.copy_tasks.domain import CopyTask, ReactionPolicy
from services.copy_tasks.event_routing import (
    CopyTaskSourceEvent,
    TaskExecutionCommand,
)
from services.copy_tasks.lifecycle import TaskExecutionDisposition
from services.execution.coordinator import ExecutionCoordinatorResult

_LEADER = "0x1111111111111111111111111111111111111111"
_NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


class FixedClock:
    """Return one deterministic UTC instant."""

    def now(self) -> datetime:
        """Return the fixed instant."""

        return _NOW


@dataclass(frozen=True, slots=True)
class Position:
    """Minimal persisted position row used by the snapshot adapter."""

    condition_id: str
    token_id: str
    size_tokens: Decimal
    avg_entry_price: Decimal


class Positions:
    """Expose deterministic position reads."""

    def __init__(self) -> None:
        self.rows = [
            Position("condition-a", "token-yes", Decimal("10"), Decimal("0.30")),
            Position("condition-b", "token-no", Decimal("2"), Decimal("0.60")),
        ]

    async def list_open_positions(self, user_id: str) -> list[Position]:
        """Return every open position for the follower."""

        assert user_id == "user-a"
        return list(self.rows)


class Reservations:
    """Expose the current active BUY quote reservation total."""

    async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        """Return deterministic reserved quote."""

        assert user_id == "user-a"
        return Decimal("3")


class BalanceReader:
    """Return the follower wallet collateral snapshot."""

    async def read(self, wallet: str) -> Decimal | None:
        """Return deterministic collateral."""

        assert wallet == "0xfollower"
        return Decimal("100")


class Coordinator:
    """Capture production execution intents."""

    def __init__(self, result: ExecutionCoordinatorResult) -> None:
        self.result = result
        self.intents: list[object] = []

    async def submit_copy_trade(self, intent: object) -> ExecutionCoordinatorResult:
        """Record and return the configured coordinator result."""

        self.intents.append(intent)
        return self.result


class WorkerRecorder:
    """Capture events accepted by the bounded leader-worker supervisor."""

    def __init__(self) -> None:
        self.events: list[tuple[str, CopyTaskSourceEvent]] = []

    async def submit_batch(
        self,
        leader: str,
        events: Iterable[CopyTaskSourceEvent],
    ) -> None:
        """Record one atomically accepted batch."""

        self.events.extend((leader, event) for event in events)


class ProcessorRecorder:
    """Capture deferred worker callbacks."""

    def __init__(self) -> None:
        self.events: list[tuple[str, CopyTaskSourceEvent]] = []

    async def __call__(self, leader: str, event: CopyTaskSourceEvent) -> None:
        """Record one callback."""

        self.events.append((leader, event))


def context(
    result: ExecutionCoordinatorResult | None = None,
    *,
    leader: str = _LEADER,
    trust_multiplier: Decimal = Decimal("1.1"),
) -> SimpleNamespace:
    """Build one production-shaped follower context."""

    coordinator = Coordinator(
        result
        or ExecutionCoordinatorResult(
            accepted=True,
            status="submitted_unconfirmed",
            order_id="order-a",
        )
    )
    return SimpleNamespace(
        user_id="user-a",
        wallet_address="0xfollower",
        leader_wallet=leader,
        positions_repo=Positions(),
        execution_coordinator=coordinator,
        rank_size_multiplier=trust_multiplier,
        extra={"reservations_repo": Reservations()},
    )


def task(*, counter_trade: bool = False, leader: str = _LEADER) -> CopyTask:
    """Build one follower task."""

    return CopyTask(
        task_id="task-a",
        name="task-a",
        leader_wallet=leader,
        follower_account="user-a",
        reaction=ReactionPolicy(counter_trade=counter_trade),
    )


def event(*, side: TradeSide = TradeSide.BUY) -> CopyTaskSourceEvent:
    """Normalize one representative source row."""

    raw: dict[str, object] = {
        "conditionId": "condition-a",
        "asset": "token-yes",
        "side": side.value,
        "size": "25",
        "price": "0.4",
        "usdcSize": "10",
        "timestamp": 1786459200000,
        "transactionHash": "0xabc",
        "outcome": "YES",
        "title": "Will the event happen?",
        "category": "politics",
        "_leader_sell_fraction": "0.25",
    }
    return LeaderEventNormalizer(clock=FixedClock()).normalize_raw(
        _LEADER,
        raw,
        mode=DetectionMode.POLLING,
    )


def command(*, side: str = "BUY", amount: str = "5") -> TaskExecutionCommand:
    """Build one accepted task execution command."""

    source = event(side=TradeSide(side))
    return TaskExecutionCommand(
        task=task(),
        source_event=source,
        side=side,
        condition_id="condition-a",
        token_id="token-yes",
        amount=Decimal(amount),
        notional_usdc=Decimal("5") if side == "BUY" else Decimal("1.6"),
        price_limit=Decimal("0.42") if side == "BUY" else Decimal("0.38"),
        order_type="FAK",
        signal_id="signal-a",
        task_budget_reservation_id="task-budget-a" if side == "BUY" else None,
    )


def test_normalizer_preserves_decimal_identity_and_sell_fraction() -> None:
    """Translate polling rows without float or timestamp precision loss."""

    normalized = event(side=TradeSide.SELL)

    assert normalized.trade.side is TradeSide.SELL
    assert normalized.trade.price == Decimal("0.4")
    assert normalized.trade.notional == Decimal("10")
    assert normalized.trade.mode is DetectionMode.POLLING
    assert normalized.trade.finality is DetectionFinality.CONFIRMED
    assert normalized.trade.venue_timestamp == datetime.fromtimestamp(
        1786459200,
        tz=UTC,
    )
    assert normalized.leader_sell_fraction == Decimal("0.25")
    assert normalized.outcome_label == "YES"


@pytest.mark.asyncio
async def test_ingress_normalizes_polling_batch_and_onchain_signal_once() -> None:
    """Feed both detection sources into the same bounded worker authority."""

    workers = WorkerRecorder()
    ingress = CopyTaskRuntimeIngress(
        workers=workers,
        normalizer=LeaderEventNormalizer(clock=FixedClock()),
    )
    polling = event().trade
    raw = {
        "conditionId": polling.condition_id,
        "asset": polling.token_id,
        "side": polling.side.value,
        "size": str(polling.shares),
        "price": str(polling.price),
        "usdcSize": str(polling.notional),
        "timestamp": 1786459200000,
        "transactionHash": "0xpoll",
    }
    onchain = SimpleNamespace(
        leader_wallet=_LEADER,
        provisional=True,
        as_raw_signal_dict=lambda: {**raw, "transactionHash": "0xonchain", "provisional": True},
    )

    accepted = await ingress.submit_raw_batch(_LEADER, [raw])
    await ingress.submit_onchain(onchain)

    assert accepted == 1
    assert [item[1].trade.mode for item in workers.events] == [
        DetectionMode.POLLING,
        DetectionMode.ON_CHAIN_PENDING,
    ]
    assert workers.events[1][1].trade.finality is DetectionFinality.PROVISIONAL


@pytest.mark.asyncio
async def test_deferred_handler_must_be_bound_before_workers_start() -> None:
    """Fail composition loudly instead of dropping early leader events."""

    handler = DeferredCopyTaskEventHandler()
    normalized = event()

    with pytest.raises(RuntimeError, match="not bound"):
        await handler(_LEADER, normalized)

    processor = ProcessorRecorder()
    handler.bind(processor)
    await handler(_LEADER, normalized)
    assert processor.events == [(_LEADER, normalized)]


@pytest.mark.asyncio
async def test_snapshot_provider_uses_persisted_positions_reservations_and_sell_ratio() -> None:
    """Construct sizing state from authoritative follower-owned sources."""

    provider = TradingContextTaskSnapshotProvider(
        contexts=[context()],
        balance_reader=BalanceReader(),
    )

    snapshot = await provider.snapshot_for(task(), event(side=TradeSide.SELL))

    assert snapshot.follower_pusd_balance == Decimal("100")
    assert snapshot.follower_reserved_notional == Decimal("3")
    assert snapshot.follower_market_exposure == Decimal("3.0")
    assert snapshot.current_open_positions == 2
    assert snapshot.is_existing_market is True
    assert snapshot.sell_shares == Decimal("2.50")
    assert snapshot.execution_reference_price == Decimal("0.4")
    assert snapshot.trust_multiplier == Decimal("1.1")


@pytest.mark.asyncio
async def test_snapshot_provider_fails_closed_for_unresolved_counter_outcome() -> None:
    """Never derive the opposite outcome token or price as a complement."""

    provider = TradingContextTaskSnapshotProvider(
        contexts=[context()],
        balance_reader=BalanceReader(),
    )

    with pytest.raises(TaskSnapshotUnavailable, match="counter outcome"):
        await provider.snapshot_for(task(counter_trade=True), event())


@pytest.mark.asyncio
async def test_execution_adapter_routes_buy_pusd_through_existing_coordinator() -> None:
    """Delegate BUYs to the single risk/reservation/execution authority."""

    follower = context()
    adapter = TradingContextTaskExecutionAdapter(contexts=[follower])

    outcome = await adapter.submit(command(side="BUY", amount="5"))

    intent = follower.execution_coordinator.intents[0]
    assert intent.size == Decimal("5")
    assert intent.notional_usdc == Decimal("5")
    assert intent.price == Decimal("0.42")
    assert intent.recovery_metadata["price_limit"] == "0.42"
    assert intent.recovery_metadata["task_budget_reservation_id"] == "task-budget-a"
    assert outcome.disposition is TaskExecutionDisposition.SUBMITTED_UNCONFIRMED


@pytest.mark.asyncio
async def test_execution_adapter_routes_sell_shares_and_classifies_pre_post_rejection() -> None:
    """Keep SELL shares exact and release only proven pre-POST failures."""

    follower = context(
        ExecutionCoordinatorResult(
            accepted=False,
            status="reservation_failed",
            skip_code="reservation_insufficient",
        )
    )
    adapter = TradingContextTaskExecutionAdapter(contexts=[follower])

    outcome = await adapter.submit(command(side="SELL", amount="4"))

    intent = follower.execution_coordinator.intents[0]
    assert intent.size == Decimal("4")
    assert intent.notional_usdc == Decimal("1.6")
    assert outcome.disposition is TaskExecutionDisposition.PRE_POST_REJECTED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    (
        "kill_switch",
        "trading_paused",
        "book_thin_depth",
        "size_below_min",
        "atomic_lock_unavailable",
        "unknown",
    ),
)
async def test_execution_adapter_releases_every_proven_unaccepted_result(
    status: str,
) -> None:
    """A coordinator rejection without ambiguity never retains task budget."""

    follower = context(
        ExecutionCoordinatorResult(
            accepted=False,
            status=status,
            lifecycle_status="failed",
        )
    )
    adapter = TradingContextTaskExecutionAdapter(contexts=[follower])

    outcome = await adapter.submit(command())

    assert outcome.disposition is TaskExecutionDisposition.PRE_POST_REJECTED


@pytest.mark.asyncio
async def test_dynamic_context_registration_keeps_one_canonical_follower_context() -> None:
    """Ignore duplicate per-leader contexts for one follower account."""

    canonical = context()
    duplicate = context()
    provider = TradingContextTaskSnapshotProvider(
        contexts=[canonical],
        balance_reader=BalanceReader(),
    )
    adapter = TradingContextTaskExecutionAdapter(contexts=[canonical])

    provider.add_contexts([duplicate])
    adapter.add_contexts([duplicate])

    snapshot = await provider.snapshot_for(task(), event())
    outcome = await adapter.submit(command())

    assert snapshot.follower_pusd_balance == Decimal("100")
    assert outcome.disposition is TaskExecutionDisposition.SUBMITTED_UNCONFIRMED
    assert len(canonical.execution_coordinator.intents) == 1
    assert duplicate.execution_coordinator.intents == []


@pytest.mark.asyncio
async def test_snapshot_uses_task_leader_specific_trust_context() -> None:
    """Keep leader-owned sizing evidence separate for a multi-leader follower."""

    second_leader = "0x2222222222222222222222222222222222222222"
    provider = TradingContextTaskSnapshotProvider(
        contexts=[
            context(trust_multiplier=Decimal("0.8")),
            context(leader=second_leader, trust_multiplier=Decimal("1.25")),
        ],
        balance_reader=BalanceReader(),
    )

    first = await provider.snapshot_for(task(), event())
    second = await provider.snapshot_for(task(leader=second_leader), event())

    assert first.trust_multiplier == Decimal("0.8")
    assert second.trust_multiplier == Decimal("1.25")


def test_task_binding_requires_exact_leader_evidence_and_account_execution() -> None:
    """Expose a side-effect-free activation check for control-plane resume."""

    follower = context()
    provider = TradingContextTaskSnapshotProvider(
        contexts=[follower],
        balance_reader=BalanceReader(),
    )
    adapter = TradingContextTaskExecutionAdapter(contexts=[follower])
    unrelated = task(leader="0x2222222222222222222222222222222222222222")

    assert provider.has_task_binding(task()) is True
    assert adapter.has_task_binding(task()) is True
    assert provider.has_task_binding(unrelated) is False
    assert adapter.has_task_binding(unrelated) is True
