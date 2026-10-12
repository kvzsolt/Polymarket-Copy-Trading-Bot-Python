"""Deterministic contracts for live execution safety and reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from pmcopy.application.execution_safety import (
    ExecutionBlockedError,
    ExecutionSafetyIntent,
    ExecutionSide,
    KillSwitch,
    OrderExecutionService,
    ReconciliationReadiness,
    RiskGate,
    RiskLimits,
    RiskSnapshot,
    TradingMode,
)
from pmcopy.application.reconciliation_service import (
    ReconciliationObservation,
    ReconciliationService,
)


class FakeClock:
    """Return a controllable monotonic timestamp."""

    def __init__(self, now: Decimal = Decimal("0")) -> None:
        self.value = now

    def monotonic(self) -> Decimal:
        """Return the current fake time."""

        return self.value


class FakeExecutor:
    """Record submissions without venue I/O."""

    def __init__(self) -> None:
        self.intents: list[ExecutionSafetyIntent] = []

    async def submit(self, intent: ExecutionSafetyIntent) -> str:
        """Record and acknowledge one intent."""

        self.intents.append(intent)
        return "order-1"


class FakeReconciliationPort:
    """Return deterministic local/exchange convergence evidence."""

    def __init__(self, *observations: ReconciliationObservation) -> None:
        self._observations = list(observations)
        self.calls = 0

    async def reconcile(self) -> ReconciliationObservation:
        """Return the next configured observation."""

        self.calls += 1
        return self._observations.pop(0)


@dataclass(frozen=True)
class Harness:
    """Hold one fully injected safety boundary."""

    service: OrderExecutionService[str]
    executor: FakeExecutor
    readiness: ReconciliationReadiness
    risk_gate: RiskGate
    kill_switch: KillSwitch


def intent(notional: str = "10") -> ExecutionSafetyIntent:
    """Build one valid BUY intent using exact decimals."""

    return ExecutionSafetyIntent(
        user_id="follower-1",
        market_id="condition-1",
        token_id="token-1",
        side=ExecutionSide.BUY,
        quantity=Decimal(notional),
        price=Decimal("1"),
        notional=Decimal(notional),
    )


def harness(*, mode: TradingMode = TradingMode.LIVE) -> Harness:
    """Build an execution boundary with no external dependencies."""

    executor = FakeExecutor()
    readiness = ReconciliationReadiness()
    risk_gate = RiskGate(
        RiskLimits(
            max_total_notional=Decimal("100"),
            max_market_notional=Decimal("50"),
            max_daily_loss=Decimal("25"),
        )
    )
    kill_switch = KillSwitch()
    return Harness(
        service=OrderExecutionService(
            mode=mode,
            kill_switch=kill_switch,
            risk_gate=risk_gate,
            reconciliation_readiness=readiness,
            executor=executor,
        ),
        executor=executor,
        readiness=readiness,
        risk_gate=risk_gate,
        kill_switch=kill_switch,
    )


@pytest.mark.asyncio
async def test_startup_reconciliation_must_succeed_before_live_submission() -> None:
    """Live submission remains closed until startup state converges."""

    built = harness()

    with pytest.raises(ExecutionBlockedError, match="startup reconciliation"):
        await built.service.execute(intent())

    built.risk_gate.replace_snapshot(
        RiskSnapshot(
            total_notional=Decimal("0"),
            daily_pnl=Decimal("0"),
            positions={},
            observed_at=Decimal("1"),
        )
    )
    built.readiness.mark_healthy(observed_at=Decimal("1"))

    assert await built.service.execute(intent()) == "order-1"


@pytest.mark.asyncio
async def test_periodic_reconciliation_updates_hot_risk_state_without_order_io() -> None:
    """Reconciliation replaces the snapshot before subsequent hot checks."""

    clock = FakeClock(Decimal("20"))
    built = harness()
    provider = FakeReconciliationPort(
        ReconciliationObservation(
            exposure_known=True,
            total_notional=Decimal("30"),
            daily_pnl=Decimal("-2"),
            positions={"condition-1": Decimal("12")},
        )
    )
    service = ReconciliationService(
        provider=provider,
        risk_gate=built.risk_gate,
        readiness=built.readiness,
        clock=clock,
    )

    await service.run_periodic_once()

    assert provider.calls == 1
    assert built.risk_gate.snapshot.total_notional == Decimal("30")
    assert built.readiness.snapshot.ready is True
    assert built.executor.intents == []


@pytest.mark.asyncio
async def test_exposure_limit_rejection_performs_no_submission() -> None:
    """A projected market exposure over the limit fails before the executor."""

    built = harness()
    built.risk_gate.replace_snapshot(
        RiskSnapshot(
            total_notional=Decimal("90"),
            daily_pnl=Decimal("0"),
            positions={"condition-1": Decimal("45")},
            observed_at=Decimal("1"),
        )
    )
    built.readiness.mark_healthy(observed_at=Decimal("1"))

    with pytest.raises(ExecutionBlockedError, match="total exposure"):
        await built.service.execute(intent("11"))

    assert built.executor.intents == []


@pytest.mark.asyncio
async def test_paper_mode_never_submits_to_live_executor() -> None:
    """Paper mode returns a paper result even when credentials could exist."""

    built = harness(mode=TradingMode.PAPER)

    result = await built.service.execute(intent())

    assert result.paper is True
    assert result.intent == intent()
    assert built.executor.intents == []


@pytest.mark.asyncio
async def test_global_halt_blocks_live_submission_in_memory() -> None:
    """The global halt is an inexpensive pre-submit state check."""

    built = harness()
    built.risk_gate.replace_snapshot(
        RiskSnapshot(
            total_notional=Decimal("0"),
            daily_pnl=Decimal("0"),
            positions={},
            observed_at=Decimal("1"),
        )
    )
    built.readiness.mark_healthy(observed_at=Decimal("1"))
    built.kill_switch.halt(reason="operator")

    with pytest.raises(ExecutionBlockedError, match="operator"):
        await built.service.execute(intent())

    assert built.executor.intents == []


@pytest.mark.asyncio
async def test_unknown_exposure_from_reconciliation_fails_closed() -> None:
    """A severe mismatch invalidates readiness and blocks live execution."""

    clock = FakeClock(Decimal("30"))
    built = harness()
    provider = FakeReconciliationPort(
        ReconciliationObservation(
            exposure_known=False,
            total_notional=Decimal("0"),
            daily_pnl=Decimal("0"),
            positions={},
            error_code="position_mismatch",
        )
    )
    service = ReconciliationService(
        provider=provider,
        risk_gate=built.risk_gate,
        readiness=built.readiness,
        clock=clock,
    )

    await service.run_startup()

    with pytest.raises(ExecutionBlockedError, match="position_mismatch"):
        await built.service.execute(intent())
    assert built.executor.intents == []
