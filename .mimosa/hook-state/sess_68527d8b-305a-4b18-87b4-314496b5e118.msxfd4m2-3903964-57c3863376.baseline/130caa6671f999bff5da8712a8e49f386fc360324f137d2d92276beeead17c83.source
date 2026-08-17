"""In-memory execution preconditions for the latency-sensitive order path.

External reconciliation updates immutable snapshots outside this module. The
checks here never perform network, database, clock, or filesystem I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Generic, Mapping, Protocol, TypeVar


class TradingMode(StrEnum):
    """Explicit trading modes; credentials never select live execution."""

    PAPER = "paper"
    LIVE = "live"


class ExecutionSide(StrEnum):
    """Supported order directions for safety evaluation."""

    BUY = "BUY"
    SELL = "SELL"


class ExecutionBlockedError(RuntimeError):
    """Raised when an in-memory execution precondition fails closed."""

    def __init__(self, code: str, reason: str) -> None:
        """Initialize a structured execution refusal.

        Args:
            code: Stable machine-readable refusal code.
            reason: Redacted operator-facing explanation.
        """

        super().__init__(reason)
        self.code = code
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ExecutionSafetyIntent:
    """Immutable monetary projection checked immediately before submission."""

    user_id: str
    market_id: str
    token_id: str
    side: ExecutionSide
    quantity: Decimal
    price: Decimal
    notional: Decimal

    def __post_init__(self) -> None:
        """Validate exact, positive execution values at the boundary.

        Raises:
            ValueError: If identity or monetary values are invalid.
            TypeError: If monetary fields are not ``Decimal`` values.
        """

        if not self.user_id or not self.market_id or not self.token_id:
            raise ValueError("execution safety intent identities are required")
        if not isinstance(self.side, ExecutionSide):
            raise TypeError("side must be an ExecutionSide")
        for name, value in (
            ("quantity", self.quantity),
            ("price", self.price),
            ("notional", self.notional),
        ):
            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal")
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be finite and positive")


@dataclass(frozen=True, slots=True)
class PaperExecutionResult:
    """Describe a paper-only decision that performed no venue submission."""

    intent: ExecutionSafetyIntent
    paper: bool = True


@dataclass(frozen=True, slots=True)
class KillSwitchSnapshot:
    """Current immutable global-halt state."""

    halted: bool
    reason: str
    generation: int


class KillSwitch:
    """Own the process-local global halt state behind one mutation boundary."""

    def __init__(self) -> None:
        """Initialize the switch in its unhalted state."""

        self._snapshot = KillSwitchSnapshot(halted=False, reason="", generation=0)

    @property
    def snapshot(self) -> KillSwitchSnapshot:
        """Return the current immutable halt snapshot."""

        return self._snapshot

    def halt(self, *, reason: str) -> KillSwitchSnapshot:
        """Halt all live submissions for a redacted reason.

        Args:
            reason: Non-secret operator or reconciliation reason code.

        Returns:
            The newly installed halt snapshot.

        Raises:
            ValueError: If the reason is empty.
        """

        normalized = reason.strip()
        if not normalized:
            raise ValueError("kill-switch reason is required")
        self._snapshot = KillSwitchSnapshot(
            halted=True,
            reason=normalized,
            generation=self._snapshot.generation + 1,
        )
        return self._snapshot

    def resume(self, *, reason: str) -> KillSwitchSnapshot:
        """Resume live submissions after an explicit operator decision.

        Args:
            reason: Non-secret audit reason for resuming.

        Returns:
            The newly installed unhalted snapshot.

        Raises:
            ValueError: If the reason is empty.
        """

        normalized = reason.strip()
        if not normalized:
            raise ValueError("kill-switch resume reason is required")
        self._snapshot = KillSwitchSnapshot(
            halted=False,
            reason=normalized,
            generation=self._snapshot.generation + 1,
        )
        return self._snapshot

    def ensure_running(self) -> None:
        """Raise when the global halt is active.

        Raises:
            ExecutionBlockedError: If submissions are globally halted.
        """

        snapshot = self._snapshot
        if snapshot.halted:
            raise ExecutionBlockedError("global_halt", f"global halt active: {snapshot.reason}")


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Exact process-local exposure and loss limits."""

    max_total_notional: Decimal
    max_market_notional: Decimal
    max_daily_loss: Decimal

    def __post_init__(self) -> None:
        """Validate non-negative finite limits.

        Raises:
            TypeError: If a limit is not a ``Decimal``.
            ValueError: If a limit is negative or non-finite.
        """

        for name, value in (
            ("max_total_notional", self.max_total_notional),
            ("max_market_notional", self.max_market_notional),
            ("max_daily_loss", self.max_daily_loss),
        ):
            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal")
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class RiskSnapshot:
    """Current locally reconciled trading risk state."""

    total_notional: Decimal
    daily_pnl: Decimal
    positions: Mapping[str, Decimal]
    observed_at: Decimal

    def __post_init__(self) -> None:
        """Freeze and validate the snapshot's exact values.

        Raises:
            TypeError: If a monetary or time value is not a ``Decimal``.
            ValueError: If a value is non-finite or exposure is negative.
        """

        for name, value in (
            ("total_notional", self.total_notional),
            ("daily_pnl", self.daily_pnl),
            ("observed_at", self.observed_at),
        ):
            if not isinstance(value, Decimal):
                raise TypeError(f"{name} must be a Decimal")
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
        if self.total_notional < 0 or self.observed_at < 0:
            raise ValueError("risk exposure and observation time cannot be negative")
        frozen_positions: dict[str, Decimal] = {}
        for market_id, exposure in self.positions.items():
            if not market_id:
                raise ValueError("risk position market identity is required")
            if not isinstance(exposure, Decimal):
                raise TypeError("risk position exposure must be a Decimal")
            if not exposure.is_finite() or exposure < 0:
                raise ValueError("risk position exposure must be finite and non-negative")
            frozen_positions[str(market_id)] = exposure
        object.__setattr__(self, "positions", MappingProxyType(frozen_positions))


class RiskGate:
    """Check an atomically replaced in-memory risk snapshot."""

    def __init__(self, limits: RiskLimits) -> None:
        """Initialize a closed gate with exact limits.

        Args:
            limits: Configured exposure and loss ceilings.
        """

        self._limits = limits
        self._snapshot: RiskSnapshot | None = None

    @property
    def snapshot(self) -> RiskSnapshot:
        """Return the current snapshot.

        Raises:
            ExecutionBlockedError: If reconciliation has not supplied state.
        """

        if self._snapshot is None:
            raise ExecutionBlockedError("risk_unknown", "risk snapshot is unavailable")
        return self._snapshot

    def replace_snapshot(self, snapshot: RiskSnapshot) -> None:
        """Atomically replace the current immutable snapshot.

        Args:
            snapshot: Newly reconciled risk state.
        """

        self._snapshot = snapshot

    def invalidate(self) -> None:
        """Fail closed after reconciliation makes exposure unknown."""

        self._snapshot = None

    def check(self, intent: ExecutionSafetyIntent) -> None:
        """Validate projected exposure using only local immutable state.

        Args:
            intent: Exact order projection to evaluate.

        Raises:
            ExecutionBlockedError: If current or projected risk is unsafe.
        """

        snapshot = self.snapshot
        if snapshot.daily_pnl < -self._limits.max_daily_loss:
            raise ExecutionBlockedError("daily_loss_limit", "daily loss limit exceeded")
        if intent.side is ExecutionSide.SELL:
            return
        projected_total = snapshot.total_notional + intent.notional
        if projected_total > self._limits.max_total_notional:
            raise ExecutionBlockedError("total_exposure_limit", "total exposure limit exceeded")
        current_market = snapshot.positions.get(intent.market_id, Decimal("0"))
        if current_market + intent.notional > self._limits.max_market_notional:
            raise ExecutionBlockedError("market_exposure_limit", "market exposure limit exceeded")


@dataclass(frozen=True, slots=True)
class ReconciliationReadinessSnapshot:
    """Immutable startup and periodic reconciliation readiness state."""

    ready: bool
    observed_at: Decimal | None
    error_code: str | None


class ReconciliationReadiness:
    """Own the fail-closed readiness flag updated by reconciliation."""

    def __init__(self) -> None:
        """Initialize live execution as disabled."""

        self._snapshot = ReconciliationReadinessSnapshot(
            ready=False,
            observed_at=None,
            error_code="startup_reconciliation_required",
        )

    @property
    def snapshot(self) -> ReconciliationReadinessSnapshot:
        """Return the current immutable readiness state."""

        return self._snapshot

    def mark_healthy(self, *, observed_at: Decimal) -> None:
        """Enable execution after a successful reconciliation.

        Args:
            observed_at: Injected monotonic observation timestamp.
        """

        self._snapshot = ReconciliationReadinessSnapshot(
            ready=True,
            observed_at=observed_at,
            error_code=None,
        )

    def mark_unhealthy(self, *, error_code: str) -> None:
        """Disable execution after a reconciliation failure.

        Args:
            error_code: Stable redacted failure code.
        """

        normalized = error_code.strip() or "reconciliation_failed"
        self._snapshot = ReconciliationReadinessSnapshot(
            ready=False,
            observed_at=self._snapshot.observed_at,
            error_code=normalized,
        )

    def ensure_ready(self) -> None:
        """Raise until startup and subsequent reconciliation are healthy.

        Raises:
            ExecutionBlockedError: If live state is not known to be reconciled.
        """

        snapshot = self._snapshot
        if snapshot.ready:
            return
        code = snapshot.error_code or "reconciliation_failed"
        reason = (
            "startup reconciliation is required"
            if code == "startup_reconciliation_required"
            else f"reconciliation blocked execution: {code}"
        )
        raise ExecutionBlockedError(code, reason)


ResultT = TypeVar("ResultT", covariant=True)


class LiveExecutionPort(Protocol[ResultT]):
    """Submit an already-authorized live intent."""

    async def submit(self, intent: ExecutionSafetyIntent) -> ResultT:
        """Submit one intent and return adapter-normalized evidence."""

        ...


class OrderExecutionService(Generic[ResultT]):
    """Enforce mode, halt, reconciliation, and risk before live submission."""

    def __init__(
        self,
        *,
        mode: TradingMode,
        kill_switch: KillSwitch,
        risk_gate: RiskGate,
        reconciliation_readiness: ReconciliationReadiness,
        executor: LiveExecutionPort[ResultT],
    ) -> None:
        """Initialize the execution boundary from narrow injected services.

        Args:
            mode: Explicit paper or live mode.
            kill_switch: Process-local global halt state.
            risk_gate: In-memory exact exposure gate.
            reconciliation_readiness: Startup/periodic convergence state.
            executor: Live venue submission port.
        """

        self._mode = mode
        self._kill_switch = kill_switch
        self._risk_gate = risk_gate
        self._readiness = reconciliation_readiness
        self._executor = executor

    async def execute(self, intent: ExecutionSafetyIntent) -> ResultT | PaperExecutionResult:
        """Return a paper decision or submit one authorized live intent.

        Args:
            intent: Exact order projection to check and submit.

        Returns:
            Paper evidence or the injected live executor result.

        Raises:
            ExecutionBlockedError: If a live precondition fails.
        """

        if self._mode is TradingMode.PAPER:
            return PaperExecutionResult(intent=intent)
        self._kill_switch.ensure_running()
        self._readiness.ensure_ready()
        self._risk_gate.check(intent)
        return await self._executor.submit(intent)


__all__ = [
    "ExecutionBlockedError",
    "ExecutionSafetyIntent",
    "ExecutionSide",
    "KillSwitch",
    "KillSwitchSnapshot",
    "LiveExecutionPort",
    "OrderExecutionService",
    "PaperExecutionResult",
    "ReconciliationReadiness",
    "ReconciliationReadinessSnapshot",
    "RiskGate",
    "RiskLimits",
    "RiskSnapshot",
    "TradingMode",
]
