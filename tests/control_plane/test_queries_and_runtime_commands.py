"""Typed control-plane query and operational command contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from services.control_plane.commands import (
    DiscoveryCommandService,
    ReentryLockCommandService,
    RuntimeCommandService,
)
from services.control_plane.contracts import (
    ActivityView,
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryCycleView,
    DiscoveryRunCommand,
    DiscoveryRunReceipt,
    EntryControlState,
    LockResetCommand,
    LockResetOutcome,
    LockResetResult,
    OrderView,
    Page,
    PageRequest,
    PortfolioView,
    PositionView,
    ReentryLockView,
    ReservationView,
    RuntimeControlCommand,
    RuntimeView,
    SystemView,
    TaskView,
)
from services.control_plane.queries import (
    ActivityQueryService,
    DiscoveryQueryService,
    OrderQueryService,
    PortfolioQueryService,
    PositionQueryService,
    ReentryLockQueryService,
    ReservationQueryService,
    RuntimeQueryService,
    SystemQueryService,
    TaskQueryService,
)


NOW = datetime(2026, 8, 11, 14, 0, tzinfo=UTC)


class FixedClock:
    """Return a deterministic UTC time."""

    def now(self) -> datetime:
        """Return the fixed value."""

        return NOW


class RecordingAuditSink:
    """Capture command audit records."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def append(self, event: AuditEvent) -> None:
        """Append one event."""

        self.events.append(event)


class QueryPort:
    """One fake implementing every narrow query port for delegation checks."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.page = Page(items=(), next_cursor=None)
        control = EntryControlState(
            entries_paused=True,
            full_stop=False,
            revision=2,
            reason_code="operator_pause",
            updated_at=NOW,
            updated_by="operator-a",
        )
        self.system = SystemView(
            service="pmcopy",
            version="test",
            environment="paper",
            observed_at=NOW,
        )
        self.runtime = RuntimeView(
            entry_control=control,
            worker_count=1,
            queue_depth=0,
            observed_at=NOW,
        )
        self.portfolio: PortfolioView | None = PortfolioView(
            follower_account="account-a",
            cash_usdc="12.50",
            reserved_usdc="2.50",
            positions_value_usdc="30.00",
            equity_usdc="42.50",
            observed_at=NOW,
            stale=False,
        )

    async def get_system(self) -> SystemView:
        """Return system state."""

        self.calls.append(("system", None))
        return self.system

    async def list_tasks(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        """Return tasks."""

        self.calls.append(("tasks", (follower_account, page)))
        return self.page

    async def list_positions(self, follower_account: str, page: PageRequest) -> Page[PositionView]:
        """Return positions."""

        self.calls.append(("positions", (follower_account, page)))
        return self.page

    async def list_orders(self, follower_account: str, page: PageRequest) -> Page[OrderView]:
        """Return orders."""

        self.calls.append(("orders", (follower_account, page)))
        return self.page

    async def list_activity(
        self, follower_account: str | None, page: PageRequest
    ) -> Page[ActivityView]:
        """Return audit activity."""

        self.calls.append(("activity", (follower_account, page)))
        return self.page

    async def list_discovery_cycles(self, page: PageRequest) -> Page[DiscoveryCycleView]:
        """Return discovery cycles."""

        self.calls.append(("discovery", page))
        return self.page

    async def list_reservations(
        self, follower_account: str, page: PageRequest
    ) -> Page[ReservationView]:
        """Return reservations."""

        self.calls.append(("reservations", (follower_account, page)))
        return self.page

    async def list_reentry_locks(
        self, follower_account: str, page: PageRequest
    ) -> Page[ReentryLockView]:
        """Return locks."""

        self.calls.append(("locks", (follower_account, page)))
        return self.page

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return only locally cached portfolio state."""

        self.calls.append(("portfolio", follower_account))
        return self.portfolio

    async def get_runtime(self) -> RuntimeView:
        """Return runtime state."""

        self.calls.append(("runtime", None))
        return self.runtime


class DiscoveryPort:
    """Fake asynchronous discovery queue."""

    def __init__(self) -> None:
        self.commands: list[DiscoveryRunCommand] = []

    async def enqueue(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Accept work without running provider I/O inline."""

        self.commands.append(command)
        return DiscoveryRunReceipt(run_id="run-a", accepted=True, reason_code="accepted")


class EntryControlPort:
    """In-memory CAS state used by the runtime command service."""

    def __init__(self) -> None:
        self.state = EntryControlState(
            entries_paused=False,
            full_stop=False,
            revision=1,
            reason_code="running",
            updated_at=NOW,
            updated_by="bootstrap",
        )
        self.calls: list[tuple[int, bool, bool, str, str]] = []

    async def get(self) -> EntryControlState:
        """Return current state."""

        return self.state

    async def compare_and_set(
        self,
        *,
        expected_revision: int,
        entries_paused: bool,
        full_stop: bool,
        actor: str,
        reason_code: str,
    ) -> EntryControlState | None:
        """Apply only a matching revision."""

        self.calls.append((expected_revision, entries_paused, full_stop, actor, reason_code))
        if expected_revision != self.state.revision:
            return None
        self.state = EntryControlState(
            entries_paused=entries_paused,
            full_stop=full_stop,
            revision=expected_revision + 1,
            reason_code=reason_code,
            updated_at=NOW,
            updated_by=actor,
        )
        return self.state


class RuntimeSignal:
    """Capture application of durable entry control to the runtime."""

    def __init__(self) -> None:
        self.states: list[EntryControlState] = []

    async def apply(self, state: EntryControlState) -> None:
        """Apply one state."""

        self.states.append(state)


class LockResetPort:
    """Return a scripted CAS reset outcome."""

    def __init__(self, outcome: LockResetOutcome) -> None:
        self.outcome = outcome
        self.commands: list[LockResetCommand] = []

    async def reset_if_current(self, command: LockResetCommand) -> LockResetResult:
        """Return one stable outcome."""

        self.commands.append(command)
        return LockResetResult(outcome=self.outcome, released_at=NOW)


@pytest.mark.asyncio
async def test_all_query_services_delegate_through_bounded_typed_pages() -> None:
    """Each application query uses its own narrow injected port."""

    port = QueryPort()
    page = PageRequest(limit=25, cursor="opaque")

    assert await SystemQueryService(port).get() is port.system
    assert (await TaskQueryService(port).list("account-a", page)).items == ()
    assert (await PositionQueryService(port).list("account-a", page)).items == ()
    assert (await OrderQueryService(port).list("account-a", page)).items == ()
    assert (await ActivityQueryService(port).list("account-a", page)).items == ()
    assert (await DiscoveryQueryService(port).list(page)).items == ()
    assert (await ReservationQueryService(port).list("account-a", page)).items == ()
    assert (await ReentryLockQueryService(port).list("account-a", page)).items == ()
    assert await PortfolioQueryService(port).get("account-a") is port.portfolio
    assert await RuntimeQueryService(port).get() is port.runtime
    assert [name for name, _ in port.calls] == [
        "system",
        "tasks",
        "positions",
        "orders",
        "activity",
        "discovery",
        "reservations",
        "locks",
        "portfolio",
        "runtime",
    ]


def test_pagination_and_decimal_boundary_contracts_fail_closed() -> None:
    """Limits are bounded and financial values are strings at the boundary."""

    with pytest.raises(ControlPlaneError) as invalid_limit:
        PageRequest(limit=101)
    with pytest.raises(ControlPlaneError) as invalid_decimal:
        PositionView(
            follower_account="account-a",
            condition_id="condition-a",
            token_id="token-a",
            leader_wallet=None,
            size=Decimal("1.25"),  # type: ignore[arg-type]
            avg_entry_price="0.50",
            updated_at=NOW,
        )

    assert invalid_limit.value.reason is ControlPlaneReason.INVALID_PAGE
    assert invalid_decimal.value.reason is ControlPlaneReason.INVALID_DECIMAL_STRING

    request = PageRequest(limit=100, cursor="cursor-a")
    with pytest.raises(FrozenInstanceError):
        request.limit = 1  # type: ignore[misc]


@pytest.mark.asyncio
async def test_cached_portfolio_missing_or_stale_uses_stable_reasons() -> None:
    """Portfolio queries never fall through to synchronous venue I/O."""

    port = QueryPort()
    port.portfolio = None
    with pytest.raises(ControlPlaneError) as missing:
        await PortfolioQueryService(port).get("account-a")
    port.portfolio = PortfolioView(
        follower_account="account-a",
        cash_usdc="1",
        reserved_usdc="0",
        positions_value_usdc="0",
        equity_usdc="1",
        observed_at=NOW,
        stale=True,
    )
    with pytest.raises(ControlPlaneError) as stale:
        await PortfolioQueryService(port).get("account-a")

    assert missing.value.reason is ControlPlaneReason.PORTFOLIO_UNAVAILABLE
    assert stale.value.reason is ControlPlaneReason.PORTFOLIO_STALE


class DetailPort:
    """Return optional follower-scoped position and order detail records."""

    def __init__(self) -> None:
        self.position: PositionView | None = PositionView(
            follower_account="account-a",
            condition_id="condition-a",
            token_id="token-a",
            leader_wallet=None,
            size="1",
            avg_entry_price="0.5",
            updated_at=NOW,
        )
        self.order: OrderView | None = OrderView(
            idempotency_key="intent-a",
            follower_account="account-a",
            leader_wallet=None,
            order_id=None,
            status="submitted_unconfirmed",
            side="BUY",
            amount="5",
            price_limit="0.5",
            matched_total="0",
            created_at=NOW,
            updated_at=NOW,
        )

    async def get_position(
        self,
        follower_account: str,
        position_id: str,
    ) -> PositionView | None:
        """Return the configured position for its exact scope."""

        assert (follower_account, position_id) == ("account-a", "token-a")
        return self.position

    async def get_order(
        self,
        follower_account: str,
        order_id: str,
    ) -> OrderView | None:
        """Return the configured order for its exact scope."""

        assert (follower_account, order_id) == ("account-a", "intent-a")
        return self.order


@pytest.mark.asyncio
async def test_position_and_order_detail_services_return_stable_not_found() -> None:
    """Missing scoped resources map to the common public not-found reason."""

    port = DetailPort()
    positions = PositionQueryService(port)
    orders = OrderQueryService(port)

    assert await positions.get("account-a", "token-a") is port.position
    assert await orders.get("account-a", "intent-a") is port.order
    port.position = None
    port.order = None
    with pytest.raises(ControlPlaneError) as missing_position:
        await positions.get("account-a", "token-a")
    with pytest.raises(ControlPlaneError) as missing_order:
        await orders.get("account-a", "intent-a")

    assert missing_position.value.reason is ControlPlaneReason.NOT_FOUND
    assert missing_order.value.reason is ControlPlaneReason.NOT_FOUND


@pytest.mark.asyncio
async def test_discovery_command_is_queued_not_executed_inline() -> None:
    """The application layer exposes a job abstraction, not provider calls."""

    port = DiscoveryPort()
    audit = RecordingAuditSink()
    service = DiscoveryCommandService(port=port, audit_sink=audit, clock=FixedClock())
    command = DiscoveryRunCommand(actor="operator-a", force=False)

    receipt = await service.request(command)

    assert receipt.accepted is True
    assert port.commands == [command]
    assert audit.events[-1].target_id == "run-a"


@pytest.mark.asyncio
async def test_runtime_pause_and_full_stop_are_durable_cas_operations() -> None:
    """Full stop implies entry pause and stale commands cannot signal runtime."""

    port = EntryControlPort()
    signal = RuntimeSignal()
    audit = RecordingAuditSink()
    service = RuntimeCommandService(
        entry_control=port,
        runtime_signal=signal,
        audit_sink=audit,
        clock=FixedClock(),
    )
    paused = await service.pause_entries(
        RuntimeControlCommand(
            expected_revision=1,
            actor="operator-a",
            reason_code="operator_pause",
        )
    )
    stopped = await service.full_stop(
        RuntimeControlCommand(
            expected_revision=paused.revision,
            actor="operator-a",
            reason_code="incident",
        )
    )

    assert paused.entries_paused is True and paused.full_stop is False
    assert stopped.entries_paused is True and stopped.full_stop is True
    assert signal.states == [paused, stopped]
    with pytest.raises(ControlPlaneError) as stale:
        await service.pause_entries(
            RuntimeControlCommand(
                expected_revision=1,
                actor="operator-a",
                reason_code="stale",
            )
        )
    assert stale.value.reason is ControlPlaneReason.REVISION_MISMATCH
    assert signal.states == [paused, stopped]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["pause_entries", "resume_entries"])
async def test_full_stop_cannot_be_cleared_by_entry_only_commands(operation: str) -> None:
    """A full stop remains fail-closed until a separate restart/recovery path."""

    port = EntryControlPort()
    port.state = EntryControlState(
        entries_paused=True,
        full_stop=True,
        revision=5,
        reason_code="incident",
        updated_at=NOW,
        updated_by="operator-a",
    )
    service = RuntimeCommandService(
        entry_control=port,
        runtime_signal=RuntimeSignal(),
        audit_sink=RecordingAuditSink(),
        clock=FixedClock(),
    )

    with pytest.raises(ControlPlaneError) as captured:
        await getattr(service, operation)(
            RuntimeControlCommand(
                expected_revision=5,
                actor="operator-a",
                reason_code="resume",
            )
        )

    assert captured.value.reason is ControlPlaneReason.FULL_STOP_ACTIVE
    assert port.calls == []


@pytest.mark.asyncio
async def test_reentry_lock_reset_maps_cas_outcome_to_stable_error() -> None:
    """A stale reset does not silently delete a newer protective lock."""

    command = LockResetCommand(
        follower_account="account-a",
        task_id="task-a",
        leader_wallet="0x1111111111111111111111111111111111111111",
        market_id="condition-a",
        expected_revision=2,
        actor="operator-a",
        reason_code="manual_review",
    )
    stale_port = LockResetPort(LockResetOutcome.REVISION_MISMATCH)
    service = ReentryLockCommandService(
        port=stale_port,
        audit_sink=RecordingAuditSink(),
        clock=FixedClock(),
    )

    with pytest.raises(ControlPlaneError) as captured:
        await service.reset(command)

    assert captured.value.reason is ControlPlaneReason.REVISION_MISMATCH
    assert stale_port.commands == [command]

    reset_service = ReentryLockCommandService(
        port=LockResetPort(LockResetOutcome.RESET),
        audit_sink=RecordingAuditSink(),
        clock=FixedClock(),
    )
    result = await reset_service.reset(command)
    assert result.outcome is LockResetOutcome.RESET
