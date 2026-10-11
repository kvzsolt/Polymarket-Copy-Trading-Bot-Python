"""Finland-local native-owner source lifecycle, independent of web and Telegram."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from pathlib import Path
from typing import Protocol, cast

from adapters.clock import SystemReceiptClock
from adapters.polygon.block_timestamp import RpcBlockTimestampReader
from clients.network.grouped_websocket import GroupedWebSocketSession, ProviderWebSocketConnector
from config.native_detection import NativeDetectionConfig
from domain.grouped_detection import TraderDetectionPolicy, TraderDetectionState
from domain.provider_budget import BudgetExhausted
from domain.source_time import TransportReceipt
from interfaces.grouped_detection import DetectionStateStore, RawObservationSink
from orchestrators.provider_runtime import ProviderRuntime
from services.grouped_detection import DetectionSubscription, GroupedDetectionPlanner
from services.onchain_detection.bounded_recovery import BoundedLogRecovery
from services.onchain_detection.condition_id_resolver import GammaConditionIdResolver
from services.onchain_detection.native_observer import NativeOwnerObserver
from services.onchain_detection.trade_detection_models import OnchainTradeSignal
from storage.detection_state import SqliteDetectionState
from utils.logger import get_logger

logger = get_logger("NativeDetection")


class SourceSession(Protocol):
    """One source connection attempt without lifecycle ownership of risk/reconciliation."""

    connected: bool

    async def run(self) -> None:
        """Serve until disconnect or fail closed."""
        ...


class DiscardingRawObservationSink:
    """RawObservationSink used when durable evidence publication is disabled.

    The source still composes and dispatches, but original frames are neither
    persisted locally nor shipped to a receiver. Pending evaluation genuinely
    requires durable evidence and therefore refuses to compose against this
    sink (``NativeDetectionRuntime.compose`` raises), rather than evaluating
    against observations it cannot retain.
    """

    __slots__ = ()

    def record(
        self,
        provider: str,
        kind: str,
        payload: object,
        receipt: TransportReceipt,
        *,
        recovered: bool,
    ) -> None:
        """Accept and drop one raw observation without persistence or I/O."""
        return None


class PendingLifecycle(Protocol):
    """An optional read-only owner has no access to mined execution callbacks."""

    @property
    def status(self) -> str:
        """Return safe sampling state separately from live detection mode."""
        ...

    async def start(self) -> None:
        """Start one finite sampling attempt."""
        ...

    def request_stop(self) -> None:
        """Revoke sampling immediately when watched owners change."""
        ...

    async def stop(self) -> None:
        """Release only optional pending resources."""
        ...


class NativeDetectionRuntime:
    """Bound reconnect attempts and expose configured versus effective owner state."""

    def __init__(
        self,
        *,
        policies: Sequence[TraderDetectionPolicy],
        state: DetectionStateStore,
        session_factory: Callable[[Sequence[DetectionSubscription]], SourceSession],
        observer: NativeOwnerObserver,
        status_changed: Callable[[], None],
        pending_runtime: PendingLifecycle | None = None,
    ) -> None:
        """Bind a fixed process roster; no network operation occurs before start."""
        self._policies, self._state = tuple(policies), state
        self._factory, self._observer, self._changed = session_factory, observer, status_changed
        self._session: SourceSession | None = None
        self._task: asyncio.Task[None] | None = None
        self._owned_state: SqliteDetectionState | None = None
        self._retained = {policy.wallet for policy in policies if policy.enabled}
        self._pending = pending_runtime
        self._last_status: tuple[bool, int, tuple[str, ...]] | None = None

    @classmethod
    def compose(
        cls,
        *,
        config: NativeDetectionConfig,
        provider_runtime: ProviderRuntime,
        wallets: Iterable[str],
        sink: Callable[[OnchainTradeSignal], Awaitable[None]],
        raw: RawObservationSink,
        status_changed: Callable[[], None],
    ) -> NativeDetectionRuntime:
        """Compose only approved metered transports and a mandatory local evidence sink."""
        policies = config.policies_for(wallets)
        state = SqliteDetectionState(
            Path(config.state_path), max_inclusions=config.maximum_inclusions
        )
        clock = SystemReceiptClock()
        observer = NativeOwnerObserver(
            wallets=tuple(policy.wallet for policy in policies if policy.enabled),
            state=state,
            condition_resolver=GammaConditionIdResolver(),
            block_timestamps=RpcBlockTimestampReader(
                provider_runtime,
                rpc_url=provider_runtime.endpoints.http_url(provider_runtime.primary),
            ),
            sink=sink,
        )
        recovery = BoundedLogRecovery(
            state=state,
            gateway=provider_runtime.gateway,
            observer=observer,
            raw=raw,
            clock=clock,
            provider="infura" if provider_runtime.endpoints.infura else "alchemy",
            overlap=config.recovery_overlap,
            maximum_gap=config.recovery_maximum_gap,
        )
        connector = ProviderWebSocketConnector(provider_runtime.endpoints)

        def session_factory(groups: Sequence[DetectionSubscription]) -> SourceSession:
            return GroupedWebSocketSession(
                groups=groups,
                connector=connector,
                admission=provider_runtime.budget,
                observer=observer,
                recovery=recovery,
                raw=raw,
                clock=clock,
                state=state,
                status_changed=lambda: runtime._source_changed(),
            )

        runtime = cls(
            policies=policies,
            state=state,
            session_factory=session_factory,
            observer=observer,
            status_changed=status_changed,
        )
        runtime._owned_state = state
        if config.pending_enabled and any(
            policy.enabled and policy.mode == "pending" for policy in policies
        ):
            from orchestrators.pending_evaluation_runtime import PendingEvaluationRuntime
            from services.pending_evaluation import BoundedPendingEvaluator, PendingEvidenceSink

            if not callable(getattr(raw, "append_event", None)) or not callable(
                getattr(raw, "invalidate", None)
            ):
                state.close()
                raise ValueError("pending evaluation requires a durable local evidence sink")
            evidence_sink = cast(PendingEvidenceSink, raw)
            runtime._pending = PendingEvaluationRuntime(
                evaluator=BoundedPendingEvaluator(
                    policies,
                    globally_enabled=True,
                    gateway=provider_runtime.gateway,
                    stream_admission=provider_runtime.budget,
                    receipt_clock=clock,
                    evidence_sink=evidence_sink,
                ),
                connector=connector,
                admission=provider_runtime.budget,
                clock=clock,
                evidence_sink=evidence_sink,
                status_changed=runtime._source_changed,
            )
        return runtime

    @property
    def connected(self) -> bool:
        """Report current acknowledged and recovered source connection state."""
        return self._session is not None and self._session.connected

    def states(self) -> tuple[TraderDetectionState, ...]:
        """Read durable per-trader configured and effective state."""
        return self._state.states()

    def active_wallets(self) -> tuple[str, ...]:
        """Return only connected, acknowledged, retained owners for task activation."""
        if not self.connected:
            return ()
        return tuple(
            state.wallet
            for state in self.states()
            if state.wallet in self._retained
            and state.enabled
            and state.effective_mode in ("mined", "pending")
        )

    async def start(self) -> None:
        """Configure desired policies and start at most one supervised source owner."""
        if self._task is not None:
            raise RuntimeError("native source already started")
        self._state.configure(self._policies)
        self._source_changed()
        if self._pending is not None:
            await self._pending.start()
        if self._retained:
            self._task = asyncio.create_task(self._run(), name="native-owner-detection")

    async def _run(self) -> None:
        wallets = tuple(self._retained)
        try:
            groups = GroupedDetectionPlanner().plan(
                tuple(policy for policy in self._policies if policy.provider == "alchemy")
            )
            if not groups:
                self._source_changed()
                return
        except ValueError:
            self._state.set_effective(wallets, "paused", "unsupported_stream_billing")
            self._source_changed()
            return
        for attempt in range(3):
            try:
                self._session = self._factory(groups)
                await self._session.run()
                reason = "source_unavailable"
            except BudgetExhausted:
                self._state.set_effective(wallets, "paused", "provider_budget_exhausted")
                self._source_changed()
                return
            except Exception:
                reason = "source_unavailable"
            self._state.set_effective(wallets, "paused", reason)
            self._source_changed()
            if attempt < 2:
                await asyncio.sleep(2**attempt)
        self._state.set_effective(wallets, "paused", "reconnect_exhausted")
        self._source_changed()

    def update_watched_wallets(self, wallets: Iterable[str]) -> None:
        """Retire immediately and persist new owners as paused until restart."""
        requested = {wallet.lower() for wallet in wallets}
        if self._pending is not None and requested != self._retained:
            self._pending.request_stop()
        original = {policy.wallet for policy in self._policies}
        self._state.await_restart(
            tuple(TraderDetectionPolicy(wallet) for wallet in sorted(requested - original))
        )
        removed = self._retained - requested
        self._retained.intersection_update(requested)
        self._observer.retain_wallets(tuple(self._retained))
        self._state.set_effective(tuple(removed), "paused", "retired")
        self._source_changed()

    def _source_changed(self) -> None:
        """Keep retired/unsupported state honest across reconnects before publishing it."""
        unsupported = tuple(
            policy.wallet
            for policy in self._policies
            if policy.enabled and policy.provider != "alchemy"
        )
        retired = tuple(
            policy.wallet
            for policy in self._policies
            if policy.enabled and policy.wallet not in self._retained
        )
        self._state.set_effective(unsupported, "paused", "unsupported_stream_billing")
        self._state.set_effective(retired, "paused", "retired")
        budget_paused: set[str] = set()
        if self.connected and self._pending is not None:
            current = {state.wallet: state for state in self._state.states()}
            for policy in self._policies:
                observed = current.get(policy.wallet)
                if (
                    policy.wallet not in self._retained
                    or not policy.enabled
                    or policy.mode != "pending"
                    or observed is None
                    or (
                        observed.effective_mode != "mined"
                        and observed.degraded_reason != "pending_budget_exhausted"
                    )
                ):
                    continue
                status = self._pending.status
                pause = status == "budget_stopped" and policy.on_budget_exhausted == "pause"
                reason = (
                    "pending_budget_exhausted"
                    if status == "budget_stopped"
                    else "pending_evaluation_only"
                    if status == "evaluating"
                    else "pending_evaluation_unavailable"
                )
                self._state.set_effective((policy.wallet,), "paused" if pause else "mined", reason)
                if pause:
                    budget_paused.add(policy.wallet)
        self._observer.retain_wallets(tuple(sorted(self._retained - budget_paused)))
        reasons = tuple(
            sorted(
                {
                    state.degraded_reason
                    for state in self.states()
                    if state.enabled and state.degraded_reason is not None
                }
            )
        )
        status = (self.connected, len(self.active_wallets()), reasons)
        if status != self._last_status:
            logger.info(
                "native_detection_state | connected=%s active_leaders=%d reasons=%s",
                status[0],
                status[1],
                ",".join(reasons) or "none",
            )
            self._last_status = status
        self._changed()

    async def wait(self) -> None:
        """Await the source owner for deterministic lifecycle checks."""
        if self._task is not None:
            await self._task

    async def stop(self) -> None:
        """Cancel source work while preserving all durable evidence and other loops."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._pending is not None:
            await self._pending.stop()
        if self._owned_state is not None:
            self._owned_state.close()
            self._owned_state = None
