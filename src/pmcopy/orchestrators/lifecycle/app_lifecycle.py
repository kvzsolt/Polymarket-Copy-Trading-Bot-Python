"""Explicit OOP lifecycle, supervision, stream, and shutdown owners for ``app.py``.

Extracts resource ownership, bounded task supervision, WebSocket runtime wiring,
dynamic leader reconciliation gating, and ordered deterministic shutdown out of
the procedural ``app._run_application`` flow while keeping top-level startup
milestones explicit at the composition root.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sized
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, final, override

from pmcopy.adapters.time.clock import SystemReceiptClock
from pmcopy.application.runtime_state import (
    resolve_reservation_service,
    resolve_risk_state,
)
from pmcopy.orchestrators.streaming.market_data_runtime import (
    MarketDataRuntime,
    extract_token_ids_from_contexts,
    extract_token_ids_from_live_positions,
)
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import (
    UserRuntimeState,
    build_trading_context,
    ensure_runtime_follows,
)
from pmcopy.orchestrators.streaming.user_data_runtime import UserDataRuntime
from pmcopy.services.accounts.account_lease import AccountLeaseManager
from pmcopy.services.market_data.market_evidence import MarketEvidenceTap
from pmcopy.services.market_data.market_readiness import MarketReadinessGroup
from pmcopy.services.observability.metrics import MetricsRegistry, metrics
from pmcopy.services.accounts.multi_user_config import RuntimeUser, UserFollowConfig
from pmcopy.services.telemetry import WssTelemetryEmitter
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.services.accounts.user_readiness import UserReadinessGroup
from pmcopy.utils.logger import get_logger, redact_error_detail

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext
    from pmcopy.orchestrators.discovery.auto_trader_runtime import AutoTraderRuntime
    from pmcopy.storage.postgres.leaders_repo import LeadersRepo

_default_logger = get_logger("AppLifecycle")


class ManagedResource(ABC):
    """Abstract async-closable runtime resource owned by :class:`RuntimeResourceStack`."""

    resource_name: str

    @abstractmethod
    async def aclose(self) -> None:
        """Idempotently release the underlying runtime resource."""

    async def close(self) -> None:
        """Alias satisfying legacy ``close()`` callers."""
        await self.aclose()


@final
@dataclass
class HealthServerResource(ManagedResource):
    """Idempotent ``ManagedResource`` owner for an aiohttp health-server runner."""

    runner: Any
    closed: bool = False
    resource_name: str = "health_server"

    @override
    async def aclose(self) -> None:
        """Close the health server at most once."""
        if self.closed:
            return
        self.closed = True
        await self.runner.cleanup()

    async def cleanup(self) -> None:
        """Provide the aiohttp-compatible cleanup alias."""
        await self.aclose()


@final
class CallableCloseResource(ManagedResource):
    """Idempotent ``ManagedResource`` adapter over ``close``/``aclose``/``stop``/``cleanup``."""

    def __init__(
        self,
        target: Any,
        *,
        resource_name: str | None = None,
    ) -> None:
        self.target = target
        self.resource_name = resource_name or type(target).__name__
        self._closed = False

    @override
    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        closer: Callable[[], Any] | None = None
        if inspect.isfunction(self.target) or inspect.ismethod(self.target):
            closer = self.target
        else:
            for attr in ("close", "aclose", "stop", "cleanup"):
                candidate = getattr(self.target, attr, None)
                if callable(candidate):
                    closer = candidate
                    break
            if closer is None and callable(self.target):
                closer = self.target
        if closer is None:
            return
        result = closer()
        if inspect.isawaitable(result):
            await result


@final
class RuntimeResourceStack:
    """Deterministic LIFO async resource stack isolating individual close failures."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        error_event: str = "polymarket_client_shutdown_failed",
    ) -> None:
        self._resources: list[ManagedResource] = []
        self._seen_targets: set[int] = set()
        self._logger = logger or _default_logger
        self._error_event = error_event

    def push(self, resource: ManagedResource) -> ManagedResource:
        """Register a typed ``ManagedResource`` for reverse-order teardown."""
        identity = id(resource)
        if identity not in self._seen_targets:
            self._seen_targets.add(identity)
            self._resources.append(resource)
        return resource

    def push_client(self, client: Any, *, name: str | None = None) -> None:
        """Adapt and register an arbitrary SDK/runtime object once by identity."""
        identity = id(client)
        if identity in self._seen_targets:
            return
        self._seen_targets.add(identity)
        if isinstance(client, ManagedResource):
            self._resources.append(client)
        else:
            self._resources.append(CallableCloseResource(client, resource_name=name))

    async def aclose(self) -> None:
        """Close all registered resources in reverse order without masking errors."""
        while self._resources:
            resource = self._resources.pop()
            try:
                await resource.aclose()
            except Exception as exc:
                self._logger.warning(
                    self._error_event,
                    extra={
                        "resource": resource.resource_name,
                        "error": redact_error_detail(exc),
                    },
                )


async def close_polymarket_clients(
    clients: Iterable[Any],
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Close each distinct client/runtime in reverse order via ``RuntimeResourceStack``."""
    stack = RuntimeResourceStack(
        logger=logger or _default_logger,
        error_event="polymarket_client_shutdown_failed",
    )
    for client in clients:
        stack.push_client(client)
    await stack.aclose()


@dataclass
class SupervisedTaskSpec:
    """A long-lived auxiliary background loop owned by :class:`TaskSupervisor`."""

    name: str
    factory: Callable[[], Coroutine[object, object, None]]
    max_restarts: int = 5
    recovery_seconds: float = 60.0
    task: asyncio.Task[Any] | None = None
    restarts: int = 0
    started: float = 0.0

    def spawn(self) -> asyncio.Task[Any]:
        """Start and track a fresh supervised background task."""
        self.started = asyncio.get_running_loop().time()
        self.task = asyncio.create_task(self.factory(), name=self.name)
        return self.task

    def record_crash(self, now: float) -> Literal["restart", "fail_closed"]:
        """Record one crash at ``now`` and return ``"restart"`` or ``"fail_closed"``."""
        if now - self.started >= self.recovery_seconds:
            self.restarts = 0
        self.restarts += 1
        return "fail_closed" if self.restarts >= self.max_restarts else "restart"


def live_monitor_count(monitor_tasks: Iterable[asyncio.Task[Any]]) -> int:
    """Count leader-monitor tasks that have not completed."""
    return sum(1 for task in monitor_tasks if not task.done())


def on_supervised_crash(
    spec: SupervisedTaskSpec,
    now: float,
) -> Literal["restart", "fail_closed"]:
    """Pure restart decision helper delegating to ``SupervisedTaskSpec.record_crash``."""
    return spec.record_crash(now)


@final
@dataclass
class TaskSupervisor:
    """Bounded-restart supervisor for auxiliary background tasks and leader monitors."""

    supervised: list[SupervisedTaskSpec]
    monitor_tasks: list[asyncio.Task[Any]]
    monitor_wallet_by_task: dict[asyncio.Task[Any], str]
    readiness: Any
    refresh_interval: float
    metrics_registry: MetricsRegistry = field(default_factory=lambda: metrics)
    logger: logging.Logger = field(default_factory=lambda: _default_logger)
    sleep_fn: Callable[[float], Awaitable[Any]] = asyncio.sleep

    def spawn_all(self) -> None:
        """Spawn every registered ``SupervisedTaskSpec``."""
        for spec in self.supervised:
            spec.spawn()

    async def run_until_stopped(self) -> int:
        """Supervise running tasks until cancelled or a restart budget exhausts."""
        loop = asyncio.get_running_loop()
        inspected_done: set[asyncio.Task[Any]] = set()
        while True:
            already_done = [
                task
                for task in self.monitor_tasks
                if task.done() and task not in inspected_done
            ]
            already_done += [
                spec.task
                for spec in self.supervised
                if spec.task is not None
                and spec.task.done()
                and spec.task not in inspected_done
            ]
            if already_done:
                done: set[asyncio.Task[Any]] = set(already_done)
            else:
                active = [task for task in self.monitor_tasks if not task.done()]
                active += [
                    spec.task
                    for spec in self.supervised
                    if spec.task is not None and not spec.task.done()
                ]
                if not active:
                    await self.sleep_fn(self.refresh_interval)
                    self.metrics_registry.inc("no_leaders_monitored_total")
                    continue
                done, _ = await asyncio.wait(active, timeout=self.refresh_interval)
            inspected_done.update(done)

            fail_closed = False
            for task in done:
                completed_spec = next((s for s in self.supervised if s.task is task), None)
                if task.cancelled():
                    if completed_spec is not None:
                        self.logger.info(
                            "supervised_task_cancelled",
                            extra={"task": completed_spec.name},
                        )
                    continue

                task_error = task.exception()
                if completed_spec is None:
                    if task_error:
                        self.logger.error(
                            "monitor_task_crashed",
                            extra={"error": redact_error_detail(task_error)},
                        )
                    self.monitor_wallet_by_task.pop(task, None)
                    self.metrics_registry.inc("leader_monitor_crashed_total")
                    self.readiness.update(
                        monitored_leader_count=live_monitor_count(self.monitor_tasks)
                    )
                    continue

                if task_error is None:
                    self.logger.warning(
                        "supervised_task_exited_clean",
                        extra={"task": completed_spec.name},
                    )
                    continue

                self.metrics_registry.inc("supervised_task_restart_total")
                decision = on_supervised_crash(completed_spec, loop.time())
                self.logger.error(
                    "supervised_task_crashed",
                    extra={
                        "task": completed_spec.name,
                        "restarts": completed_spec.restarts,
                        "decision": decision,
                        "error": redact_error_detail(task_error),
                    },
                )
                if decision == "restart":
                    backoff = min(5.0, 0.5 * (2 ** max(0, completed_spec.restarts - 1)))
                    await self.sleep_fn(backoff)
                    completed_spec.spawn()
                else:
                    self.logger.critical(
                        "supervised_task_fail_closed",
                        extra={
                            "task": completed_spec.name,
                            "restarts": completed_spec.restarts,
                        },
                    )
                    self.readiness.update(supervisor_ok=False)
                    fail_closed = True

            if fail_closed:
                return 1


def flatten_contexts(
    contexts_by_leader: Mapping[str, list[TradingContext]],
) -> list[TradingContext]:
    """Flatten ``contexts_by_leader`` preserving insertion order."""
    return [context for contexts in contexts_by_leader.values() for context in contexts]


def empty_user_ws_api_creds() -> dict[str, str]:
    """Return the unauthenticated user-stream credential shape."""
    return {field: "" for field in ("api_key", "api_secret", "api_passphrase")}


def normalise_user_ws_api_creds(raw_creds: Any) -> dict[str, str]:
    """Normalize derived CLOB L2 credentials into ``UserWebSocket``'s expected shape."""
    if raw_creds is None:
        return empty_user_ws_api_creds()

    def read(*names: str) -> str:
        if isinstance(raw_creds, dict):
            for name in names:
                value = raw_creds.get(name)
                if value:
                    return str(value)
        for name in names:
            value = getattr(raw_creds, name, None)
            if value:
                return str(value)
        return ""

    return {
        "api_key": read("api_key", "key", "apiKey"),
        "api_secret": read("api_secret", "secret", "apiSecret"),
        "api_passphrase": read("api_passphrase", "passphrase", "apiPassphrase"),
    }


def get_clob_wrapper_from_user_state(user_state: UserRuntimeState) -> Any | None:
    """Best-effort access to the CCXT venue client that owns derived L2 credentials."""
    for attr_name in ("client", "clob_client", "clob_wrapper", "clob"):
        value = getattr(user_state, attr_name, None)
        if value is not None:
            return value

    execution_service = getattr(user_state, "execution_service", None)
    if execution_service is not None:
        for attr_name in ("clob_client", "_clob_client", "client", "_client"):
            value = getattr(execution_service, attr_name, None)
            if value is not None:
                return value

    clob_gateway = getattr(user_state, "clob_gateway", None)
    if clob_gateway is not None:
        for attr_name in (
            "clob_client",
            "_clob_client",
            "client",
            "_client",
            "wrapper",
            "_wrapper",
        ):
            value = getattr(clob_gateway, attr_name, None)
            if value is not None:
                return value

    return None


def user_ws_api_creds(user_state: UserRuntimeState) -> dict[str, str]:
    """Read already-derived CLOB L2 API credentials from ``user_state``."""
    wrapper = get_clob_wrapper_from_user_state(user_state)
    if wrapper is None:
        return empty_user_ws_api_creds()

    for attr_name in ("api_creds", "user_api_creds", "creds"):
        creds = normalise_user_ws_api_creds(getattr(wrapper, attr_name, None))
        if all(creds.values()):
            return creds

    inner_client = getattr(wrapper, "client", None)
    if inner_client is not None:
        for attr_name in ("api_creds", "creds", "_api_creds", "_creds"):
            creds = normalise_user_ws_api_creds(getattr(inner_client, attr_name, None))
            if all(creds.values()):
                return creds

    return empty_user_ws_api_creds()


@final
class RuntimeStreamCoordinator:
    """Coordinate market/user WSS runtimes, callbacks, and dynamic leader diffs."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _default_logger

    def build_reconcile_wake_closure(self, context: Any) -> Callable[[Any], Coroutine[Any, Any, None]]:
        """Build a lock-throttled async wake closure converging ``ReconciliationService``."""
        from pmcopy.orchestrators.pipeline.trade_pipeline import (
            get_reconciliation_service,
            reconcile_submitted_unconfirmed,
        )

        lock = asyncio.Lock()
        log = self._logger

        async def _wake(event: Any) -> None:
            try:
                reconciler = get_reconciliation_service(context)
                await reconciler.record_user_order_evidence(event)
            except Exception as exc:
                log.warning(
                    "user_wss_wake_reconcile_error",
                    extra={
                        "user_id": context.user_id,
                        "error": redact_error_detail(exc),
                    },
                )
                return
            if lock.locked():
                return
            async with lock:
                try:
                    reconciled = await reconcile_submitted_unconfirmed(context)
                    reservation_reaped = 0
                    reservation_service = resolve_reservation_service(context)
                    release_expired = getattr(
                        reservation_service,
                        "release_expired_active",
                        None,
                    )
                    if callable(release_expired):
                        release_result = release_expired()
                        released_ids = (
                            await release_result
                            if inspect.isawaitable(release_result)
                            else release_result
                        )
                        reservation_reaped = (
                            len(released_ids) if isinstance(released_ids, Sized) else 0
                        )
                    if reconciled or reservation_reaped:
                        log.info(
                            "user_wss_wake_reconcile_complete | user=%s reconciled=%d "
                            "reservations_reaped=%d order_id=%s",
                            context.user_id,
                            reconciled,
                            reservation_reaped,
                            (event.get("order_id") or event.get("orderID") or event.get("id"))
                            if isinstance(event, dict)
                            else None,
                        )
                except Exception as exc:
                    log.warning(
                        "user_wss_wake_reconcile_error",
                        extra={
                            "user_id": context.user_id,
                            "error": redact_error_detail(exc),
                        },
                    )

        return _wake

    @staticmethod
    def build_risk_cache_evict_closure(context: Any) -> Callable[[str], None]:
        """Build a best-effort token-eviction callback for ``RiskPriceCache``."""

        def _evict(token_id: str) -> None:
            cache = resolve_risk_state(context).price_cache
            if cache is None:
                return
            try:
                cache.evict(token_id)
            except Exception:
                return

        return _evict

    @staticmethod
    def build_market_token_subscribe_closure(
        runtime: MarketDataRuntime,
    ) -> Callable[[str], Coroutine[Any, Any, None]]:
        """Build a user-fill hook subscribing newly seen tokens on ``MarketDataRuntime``."""

        async def _subscribe(token_id: str) -> None:
            normalized = str(token_id or "").strip()
            if not normalized:
                return
            await runtime.subscribe_tokens([normalized])

        return _subscribe

    async def start_market_data_runtimes(
        self,
        *,
        runtime_users: list[RuntimeUser],
        user_states: dict[str, UserRuntimeState],
        live_positions_by_user: dict[str, list[dict[str, Any]]],
        contexts_by_leader: dict[str, list[TradingContext]],
        readiness: Any,
        config: Any,
        quote_sink: Any = None,
        telemetry: WssTelemetryEmitter | None = None,
        latency: PipelineLatency | None = None,
        evidence_runtime: Any | None = None,
        market_runtime_cls: Any = MarketDataRuntime,
        runtimes_out: dict[str, MarketDataRuntime] | None = None,
    ) -> dict[str, MarketDataRuntime]:
        """Start WSS-backed ``MarketDataRuntime`` instances keyed by ``user_id``."""
        market_data_runtimes = runtimes_out if runtimes_out is not None else {}
        context_tokens = extract_token_ids_from_contexts(flatten_contexts(contexts_by_leader))
        market_readiness = MarketReadinessGroup(
            readiness, tuple(user.user_id for user in runtime_users)
        )

        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            if user_state is None:
                self._logger.error(
                    "market_data_runtime_skipped",
                    extra={
                        "user_id": runtime_user.user_id,
                        "reason": "missing user runtime state",
                    },
                )
                continue

            market_store = getattr(user_state, "market_store", None)
            clob_gateway = getattr(user_state, "clob_gateway", None)
            if market_store is None or clob_gateway is None:
                self._logger.warning(
                    "market_data_runtime_skipped",
                    extra={
                        "user_id": runtime_user.user_id,
                        "reason": "missing market_store or clob_gateway",
                    },
                )
                continue

            live_position_tokens = extract_token_ids_from_live_positions(
                live_positions_by_user.get(runtime_user.user_id, [])
            )
            initial_tokens = sorted(set(live_position_tokens) | set(context_tokens))

            market_runtime = market_runtime_cls(
                market_store=market_store,
                gateway=clob_gateway,
                readiness=market_readiness.scope(runtime_user.user_id),
                config=config,
                quote_sink=quote_sink,
                telemetry=telemetry,
                latency=latency,
                evidence=(
                    MarketEvidenceTap(
                        evidence_runtime, SystemReceiptClock(), stream_id=runtime_user.user_id
                    )
                    if evidence_runtime is not None
                    else None
                ),
            )
            await market_runtime.start(initial_tokens)
            market_data_runtimes[runtime_user.user_id] = market_runtime
            self._logger.info(
                "market_data_runtime_started_for_user",
                extra={
                    "user_id": runtime_user.user_id,
                    "subscribed_token_count": len(initial_tokens),
                },
            )

        return market_data_runtimes

    @staticmethod
    def attach_market_data_runtime_to_contexts(
        *,
        contexts_by_leader: dict[str, list[TradingContext]],
        risk_contexts_by_user: dict[str, TradingContext],
        runtimes_by_user: dict[str, MarketDataRuntime],
    ) -> None:
        """Bind each account's ``MarketDataRuntime`` onto its ``TradingContext`` objects."""
        for contexts in contexts_by_leader.values():
            for context in contexts:
                runtime = runtimes_by_user.get(context.user_id)
                if runtime is not None:
                    context.market_data_runtime = runtime
        for context in risk_contexts_by_user.values():
            runtime = runtimes_by_user.get(context.user_id)
            if runtime is not None:
                context.market_data_runtime = runtime

    async def start_user_data_runtimes(
        self,
        *,
        runtime_users: list[RuntimeUser],
        user_states: dict[str, UserRuntimeState],
        readiness: Any,
        risk_contexts_by_user: dict[str, TradingContext] | None = None,
        market_data_runtimes: dict[str, MarketDataRuntime] | None = None,
        telemetry: WssTelemetryEmitter | None = None,
        api_creds_resolver: Callable[[UserRuntimeState], dict[str, str]] = user_ws_api_creds,
        user_runtime_cls: Any = UserDataRuntime,
        runtimes_out: list[UserDataRuntime] | None = None,
    ) -> list[UserDataRuntime]:
        """Start authenticated ``UserDataRuntime`` instances for all runtime users."""
        user_data_runtimes = runtimes_out if runtimes_out is not None else []
        user_readiness = UserReadinessGroup(
            readiness, tuple(user.user_id for user in runtime_users)
        )

        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            if user_state is None:
                self._logger.error(
                    "user_data_runtime_skipped",
                    extra={
                        "user_id": runtime_user.user_id,
                        "reason": "missing user runtime state",
                    },
                )
                continue

            market_store = getattr(user_state, "market_store", None)
            if market_store is None:
                self._logger.warning(
                    "user_data_runtime_skipped",
                    extra={
                        "user_id": runtime_user.user_id,
                        "reason": "missing market_store",
                    },
                )
                continue

            api_creds = api_creds_resolver(user_state)
            wake_closure = None
            risk_cache_evict = None
            market_token_subscribe = None
            if risk_contexts_by_user is not None:
                user_context = risk_contexts_by_user.get(runtime_user.user_id)
                if user_context is not None:
                    wake_closure = self.build_reconcile_wake_closure(user_context)
                    risk_cache_evict = self.build_risk_cache_evict_closure(user_context)
            if market_data_runtimes is not None:
                market_runtime = market_data_runtimes.get(runtime_user.user_id)
                if market_runtime is not None:
                    market_token_subscribe = self.build_market_token_subscribe_closure(
                        market_runtime
                    )

            runtime = user_runtime_cls(
                market_store=market_store,
                readiness=user_readiness.scope(runtime_user.user_id),
                api_creds=api_creds,
                user_id=runtime_user.user_id,
                on_reconcile_signal=wake_closure,
                manual_sync_service=getattr(user_state, "manual_sync_service", None),
                on_risk_cache_evict=risk_cache_evict,
                on_market_token_seen=market_token_subscribe,
                telemetry=telemetry,
            )
            await runtime.start()
            user_data_runtimes.append(runtime)

        return user_data_runtimes

    async def subscribe_context_tokens(
        self,
        *,
        market_data_runtimes: dict[str, MarketDataRuntime],
        contexts_by_leader: dict[str, list[Any]],
    ) -> None:
        """Subscribe tokens visible from runtime contexts across all market WSS streams."""
        token_ids = extract_token_ids_from_contexts(flatten_contexts(contexts_by_leader))
        if not token_ids:
            return
        for runtime in market_data_runtimes.values():
            try:
                await runtime.subscribe_tokens(token_ids)
            except Exception as exc:
                self._logger.warning(
                    "market_data_dynamic_subscribe_failed",
                    extra={"error": redact_error_detail(exc)},
                )

    def refresh_onchain_watched_wallets(
        self,
        onchain_runtime: object | None,
        wallets: Iterable[str],
    ) -> bool:
        """Update watched wallets on the active on-chain detection runtime."""
        if onchain_runtime is None:
            return False
        update = getattr(onchain_runtime, "update_watched_wallets", None)
        if not callable(update):
            return False
        watched = {str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}
        try:
            update(watched)
        except Exception as exc:
            self._logger.warning(
                "onchain_watched_wallets_update_failed | error=%s — added "
                "leaders remain paused pending restart",
                redact_error_detail(exc),
            )
            return False
        self._logger.info("onchain_watched_wallets_updated | watched=%d", len(watched))
        return True

    @staticmethod
    def wallets_blocked_by_reconcile_failures(
        started_wallets: list[str],
        contexts_by_leader: dict[str, list[TradingContext]],
        failed_user_ids: list[str],
    ) -> list[str]:
        """Return wallets whose downstream contexts include any failed ``user_id``."""
        if not failed_user_ids:
            return []
        failed = set(failed_user_ids)
        return [
            wallet
            for wallet in started_wallets
            if any(ctx.user_id in failed for ctx in contexts_by_leader.get(wallet, []))
        ]

    @classmethod
    def cold_start_monitored_after_reconcile(
        cls,
        monitored_leaders: list[str],
        contexts_by_leader: dict[str, list[TradingContext]],
        reconcile_failures: list[str],
    ) -> list[str]:
        """Filter the cold-start leader set to exclude failed-reconcile accounts."""
        blocked = set(
            cls.wallets_blocked_by_reconcile_failures(
                monitored_leaders, contexts_by_leader, reconcile_failures
            )
        )
        if not blocked:
            return monitored_leaders
        return [wallet for wallet in monitored_leaders if wallet not in blocked]

    async def refresh_contexts_for_new_leaders(
        self,
        *,
        new_wallets: list[str],
        runtime_users: list[RuntimeUser],
        user_states: dict[str, UserRuntimeState],
        contexts_by_leader: dict[str, list[Any]],
        leaders_repo: LeadersRepo | AutoTraderRuntime,
        config: Any,
        readiness: Any,
        context_builder: Any = build_trading_context,
    ) -> list[str]:
        """Build ``TradingContext`` bindings for newly promoted leader wallets."""
        started_wallets: list[str] = []
        ensure_runtime_follows(runtime_users, new_wallets, config)

        for wallet in new_wallets:
            if wallet in contexts_by_leader:
                continue
            contexts_by_leader[wallet] = []
            for runtime_user in runtime_users:
                user_state = user_states.get(runtime_user.user_id)
                follow: UserFollowConfig | None = runtime_user.follows.get(wallet)
                if not user_state or not follow or not follow.enabled:
                    continue
                context = await context_builder(
                    user_state,
                    follow,
                    config,
                    leaders_repo=leaders_repo,
                    readiness=readiness,
                )
                contexts_by_leader[wallet].append(context)
            if contexts_by_leader[wallet]:
                started_wallets.append(wallet)
        return started_wallets


@final
class RuntimeShutdownCoordinator:
    """Deterministic ordered shutdown of tasks, streams, leases, HTTP, metrics, and DB."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _default_logger

    async def cancel_tasks_and_stop_streams(
        self,
        *,
        monitor_tasks: list[asyncio.Task[Any]],
        supervised: list[SupervisedTaskSpec],
        market_data_runtimes: dict[str, MarketDataRuntime],
        user_data_runtimes: list[UserDataRuntime],
        onchain_runtime: Any = None,
        pending_copy_tracker: Any = None,
        v1_runtime: Any = None,
        evidence_drain: Any = None,
    ) -> None:
        """Cancel supervised/monitor tasks and stop runtime streams with error isolation."""
        shutdown_tasks = list(monitor_tasks)
        shutdown_tasks.extend(spec.task for spec in supervised if spec.task is not None)
        for task in shutdown_tasks:
            task.cancel()
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        if onchain_runtime is not None:
            try:
                await onchain_runtime.stop()
            except Exception as exc:
                self._logger.warning(
                    "onchain_runtime_shutdown_failed",
                    extra={"error": redact_error_detail(exc)},
                )

        if pending_copy_tracker is not None:
            try:
                await pending_copy_tracker.stop()
            except Exception as exc:
                self._logger.warning(
                    "pending_copy_tracker_shutdown_failed",
                    extra={"error": redact_error_detail(exc)},
                )

        if v1_runtime is not None:
            try:
                await v1_runtime.close()
            except Exception as exc:
                self._logger.warning(
                    "v1_runtime_shutdown_failed",
                    extra={"error": redact_error_detail(exc)},
                )

        for user_runtime in user_data_runtimes:
            try:
                await user_runtime.stop()
            except Exception as exc:
                self._logger.warning(
                    "user_data_runtime_shutdown_failed",
                    extra={"error": redact_error_detail(exc)},
                )

        for market_runtime in market_data_runtimes.values():
            try:
                await market_runtime.stop()
            except Exception as exc:
                self._logger.warning(
                    "market_data_runtime_shutdown_failed",
                    extra={"error": redact_error_detail(exc)},
                )

        if evidence_drain is not None:
            await evidence_drain.aclose()

    async def release_account_leases(
        self,
        account_lease_manager: AccountLeaseManager | None,
    ) -> None:
        """Release held account leases before database pool teardown."""
        if account_lease_manager is None:
            return
        try:
            await account_lease_manager.close()
        except Exception as exc:
            self._logger.warning(
                "account_lease_release_failed",
                extra={"error": redact_error_detail(exc)},
            )


__all__ = [
    "CallableCloseResource",
    "HealthServerResource",
    "ManagedResource",
    "RuntimeResourceStack",
    "RuntimeShutdownCoordinator",
    "RuntimeStreamCoordinator",
    "SupervisedTaskSpec",
    "TaskSupervisor",
    "close_polymarket_clients",
    "empty_user_ws_api_creds",
    "flatten_contexts",
    "get_clob_wrapper_from_user_state",
    "live_monitor_count",
    "normalise_user_ws_api_creds",
    "on_supervised_crash",
    "user_ws_api_creds",
]
