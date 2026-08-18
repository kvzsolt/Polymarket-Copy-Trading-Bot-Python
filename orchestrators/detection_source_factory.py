"""Select and compose the on-chain leader-detection runtime per ``DetectionMode``.

``app.main`` no longer branches on detection booleans. It asks one
``DetectionSourceFactory`` — keyed by ``domain.detection.DetectionMode`` — to
build the single on-chain source for the configured mode:

* ``POLLING`` builds nothing (REST polling always runs and is the fallback in
  every mode);
* ``ON_CHAIN_PENDING`` builds the gen-1 ``OnchainDetectionRuntime`` with the
  provisional ``alchemy_pendingTransactions`` subscription only;
* ``ON_CHAIN_CONFIRMED`` builds the gen-2 ``PolygonSourceTransactionRuntime``
  (mempool-observed, receipt-confirmed) with its durable state store.

Every builder receives the same immutable ``DetectionSourceRequest`` and
returns an object satisfying ``DetectionSourceRuntime``. WebSocket URLs are
carried in the request but never logged here.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, Coroutine, Protocol

from clients.onchain_ws import OnchainWebSocket
from domain.detection import DetectionMode
from interfaces.source_transactions import SourceTransactionStateStore
from orchestration.bootstrap import (
    PolygonSourceRuntimeBootstrap,
    PolygonSourceRuntimeConfig,
)
from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
from services.onchain_detection.pending_tx_detector import PendingTxConfig
from services.onchain_detection.trade_detection_models import OnchainTradeSignal

SignalSink = Callable[[OnchainTradeSignal], Awaitable[None]]
ConditionIdResolver = Callable[[str], Coroutine[Any, Any, str]]
StateStoreProvider = Callable[[], Awaitable[SourceTransactionStateStore | None]]


class UnsupportedDetectionModeError(LookupError):
    """Raised when no registered builder handles the requested mode."""


class DetectionSourceRuntime(Protocol):
    """Lifecycle contract every on-chain detection runtime satisfies."""

    async def start(self) -> None:
        """Connect, subscribe, and begin dispatching signals."""
        ...

    async def stop(self) -> None:
        """Stop dispatching and close every socket."""
        ...

    def update_watched_wallets(self, wallets: set[str]) -> None:
        """Replace the watched leader-wallet set used by the decoders."""
        ...


@dataclass(frozen=True, slots=True)
class DetectionSourceRequest:
    """Immutable inputs shared by every detection-source builder.

    Attributes:
        mode: Detection source selected by ``detection.mode``.
        watched_wallets: Startup leader wallet snapshot (any case).
        ws_url: Alchemy Polygon WebSocket endpoint (contains the API key;
            never log it).
        chain_id: Configured Polygon EVM chain identifier.
        signal_sink: Consumer that receives every ``OnchainTradeSignal``.
    """

    mode: DetectionMode
    watched_wallets: frozenset[str]
    ws_url: str
    chain_id: int
    signal_sink: SignalSink

    def __post_init__(self) -> None:
        if not isinstance(self.mode, DetectionMode):
            raise ValueError("mode must be a DetectionMode")
        if not str(self.ws_url or "").strip():
            raise ValueError("ws_url is required")
        if int(self.chain_id) <= 0:
            raise ValueError("chain_id must be positive")
        object.__setattr__(
            self,
            "watched_wallets",
            frozenset(
                str(wallet).strip().lower()
                for wallet in self.watched_wallets
                if str(wallet).strip()
            ),
        )


class DetectionSourceBuilder(Protocol):
    """Strategy that composes the runtime for exactly one detection mode."""

    @property
    def mode(self) -> DetectionMode:
        """Return the mode this builder serves."""
        ...

    async def build(self, request: DetectionSourceRequest) -> DetectionSourceRuntime:
        """Compose a not-yet-started runtime for ``request``."""
        ...


class ProvisionalPendingSourceBuilder:
    """Build the gen-1 provisional mempool runtime (``ON_CHAIN_PENDING``).

    The mined ``OrderFilled`` subscription stays disabled on purpose: gen-1
    mined signals carry a ``log_index`` that changes the copy identity, so
    running both would copy the same fill twice (pending then mined). As a
    consequence there is **no mined correction path** in this mode: the
    per-leader supersession check lives in the mined handler and can never
    fire, so a provisional copy of a mempool sighting that is dropped or
    replaced before mining is not unwound by this runtime. Only the
    dispatcher's staleness budget and confidence floor remain active. Operate
    this mode only after a paper soak and with an explicit unwind procedure
    (see ``docs/audit/pending_detection_notes.md``).
    """

    def __init__(
        self,
        *,
        pending_config: PendingTxConfig,
        condition_id_resolver: ConditionIdResolver,
        runtime_factory: Callable[..., DetectionSourceRuntime] = OnchainDetectionRuntime,
        websocket_factory: Callable[..., Any] = OnchainWebSocket,
    ) -> None:
        """Store the pending-path policy, runtime, and socket constructors.

        Args:
            pending_config: Staleness / calldata / confidence policy.
            condition_id_resolver: Async token-id -> condition-id lookup.
            runtime_factory: ``OnchainDetectionRuntime``-compatible constructor;
                injectable for tests.
            websocket_factory: ``OnchainWebSocket``-compatible constructor the
                runtime uses for each subscription. The composition root binds
                the health reporter here
                (``functools.partial(OnchainWebSocket, state_listener=...)``)
                so a dropped subscription becomes visible.
        """

        self._pending_config = pending_config
        self._condition_id_resolver = condition_id_resolver
        self._runtime_factory = runtime_factory
        self._websocket_factory = websocket_factory

    @property
    def mode(self) -> DetectionMode:
        """Return ``DetectionMode.ON_CHAIN_PENDING``."""

        return DetectionMode.ON_CHAIN_PENDING

    async def build(self, request: DetectionSourceRequest) -> DetectionSourceRuntime:
        """Compose the pending-only gen-1 runtime.

        Args:
            request: Shared detection-source inputs.

        Returns:
            A not-yet-started runtime with ``enable_pending=True`` and
            ``enable_mined=False``.
        """

        return self._runtime_factory(
            watched_wallets=set(request.watched_wallets),
            ws_url=request.ws_url,
            signal_sink=request.signal_sink,
            enable_pending=True,
            enable_mined=False,
            pending_config=self._pending_config,
            condition_id_resolver=self._condition_id_resolver,
            websocket_factory=self._websocket_factory,
        )


class ConfirmedSourceBuilder:
    """Build the gen-2 receipt-confirmed runtime (``ON_CHAIN_CONFIRMED``)."""

    def __init__(
        self,
        *,
        exchange_addresses: tuple[str, ...],
        state_store_provider: StateStoreProvider,
        bootstrap: PolygonSourceRuntimeBootstrap | None = None,
    ) -> None:
        """Store the exchange filter, state-store provider, and composer.

        Args:
            exchange_addresses: CTF exchange contracts to filter mempool and
                ``OrderFilled`` logs on.
            state_store_provider: Awaitable factory for the durable source
                reservation/idempotency store (Postgres in production).
            bootstrap: Runtime composer; defaults to the production
                ``PolygonSourceRuntimeBootstrap``.
        """

        self._exchange_addresses = tuple(exchange_addresses)
        self._state_store_provider = state_store_provider
        self._bootstrap = bootstrap if bootstrap is not None else PolygonSourceRuntimeBootstrap()

    @property
    def mode(self) -> DetectionMode:
        """Return ``DetectionMode.ON_CHAIN_CONFIRMED``."""

        return DetectionMode.ON_CHAIN_CONFIRMED

    async def build(self, request: DetectionSourceRequest) -> DetectionSourceRuntime:
        """Await the state store, then compose the confirmation runtime.

        Args:
            request: Shared detection-source inputs.

        Returns:
            A not-yet-started ``PolygonSourceTransactionRuntime``.
        """

        state_store = await self._state_store_provider()
        return self.compose(request, state_store)

    def compose(
        self,
        request: DetectionSourceRequest,
        state_store: SourceTransactionStateStore | None,
    ) -> DetectionSourceRuntime:
        """Compose the confirmation runtime with an already-resolved store.

        Args:
            request: Shared detection-source inputs.
            state_store: Durable source state store, or ``None`` for an
                in-memory registry.

        Returns:
            A not-yet-started ``PolygonSourceTransactionRuntime``.
        """

        config = PolygonSourceRuntimeConfig(
            chain_id=request.chain_id,
            websocket_url=request.ws_url,
            exchange_addresses=self._exchange_addresses,
            watched_wallets=request.watched_wallets,
        )
        return self._bootstrap.build(config, request.signal_sink, state_store=state_store)


class DetectionSourceFactory:
    """Dispatch ``DetectionSourceRequest`` to the builder registered for its mode."""

    def __init__(self, builders: Iterable[DetectionSourceBuilder]) -> None:
        """Index builders by mode.

        Args:
            builders: Strategy objects, at most one per ``DetectionMode``.

        Raises:
            ValueError: If two builders claim the same mode or one claims
                ``POLLING`` (polling never builds an on-chain runtime).
        """

        indexed: dict[DetectionMode, DetectionSourceBuilder] = {}
        for builder in builders:
            mode = builder.mode
            if mode is DetectionMode.POLLING:
                raise ValueError("polling mode does not take a detection-source builder")
            if mode in indexed:
                raise ValueError(f"duplicate detection-source builder for {mode.value}")
            indexed[mode] = builder
        self._builders = indexed

    @property
    def supported_modes(self) -> frozenset[DetectionMode]:
        """Return the on-chain modes this factory can build."""

        return frozenset(self._builders)

    async def build(self, request: DetectionSourceRequest) -> DetectionSourceRuntime | None:
        """Build the runtime for ``request.mode``.

        Args:
            request: Shared detection-source inputs.

        Returns:
            ``None`` for ``POLLING``; otherwise the not-yet-started runtime.

        Raises:
            UnsupportedDetectionModeError: If no builder serves the mode.
        """

        if request.mode is DetectionMode.POLLING:
            return None
        builder = self._builders.get(request.mode)
        if builder is None:
            raise UnsupportedDetectionModeError(
                f"no detection-source builder registered for {request.mode.value}"
            )
        return await builder.build(request)


__all__ = [
    "ConfirmedSourceBuilder",
    "DetectionSourceBuilder",
    "DetectionSourceFactory",
    "DetectionSourceRequest",
    "DetectionSourceRuntime",
    "ProvisionalPendingSourceBuilder",
    "StateStoreProvider",
    "UnsupportedDetectionModeError",
]
