"""Tests for ``orchestrators.detection_source_factory``.

The factory maps ``DetectionMode`` onto exactly one on-chain detection
runtime builder. Polling builds nothing (REST polling always runs), the
provisional pending mode builds the gen-1 ``OnchainDetectionRuntime`` with
the pending subscription only, and the confirmed mode builds the gen-2
``PolygonSourceTransactionRuntime`` after awaiting its state store.
"""

from __future__ import annotations

import pytest

from domain.detection import DetectionMode
from orchestrators.detection_source_factory import (
    ConfirmedSourceBuilder,
    DetectionSourceFactory,
    DetectionSourceRequest,
    ProvisionalPendingSourceBuilder,
    UnsupportedDetectionModeError,
)
from services.onchain_detection.pending_tx_detector import PendingTxConfig
from services.onchain_detection.trade_detection_models import DetectionConfidence

_LEADER = "0x" + "11" * 20
_WS_URL = "wss://polygon.example.invalid/v2/redacted"
_EXCHANGES = ("0x" + "aa" * 20, "0x" + "bb" * 20)


async def _sink(_signal) -> None:
    return None


async def _resolver(_token_id: str) -> str:
    return ""


def _request(mode: DetectionMode) -> DetectionSourceRequest:
    return DetectionSourceRequest(
        mode=mode,
        watched_wallets=frozenset({_LEADER}),
        ws_url=_WS_URL,
        chain_id=137,
        signal_sink=_sink,
    )


class _RecordingRuntime:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    def update_watched_wallets(self, wallets: set[str]) -> None:
        self.kwargs["watched_wallets"] = wallets


def _pending_builder() -> ProvisionalPendingSourceBuilder:
    return ProvisionalPendingSourceBuilder(
        pending_config=PendingTxConfig(
            max_signal_age_ms=250,
            hashes_only=False,
            confidence_required=DetectionConfidence.PENDING_PROBABLE,
        ),
        condition_id_resolver=_resolver,
        runtime_factory=_RecordingRuntime,
    )


class _FakeStateStore:
    pass


class _StateStoreProvider:
    def __init__(self) -> None:
        self.calls = 0
        self.store = _FakeStateStore()

    async def __call__(self):
        self.calls += 1
        return self.store


class _RecordingBootstrap:
    def __init__(self) -> None:
        self.config = None
        self.state_store = None
        self.runtime = _RecordingRuntime()

    def build(self, config, signal_callback, state_store=None):
        self.config = config
        self.state_store = state_store
        self.signal_callback = signal_callback
        return self.runtime


def _confirmed_builder(
    provider: _StateStoreProvider | None = None,
    bootstrap: _RecordingBootstrap | None = None,
) -> ConfirmedSourceBuilder:
    return ConfirmedSourceBuilder(
        exchange_addresses=_EXCHANGES,
        state_store_provider=provider or _StateStoreProvider(),
        bootstrap=bootstrap or _RecordingBootstrap(),
    )


@pytest.mark.asyncio
async def test_polling_mode_builds_no_runtime() -> None:
    factory = DetectionSourceFactory([_pending_builder(), _confirmed_builder()])

    assert await factory.build(_request(DetectionMode.POLLING)) is None


@pytest.mark.asyncio
async def test_on_chain_pending_builds_gen1_runtime_with_pending_only() -> None:
    factory = DetectionSourceFactory([_pending_builder(), _confirmed_builder()])

    runtime = await factory.build(_request(DetectionMode.ON_CHAIN_PENDING))

    assert isinstance(runtime, _RecordingRuntime)
    assert runtime.kwargs["enable_pending"] is True
    # Deliberate: gen-1 mined logs carry no ``log_index``-free key that dedups
    # against the pending signal, so mined stays off in pending mode.
    assert runtime.kwargs["enable_mined"] is False
    assert runtime.kwargs["watched_wallets"] == {_LEADER}
    assert runtime.kwargs["ws_url"] == _WS_URL
    assert runtime.kwargs["signal_sink"] is _sink
    assert runtime.kwargs["pending_config"].max_signal_age_ms == 250
    assert runtime.kwargs["condition_id_resolver"] is _resolver
    assert runtime.started is False


@pytest.mark.asyncio
async def test_on_chain_pending_default_factory_is_gen1_runtime() -> None:
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime

    builder = ProvisionalPendingSourceBuilder(
        pending_config=PendingTxConfig(),
        condition_id_resolver=_resolver,
    )

    runtime = await builder.build(_request(DetectionMode.ON_CHAIN_PENDING))

    assert isinstance(runtime, OnchainDetectionRuntime)
    assert runtime._enable_pending is True
    assert runtime._enable_mined is False


@pytest.mark.asyncio
async def test_pending_builder_forwards_websocket_factory() -> None:
    """The health-reporting socket factory must reach the gen-1 runtime.

    ``app.main`` injects ``functools.partial(OnchainWebSocket,
    state_listener=reporter)`` here; if the builder swallowed it the WS
    connection state would silently stop being reported.
    """
    from clients.onchain_ws import OnchainWebSocket

    def _socket_factory(**_kwargs: object) -> object:
        raise AssertionError("never constructed in this test")

    builder = ProvisionalPendingSourceBuilder(
        pending_config=PendingTxConfig(),
        condition_id_resolver=_resolver,
        runtime_factory=_RecordingRuntime,
        websocket_factory=_socket_factory,
    )

    runtime = await builder.build(_request(DetectionMode.ON_CHAIN_PENDING))

    assert isinstance(runtime, _RecordingRuntime)
    assert runtime.kwargs["websocket_factory"] is _socket_factory

    default_builder = ProvisionalPendingSourceBuilder(
        pending_config=PendingTxConfig(),
        condition_id_resolver=_resolver,
        runtime_factory=_RecordingRuntime,
    )
    default_runtime = await default_builder.build(_request(DetectionMode.ON_CHAIN_PENDING))

    assert isinstance(default_runtime, _RecordingRuntime)
    assert default_runtime.kwargs["websocket_factory"] is OnchainWebSocket


@pytest.mark.asyncio
async def test_pending_builder_websocket_factory_reaches_the_real_runtime() -> None:
    """Contract check against ``OnchainDetectionRuntime``, not just a stand-in."""
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime

    def _socket_factory(**_kwargs: object) -> object:
        raise AssertionError("never constructed in this test")

    builder = ProvisionalPendingSourceBuilder(
        pending_config=PendingTxConfig(),
        condition_id_resolver=_resolver,
        websocket_factory=_socket_factory,
    )

    runtime = await builder.build(_request(DetectionMode.ON_CHAIN_PENDING))

    assert isinstance(runtime, OnchainDetectionRuntime)
    assert runtime._websocket_factory is _socket_factory


@pytest.mark.asyncio
async def test_on_chain_confirmed_builds_polygon_source_runtime_and_awaits_state_store() -> None:
    provider = _StateStoreProvider()
    bootstrap = _RecordingBootstrap()
    factory = DetectionSourceFactory([_confirmed_builder(provider, bootstrap)])

    runtime = await factory.build(_request(DetectionMode.ON_CHAIN_CONFIRMED))

    assert runtime is bootstrap.runtime
    assert provider.calls == 1
    assert bootstrap.state_store is provider.store
    assert bootstrap.signal_callback is _sink
    assert bootstrap.config is not None
    assert bootstrap.config.chain_id == 137
    assert bootstrap.config.websocket_url == _WS_URL
    assert bootstrap.config.exchange_addresses == _EXCHANGES
    assert bootstrap.config.watched_wallets == frozenset({_LEADER})


@pytest.mark.asyncio
async def test_on_chain_confirmed_default_bootstrap_builds_polygon_runtime() -> None:
    from orchestration.polygon_source_transaction_runtime import (
        PolygonSourceTransactionRuntime,
    )

    builder = ConfirmedSourceBuilder(
        exchange_addresses=_EXCHANGES,
        state_store_provider=_StateStoreProvider(),
    )

    runtime = await builder.build(_request(DetectionMode.ON_CHAIN_CONFIRMED))

    assert isinstance(runtime, PolygonSourceTransactionRuntime)


@pytest.mark.asyncio
async def test_unknown_mode_raises() -> None:
    factory = DetectionSourceFactory([_pending_builder()])

    with pytest.raises(UnsupportedDetectionModeError):
        await factory.build(_request(DetectionMode.ON_CHAIN_CONFIRMED))


def test_duplicate_builder_mode_rejected() -> None:
    with pytest.raises(ValueError):
        DetectionSourceFactory([_pending_builder(), _pending_builder()])
