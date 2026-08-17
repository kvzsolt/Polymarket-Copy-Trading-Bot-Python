"""Regression test for the on-chain detection wiring in ``app.main()``.

The wiring code in ``app.py`` uses lazy imports inside a ``try/except`` so
an ``ImportError`` is silently swallowed and on-chain detection is
disabled without any visible failure. This test verifies the exact import
paths used by the wiring code so a wrong-path regression (e.g. importing
``PendingTxConfig`` from ``orchestrators/`` instead of ``services/``) is
caught at test time, not at runtime.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_onchain_wiring_imports_resolve() -> None:
    """The lazy imports in app.main()'s on-chain wiring must resolve."""
    from services.onchain_detection.condition_id_resolver import GammaConditionIdResolver
    from orchestrators.detection_source_factory import DetectionSourceFactory
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
    from orchestrators.onchain_signal_dispatch import OnchainSignalDispatchAdapter
    from services.onchain_detection.pending_tx_detector import PendingTxConfig
    from orchestrators.trade_pipeline import execute_trades

    assert GammaConditionIdResolver is not None
    assert DetectionSourceFactory is not None
    assert OnchainSignalDispatchAdapter is not None
    assert OnchainDetectionRuntime is not None
    assert PendingTxConfig is not None
    assert execute_trades is not None


@pytest.mark.asyncio
async def test_app_composes_factory_from_config() -> None:
    """``app.main`` builds one factory covering both on-chain modes from config."""

    from app import _build_detection_source_factory
    from config.app_config import DetectionConfig, OnchainDetectionConfig
    from domain.detection import DetectionMode
    from orchestration.polygon_source_transaction_runtime import (
        PolygonSourceTransactionRuntime,
    )
    from orchestrators.detection_source_factory import (
        DetectionSourceFactory,
        DetectionSourceRequest,
    )
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
    from services.onchain_detection.trade_detection_models import DetectionConfidence

    config = SimpleNamespace(
        detection=DetectionConfig(
            mode="on_chain_pending",
            onchain=OnchainDetectionConfig(
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_tx_max_signal_age_ms=321,
                pending_tx_detection_confidence_required="mined_authoritative",
            ),
        ),
        wallet=SimpleNamespace(chain_id=137),
    )

    async def _state_store_provider():
        return None

    async def _sink(_signal) -> None:
        return None

    factory = _build_detection_source_factory(config, state_store_provider=_state_store_provider)

    assert isinstance(factory, DetectionSourceFactory)
    assert factory.supported_modes == frozenset(
        {DetectionMode.ON_CHAIN_PENDING, DetectionMode.ON_CHAIN_CONFIRMED}
    )

    def _request(mode: DetectionMode) -> DetectionSourceRequest:
        return DetectionSourceRequest(
            mode=mode,
            watched_wallets=frozenset({"0x" + "11" * 20}),
            ws_url="wss://polygon.example.invalid/v2/redacted",
            chain_id=137,
            signal_sink=_sink,
        )

    assert await factory.build(_request(DetectionMode.POLLING)) is None
    pending = await factory.build(_request(DetectionMode.ON_CHAIN_PENDING))
    assert isinstance(pending, OnchainDetectionRuntime)
    assert pending._enable_pending is True
    assert pending._enable_mined is False
    assert pending._pending_config.max_signal_age_ms == 321
    assert pending._pending_config.confidence_required is DetectionConfidence.MINED_AUTHORITATIVE
    confirmed = await factory.build(_request(DetectionMode.ON_CHAIN_CONFIRMED))
    assert isinstance(confirmed, PolygonSourceTransactionRuntime)


def test_confirmed_onchain_runtime_builder_uses_new_polygon_composition() -> None:
    """The app's confirmed builder composes the head-driven confirmation runtime."""

    from app import _confirmed_source_builder
    from domain.detection import DetectionMode
    from orchestration.polygon_source_transaction_runtime import (
        PolygonSourceTransactionRuntime,
    )
    from orchestrators.detection_source_factory import (
        ConfirmedSourceBuilder,
        DetectionSourceRequest,
    )

    async def sink(_signal) -> None:
        return None

    async def _no_store():
        return None

    builder = _confirmed_source_builder(_no_store)
    assert isinstance(builder, ConfirmedSourceBuilder)
    assert not hasattr(__import__("app"), "_build_confirmed_onchain_runtime")

    runtime = builder.compose(
        DetectionSourceRequest(
            mode=DetectionMode.ON_CHAIN_CONFIRMED,
            watched_wallets=frozenset({"0x" + "11" * 20}),
            ws_url="wss://polygon.example.invalid/v2/redacted",
            chain_id=137,
            signal_sink=sink,
        ),
        None,
    )

    assert isinstance(runtime, PolygonSourceTransactionRuntime)


@pytest.mark.asyncio
async def test_failed_runtime_start_is_stopped_before_being_dropped() -> None:
    """A runtime whose ``start()`` raises is stopped (WS/dispatcher tasks
    released) before ``app.main`` continues without on-chain detection."""
    from app import _start_onchain_runtime
    from domain.detection import DetectionMode

    class _Runtime:
        def __init__(self, *, stop_raises: bool = False) -> None:
            self.started = 0
            self.stopped = 0
            self._stop_raises = stop_raises

        async def start(self) -> None:
            self.started += 1
            raise RuntimeError("ws connect failed")

        async def stop(self) -> None:
            self.stopped += 1
            if self._stop_raises:
                raise RuntimeError("stop failed too")

    runtime = _Runtime()
    with pytest.raises(RuntimeError, match="ws connect failed"):
        await _start_onchain_runtime(runtime, mode=DetectionMode.ON_CHAIN_CONFIRMED)
    assert runtime.started == 1
    assert runtime.stopped == 1

    noisy = _Runtime(stop_raises=True)
    with pytest.raises(RuntimeError, match="ws connect failed"):
        await _start_onchain_runtime(noisy, mode=DetectionMode.ON_CHAIN_CONFIRMED)
    assert noisy.stopped == 1


@pytest.mark.asyncio
async def test_successful_runtime_start_is_not_stopped() -> None:
    from app import _start_onchain_runtime
    from domain.detection import DetectionMode

    class _Runtime:
        def __init__(self) -> None:
            self.started = 0
            self.stopped = 0

        async def start(self) -> None:
            self.started += 1

        async def stop(self) -> None:
            self.stopped += 1

    runtime = _Runtime()
    await _start_onchain_runtime(runtime, mode=DetectionMode.ON_CHAIN_PENDING)
    assert (runtime.started, runtime.stopped) == (1, 0)


def test_pending_tx_config_fields_match_onchain_detection_config() -> None:
    """``PendingTxConfig`` fields must match the ``OnchainDetectionConfig``
    fields that ``app.main()`` maps into them. If a field is renamed on one
    side without the other, the wiring silently uses the default.
    """
    from app import _build_pending_tx_config
    from config.app_config import OnchainDetectionConfig
    from services.onchain_detection.trade_detection_models import DetectionConfidence

    oc = OnchainDetectionConfig(pending_tx_detection_confidence_required="mined_authoritative")
    ptx = _build_pending_tx_config(oc)
    assert ptx.max_signal_age_ms == oc.pending_tx_max_signal_age_ms
    assert ptx.hashes_only == oc.pending_tx_hashes_only
    assert ptx.confidence_required == DetectionConfidence.MINED_AUTHORITATIVE


def test_onchain_condition_resolver_builder_returns_callable() -> None:
    from app import _build_onchain_condition_id_resolver

    assert callable(_build_onchain_condition_id_resolver())


def test_hot_added_leader_is_pushed_to_the_onchain_watched_set() -> None:
    """Discovery promotes leaders after startup; the WS filters must follow.

    ``OnchainDetectionRuntime`` is constructed once with the startup leaders,
    so without this refresh a hot-added leader never matches the mined/pending
    decoders and silently loses the fast-path until the next restart.
    """
    from app import _refresh_onchain_watched_wallets

    class _Runtime:
        def __init__(self) -> None:
            self.watched: set[str] | None = None

        def update_watched_wallets(self, wallets: set[str]) -> None:
            self.watched = wallets

    runtime = _Runtime()

    assert _refresh_onchain_watched_wallets(runtime, {"0xAAA", "0xBbB"}) is True
    assert runtime.watched == {"0xaaa", "0xbbb"}


def test_refresh_onchain_watched_wallets_without_a_runtime_is_a_noop() -> None:
    """On-chain detection is optional: absent/disabled must not break hot-add."""
    from app import _refresh_onchain_watched_wallets

    assert _refresh_onchain_watched_wallets(None, {"0xAAA"}) is False


def test_refresh_onchain_watched_wallets_swallows_runtime_errors() -> None:
    """A detection-layer failure must not take down leader hot-add."""
    from app import _refresh_onchain_watched_wallets

    class _Broken:
        def update_watched_wallets(self, wallets: set[str]) -> None:
            raise RuntimeError("ws update failed")

    assert _refresh_onchain_watched_wallets(_Broken(), {"0xAAA"}) is False


def test_onchain_runtime_accepts_the_refresh_the_wiring_performs() -> None:
    """Contract check against the real runtime, not just a stand-in."""
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime

    async def _sink(signal) -> None:  # pragma: no cover - never dispatched
        raise AssertionError("no signal expected")

    runtime = OnchainDetectionRuntime(
        watched_wallets={"0xstartup"},
        ws_url="wss://example.invalid",
        signal_sink=_sink,
    )

    from app import _refresh_onchain_watched_wallets

    assert _refresh_onchain_watched_wallets(runtime, {"0xSTARTUP", "0xNew"}) is True
    assert runtime._watched == {"0xstartup", "0xnew"}
