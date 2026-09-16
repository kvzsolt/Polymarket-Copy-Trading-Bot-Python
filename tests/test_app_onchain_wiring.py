"""Regression test for the on-chain detection wiring in ``app.main()``.

The wiring code in ``app.py`` uses lazy imports inside a ``try/except`` so
an ``ImportError`` is silently swallowed and on-chain detection is
disabled without any visible failure. This test verifies the exact import
paths used by the wiring code so a wrong-path regression (e.g. importing
``PendingTxConfig`` from ``orchestrators/`` instead of ``services/``) is
caught at test time, not at runtime.
"""

from __future__ import annotations

import ast
from pathlib import Path
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
                pending_tx_detection_confidence_required="pending_probable",
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
    assert pending._pending_config.confidence_required is DetectionConfidence.PENDING_PROBABLE
    confirmed = await factory.build(_request(DetectionMode.ON_CHAIN_CONFIRMED))
    assert isinstance(confirmed, PolygonSourceTransactionRuntime)


@pytest.mark.asyncio
async def test_app_composes_the_logs_only_confirmed_runtime_from_config() -> None:
    """``confirmed_source: logs_only`` routes the confirmed mode to the gen-1
    mined-only runtime whose subscriptions embed the followed leaders."""
    from app import _build_detection_source_factory
    from config.app_config import DetectionConfig, OnchainDetectionConfig
    from domain.detection import DetectionMode
    from orchestrators.detection_source_factory import DetectionSourceRequest
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime

    config = SimpleNamespace(
        detection=DetectionConfig(
            mode="on_chain_confirmed",
            onchain=OnchainDetectionConfig(
                confirmed_source="logs_only",
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
            ),
        ),
        wallet=SimpleNamespace(chain_id=137),
    )

    async def _state_store_provider():
        return None

    async def _sink(_signal) -> None:
        return None

    factory = _build_detection_source_factory(config, state_store_provider=_state_store_provider)
    runtime = await factory.build(
        DetectionSourceRequest(
            mode=DetectionMode.ON_CHAIN_CONFIRMED,
            confirmed_source="logs_only",
            watched_wallets=frozenset({"0x" + "11" * 20}),
            ws_url="wss://polygon.example.invalid/v2/redacted",
            chain_id=137,
            signal_sink=_sink,
        )
    )

    assert isinstance(runtime, OnchainDetectionRuntime)
    assert runtime._enable_pending is False
    assert runtime._enable_mined is True
    assert runtime._mined_params_factory is not None
    maker_params, taker_params = runtime._mined_params_factory(frozenset({"0x" + "11" * 20}))
    assert maker_params[1]["topics"][2] == ["0x" + "0" * 24 + "11" * 20]
    assert taker_params[1]["topics"][3] == ["0x" + "0" * 24 + "11" * 20]


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


def test_detection_confidence_from_config_fails_closed() -> None:
    """An unparseable confidence floor must raise, never default silently.

    Defaulting to ``PENDING_PROBABLE`` turned a typo (or a future enum rename)
    into a live detection policy the operator never configured. The config
    loader already rejects the value; this is the defence-in-depth for a
    directly constructed config.
    """
    from app import _detection_confidence_from_config
    from config.app_config import EnvValidationError
    from services.onchain_detection.trade_detection_models import DetectionConfidence

    for unparseable in ("almost_certain", "PENDING-PROBABLE", "", None):
        with pytest.raises(EnvValidationError) as excinfo:
            _detection_confidence_from_config(unparseable)
        message = str(excinfo.value)
        assert "pending_tx_detection_confidence_required" in message
        # The raw value is never echoed: a mis-mapped override could carry a
        # secret, and the config layer's message for this field omits it too.
        if unparseable:
            assert str(unparseable) not in message

    assert (
        _detection_confidence_from_config("pending_probable")
        is DetectionConfidence.PENDING_PROBABLE
    )
    assert (
        _detection_confidence_from_config("  Mined_Authoritative  ")
        is DetectionConfidence.MINED_AUTHORITATIVE
    )


def test_build_pending_tx_config_propagates_the_fail_closed_error() -> None:
    """The startup builder must not absorb the invalid-confidence failure."""
    from app import _build_pending_tx_config
    from config.app_config import EnvValidationError, OnchainDetectionConfig

    broken = OnchainDetectionConfig(pending_tx_detection_confidence_required="not_a_confidence")
    with pytest.raises(EnvValidationError):
        _build_pending_tx_config(broken)


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


# ---------------------------------------------------------------------------
# Pending-copy confirmation tracker + WS health reporter composition
# ---------------------------------------------------------------------------


def _pending_config(**overrides):
    from config.app_config import DetectionConfig, OnchainDetectionConfig

    onchain_kwargs = {
        "pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted",
        "pending_tx_detection_confidence_required": "pending_probable",
    }
    onchain_kwargs.update(overrides.pop("onchain", {}))
    return SimpleNamespace(
        detection=DetectionConfig(
            mode=overrides.pop("mode", "on_chain_pending"),
            onchain=OnchainDetectionConfig(**onchain_kwargs),
        ),
        polymarket=SimpleNamespace(
            rpc_url=overrides.pop("rpc_url", "https://polygon.example.invalid/v2/primary"),
            fallback_rpc_url=overrides.pop("fallback_rpc_url", ""),
        ),
        wallet=SimpleNamespace(chain_id=137),
        execution=SimpleNamespace(paper_trade_only=overrides.pop("paper_trade_only", False)),
    )


def _captured_builder_records(config, level: int) -> list[str]:
    """Build the tracker while capturing ``app.logger`` at ``level``.

    ``app``'s child logger does not propagate to the root, so ``caplog`` alone
    would observe nothing; attach a handler directly to it.

    Args:
        config: Configuration to hand :func:`app._build_pending_copy_tracker`.
        level: Minimum record level to capture.

    Returns:
        The formatted messages emitted during the build.
    """

    import logging

    import app as app_module
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture(level=level)
    previous_level = app_module.logger.level
    app_module.logger.addHandler(handler)
    app_module.logger.setLevel(level)
    try:
        app_module._build_pending_copy_tracker(
            config,
            telemetry_service=None,
            metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        )
    finally:
        app_module.logger.setLevel(previous_level)
        app_module.logger.removeHandler(handler)
    return [record.getMessage() for record in records]


def test_pending_copy_tracker_is_built_in_pending_mode() -> None:
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
    from services.onchain_detection.pending_copy_confirmation_tracker import (
        PendingCopyConfirmationTracker,
    )

    tracker = _build_pending_copy_tracker(
        _pending_config(),
        telemetry_service=None,
        metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
    )

    assert isinstance(tracker, PendingCopyConfirmationTracker)
    assert tracker.tracked_count == 0


def test_pending_copy_tracker_is_not_built_outside_pending_mode() -> None:
    """The tracker is a pending-mode safeguard; other modes must not pay for it."""
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    metrics = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)

    for mode in ("polling", "on_chain_confirmed"):
        assert (
            _build_pending_copy_tracker(
                _pending_config(mode=mode), telemetry_service=None, metrics_registry=metrics
            )
            is None
        )


def test_pending_copy_tracker_respects_the_disable_flag() -> None:
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    config = _pending_config(onchain={"pending_copy_confirmation_enabled": False})

    assert (
        _build_pending_copy_tracker(
            config,
            telemetry_service=None,
            metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        )
        is None
    )


def test_pending_copy_tracker_is_not_built_without_an_rpc_endpoint() -> None:
    """Receipt confirmation needs HTTP RPC; without it the tracker stays off."""
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    assert (
        _build_pending_copy_tracker(
            _pending_config(rpc_url=""),
            telemetry_service=None,
            metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        )
        is None
    )


def test_pending_copy_tracker_uses_the_configured_bounds() -> None:
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    config = _pending_config(
        onchain={
            "pending_copy_confirmation_deadline_seconds": 42.0,
            "pending_copy_confirmation_max_tracked": 7,
        }
    )

    tracker = _build_pending_copy_tracker(
        config,
        telemetry_service=None,
        metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
    )

    assert tracker is not None
    assert tracker.deadline_seconds == 42.0
    assert tracker.max_tracked == 7
    # Auto-unwind is opt-in and off by default: no hook may be wired.
    assert tracker.unwind_enabled is False


def test_auto_unwind_flag_does_not_wire_a_hook_yet() -> None:
    """The flag exists but the requester is design-only; enabling must be inert."""
    from app import _build_pending_copy_tracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    tracker = _build_pending_copy_tracker(
        _pending_config(onchain={"pending_copy_auto_unwind_enabled": True}),
        telemetry_service=None,
        metrics_registry=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
    )

    assert tracker is not None
    assert tracker.unwind_enabled is False


def test_onchain_ws_health_reporter_is_registered_in_pending_mode() -> None:
    from app import _build_onchain_ws_health_reporter
    from domain.detection import DetectionMode
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
    from services.onchain_detection.ws_health_reporter import OnchainWsHealthReporter

    readiness = ReadinessTracker()
    reporter = _build_onchain_ws_health_reporter(
        readiness,
        MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        mode=DetectionMode.ON_CHAIN_PENDING,
    )

    assert isinstance(reporter, OnchainWsHealthReporter)
    assert readiness.snapshot()["checks"]["onchain_ws_ok"]["required"] is False


def test_no_ws_health_check_is_registered_outside_pending_mode() -> None:
    """Only the gen-1 runtime opens ``OnchainWebSocket``.

    The gen-2 confirmed runtime uses its own connection manager, so an
    ``onchain_ws_ok`` check there could never be marked ready — a permanent
    false "WS down" in the operator snapshot.
    """
    from app import _build_onchain_ws_health_reporter
    from domain.detection import DetectionMode
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    for mode in (DetectionMode.POLLING, DetectionMode.ON_CHAIN_CONFIRMED):
        readiness = ReadinessTracker()

        reporter = _build_onchain_ws_health_reporter(
            readiness,
            MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
            mode=mode,
        )

        assert reporter is None, mode
        assert "onchain_ws_ok" not in readiness.snapshot()["checks"], mode


def test_onchain_ws_health_reporter_is_registered_for_confirmed_logs_only() -> None:
    """The logs-only confirmed runtime opens gen-1 ``OnchainWebSocket``
    subscriptions (stream ``orderfilled``), so its health is observable."""
    from app import _build_onchain_ws_health_reporter
    from domain.detection import DetectionMode
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
    from services.onchain_detection.ws_health_reporter import OnchainWsHealthReporter

    readiness = ReadinessTracker()
    reporter = _build_onchain_ws_health_reporter(
        readiness,
        MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        mode=DetectionMode.ON_CHAIN_CONFIRMED,
        confirmed_source="logs_only",
    )

    assert isinstance(reporter, OnchainWsHealthReporter)
    assert readiness.snapshot()["checks"]["onchain_ws_ok"]["required"] is False


def test_no_ws_health_check_is_registered_for_confirmed_mempool_reserved() -> None:
    from app import _build_onchain_ws_health_reporter
    from domain.detection import DetectionMode
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    readiness = ReadinessTracker()
    reporter = _build_onchain_ws_health_reporter(
        readiness,
        MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        mode=DetectionMode.ON_CHAIN_CONFIRMED,
        confirmed_source="mempool_reserved",
    )

    assert reporter is None
    assert "onchain_ws_ok" not in readiness.snapshot()["checks"]


@pytest.mark.asyncio
async def test_detection_factory_forwards_the_health_reporting_socket_factory() -> None:
    """The reporter must reach the socket the gen-1 runtime actually opens."""
    from app import (
        _build_detection_source_factory,
        _build_onchain_websocket_factory,
        _build_onchain_ws_health_reporter,
    )
    from domain.detection import DetectionMode
    from orchestrators.detection_source_factory import DetectionSourceRequest
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS

    reporter = _build_onchain_ws_health_reporter(
        ReadinessTracker(),
        MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        mode=DetectionMode.ON_CHAIN_PENDING,
    )

    async def _state_store_provider():
        return None

    async def _sink(_signal) -> None:
        return None

    factory = _build_detection_source_factory(
        _pending_config(),
        state_store_provider=_state_store_provider,
        websocket_factory=_build_onchain_websocket_factory(reporter),
    )
    runtime = await factory.build(
        DetectionSourceRequest(
            mode=DetectionMode.ON_CHAIN_PENDING,
            watched_wallets=frozenset({"0x" + "11" * 20}),
            ws_url="wss://polygon.example.invalid/v2/redacted",
            chain_id=137,
            signal_sink=_sink,
        )
    )

    built = runtime._websocket_factory(
        ws_url="wss://polygon.example.invalid/v2/redacted",
        subscribe_params=["alchemy_pendingTransactions", {}],
        name="pendingtx",
        on_message=_sink,
    )
    assert built._state_listener is reporter


def test_websocket_factory_binds_the_listener_and_degrades_to_the_plain_client() -> None:
    """``app`` builds the socket constructor; the listener is optional."""
    from app import _build_onchain_websocket_factory
    from clients.onchain_ws import OnchainWebSocket

    assert _build_onchain_websocket_factory(None) is OnchainWebSocket

    listener = object()
    factory = _build_onchain_websocket_factory(listener)
    built = factory(
        ws_url="wss://polygon.example.invalid/v2/redacted",
        subscribe_params=["alchemy_pendingTransactions", {}],
        name="pendingtx",
        on_message=_noop_on_message,
    )

    assert isinstance(built, OnchainWebSocket)
    assert built._state_listener is listener


async def _noop_on_message(_message: object) -> None:
    return None


def test_the_confidence_parse_sits_outside_the_onchain_start_guard() -> None:
    """Inside that guard a bad floor degraded silently to REST polling.

    The operator asked for ``on_chain_pending`` and would have got polling
    with only a warning line. Structural guard for the behavioural test below.
    """
    import inspect

    import app

    source = inspect.getsource(app._run_application)
    parse_at = source.index("_validate_detection_confidence_floor(")
    guard_at = source.index("onchain_detection_start_failed")
    try_at = source.rindex("try:", 0, guard_at)

    assert parse_at < try_at, (
        "the confidence floor is parsed inside the on-chain start try/except, "
        "so an invalid floor degrades to polling instead of aborting startup"
    )


@pytest.mark.asyncio
async def test_startup_aborts_on_an_unparseable_confidence_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-polling mode with an invalid floor must fail closed at startup.

    The abort happens before any runtime, socket, or database pool is built,
    so the process exits 1 with nothing half-started.
    """
    import app as app_module
    from config.app_config import AppConfig, DetectionConfig, OnchainDetectionConfig

    config = AppConfig(
        detection=DetectionConfig(
            mode="on_chain_pending",
            onchain=OnchainDetectionConfig(
                pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
                pending_tx_detection_confidence_required="almost_certain",
            ),
        )
    )

    errors: list[str] = []
    monkeypatch.setattr(app_module, "load_config", lambda *_a, **_k: config)
    monkeypatch.setattr(app_module, "configure_logging", lambda *_a, **_k: None)
    monkeypatch.setattr(app_module, "print_error", errors.append)

    def _never_called(*_args: object, **_kwargs: object):
        raise AssertionError("startup must abort before composing the application")

    monkeypatch.setattr(app_module, "build_app", _never_called)

    assert await app_module._run_application([]) == 1
    assert errors
    assert "pending_tx_detection_confidence_required" in errors[0]
    assert "almost_certain" not in errors[0]


@pytest.mark.asyncio
async def test_polling_mode_startup_does_not_validate_the_pending_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The floor is a pending-path policy; polling must not fail on it."""
    import app as app_module
    from config.app_config import AppConfig, DetectionConfig, OnchainDetectionConfig

    config = AppConfig(
        detection=DetectionConfig(
            mode="polling",
            onchain=OnchainDetectionConfig(
                pending_tx_detection_confidence_required="almost_certain",
            ),
        )
    )

    errors: list[str] = []
    calls: list[int] = []
    monkeypatch.setattr(app_module, "load_config", lambda *_a, **_k: config)
    monkeypatch.setattr(app_module, "print_error", errors.append)

    def _stop_after_the_validation_block(*_args: object, **_kwargs: object) -> None:
        # ``configure_logging`` runs once at entry and again once the config
        # is loaded and validated; stopping on the second call proves startup
        # got PAST the confidence-floor check instead of returning 1.
        calls.append(1)
        if len(calls) >= 2:
            raise RuntimeError("stop the test here")

    monkeypatch.setattr(app_module, "configure_logging", _stop_after_the_validation_block)

    with pytest.raises(RuntimeError, match="stop the test here"):
        await app_module._run_application([])

    assert len(calls) == 2
    assert errors == []


class _StoppableTracker:
    """Records ``stop()`` awaits; optionally raises to prove teardown continues."""

    def __init__(self, *, raises: bool = False) -> None:
        self.stops = 0
        self._raises = raises

    async def stop(self) -> None:
        self.stops += 1
        if self._raises:
            raise RuntimeError("tracker stop exploded")


@pytest.mark.asyncio
async def test_shutdown_stops_the_pending_copy_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    """A live confirmation loop must not survive shutdown."""
    import app as app_module
    import storage.postgres.connection as connection

    pool_closed = {"n": 0}

    async def _spy() -> None:
        pool_closed["n"] += 1

    monkeypatch.setattr(connection, "shutdown_pool", _spy)
    tracker = _StoppableTracker()

    await app_module._shutdown_runtime(
        monitor_tasks=[],
        supervised=[],
        market_data_runtimes={},
        user_data_runtimes=[],
        metrics_runner=None,
        pending_copy_tracker=tracker,
    )

    assert tracker.stops == 1
    assert pool_closed["n"] == 1


@pytest.mark.asyncio
async def test_a_failing_tracker_stop_is_logged_and_shutdown_continues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A teardown hiccup must never mask the real shutdown reason."""
    import logging

    import app as app_module
    import storage.postgres.connection as connection

    pool_closed = {"n": 0}

    async def _spy() -> None:
        pool_closed["n"] += 1

    monkeypatch.setattr(connection, "shutdown_pool", _spy)
    tracker = _StoppableTracker(raises=True)

    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    # ``app``'s child logger does not propagate to the root, so caplog alone
    # would see nothing; attach directly to it.
    handler = _Capture(level=logging.WARNING)
    app_module.logger.addHandler(handler)
    try:
        await app_module._shutdown_runtime(
            monitor_tasks=[],
            supervised=[],
            market_data_runtimes={},
            user_data_runtimes=[],
            metrics_runner=None,
            pending_copy_tracker=tracker,
        )
    finally:
        app_module.logger.removeHandler(handler)

    assert tracker.stops == 1
    # The remaining steps still ran.
    assert pool_closed["n"] == 1
    assert any("pending_copy_tracker_shutdown_failed" in record.getMessage() for record in records)


def _run_application_node() -> ast.AsyncFunctionDef:
    """Return the parsed ``_run_application`` coroutine from ``app.py``."""

    source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_run_application":
            return node
    raise AssertionError("app.py must define _run_application")


def _shutdown_guarded_try() -> ast.Try:
    """Return the ``try`` in ``_run_application`` whose ``finally`` tears down."""

    for node in ast.walk(_run_application_node()):
        if not isinstance(node, ast.Try) or not node.finalbody:
            continue
        for final_node in ast.walk(ast.Module(body=node.finalbody, type_ignores=[])):
            func = final_node.func if isinstance(final_node, ast.Call) else None
            if isinstance(func, ast.Name) and func.id == "_shutdown_runtime":
                return node
    raise AssertionError("_run_application must tear down runtimes in a finally block")


def _call_lines(node: ast.AST, func_name: str) -> list[int]:
    """Return every line in ``node`` that invokes the named bare function."""

    lines: list[int] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        if isinstance(func, ast.Name) and func.id == func_name:
            lines.append(child.lineno)
    return sorted(lines)


@pytest.mark.parametrize(
    "builder",
    ("_build_pending_copy_tracker", "_build_onchain_ws_health_reporter"),
)
def test_observability_collaborators_are_built_under_the_shutdown_guard(builder: str) -> None:
    """A construction failure must still tear down the runtimes already started.

    The tracker and the WS health reporter are deliberately built outside the
    ``except`` that degrades on-chain detection to REST polling — a structural
    fault must fail loud, not silently downgrade the operator's chosen mode.
    Being outside the ``try`` that owns ``_shutdown_runtime`` is a different
    thing entirely: it leaked every started market/user runtime, supervised
    task, and connection pool on the way out.
    """

    guarded = _shutdown_guarded_try()
    built = _call_lines(_run_application_node(), builder)

    assert built, f"app.py must call {builder}"
    body_start = guarded.body[0].lineno
    body_end = max(guarded.body[-1].end_lineno or 0, body_start)
    for line in built:
        assert body_start <= line <= body_end, (
            f"{builder} is called at line {line}, outside the try "
            f"({body_start}-{body_end}) whose finally runs _shutdown_runtime — "
            "a construction failure there leaks every started runtime"
        )


def test_paper_mode_composition_announces_that_the_tracker_records_nothing() -> None:
    """Paper mode never reaches the venue, so the tracker can never be exercised.

    ``OnchainSignalDispatchAdapter`` only tracks a copy whose pipeline outcome
    reached the venue; in paper mode every copy stops at the ``paper`` tier
    before submission, so ``track()`` is never called. Without this line an
    operator reads a soak with zero ``pending_copy_*`` output as healthy when
    it in fact proves nothing.
    """

    import logging

    messages = _captured_builder_records(_pending_config(paper_trade_only=True), logging.INFO)

    assert any("pending_copy_tracker_idle_in_paper_mode" in message for message in messages)


def test_live_mode_composition_stays_quiet_about_the_paper_caveat() -> None:
    """The idle notice must not fire for a deployment that does reach the venue."""

    import logging

    messages = _captured_builder_records(_pending_config(paper_trade_only=False), logging.INFO)

    assert not any("pending_copy_tracker_idle_in_paper_mode" in message for message in messages)
