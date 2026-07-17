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


def test_onchain_wiring_imports_resolve() -> None:
    """The 3 lazy imports in app.main()'s on-chain wiring must resolve."""
    from services.onchain_detection.condition_id_resolver import GammaConditionIdResolver
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
    from services.onchain_detection.pending_tx_detector import PendingTxConfig
    from orchestrators.trade_pipeline import execute_trades

    assert GammaConditionIdResolver is not None
    assert OnchainDetectionRuntime is not None
    assert PendingTxConfig is not None
    assert execute_trades is not None


def test_pending_tx_config_fields_match_onchain_detection_config() -> None:
    """``PendingTxConfig`` fields must match the ``OnchainDetectionConfig``
    fields that ``app.main()`` maps into them. If a field is renamed on one
    side without the other, the wiring silently uses the default.
    """
    from app import _build_pending_tx_config
    from config.app_config import OnchainDetectionConfig
    from services.onchain_detection.trade_detection_models import DetectionConfidence

    oc = OnchainDetectionConfig(
        pending_tx_detection_confidence_required="mined_authoritative"
    )
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


def test_missing_wss_warning_reflects_typed_detection_config() -> None:
    from app import _onchain_missing_wss_warning_reason
    from config.app_config import OnchainDetectionConfig

    default_cfg = SimpleNamespace(onchain_detection=OnchainDetectionConfig())
    assert _onchain_missing_wss_warning_reason(default_cfg) is None

    pending_cfg = SimpleNamespace(
        onchain_detection=OnchainDetectionConfig(
            enable_onchain_pending_detection=True,
            enable_onchain_mined_detection=True,
        ),
    )
    assert _onchain_missing_wss_warning_reason(pending_cfg) == "pending_enabled"

    mined_cfg = SimpleNamespace(
        onchain_detection=OnchainDetectionConfig(
            enable_onchain_pending_detection=False,
            enable_onchain_mined_detection=True,
        ),
    )
    assert _onchain_missing_wss_warning_reason(mined_cfg) == "mined_enabled"

    with_ws_cfg = SimpleNamespace(
        onchain_detection=OnchainDetectionConfig(
            enable_onchain_pending_detection=True,
            onchain_ws_url="wss://example.invalid",
        ),
    )
    assert _onchain_missing_wss_warning_reason(with_ws_cfg) is None
