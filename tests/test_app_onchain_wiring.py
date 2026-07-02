"""Regression test for the on-chain detection wiring in ``app.main()``.

The wiring code in ``app.py`` uses lazy imports inside a ``try/except`` so
an ``ImportError`` is silently swallowed and on-chain detection is
disabled without any visible failure. This test verifies the exact import
paths used by the wiring code so a wrong-path regression (e.g. importing
``PendingTxConfig`` from ``orchestrators/`` instead of ``services/``) is
caught at test time, not at runtime.
"""
from __future__ import annotations


def test_onchain_wiring_imports_resolve() -> None:
    """The 3 lazy imports in app.main()'s on-chain wiring must resolve."""
    from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
    from services.onchain_detection.pending_tx_detector import PendingTxConfig
    from orchestrators.trade_pipeline import execute_trades

    assert OnchainDetectionRuntime is not None
    assert PendingTxConfig is not None
    assert execute_trades is not None


def test_pending_tx_config_fields_match_onchain_detection_config() -> None:
    """``PendingTxConfig`` fields must match the ``OnchainDetectionConfig``
    fields that ``app.main()`` maps into them. If a field is renamed on one
    side without the other, the wiring silently uses the default.
    """
    from config.app_config import OnchainDetectionConfig
    from services.onchain_detection.pending_tx_detector import PendingTxConfig

    oc = OnchainDetectionConfig()
    ptx = PendingTxConfig(
        max_signal_age_ms=oc.pending_tx_max_signal_age_ms,
        hashes_only=oc.pending_tx_hashes_only,
    )
    assert ptx.max_signal_age_ms == oc.pending_tx_max_signal_age_ms
    assert ptx.hashes_only == oc.pending_tx_hashes_only