"""Native-owner detection must not depend on durable evidence publication.

The operator runs with ``evidence.enabled: false`` and no pmrecon receiver.
Native detection is the sole production leader-entry source, so an absent
evidence owner must not pause it. Raw observations are discarded instead of
durably recorded; only pending evaluation, which genuinely needs durable
evidence, stays unavailable.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from pmcopy.domain.source_time import TransportReceipt


def _receipt() -> TransportReceipt:
    return TransportReceipt(
        wall_timestamp_ms=1_700_000_000_000,
        monotonic_ns=1,
        boot_id="boot",
        wall_timestamp_ns=1_700_000_000_000_000_000,
    )


def test_discarding_sink_accepts_and_drops_raw_observations() -> None:
    """The no-evidence sink satisfies RawObservationSink without persisting."""
    from pmcopy.orchestrators.native_detection_runtime import DiscardingRawObservationSink

    sink = DiscardingRawObservationSink()
    assert sink.record("alchemy", "mined", {"any": "payload"}, _receipt(), recovered=False) is None
    assert (
        sink.record("alchemy", "block_header", {"number": 1}, _receipt(), recovered=True) is None
    )


def test_application_has_no_durable_evidence_pause_gate() -> None:
    """The evidence opt-out must not pause native detection."""
    import pmcopy.app as app

    source = inspect.getsource(app._run_application)
    assert "durable_evidence_unavailable" not in source
    assert "DiscardingRawObservationSink" in source


@pytest.mark.asyncio
async def test_native_detection_composes_with_a_discarding_sink(tmp_path, monkeypatch) -> None:
    """A default mined-only roster composes against the discarding sink."""
    from pmcopy.config.native_detection import NativeDetectionConfig
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.orchestrators.native_detection_runtime import (
        DiscardingRawObservationSink,
        NativeDetectionRuntime,
    )

    class Clock:
        def capture(self):
            return _receipt()

    monkeypatch.setattr("pmcopy.orchestrators.native_detection_runtime.SystemReceiptClock", Clock)
    wallet = "0x" + "11" * 20
    owner = SimpleNamespace(
        primary="alchemy",
        gateway=Mock(),
        budget=Mock(),
        endpoints=SimpleNamespace(http_url=lambda _: "https://synthetic.invalid", infura=""),
    )
    runtime = NativeDetectionRuntime.compose(
        config=NativeDetectionConfig(
            state_path=str(tmp_path / "source.sqlite"),
            traders=(TraderDetectionPolicy(wallet),),
        ),
        provider_runtime=owner,
        wallets=(wallet,),
        sink=AsyncMock(),
        raw=DiscardingRawObservationSink(),
        status_changed=Mock(),
    )
    try:
        assert runtime.connected is False
        assert runtime.active_wallets() == ()
    finally:
        await runtime.stop()
