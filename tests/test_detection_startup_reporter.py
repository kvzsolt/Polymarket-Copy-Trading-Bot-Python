"""Tests for ``orchestrators.detection_startup_reporter.DetectionStartupReporter``.

Startup must say which leader-detection source is active without ever
printing the WebSocket URL (it carries the Alchemy API key), and must warn
when a WS URL is configured but polling is selected — today's silent case.
"""

from __future__ import annotations

import logging

import pytest

from pmcopy.config.app_config import DetectionConfig, OnchainDetectionConfig
from pmcopy.orchestrators.detection.detection_startup_reporter import DetectionStartupReporter
from pmcopy.services.observability.metrics import (
    GAUGE_DETECTION_MODE_SELECTED,
    TELEMETRY_METRIC_SCHEMAS,
    MetricKind,
    MetricsRegistry,
)

_SECRET = "SuperSecretAlchemyKey123"
_WS_URL = f"wss://polygon-mainnet.g.alchemy.com/v2/{_SECRET}"


class _Config:
    def __init__(self, mode: str, ws_url: str) -> None:
        self.detection = DetectionConfig(
            mode=mode,
            onchain=OnchainDetectionConfig(alchemy_wss_url=ws_url),
        )


def _report(
    mode: str, ws_url: str, caplog: pytest.LogCaptureFixture
) -> tuple[list[str], MetricsRegistry]:
    metrics = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    reporter = DetectionStartupReporter(
        logger=logging.getLogger("DetectionStartupReporterTest"),
        metrics=metrics,
    )
    with caplog.at_level(logging.DEBUG, logger="DetectionStartupReporterTest"):
        reporter.report(_Config(mode, ws_url))
    return [record.getMessage() for record in caplog.records], metrics


def test_logs_only_without_ws_url_reports_unset_flag(caplog: pytest.LogCaptureFixture) -> None:
    lines, metrics = _report("on_chain_logs_only", "", caplog)

    assert lines == ["detection_mode_selected | mode=on_chain_logs_only alchemy_ws=unset provider=alchemy"]
    assert metrics.gauges[f'{GAUGE_DETECTION_MODE_SELECTED}{{mode="on_chain_logs_only"}}'] == 1
    assert metrics.gauges[f'{GAUGE_DETECTION_MODE_SELECTED}{{mode="on_chain_pending"}}'] == 0
    assert (
        metrics.gauges[f'{GAUGE_DETECTION_MODE_SELECTED}{{mode="confirmed+mempool_reserved"}}'] == 0
    )


@pytest.mark.parametrize(
    "mode",
    ["on_chain_logs_only", "on_chain_pending", "confirmed+mempool_reserved"],
)
def test_on_chain_modes_report_ws_set(
    mode: str, caplog: pytest.LogCaptureFixture
) -> None:
    lines, metrics = _report(mode, _WS_URL, caplog)

    assert lines == [f"detection_mode_selected | mode={mode} alchemy_ws=set provider=alchemy"]
    assert metrics.gauges[f'{GAUGE_DETECTION_MODE_SELECTED}{{mode="{mode}"}}'] == 1


@pytest.mark.parametrize(
    "mode",
    ["on_chain_logs_only", "on_chain_pending", "confirmed+mempool_reserved"],
)
def test_no_record_ever_contains_the_ws_url_or_key(
    mode: str, caplog: pytest.LogCaptureFixture
) -> None:
    lines, _ = _report(mode, _WS_URL, caplog)

    assert lines
    for line in lines:
        assert _SECRET not in line
        assert "wss://" not in line


def test_detection_mode_gauge_schema_is_bounded() -> None:
    schema = {s.name: s for s in TELEMETRY_METRIC_SCHEMAS}[GAUGE_DETECTION_MODE_SELECTED]
    assert schema.kind is MetricKind.GAUGE
    assert schema.allowed_label_values == (
        ("mode", ("on_chain_logs_only", "on_chain_pending", "confirmed+mempool_reserved")),
    )
