"""Report the selected leader-detection source at startup, without secrets.

``DetectionStartupReporter`` emits exactly one structured INFO line per
process start (``detection_mode_selected | mode=... onchain_ws=set|unset
polling_fallback=true provider=...``), one WARN when a WebSocket URL is
configured but ``detection.mode`` is ``polling`` (the URL is inert), and a
``detection_mode_selected{mode}`` gauge so dashboards can see the active
source. The WebSocket URL embeds the Alchemy API key and is never logged.
"""

from __future__ import annotations

import logging
from typing import Protocol

from pmcopy.domain.detection import DetectionMode
from pmcopy.services.observability.metrics import (
    DETECTION_SOURCE_LABEL_VALUES,
    GAUGE_DETECTION_MODE_SELECTED,
    MetricsRegistry,
)


class _OnchainDetectionSettings(Protocol):
    @property
    def alchemy_wss_url(self) -> str: ...

    @property
    def pending_tx_provider(self) -> str: ...


class _DetectionSettings(Protocol):
    @property
    def onchain(self) -> _OnchainDetectionSettings: ...

    @property
    def detection_mode(self) -> DetectionMode: ...


class DetectionSettingsSource(Protocol):
    """Subset of ``AppConfig`` the reporter reads."""

    @property
    def detection(self) -> _DetectionSettings: ...


class DetectionStartupReporter:
    """Log and gauge the selected detection source once at startup."""

    def __init__(self, logger: logging.Logger, metrics: MetricsRegistry) -> None:
        """Bind the structured logger and metrics registry.

        Args:
            logger: Structured logger receiving the startup lines.
            metrics: Registry receiving the ``detection_mode_selected`` gauge.
        """

        self._logger = logger
        self._metrics = metrics

    def report(self, config: DetectionSettingsSource) -> DetectionMode:
        """Emit the startup lines and gauge for ``config``.

        Args:
            config: Typed application configuration (``detection`` section).

        Returns:
            The selected ``DetectionMode`` for the caller's convenience.
        """

        detection = config.detection
        mode = detection.detection_mode
        onchain = detection.onchain
        ws_configured = bool(str(getattr(onchain, "alchemy_wss_url", "") or "").strip())
        provider = str(getattr(onchain, "pending_tx_provider", "") or "alchemy").strip().lower()
        self._logger.info(
            "detection_mode_selected | mode=%s alchemy_ws=%s provider=%s",
            mode.value,
            "set" if ws_configured else "unset",
            provider or "alchemy",
        )
        for label in DETECTION_SOURCE_LABEL_VALUES:
            self._metrics.set_gauge(
                GAUGE_DETECTION_MODE_SELECTED,
                1.0 if label == mode.value else 0.0,
                labels={"mode": label},
            )
        return mode


__all__ = ["DetectionSettingsSource", "DetectionStartupReporter"]
