"""Contracts for the source-finality monitoring mode policy."""

from __future__ import annotations

from domain.detection import DetectionFinality
from services.copy_tasks.domain import MonitoringMode
from services.copy_tasks.monitoring import CopyMonitoringModePolicy


def test_normal_mode_allows_confirmed_evidence() -> None:
    decision = CopyMonitoringModePolicy().evaluate(
        mode=MonitoringMode.NORMAL,
        finality=DetectionFinality.CONFIRMED,
    )
    assert decision.allowed is True
    assert decision.reason == "confirmed_source_allowed"


def test_normal_mode_rejects_provisional_evidence() -> None:
    decision = CopyMonitoringModePolicy().evaluate(
        mode=MonitoringMode.NORMAL,
        finality=DetectionFinality.PROVISIONAL,
    )
    assert decision.allowed is False
    assert decision.reason == "normal_mode_waiting_for_confirmation"


def test_priority_mode_allows_provisional_evidence() -> None:
    decision = CopyMonitoringModePolicy().evaluate(
        mode=MonitoringMode.PRIORITY,
        finality=DetectionFinality.PROVISIONAL,
    )
    assert decision.allowed is True
    assert decision.reason == "priority_mode_provisional_allowed"


def test_priority_mode_still_allows_confirmed_evidence() -> None:
    decision = CopyMonitoringModePolicy().evaluate(
        mode=MonitoringMode.PRIORITY,
        finality=DetectionFinality.CONFIRMED,
    )
    assert decision.allowed is True
    assert decision.reason == "confirmed_source_allowed"
