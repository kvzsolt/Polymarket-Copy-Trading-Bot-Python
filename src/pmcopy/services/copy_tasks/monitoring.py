"""Source-finality policy for normal and priority copy tasks.

The policy is pure: it decides whether a normalized source event may create
a copy intent based on the task monitoring mode and the event finality. It
performs no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

from pmcopy.domain.detection import DetectionFinality
from pmcopy.services.copy_tasks.domain import MonitoringMode


@dataclass(frozen=True, slots=True)
class MonitoringDecision:
    """Immutable source-finality admission result."""

    allowed: bool
    reason: str


class CopyMonitoringModePolicy:
    """Apply Kreo-style confirmed or provisional monitoring semantics."""

    def evaluate(
        self,
        *,
        mode: MonitoringMode,
        finality: DetectionFinality,
    ) -> MonitoringDecision:
        """Return whether one normalized source event may create an intent.

        Args:
            mode: Task monitoring policy. Normal waits for confirmed evidence;
                priority permits a still-retractable provisional event.
            finality: Canonical finality from the selected detection source.

        Returns:
            Explicit allow/skip result suitable for structured audit logging.
        """

        if finality is DetectionFinality.CONFIRMED:
            return MonitoringDecision(True, "confirmed_source_allowed")
        if mode is MonitoringMode.PRIORITY:
            return MonitoringDecision(True, "priority_mode_provisional_allowed")
        return MonitoringDecision(False, "normal_mode_waiting_for_confirmation")


__all__ = ["CopyMonitoringModePolicy", "MonitoringDecision"]
