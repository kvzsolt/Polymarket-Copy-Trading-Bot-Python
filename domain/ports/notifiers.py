"""Outbound notification port.

Reserved as the extension seam for Discord webhooks, Slack messages, or any
other out-of-band notifier. No concrete implementation exists yet — the port
is defined up-front so services depend on the abstraction, not a future
concrete class.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AbstractNotifier(ABC):
    """One-shot fire-and-forget notification surface."""

    @abstractmethod
    async def notify(
        self,
        *,
        title: str,
        message: str,
        severity: str = "info",
        context: dict[str, Any] | None = None,
    ) -> None: ...
