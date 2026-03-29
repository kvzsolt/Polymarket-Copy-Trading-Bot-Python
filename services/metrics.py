from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class MetricsRegistry:
    counters: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    gauges: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self, name: str, value: float = 1.0) -> None:
        async with self._lock:
            self.counters[name] += value

    async def set_gauge(self, name: str, value: float) -> None:
        async with self._lock:
            self.gauges[name] = value

    async def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
        async with self._lock:
            return dict(self.counters), dict(self.gauges)

    async def render_prometheus(self) -> str:
        counters, gauges = await self.snapshot()
        lines: list[str] = []
        for name, value in sorted(counters.items()):
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        for name, value in sorted(gauges.items()):
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


metrics = MetricsRegistry()
