from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field



def _metric_key(name: str, labels: dict[str, str] | None = None) -> str:
    if not labels:
        return name
    parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
    return f"{name}{{{','.join(parts)}}}"


@dataclass
class MetricsRegistry:
    counters: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    gauges: dict[str, float] = field(default_factory=lambda: defaultdict(float))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        async with self._lock:
            self.counters[key] += value

    async def set_gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        key = _metric_key(name, labels)
        async with self._lock:
            self.gauges[key] = value

    async def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
        async with self._lock:
            return dict(self.counters), dict(self.gauges)

    async def render_prometheus(self) -> str:
        counters, gauges = await self.snapshot()
        lines: list[str] = []
        for name, value in sorted(counters.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} counter")
            lines.append(f"{name} {value}")
        for name, value in sorted(gauges.items()):
            metric_name = name.split("{", 1)[0]
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{name} {value}")
        return "\n".join(lines) + "\n"


metrics = MetricsRegistry()
