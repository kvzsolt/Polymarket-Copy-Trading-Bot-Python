"""Real-time Alchemy WebSocket UTF-8 byte / CU meter and auto-downgrade circuit breaker.

Alchemy bills WebSocket stream notifications by raw UTF-8 byte payload size at
``0.04 CU / byte``. This module measures every incoming frame before JSON
decoding, emits structured 60-second telemetry, and trips an automatic circuit
breaker if rolling hourly ingress exceeds ``PMCOPY__MAX_HOURLY_WS_MB`` while in
``on_chain_pending`` or ``confirmed+mempool_reserved``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal

from pmcopy.domain.detection import DetectionMode
from pmcopy.utils.logger import get_logger

CU_PER_BYTE = Decimal("0.04")
MINUTES_PER_DAY = Decimal("1440")
BYTES_PER_MB = Decimal("1048576")
ROLLING_HOUR_SECONDS = 3600.0


@dataclass(frozen=True, slots=True)
class AlchemyWsTelemetrySnapshot:
    """Structured 60-second WebSocket bandwidth and CU burn snapshot."""

    ws_frames_in: int
    ws_bytes_in: int
    estimated_cu_min: Decimal
    estimated_cu_day_projection: Decimal
    active_subscriptions: int
    reconnect_count: int
    matched_user_events: int
    rolling_hour_bytes: int
    effective_mode: str


class AlchemyWsMeter:
    """Measure raw UTF-8 WS frame bytes, compute CUs, and enforce hourly MB ceiling."""

    def __init__(
        self,
        *,
        max_hourly_ws_mb: Decimal = Decimal("250"),
        mode_getter: Callable[[], DetectionMode],
        active_subscriptions_getter: Callable[[], int],
        on_hourly_limit_exceeded: Callable[[int], Awaitable[None]] | None = None,
        clock: Callable[[], float] = time.monotonic,
        logger: logging.Logger | None = None,
    ) -> None:
        parsed_mb = Decimal(str(max_hourly_ws_mb))
        if not parsed_mb.is_finite() or parsed_mb <= Decimal("0"):
            raise ValueError("max_hourly_ws_mb must be finite and > 0")
        self._max_hourly_ws_mb = parsed_mb
        self._max_hourly_bytes = max(1, int(parsed_mb * BYTES_PER_MB))
        self._mode_getter = mode_getter
        self._active_subscriptions_getter = active_subscriptions_getter
        self._on_hourly_limit_exceeded = on_hourly_limit_exceeded
        self._clock = clock
        self._logger = logger if logger is not None else get_logger("AlchemyWsMeter")

        # Coalesced (bucket_start_monotonic, byte_sum) ring over 3,600 seconds (<= 3,602 entries).
        self._hourly_buckets: deque[list[float | int]] = deque()
        self._rolling_hour_bytes: int = 0

        # Per-60s telemetry interval accumulators
        self._interval_frames_in: int = 0
        self._interval_bytes_in: int = 0
        self._interval_matched_user_events: int = 0

        # Lifetime totals
        self._total_frames_in: int = 0
        self._total_bytes_in: int = 0
        self._reconnect_count: int = 0
        self._matched_user_events_total: int = 0
        self._breaker_tripped: bool = False
        self._enforce_lock = asyncio.Lock()

    @property
    def max_hourly_ws_mb(self) -> Decimal:
        """Return the configured hourly bandwidth ceiling in MiB."""
        return self._max_hourly_ws_mb

    @property
    def max_hourly_bytes(self) -> int:
        """Return the configured hourly byte ceiling."""
        return self._max_hourly_bytes

    @property
    def rolling_hour_bytes(self) -> int:
        """Return bytes received within the trailing 3,600-second window."""
        self._evict_expired(self._clock())
        return self._rolling_hour_bytes

    @property
    def reconnect_count(self) -> int:
        """Return cumulative WebSocket reconnect count."""
        return self._reconnect_count

    @property
    def breaker_tripped(self) -> bool:
        """Return whether the hourly circuit breaker has fired."""
        return self._breaker_tripped

    def record_frame(self, raw_frame: str | bytes) -> int:
        """Measure exact UTF-8 byte count of one incoming WebSocket frame."""
        if isinstance(raw_frame, bytes):
            byte_count = len(raw_frame)
        elif isinstance(raw_frame, str):
            byte_count = len(raw_frame.encode("utf-8"))
        else:
            raise TypeError("raw_frame must be str or bytes")

        now = self._clock()
        self._evict_expired(now)
        if self._hourly_buckets and (now - float(self._hourly_buckets[-1][0])) < 1.0:
            self._hourly_buckets[-1][1] = int(self._hourly_buckets[-1][1]) + byte_count
        else:
            self._hourly_buckets.append([now, byte_count])
        self._rolling_hour_bytes += byte_count

        self._interval_frames_in += 1
        self._interval_bytes_in += byte_count
        self._total_frames_in += 1
        self._total_bytes_in += byte_count
        return byte_count

    def record_reconnect(self) -> None:
        """Increment the cumulative WebSocket reconnect counter."""
        self._reconnect_count += 1

    def record_matched_user_event(self, count: int = 1) -> None:
        """Increment matched tracked-user event counters."""
        if count > 0:
            self._interval_matched_user_events += count
            self._matched_user_events_total += count

    async def check_and_enforce(self) -> bool:
        """Trip CRITICAL alert and auto-downgrade if hourly WS MB ceiling is exceeded.

        Returns:
            True when the circuit breaker newly tripped and completed enforcement.
        """
        async with self._enforce_lock:
            self._evict_expired(self._clock())
            mode = self._mode_getter()
            if mode not in {
                DetectionMode.ON_CHAIN_PENDING,
                DetectionMode.CONFIRMED_MEMPOOL_RESERVED,
            }:
                return False
            if self._breaker_tripped or self._rolling_hour_bytes <= self._max_hourly_bytes:
                return False

            rolling_bytes = self._rolling_hour_bytes
            estimated_cu_hour = (Decimal(rolling_bytes) * CU_PER_BYTE).quantize(Decimal("0.01"))
            self._logger.critical(
                "alchemy_ws_bandwidth_circuit_breaker_tripped | "
                "previous_mode=%s degraded_mode=on_chain_logs_only "
                "rolling_hour_bytes=%d max_hourly_bytes=%d max_hourly_ws_mb=%s "
                "estimated_cu_hour=%s",
                mode.value,
                rolling_bytes,
                self._max_hourly_bytes,
                self._max_hourly_ws_mb,
                estimated_cu_hour,
            )
            if self._on_hourly_limit_exceeded is not None:
                await self._on_hourly_limit_exceeded(rolling_bytes)
            self._breaker_tripped = True
            return True

    def live_snapshot(self) -> dict[str, object]:
        """Return a non-destructive read-only snapshot for telemetry monitoring."""
        self._evict_expired(self._clock())
        cu_min = (Decimal(self._interval_bytes_in) * CU_PER_BYTE).quantize(Decimal("0.01"))
        cu_day = (cu_min * MINUTES_PER_DAY).quantize(Decimal("0.01"))
        cu_hour = (Decimal(self._rolling_hour_bytes) * CU_PER_BYTE).quantize(Decimal("0.01"))
        return {
            "ws_frames_in": self._total_frames_in,
            "ws_bytes_in": self._total_bytes_in,
            "interval_frames_in": self._interval_frames_in,
            "interval_bytes_in": self._interval_bytes_in,
            "rolling_hour_bytes": self._rolling_hour_bytes,
            "max_hourly_bytes": self._max_hourly_bytes,
            "max_hourly_ws_mb": str(self._max_hourly_ws_mb),
            "estimated_cu_min": str(cu_min),
            "estimated_cu_hour": str(cu_hour),
            "estimated_cu_day_projection": str(cu_day),
            "active_subscriptions": int(self._active_subscriptions_getter()),
            "reconnect_count": self._reconnect_count,
            "matched_user_events": self._matched_user_events_total,
            "breaker_tripped": self._breaker_tripped,
            "effective_mode": self._mode_getter().value,
        }

    def emit_telemetry_interval(self) -> AlchemyWsTelemetrySnapshot:
        """Emit one 60-second structured telemetry log and reset interval counters."""
        self._evict_expired(self._clock())
        frames = self._interval_frames_in
        bytes_in = self._interval_bytes_in
        matched = self._interval_matched_user_events
        self._interval_frames_in = 0
        self._interval_bytes_in = 0
        self._interval_matched_user_events = 0

        cu_min = (Decimal(bytes_in) * CU_PER_BYTE).quantize(Decimal("0.01"))
        cu_day = (cu_min * MINUTES_PER_DAY).quantize(Decimal("0.01"))
        active_subs = int(self._active_subscriptions_getter())
        mode = self._mode_getter().value

        snapshot = AlchemyWsTelemetrySnapshot(
            ws_frames_in=frames,
            ws_bytes_in=bytes_in,
            estimated_cu_min=cu_min,
            estimated_cu_day_projection=cu_day,
            active_subscriptions=active_subs,
            reconnect_count=self._reconnect_count,
            matched_user_events=matched,
            rolling_hour_bytes=self._rolling_hour_bytes,
            effective_mode=mode,
        )
        self._logger.info(
            "alchemy_ws_telemetry | "
            "ws_frames_in=%d ws_bytes_in=%d estimated_cu_min=%s "
            "estimated_cu_day_projection=%s active_subscriptions=%d "
            "reconnect_count=%d matched_user_events=%d "
            "rolling_hour_bytes=%d effective_mode=%s",
            snapshot.ws_frames_in,
            snapshot.ws_bytes_in,
            f"{snapshot.estimated_cu_min:.2f}",
            f"{snapshot.estimated_cu_day_projection:.2f}",
            snapshot.active_subscriptions,
            snapshot.reconnect_count,
            snapshot.matched_user_events,
            snapshot.rolling_hour_bytes,
            snapshot.effective_mode,
        )
        return snapshot

    async def run_telemetry_loop(
        self,
        stop_event: asyncio.Event,
        *,
        interval_seconds: float = 60.0,
    ) -> None:
        """Emit structured WebSocket bandwidth telemetry every ``interval_seconds``."""
        cadence = max(0.05, float(interval_seconds))
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=cadence)
                return
            except TimeoutError:
                self.emit_telemetry_interval()

    def _evict_expired(self, now: float) -> None:
        cutoff = now - ROLLING_HOUR_SECONDS
        while self._hourly_buckets and float(self._hourly_buckets[0][0]) < cutoff:
            _, old_bytes = self._hourly_buckets.popleft()
            self._rolling_hour_bytes = max(0, self._rolling_hour_bytes - int(old_bytes))


__all__ = [
    "BYTES_PER_MB",
    "CU_PER_BYTE",
    "AlchemyWsMeter",
    "AlchemyWsTelemetrySnapshot",
]
