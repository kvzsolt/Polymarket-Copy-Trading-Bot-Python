#!/usr/bin/env python3
"""Latency-safety bench for the synchronous telemetry emit path.

Telemetry is the only observability hook the detection-to-submit path is
allowed to pay for, so its per-call cost is a safety property, not a nice
number. This bench measures that cost end to end against the real
:class:`~services.telemetry.hub.TelemetryHub`,
:class:`~services.telemetry.service.TelemetryService`, and
:class:`~services.metrics.MetricsRegistry` — no fakes, no null objects — with a
running event loop and one drain subscription, which is the shape production
composes.

Two emitters are timed because they represent the two distinct call shapes on
the copy path:

* :meth:`DetectionTelemetryEmitter.signal_received` is the *only* pre-submit
  emit, exactly once per detected leader signal;
* :meth:`TradeTelemetryEmitter.pipeline_results` is the post-submission
  projection choke point, which walks a result batch.

Each call is timed individually with :func:`time.perf_counter_ns`. Between
chunks the bench yields to the loop so the hub dispatcher drains the bounded
ingress queue. That yield is deliberately outside every measured window: it
reproduces production, where emits are spread out in time and the dispatcher
keeps up, instead of measuring the cheaper-but-unrealistic overflow-drop path
that a tight 50k loop against a 1024-slot queue would otherwise hit. The
accepted-sample count is reported so a run that did overflow is visible rather
than silently optimistic.

Usage::

    python scripts/bench_telemetry_emit.py
    python scripts/bench_telemetry_emit.py --iterations 50000 --assert-max-us 20

``--assert-max-us`` turns the bench into a gate: any scenario whose mean
exceeds the budget exits ``1``, as does any run that failed to keep every
sample on the accepted path.

Measured on the development host (Python 3.12, 50k iterations, three runs):
``detection.signal_received`` 18.2-19.0 us mean, ``trade.pipeline_results``
19.7-20.0 us mean. Only the first is pre-submit, so only the first is a
hot-path budget; the second is projected after submission and its cost cannot
delay an order. A 20 us budget therefore holds comfortably for the pre-submit
emit and sits exactly on the boundary for the post-submit projection, which is
why the report labels each scenario's scope. Pick the budget for the machine
the gate runs on.

The script lives outside ``testpaths`` (``pytest.ini`` collects ``tests``
only), so pytest never collects it; it is an operator/CI tool, not a unit test.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.metrics import MetricsRegistry  # noqa: E402
from services.telemetry.emitters import (  # noqa: E402
    DetectionTelemetryEmitter,
    TradeTelemetryEmitter,
)
from services.telemetry.hub import TelemetryHub, TelemetrySubscription  # noqa: E402
from services.telemetry.service import TelemetryService  # noqa: E402

_SUMMARY = "Latency-safety bench for the synchronous telemetry emit path."
_NS_PER_US = 1_000.0
_LEADER_WALLET = "0xc4ac7dee4d55d1698e88a8160d5c61ede96b9722"
_TX_HASH = "0x" + "ab" * 32
_TRADE_KEY = "bench-trade-key"


@dataclass(frozen=True, slots=True)
class _BenchOutcome:
    """One pipeline result row shaped like ``PipelineOutcomeLike``."""

    trade_key: str
    status: str
    reason: str | None


@dataclass(frozen=True, slots=True)
class _BenchSignal:
    """One normalized leader signal shaped like ``SignalLike``."""

    side: str
    tx_hash: str


@dataclass(frozen=True, slots=True)
class LatencySummary:
    """Immutable per-scenario latency summary in microseconds."""

    label: str
    hot_path: bool
    samples: int
    accepted: int
    mean_us: float
    p50_us: float
    p99_us: float
    max_us: float

    @property
    def fully_accepted(self) -> bool:
        """Return whether every measured call was accepted by the hub."""

        return self.accepted == self.samples

    def render(self) -> str:
        """Return one fixed-width report row."""

        scope = "pre-submit" if self.hot_path else "post-submit"
        return (
            f"{self.label:<28} {scope:<12} n={self.samples:>6}  "
            f"mean={self.mean_us:>7.3f}us  "
            f"p50={self.p50_us:>7.3f}us  "
            f"p99={self.p99_us:>7.3f}us  "
            f"max={self.max_us:>8.3f}us  "
            f"accepted={self.accepted}/{self.samples}"
        )


class LatencySampler:
    """Turn raw nanosecond samples into an immutable microsecond summary."""

    def summarize(
        self,
        label: str,
        samples_ns: Sequence[int],
        accepted: int,
        *,
        hot_path: bool,
    ) -> LatencySummary:
        """Summarize one scenario.

        Args:
            label: Human-readable scenario name.
            samples_ns: Per-call durations in nanoseconds.
            accepted: Number of calls the hub accepted.
            hot_path: Whether the scenario runs before order submission.

        Returns:
            The immutable summary for this scenario.

        Raises:
            ValueError: If no samples were collected.
        """
        if not samples_ns:
            raise ValueError("a latency scenario must collect at least one sample")
        ordered = sorted(samples_ns)
        total = len(ordered)
        return LatencySummary(
            label=label,
            hot_path=hot_path,
            samples=total,
            accepted=accepted,
            mean_us=sum(ordered) / total / _NS_PER_US,
            p50_us=self._quantile(ordered, 0.50) / _NS_PER_US,
            p99_us=self._quantile(ordered, 0.99) / _NS_PER_US,
            max_us=ordered[-1] / _NS_PER_US,
        )

    @staticmethod
    def _quantile(ordered: Sequence[int], quantile: float) -> int:
        """Return the nearest-rank quantile of an already-sorted sequence.

        Args:
            ordered: Non-empty ascending sample sequence.
            quantile: Target quantile in ``(0, 1]``.

        Returns:
            The sample at the nearest rank.
        """
        rank = int(-(-quantile * len(ordered) // 1)) - 1
        return ordered[min(max(rank, 0), len(ordered) - 1)]


class TelemetryEmitBench:
    """Measure per-call emit latency against a real, running telemetry stack.

    Args:
        iterations: Measured calls per scenario.
        warmup: Unmeasured calls per scenario, discarded before sampling.
        chunk_size: Calls between dispatcher-drain yields. Keep it below the
            hub ingress capacity so the measured calls take the accepted path.
        ingress_capacity: Hub ingress queue capacity.
        subscriber_capacity: Capacity of the single drain subscription.
        sampler: Injected summary strategy.
    """

    def __init__(
        self,
        *,
        iterations: int = 50_000,
        warmup: int = 1_000,
        chunk_size: int = 256,
        ingress_capacity: int = 1_024,
        subscriber_capacity: int = 1_024,
        sampler: LatencySampler | None = None,
    ) -> None:
        if iterations <= 0:
            raise ValueError("iterations must be positive")
        if warmup < 0:
            raise ValueError("warmup must be non-negative")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if chunk_size > ingress_capacity:
            raise ValueError("chunk_size must not exceed the hub ingress capacity")
        self._iterations = iterations
        self._warmup = warmup
        self._chunk_size = chunk_size
        self._ingress_capacity = ingress_capacity
        self._subscriber_capacity = subscriber_capacity
        self._sampler = sampler if sampler is not None else LatencySampler()

    async def run(self) -> tuple[LatencySummary, ...]:
        """Run every scenario against one freshly composed telemetry stack.

        Returns:
            One summary per scenario, in declaration order.
        """
        metrics = MetricsRegistry()
        hub = TelemetryHub(
            ingress_capacity=self._ingress_capacity,
            default_subscriber_capacity=self._subscriber_capacity,
            metrics=metrics,
        )
        service = TelemetryService(hub=hub, metrics=metrics)
        await service.start()
        subscription = hub.subscribe(capacity=self._subscriber_capacity)
        drain = asyncio.create_task(self._drain(subscription), name="bench-telemetry-drain")
        try:
            detection = DetectionTelemetryEmitter(service)
            trade = TradeTelemetryEmitter(service)
            results = (_BenchOutcome(_TRADE_KEY, "skipped", "market_data_stale"),)
            signals: Mapping[str, _BenchSignal] = MappingProxyType(
                {_TRADE_KEY: _BenchSignal(side="BUY", tx_hash=_TX_HASH)}
            )
            summaries = (
                await self._measure(
                    hub,
                    "detection.signal_received",
                    lambda: detection.signal_received("on_chain_pending", _LEADER_WALLET, _TX_HASH),
                    hot_path=True,
                ),
                await self._measure(
                    hub,
                    "trade.pipeline_results",
                    lambda: trade.pipeline_results("account-a", _LEADER_WALLET, results, signals),
                    hot_path=False,
                ),
            )
        finally:
            drain.cancel()
            await asyncio.gather(drain, return_exceptions=True)
            await hub.close()
        return summaries

    async def _measure(
        self,
        hub: TelemetryHub,
        label: str,
        call: Callable[[], bool],
        *,
        hot_path: bool,
    ) -> LatencySummary:
        """Time one emitter call repeatedly, yielding between chunks.

        Args:
            hub: Hub whose ingress depth gates the drain yields.
            label: Scenario name for the report.
            call: Zero-argument emitter call returning acceptance.
            hot_path: Whether the scenario runs before order submission.

        Returns:
            The scenario summary.
        """
        for _ in range(self._warmup):
            call()
        await self._settle(hub)

        samples_ns: list[int] = []
        accepted = 0
        for index in range(self._iterations):
            start = time.perf_counter_ns()
            delivered = call()
            samples_ns.append(time.perf_counter_ns() - start)
            accepted += 1 if delivered else 0
            if (index + 1) % self._chunk_size == 0:
                await self._settle(hub)
        await self._settle(hub)
        return self._sampler.summarize(label, samples_ns, accepted, hot_path=hot_path)

    @staticmethod
    async def _settle(hub: TelemetryHub, *, max_yields: int = 64) -> None:
        """Yield until the dispatcher has drained the bounded ingress queue.

        Args:
            hub: Hub to observe.
            max_yields: Hard bound so a stalled dispatcher cannot hang the bench.
        """
        for _ in range(max_yields):
            if hub.ingress_depth == 0:
                return
            await asyncio.sleep(0)

    @staticmethod
    async def _drain(subscription: TelemetrySubscription) -> None:
        """Consume deliveries so the single subscriber never backs up.

        Args:
            subscription: The bounded subscription owned by this bench run.
        """
        while True:
            await subscription.get()


class BenchCli:
    """Argument parsing, reporting, and the optional latency budget gate."""

    def build_parser(self) -> argparse.ArgumentParser:
        """Return the bench argument parser."""

        parser = argparse.ArgumentParser(description=_SUMMARY)
        parser.add_argument(
            "--iterations",
            type=int,
            default=50_000,
            help="Measured emitter calls per scenario (default: 50000).",
        )
        parser.add_argument(
            "--warmup",
            type=int,
            default=1_000,
            help="Unmeasured calls discarded before sampling (default: 1000).",
        )
        parser.add_argument(
            "--chunk-size",
            type=int,
            default=256,
            help="Calls between dispatcher-drain yields (default: 256).",
        )
        parser.add_argument(
            "--assert-max-us",
            type=float,
            default=None,
            metavar="BUDGET",
            help=(
                "Exit 1 when any scenario mean exceeds this microsecond budget. "
                "Applies to every scenario, not only the pre-submit one."
            ),
        )
        return parser

    def run(self, argv: Sequence[str] | None = None) -> int:
        """Execute the bench and report.

        Args:
            argv: Command-line arguments, defaulting to ``sys.argv[1:]``.

        Returns:
            Process exit status: ``0`` on success, ``1`` on a budget breach or
            an overflowed run, ``2`` on invalid arguments.
        """
        args = self.build_parser().parse_args(argv)
        try:
            bench = TelemetryEmitBench(
                iterations=args.iterations,
                warmup=args.warmup,
                chunk_size=args.chunk_size,
            )
        except ValueError as error:
            print(f"invalid bench configuration: {error}", file=sys.stderr)
            return 2

        summaries = asyncio.run(bench.run())
        print("telemetry emit latency (real hub + service + metrics, one drain subscriber)")
        for summary in summaries:
            print(summary.render())

        status = 0
        for summary in summaries:
            if not summary.fully_accepted:
                print(
                    f"FAIL {summary.label}: only {summary.accepted}/{summary.samples} calls were "
                    "accepted; the measurement is not representative",
                    file=sys.stderr,
                )
                status = 1
        if args.assert_max_us is not None:
            for summary in summaries:
                if summary.mean_us > args.assert_max_us:
                    print(
                        f"FAIL {summary.label}: mean {summary.mean_us:.3f}us exceeds the "
                        f"{args.assert_max_us:.3f}us budget",
                        file=sys.stderr,
                    )
                    status = 1
        return status


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(BenchCli().run())
