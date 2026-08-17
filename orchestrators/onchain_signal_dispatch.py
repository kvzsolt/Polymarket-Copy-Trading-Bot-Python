"""Route on-chain leader signals into the copy path with per-signal provenance.

``OnchainSignalDispatchAdapter`` is the single ``signal_sink`` handed to
whichever on-chain detection runtime ``DetectionSourceFactory`` builds. For
each ``OnchainTradeSignal`` it:

1. logs ``detection_signal | source=<on_chain_pending|on_chain_confirmed> ...``
   so operators can see which source produced a copy (REST polling logs the
   same line with ``source=polling``);
2. increments ``detection_signals_total{source=...}``;
3. routes the signal to the v1 copy-task runtime when present, otherwise to
   the legacy per-context trade pipeline (case-insensitive leader lookup,
   concurrent per-context execution, exceptions logged never raised).

The provenance rule matches ``orchestrators.v1_event_adapters``: a
``provisional`` signal is ``ON_CHAIN_PENDING``; anything else is
``ON_CHAIN_CONFIRMED``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Protocol

from domain.detection import DetectionMode
from services.metrics import METRIC_DETECTION_SIGNALS, MetricsRegistry, metrics as _metrics
from services.onchain_detection.trade_detection_models import OnchainTradeSignal
from utils.logger import get_logger, redact_error_detail

TradePipeline = Callable[[Any, str, list[dict[str, Any]], float], Awaitable[Any]]


class OnchainSignalSubmitter(Protocol):
    """Subset of the v1 copy-task runtime used by the dispatcher."""

    async def submit_onchain(self, signal: OnchainTradeSignal) -> None:
        """Normalize and enqueue one on-chain signal."""
        ...


class OnchainSignalDispatchAdapter:
    """Log, count, and route every on-chain signal into the copy path."""

    def __init__(
        self,
        *,
        contexts_by_leader: Mapping[str, Sequence[Any]],
        v1_runtime: OnchainSignalSubmitter | None,
        execute_trades: TradePipeline,
        clock: Callable[[], float] = time.time,
        metrics: MetricsRegistry = _metrics,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind the live follower map and routing collaborators.

        Args:
            contexts_by_leader: Live ``leader wallet -> trading contexts`` map
                (mutated in place by discovery; held by reference, keys may be
                checksummed or mixed-case).
            v1_runtime: Copy-task runtime; when present every signal is
                submitted there and the legacy pipeline is bypassed.
            execute_trades: Legacy pipeline entry point
                ``(context, leader_id, raw_signals, now_ts)``.
            clock: Wall-clock seconds source for the pipeline timestamp.
            metrics: Registry receiving ``detection_signals_total``.
            logger: Structured logger; defaults to the app child logger.
        """

        self._contexts_by_leader = contexts_by_leader
        self._v1_runtime = v1_runtime
        self._execute_trades = execute_trades
        self._clock = clock
        self._metrics = metrics
        self._logger = logger if logger is not None else get_logger("OnchainSignalDispatch")

    async def __call__(self, signal: OnchainTradeSignal) -> None:
        """Record provenance for ``signal`` and dispatch it.

        Args:
            signal: Canonical on-chain trade signal from either runtime.
        """

        source = self.source_for(signal)
        self._logger.info(
            "detection_signal | source=%s leader=%s tx=%s side=%s token=%s "
            "confidence=%s provisional=%s",
            source.value,
            signal.leader_wallet,
            signal.tx_hash,
            signal.side,
            signal.token_id,
            str(signal.confidence),
            signal.provisional,
        )
        self._metrics.inc(METRIC_DETECTION_SIGNALS, labels={"source": source.value})
        if self._v1_runtime is not None:
            await self._v1_runtime.submit_onchain(signal)
            return
        await self._dispatch_legacy(signal)

    @staticmethod
    def source_for(signal: OnchainTradeSignal) -> DetectionMode:
        """Return the provenance label for ``signal``.

        Args:
            signal: Canonical on-chain trade signal.

        Returns:
            ``ON_CHAIN_PENDING`` for provisional signals, else
            ``ON_CHAIN_CONFIRMED``.
        """

        return (
            DetectionMode.ON_CHAIN_PENDING
            if signal.provisional
            else DetectionMode.ON_CHAIN_CONFIRMED
        )

    def contexts_for(self, leader_wallet: str) -> Sequence[Any]:
        """Return the follower contexts for ``leader_wallet`` (case-insensitive).

        Args:
            leader_wallet: Leader address in any case.

        Returns:
            The registered contexts, or an empty sequence when nobody follows.
        """

        leader_lower = str(leader_wallet or "").strip().lower()
        for key, contexts in self._contexts_by_leader.items():
            if str(key).lower() == leader_lower:
                return contexts
        return ()

    async def _dispatch_legacy(self, signal: OnchainTradeSignal) -> None:
        leader_lower = signal.leader_wallet.lower()
        contexts = list(self.contexts_for(leader_lower))
        if not contexts:
            self._logger.debug(
                "onchain_signal_no_followers | leader=%s tx=%s",
                signal.leader_wallet,
                signal.tx_hash,
            )
            return
        raw = signal.as_raw_signal_dict()
        now_ts = self._clock()
        results = await asyncio.gather(
            *(self._execute_trades(ctx, leader_lower, [dict(raw)], now_ts) for ctx in contexts),
            return_exceptions=True,
        )
        for ctx, result in zip(contexts, results, strict=True):
            if isinstance(result, BaseException):
                self._logger.warning(
                    "onchain_signal_pipeline_error | leader=%s user=%s tx=%s error=%s",
                    leader_lower,
                    getattr(ctx, "user_id", "?"),
                    signal.tx_hash,
                    redact_error_detail(result),
                )


__all__ = ["OnchainSignalDispatchAdapter", "OnchainSignalSubmitter"]
