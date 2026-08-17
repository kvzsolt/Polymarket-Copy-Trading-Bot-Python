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

Before the legacy pipeline runs for a context, ``CrossSourceTradeClaimPolicy``
claims the *tx-level* copy identity (``user|leader|tx|side|asset`` — the key
REST polling builds, since data-api rows carry no ``logIndex``/``orderHash``)
so the same fill is copied once whichever source saw it first. The pipeline
still claims its own per-fill key, keeping per-fill attribution intact.

The provenance rule matches ``orchestrators.v1_event_adapters``: a
``provisional`` signal is ``ON_CHAIN_PENDING``; anything else is
``ON_CHAIN_CONFIRMED``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from application.copy_trade_service import CopyTradeIdentityBuilder
from domain.detection import DetectionMode
from services.metrics import (
    METRIC_DETECTION_SIGNALS,
    METRIC_DETECTION_SIGNALS_DEDUPED,
    MetricsRegistry,
    metrics as _metrics,
)
from services.onchain_detection.trade_detection_models import OnchainTradeSignal
from utils.logger import get_logger, redact_error_detail

TradePipeline = Callable[[Any, str, list[dict[str, Any]], float], Awaitable[Any]]
_FILL_DISCRIMINATOR_KEYS = ("log_index", "logIndex", "order_hash", "orderHash")


class ProcessedTradeClaimRepository(Protocol):
    """Subset of ``OrdersRepo`` the cross-source claim policy uses."""

    async def try_claim_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str = "in_progress",
    ) -> bool:
        """Atomically insert the processed-trade row; ``False`` when taken."""
        ...


@dataclass(frozen=True, slots=True)
class CrossSourceClaimDecision:
    """Outcome of one tx-level cross-source claim.

    Attributes:
        dispatch: Whether the on-chain signal may reach the pipeline.
        trade_key: The tx-level identity that was (or would be) claimed.
        winner: ``"on_chain"`` when this policy holds the claim (or no
            pre-claim was needed), ``"polling"`` when REST polling won,
            ``"error"`` when the claim could not be evaluated.
    """

    dispatch: bool
    trade_key: str
    winner: str


class CrossSourceTradeClaimPolicy:
    """Claim the tx-level copy identity before dispatching an on-chain fill.

    ``CopyTradeIdentityBuilder`` appends ``log:<index>`` / ``order:<hash>``
    when present, so an on-chain per-fill key differs from the key REST
    polling builds for the same fill. Claiming the fill's *tx-level* key
    (same raw signal without those discriminators, status
    ``cross_source_claimed``) makes polling's later claim on that key fail,
    and a polling claim that landed first makes this pre-claim fail. When the
    per-fill key already equals the tx-level key (gen-1 pending signals) no
    pre-claim is made: the pipeline's own claim is the cross-source dedup.
    """

    STATUS = "cross_source_claimed"

    def __init__(
        self,
        *,
        identity_builder: CopyTradeIdentityBuilder | None = None,
        metrics: MetricsRegistry = _metrics,
        logger: logging.Logger | None = None,
    ) -> None:
        """Bind the identity builder and observability collaborators.

        Args:
            identity_builder: Canonical copy-identity builder; defaults to
                the production ``CopyTradeIdentityBuilder``.
            metrics: Registry receiving ``detection_signals_deduped_total``.
            logger: Structured logger for dedup / failure lines.
        """

        self._identity = (
            identity_builder if identity_builder is not None else CopyTradeIdentityBuilder()
        )
        self._metrics = metrics
        self._logger = logger if logger is not None else get_logger("OnchainSignalDispatch")

    async def try_claim(
        self,
        context: Any,
        leader_id: str,
        raw: Mapping[str, Any],
        *,
        source: DetectionMode,
    ) -> CrossSourceClaimDecision:
        """Claim the tx-level identity of ``raw`` for ``context``.

        Args:
            context: Trading context (``user_id`` and ``orders_repo`` used).
            leader_id: Lower-cased leader wallet.
            raw: On-chain raw signal dict (``OnchainTradeSignal.as_raw_signal_dict``).
            source: Provenance label for logs and metrics.

        Returns:
            The decision; ``dispatch=False`` when polling (or an earlier
            on-chain fill of the same tx/side/asset) already holds the key,
            or when the claim could not be evaluated (fail closed).
        """

        user_id = str(getattr(context, "user_id", "") or "")
        tx_level_raw = {
            key: value for key, value in raw.items() if key not in _FILL_DISCRIMINATOR_KEYS
        }
        tx_key = self._identity.build(user_id, leader_id, tx_level_raw)
        fill_key = self._identity.build(user_id, leader_id, raw)
        if tx_key == fill_key:
            return CrossSourceClaimDecision(dispatch=True, trade_key=tx_key, winner="on_chain")
        repo: ProcessedTradeClaimRepository = context.orders_repo
        tx_hash = str(raw.get("tx_hash") or raw.get("transactionHash") or "")
        try:
            claimed = await repo.try_claim_processed_trade(
                trade_key=tx_key,
                user_id=user_id,
                leader_id=leader_id,
                tx_hash=tx_hash,
                side=str(raw.get("side") or "").strip().upper(),
                asset=str(raw.get("token_id") or raw.get("asset") or ""),
                condition_id=str(raw.get("condition_id") or raw.get("conditionId") or ""),
                status=self.STATUS,
            )
        except Exception as exc:
            self._logger.warning(
                "cross_source_claim_failed | source=%s user=%s tx=%s error=%s — signal not "
                "dispatched to this follower (fail closed)",
                source.value,
                user_id,
                tx_hash,
                redact_error_detail(exc),
            )
            return CrossSourceClaimDecision(dispatch=False, trade_key=tx_key, winner="error")
        if claimed:
            return CrossSourceClaimDecision(dispatch=True, trade_key=tx_key, winner="on_chain")
        winner = await self._winner_for(repo, tx_key)
        self._logger.info(
            "detection_signal_deduped | source=%s winner=%s tx=%s user=%s",
            source.value,
            winner,
            tx_hash,
            user_id,
        )
        self._metrics.inc(METRIC_DETECTION_SIGNALS_DEDUPED, labels={"source": source.value})
        return CrossSourceClaimDecision(dispatch=False, trade_key=tx_key, winner=winner)

    async def _winner_for(self, repo: Any, trade_key: str) -> str:
        probe = getattr(repo, "get_processed_trade", None)
        if not callable(probe):
            return "polling"
        try:
            row = await cast(Awaitable[Mapping[str, Any] | None], probe(trade_key))
        except Exception:
            return "polling"
        status = str((row or {}).get("status") or "")
        return "on_chain" if status == self.STATUS else "polling"


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
        claim_policy: CrossSourceTradeClaimPolicy | None = None,
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
            claim_policy: Tx-level cross-source claim applied before the
                legacy pipeline; defaults to ``CrossSourceTradeClaimPolicy``
                sharing ``metrics`` and ``logger``.
        """

        self._contexts_by_leader = contexts_by_leader
        self._v1_runtime = v1_runtime
        self._execute_trades = execute_trades
        self._clock = clock
        self._metrics = metrics
        self._logger = logger if logger is not None else get_logger("OnchainSignalDispatch")
        self._claim_policy = (
            claim_policy
            if claim_policy is not None
            else CrossSourceTradeClaimPolicy(metrics=metrics, logger=self._logger)
        )

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
        source = self.source_for(signal)
        admitted = []
        for ctx in contexts:
            decision = await self._claim_policy.try_claim(ctx, leader_lower, raw, source=source)
            if decision.dispatch:
                admitted.append(ctx)
        if not admitted:
            return
        now_ts = self._clock()
        results = await asyncio.gather(
            *(self._execute_trades(ctx, leader_lower, [dict(raw)], now_ts) for ctx in admitted),
            return_exceptions=True,
        )
        for ctx, result in zip(admitted, results, strict=True):
            if isinstance(result, BaseException):
                self._logger.warning(
                    "onchain_signal_pipeline_error | leader=%s user=%s tx=%s error=%s",
                    leader_lower,
                    getattr(ctx, "user_id", "?"),
                    signal.tx_hash,
                    redact_error_detail(result),
                )


__all__ = [
    "CrossSourceClaimDecision",
    "CrossSourceTradeClaimPolicy",
    "OnchainSignalDispatchAdapter",
    "OnchainSignalSubmitter",
    "ProcessedTradeClaimRepository",
]
