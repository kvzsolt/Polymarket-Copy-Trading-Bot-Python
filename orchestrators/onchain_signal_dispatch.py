"""Route on-chain leader signals into the copy path with per-signal provenance.

``OnchainSignalDispatchAdapter`` is the single ``signal_sink`` handed to
whichever on-chain detection runtime ``DetectionSourceFactory`` builds. For
each ``OnchainTradeSignal`` it:

1. logs ``detection_signal | source=<on_chain_pending|on_chain_confirmed> ...``
   so operators can see which source produced a copy (REST polling logs the
   same line with ``source=polling``);
2. increments ``detection_signals_total{source=...}`` and publishes exactly
   one operator ``signal_received`` event — the only pre-submit telemetry
   call on the copy path, synchronous and exception-guarded by contract;
3. routes the signal to the v1 copy-task runtime when present, otherwise to
   the legacy per-context trade pipeline (case-insensitive leader lookup,
   concurrent per-context execution, exceptions logged never raised).

Before the legacy pipeline runs for a context, ``CrossSourceTradeClaimPolicy``
claims the *tx-level* copy identity (``user|leader|tx|side|asset`` — the key
REST polling builds, since data-api rows carry no ``logIndex``/``orderHash``)
so the same fill is copied once whichever source saw it first. The pipeline
still claims its own per-fill key, keeping per-fill attribution intact. A
sibling fill of the same tx/side/asset (a second ``OrderFilled`` log for the
leader in one settlement transaction) loses the tx-level claim to *this*
policy, not to polling, and is dispatched anyway — sizes are additive and the
per-fill key keeps idempotency. When a freshly claimed context demonstrably
submitted nothing the tx-level claim is released again so REST polling really
is a fallback for that fill. "Submitted nothing" is deliberately narrow: only
pre-submission skips (``order_id`` absent, reason not ``duplicate_ignored``)
and a raise whose per-fill ``processed_trades`` row carries no ``order_id``
qualify, and the release itself is a status-guarded delete. A post-submission
outcome keeps the claim — releasing it would let the next polling tick submit
a second copy of the same leader fill.

The provenance rule matches ``orchestrators.v1_event_adapters``: a
``provisional`` signal is ``ON_CHAIN_PENDING``; anything else is
``ON_CHAIN_CONFIRMED``.

When a ``pending_copy_tracker`` is injected, a *provisional* dispatch whose
copy actually reached the venue is handed to it for receipt follow-up. That
call is synchronous, never awaited, and exception-guarded: bookkeeping for a
placed copy must never fail the copy.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
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
from services.onchain_detection.pending_copy_confirmation_tracker import TrackedPendingCopy
from services.onchain_detection.leader_sell_fraction_resolver import (
    LeaderSellFractionResolver,
)
from services.onchain_detection.trade_detection_models import OnchainTradeSignal
from services.telemetry import DetectionTelemetryEmitter
from storage.postgres.orders_repo import CROSS_SOURCE_CLAIM_STATUS
from utils.logger import get_logger, redact_error_detail

TradePipeline = Callable[[Any, str, list[dict[str, Any]], float], Awaitable[Any]]
_FILL_DISCRIMINATOR_KEYS = ("log_index", "logIndex", "order_hash", "orderHash")
_DEFAULT_OWN_CLAIM_MEMORY = 4096
_WINNER_ON_CHAIN = "on_chain"
_WINNER_POLLING = "polling"
_WINNER_UNKNOWN = "unknown"
_WINNER_ERROR = "error"
# ``TradePipelineResult.reason`` reported when the copy already reached the
# venue (execution dedup, reservation conflict, or an already-persisted fill).
# Such a "skip" is a post-submission outcome, never "nothing happened".
_REASON_DUPLICATE_IGNORED = "duplicate_ignored"
# ``TradePipelineResult.status`` values that mean an order was actually
# submitted. Only those need pending-copy confirmation: a skipped or failed
# copy leaves no position to orphan.
#
# ``"paper"`` is deliberately NOT here. It is the tier gate
# (``trade_pipeline.py`` "Tier gate: paper follow"), which returns
# ``order_id=None`` and ``continue``s *before* any execution — and
# ``execution.paper_trade_only`` downgrades every ``live_follow`` leader to
# that tier, so during a paper soak every copy would report as tracked and
# then alarm as ``pending_copy_unconfirmed`` for a position that was never
# opened.
_VENUE_REACHED_STATUSES = frozenset({"executed", "submitted_unconfirmed"})


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

    async def get_processed_trade(self, trade_key: str) -> Mapping[str, Any] | None:
        """Return the processed-trade row for ``trade_key``, or ``None``."""
        ...

    async def release_cross_source_claim(self, trade_key: str) -> bool:
        """Delete the row only while it still carries the claim sentinel."""
        ...


@dataclass(frozen=True, slots=True)
class CrossSourceClaimDecision:
    """Outcome of one tx-level cross-source claim.

    Attributes:
        dispatch: Whether the on-chain signal may reach the pipeline.
        trade_key: The tx-level identity that was (or would be) claimed.
        winner: ``"on_chain"`` when this policy holds the claim (fresh or a
            sibling fill of a tx it already claimed, or no pre-claim was
            needed), ``"polling"`` when REST polling won, ``"unknown"`` when
            the claim was lost but the holder could not be determined,
            ``"error"`` when the claim itself could not be evaluated.
        claimed: ``True`` only when *this* call inserted the tx-level row.
            Such a claim is released again if the dispatch demonstrably did
            nothing; sibling admissions never release anything.
        fill_key: The per-fill identity the pipeline claims for this signal
            (``trade_key`` plus the ``log:N`` / ``order:H`` discriminator).
            The ambiguous-exception release reads that row to tell "raised
            before submitting" from "raised after an order was out".
    """

    dispatch: bool
    trade_key: str
    winner: str
    claimed: bool = False
    fill_key: str = ""


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

    A lost claim is attributed before it is acted on: if the tx-level key was
    claimed by this policy earlier (bounded in-process memory, or the existing
    row carries ``cross_source_claimed`` after a restart) the signal is a
    *sibling fill* of the same settlement transaction and is dispatched — the
    pipeline's per-fill ``|log:N`` claim keeps idempotency and the sizes are
    additive. Only a row held by REST polling makes the signal skip. When the
    holder cannot be determined (probe error / no row) the signal is skipped
    with ``winner=unknown``; the loss is never attributed to polling on error.
    """

    STATUS = CROSS_SOURCE_CLAIM_STATUS

    def __init__(
        self,
        *,
        identity_builder: CopyTradeIdentityBuilder | None = None,
        metrics: MetricsRegistry = _metrics,
        logger: logging.Logger | None = None,
        own_claim_memory: int = _DEFAULT_OWN_CLAIM_MEMORY,
        telemetry: DetectionTelemetryEmitter | None = None,
    ) -> None:
        """Bind the identity builder and observability collaborators.

        Args:
            identity_builder: Canonical copy-identity builder; defaults to
                the production ``CopyTradeIdentityBuilder``.
            metrics: Registry receiving ``detection_signals_deduped_total``.
            logger: Structured logger for dedup / failure lines.
            own_claim_memory: Maximum number of tx-level keys remembered as
                claimed by this process (oldest evicted first; the row status
                probe covers evicted keys).
            telemetry: Operator detection emitter. Only the *deduplicated*
                decision is published here, because this is the one place
                that knows a cross-source loss actually happened. The call is
                synchronous, never awaited, and never raises by contract.
        """

        self._identity = (
            identity_builder if identity_builder is not None else CopyTradeIdentityBuilder()
        )
        self._metrics = metrics
        self._telemetry = telemetry
        self._logger = logger if logger is not None else get_logger("OnchainSignalDispatch")
        self._own_claim_memory = max(1, int(own_claim_memory))
        # tx-level key -> number of sibling fills admitted on that claim.
        self._own_claims: OrderedDict[str, int] = OrderedDict()

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
            The decision; ``dispatch=False`` when REST polling already holds
            the key, when the holder is unknown, or when the claim could not
            be evaluated (fail closed). A sibling fill of a tx this policy
            already claimed is admitted with ``claimed=False``.
        """

        user_id = str(getattr(context, "user_id", "") or "")
        tx_level_raw = {
            key: value for key, value in raw.items() if key not in _FILL_DISCRIMINATOR_KEYS
        }
        tx_key = self._identity.build(user_id, leader_id, tx_level_raw)
        fill_key = self._identity.build(user_id, leader_id, raw)
        if tx_key == fill_key:
            return CrossSourceClaimDecision(
                dispatch=True, trade_key=tx_key, winner=_WINNER_ON_CHAIN, fill_key=fill_key
            )
        tx_hash = str(raw.get("tx_hash") or raw.get("transactionHash") or "")
        try:
            repo: ProcessedTradeClaimRepository = context.orders_repo
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
            return CrossSourceClaimDecision(
                dispatch=False, trade_key=tx_key, winner=_WINNER_ERROR, fill_key=fill_key
            )
        if claimed:
            self._remember_own_claim(tx_key)
            return CrossSourceClaimDecision(
                dispatch=True,
                trade_key=tx_key,
                winner=_WINNER_ON_CHAIN,
                claimed=True,
                fill_key=fill_key,
            )
        holder = (
            _WINNER_ON_CHAIN
            if tx_key in self._own_claims
            else await self._holder_of(
                repo, tx_key, source=source, user_id=user_id, tx_hash=tx_hash
            )
        )
        if holder == _WINNER_ON_CHAIN:
            self._record_sibling(tx_key)
            self._logger.info(
                "cross_source_sibling_fill | source=%s tx=%s user=%s — same-tx sibling fill "
                "dispatched; per-fill claim in the pipeline keeps idempotency",
                source.value,
                tx_hash,
                user_id,
            )
            return CrossSourceClaimDecision(
                dispatch=True, trade_key=tx_key, winner=_WINNER_ON_CHAIN, fill_key=fill_key
            )
        if holder == _WINNER_UNKNOWN:
            return CrossSourceClaimDecision(
                dispatch=False, trade_key=tx_key, winner=_WINNER_UNKNOWN, fill_key=fill_key
            )
        self._logger.info(
            "detection_signal_deduped | source=%s winner=%s tx=%s user=%s",
            source.value,
            holder,
            tx_hash,
            user_id,
        )
        self._metrics.inc(METRIC_DETECTION_SIGNALS_DEDUPED, labels={"source": source.value})
        if self._telemetry is not None:
            self._telemetry.signal_deduplicated(source.value, leader_id)
        return CrossSourceClaimDecision(
            dispatch=False, trade_key=tx_key, winner=holder, fill_key=fill_key
        )

    async def release(
        self,
        context: Any,
        decision: CrossSourceClaimDecision,
        *,
        reason: str,
    ) -> None:
        """Release a tx-level claim this policy inserted for ``context``.

        Call this only when the dispatch demonstrably produced nothing at the
        venue. Only a decision with ``claimed=True`` is released, and only
        when no sibling fill was admitted on that claim in the meantime
        (releasing then would let polling's aggregate copy the sibling a
        second time). Failures are logged, never raised.

        Args:
            context: Trading context whose ``orders_repo`` holds the row.
            decision: The decision returned by ``try_claim``.
            reason: Short operator-facing reason for the release.
        """

        if not decision.claimed:
            return
        user_id = str(getattr(context, "user_id", "") or "")
        siblings = self._own_claims.get(decision.trade_key, 0)
        if siblings > 0:
            self._logger.info(
                "cross_source_claim_kept | reason=%s user=%s key=%s siblings=%d — a sibling "
                "fill was admitted on this claim; polling stays refused",
                reason,
                user_id,
                decision.trade_key,
                siblings,
            )
            return
        self._own_claims.pop(decision.trade_key, None)
        try:
            repo: ProcessedTradeClaimRepository = context.orders_repo
            released = await repo.release_cross_source_claim(decision.trade_key)
        except Exception as exc:
            self._logger.warning(
                "cross_source_claim_release_failed | reason=%s user=%s key=%s error=%s",
                reason,
                user_id,
                decision.trade_key,
                redact_error_detail(exc),
            )
            return
        if not released:
            self._logger.info(
                "cross_source_claim_release_skipped | reason=%s user=%s key=%s — the row is "
                "gone or no longer carries this policy's claim; nothing removed",
                reason,
                user_id,
                decision.trade_key,
            )
            return
        self._logger.info(
            "cross_source_claim_released | reason=%s user=%s key=%s — polling may copy this fill",
            reason,
            user_id,
            decision.trade_key,
        )

    async def release_after_pipeline_error(
        self,
        context: Any,
        decision: CrossSourceClaimDecision,
        *,
        reason: str,
    ) -> None:
        """Release a claim only when the pipeline never reached its own claim.

        An exception out of ``execute_trades`` is ambiguous: the pipeline may
        have raised before taking its own per-fill claim (nothing happened —
        REST polling must be free to copy the fill), or any time after that
        claim, including after an order already reached the venue. The
        per-fill ``processed_trades`` row decides: only its absence proves
        nothing was submitted, because the row is written by
        ``try_claim_processed_trade`` *before* submission and
        ``mark_processed_trade`` never adds an ``order_id`` to it, so an
        existing row's ``order_id`` (or lack of one) proves nothing either
        way. Anything the probe cannot prove also keeps the claim
        (under-copy beats double-copy).

        Args:
            context: Trading context whose ``orders_repo`` holds both rows.
            decision: The decision returned by ``try_claim``.
            reason: Short operator-facing reason for the release.
        """

        if not decision.claimed:
            return
        if not await self._pipeline_left_no_order(context, decision, reason=reason):
            return
        await self.release(context, decision, reason=reason)

    async def _pipeline_left_no_order(
        self,
        context: Any,
        decision: CrossSourceClaimDecision,
        *,
        reason: str,
    ) -> bool:
        """Whether the per-fill row proves the pipeline never reached its claim.

        The pipeline's own per-fill claim (``try_claim_processed_trade`` in
        ``orchestrators/trade_pipeline.py``, called before any submission
        attempt) always writes the per-fill ``processed_trades`` row first.
        ``mark_processed_trade`` — called after a successful submission —
        never adds an ``order_id`` to that row, so an existing row's
        ``order_id`` proves nothing about whether an order reached the venue;
        only the row's *absence* proves the pipeline never got that far. Any
        row at all, whatever its ``status``, therefore means "polling must
        stay refused, fail closed" (under-copy beats double-copy).

        Args:
            context: Trading context whose ``orders_repo`` holds both rows.
            decision: The decision returned by ``try_claim``.
            reason: Short operator-facing reason for the release.

        Returns:
            ``True`` only when the per-fill probe proves the row is absent.
        """

        user_id = str(getattr(context, "user_id", "") or "")
        probe = getattr(getattr(context, "orders_repo", None), "get_processed_trade", None)
        if not callable(probe):
            self._logger.warning(
                "cross_source_claim_kept | reason=%s user=%s key=%s detail=no_probe — cannot "
                "prove the pipeline submitted nothing; polling stays refused",
                reason,
                user_id,
                decision.trade_key,
            )
            return False
        try:
            row = await cast(Awaitable[Mapping[str, Any] | None], probe(decision.fill_key))
        except Exception as exc:
            self._logger.warning(
                "cross_source_claim_kept | reason=%s user=%s key=%s detail=%s — per-fill probe "
                "failed; polling stays refused",
                reason,
                user_id,
                decision.trade_key,
                redact_error_detail(exc),
            )
            return False
        if row is None:
            return True
        status = str(row.get("status") or "")
        self._logger.info(
            "cross_source_claim_kept | reason=%s user=%s key=%s detail=per_fill_row_present "
            "status=%s — the pipeline already reached its own claim for this fill; polling "
            "stays refused",
            reason,
            user_id,
            decision.trade_key,
            status,
        )
        return False

    def _remember_own_claim(self, tx_key: str) -> None:
        self._own_claims[tx_key] = 0
        self._own_claims.move_to_end(tx_key)
        while len(self._own_claims) > self._own_claim_memory:
            self._own_claims.popitem(last=False)

    def _record_sibling(self, tx_key: str) -> None:
        self._own_claims[tx_key] = self._own_claims.get(tx_key, 0) + 1
        self._own_claims.move_to_end(tx_key)
        while len(self._own_claims) > self._own_claim_memory:
            self._own_claims.popitem(last=False)

    async def _holder_of(
        self,
        repo: Any,
        trade_key: str,
        *,
        source: DetectionMode,
        user_id: str,
        tx_hash: str,
    ) -> str:
        """Return who holds ``trade_key``: ``on_chain``, ``polling`` or ``unknown``."""

        probe = getattr(repo, "get_processed_trade", None)
        error: str | None = None
        row: Mapping[str, Any] | None = None
        if not callable(probe):
            error = "no_probe"
        else:
            try:
                row = await cast(Awaitable[Mapping[str, Any] | None], probe(trade_key))
            except Exception as exc:
                error = redact_error_detail(exc)
        if error is None and row is None:
            error = "no_row"
        if error is not None:
            self._logger.warning(
                "cross_source_claim_unresolved | source=%s winner=unknown user=%s tx=%s "
                "error=%s — claim lost but holder unknown; signal not dispatched",
                source.value,
                user_id,
                tx_hash,
                error,
            )
            return _WINNER_UNKNOWN
        status = str((row or {}).get("status") or "")
        return _WINNER_ON_CHAIN if status == self.STATUS else _WINNER_POLLING


class OnchainSignalSubmitter(Protocol):
    """Subset of the v1 copy-task runtime used by the dispatcher."""

    async def submit_onchain(self, signal: OnchainTradeSignal) -> None:
        """Normalize and enqueue one on-chain signal."""
        ...


class PendingCopyTracker(Protocol):
    """Subset of ``PendingCopyConfirmationTracker`` the dispatcher uses."""

    def track(self, entry: TrackedPendingCopy) -> bool:
        """Record one provisional copy synchronously; never raises."""
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
        pending_copy_tracker: PendingCopyTracker | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        telemetry: DetectionTelemetryEmitter | None = None,
        sell_fraction_resolver: LeaderSellFractionResolver | None = None,
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
            pending_copy_tracker: Optional follow-up tracker for copies taken
                off a *provisional* mempool sighting. Only ever handed
                ``provisional`` signals whose copy reached the venue.
            monotonic: Monotonic seconds source used to stamp tracked copies.
                It must share the tracker's time base (both default to
                ``time.monotonic``): the tracker ages entries as
                ``now - recorded_at``, so a wall-clock stamp would make every
                age negative and no copy would ever be polled.
            telemetry: Operator detection emitter. This is the only pre-submit
                telemetry call on the on-chain copy path: exactly one
                ``signal_received`` per signal, synchronous, never awaited,
                never raising. It is also shared with the default claim policy
                so a cross-source dedup is reported from the one place that
                knows about it.
            sell_fraction_resolver: Optional authoritative SELL-fraction
                resolver applied before either copy runtime sees the signal.
        """

        self._contexts_by_leader = contexts_by_leader
        self._v1_runtime = v1_runtime
        self._execute_trades = execute_trades
        self._clock = clock
        self._monotonic = monotonic
        self._pending_copy_tracker = pending_copy_tracker
        self._metrics = metrics
        self._telemetry = telemetry
        self._sell_fraction_resolver = sell_fraction_resolver
        self._logger = logger if logger is not None else get_logger("OnchainSignalDispatch")
        self._claim_policy = (
            claim_policy
            if claim_policy is not None
            else CrossSourceTradeClaimPolicy(
                metrics=metrics,
                logger=self._logger,
                telemetry=telemetry,
            )
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
        if self._telemetry is not None:
            self._telemetry.signal_received(source.value, signal.leader_wallet, signal.tx_hash)
        resolver = self._sell_fraction_resolver
        if resolver is not None:
            outcome = await resolver.resolve(signal)
            if not outcome.dispatch:
                self._logger.warning(
                    "onchain_signal_safety_blocked | leader=%s token=%s tx=%s reason=%s",
                    signal.leader_wallet,
                    signal.token_id,
                    signal.tx_hash,
                    outcome.reason,
                )
                return
            signal = outcome.signal
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
        decisions = await asyncio.gather(
            *(
                self._claim_policy.try_claim(ctx, leader_lower, raw, source=source)
                for ctx in contexts
            )
        )
        admitted = [
            (ctx, decision)
            for ctx, decision in zip(contexts, decisions, strict=True)
            if decision.dispatch
        ]
        if not admitted:
            return
        now_ts = self._clock()
        results = await asyncio.gather(
            *(
                self._execute_trades(ctx, leader_lower, [dict(raw)], now_ts)
                for ctx, _decision in admitted
            ),
            return_exceptions=True,
        )
        copied_user_ids: list[str] = []
        for (ctx, decision), result in zip(admitted, results, strict=True):
            if isinstance(result, BaseException):
                self._logger.warning(
                    "onchain_signal_pipeline_error | leader=%s user=%s tx=%s error=%s",
                    leader_lower,
                    getattr(ctx, "user_id", "?"),
                    signal.tx_hash,
                    redact_error_detail(result),
                )
                await self._claim_policy.release_after_pipeline_error(
                    ctx, decision, reason="pipeline_error"
                )
                continue
            if self._is_pre_submission_only(result):
                await self._claim_policy.release(ctx, decision, reason="no_executed_outcome")
            if self._reached_venue(result):
                copied_user_ids.append(str(getattr(ctx, "user_id", "") or ""))
        self._track_pending_copy(signal, copied_user_ids)

    @staticmethod
    def _reached_venue(result: Any) -> bool:
        """Whether any pipeline outcome in ``result`` submitted an order.

        ``executed`` and ``submitted_unconfirmed`` are the only statuses that
        follow a submission attempt, so they are the only ones that can leave
        a position to reconcile against the leader transaction actually
        mining. Skips, failures, and the ``paper`` tier gate (which returns
        before execution with no order id) leave nothing to unwind.
        """

        outcomes = list(result) if isinstance(result, (list, tuple)) else []
        return any(
            str(getattr(item, "status", "") or "") in _VENUE_REACHED_STATUSES for item in outcomes
        )

    def _track_pending_copy(self, signal: OnchainTradeSignal, user_ids: Sequence[str]) -> None:
        """Hand a provisional copy to the confirmation tracker.

        Synchronous and never awaited: ``track`` is O(1) and exception-free by
        contract, and this call sits on the dispatch path. The extra
        ``try/except`` is defence in depth for a third-party tracker — a
        follow-up bookkeeping failure must never fail a placed copy.

        Args:
            signal: The dispatched signal (ignored unless ``provisional``).
            user_ids: Followers whose copy reached the venue.
        """

        tracker = self._pending_copy_tracker
        if tracker is None or not signal.provisional or not user_ids:
            return
        try:
            tracker.track(
                TrackedPendingCopy(
                    tx_hash=signal.tx_hash,
                    leader_wallet=signal.leader_wallet,
                    user_ids=tuple(user_ids),
                    token_id=signal.token_id,
                    condition_id=signal.condition_id,
                    side=signal.side,
                    recorded_at=self._monotonic(),
                )
            )
        except Exception as exc:
            self._logger.debug(
                "pending_copy_track_skipped | tx=%s leader=%s error=%s",
                signal.tx_hash,
                signal.leader_wallet,
                redact_error_detail(exc),
            )

    @staticmethod
    def _is_pre_submission_only(result: Any) -> bool:
        """Whether every pipeline result is a skip taken *before* submitting.

        The legacy pipeline returns a list of ``TradePipelineResult``. Only a
        pre-submission skip means "nothing reached the venue, polling may copy
        this fill": those carry ``order_id=None`` and a gate reason. A
        ``skipped`` entry that carries an ``order_id``, or the reason
        ``duplicate_ignored`` (the coordinator's ``ReservationConflict`` path
        returns that with no order id), reports a copy that already reached
        the venue — releasing then re-opens the cross-source double copy. Any
        non-skip status (executed, paper, failed, submitted_unconfirmed) means
        the pipeline owns that fill's retry. An empty or non-list result
        stays releasable: the dispatch produced no outcome at all.
        """

        outcomes = list(result) if isinstance(result, (list, tuple)) else []
        return all(
            str(getattr(item, "status", "") or "") == "skipped"
            and getattr(item, "order_id", None) is None
            and str(getattr(item, "reason", "") or "") != _REASON_DUPLICATE_IGNORED
            for item in outcomes
        )


__all__ = [
    "CrossSourceClaimDecision",
    "CrossSourceTradeClaimPolicy",
    "OnchainSignalDispatchAdapter",
    "OnchainSignalSubmitter",
    "PendingCopyTracker",
    "ProcessedTradeClaimRepository",
]
