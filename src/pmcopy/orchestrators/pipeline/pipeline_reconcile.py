"""Unconfirmed-order reconciliation batch helpers and pipeline telemetry projection."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Any

from pmcopy.application.runtime_state import resolve_account_execution_state
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.position_key import PositionKey
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _DISABLED_TELEMETRY,
    _TX_HASH_KEYS,
    _pipeline_facade_attr,
    _reconcile_semaphore,
    get_reconciliation_service,
)
from pmcopy.orchestrators.pipeline.pipeline_sizing_and_caps import (
    TradePipelineResult,
    build_trade_key,
)
from pmcopy.services.execution.pending_order_reconciliation import (
    PendingOrderReconciliationQueryService,
)
from pmcopy.services.observability.metrics import metrics
from pmcopy.services.telemetry import TradeTelemetryEmitter
from pmcopy.utils.logger import get_logger

logger = get_logger("TradePipeline")


async def _reconcile_one(
    context: TradingContext,
    record: dict,
    side: str,
    *,
    startup_recovery: bool = False,
) -> bool:
    """Reconcile a single unconfirmed BUY/SELL via the canonical reconciler.

    Issue #71 — terminal settlement is owned by ``ReconciliationService``;
    this wrapper supplies the per-context dependencies (fill confirmation,
    position lookup, PnL tracker) and preserves caller-facing logging tags.
    """
    if context.execution_service is None:
        return False

    reconciler = _pipeline_facade_attr("get_reconciliation_service", get_reconciliation_service)(context)
    request = record.get("request")
    signal_id = str(request.get("signal_id") or "") if isinstance(request, dict) else ""
    is_risk_exit = side == "SELL" and (
        signal_id.startswith("risk:") or str(record.get("trade_key") or "").startswith("risk:")
    )
    if is_risk_exit:
        risk_record = record
        if not isinstance(request, dict):
            persisted = await context.orders_repo.get_by_order_id(str(record["order_id"]))
            if persisted is None or not isinstance(persisted.get("request"), dict):
                raise RuntimeError("Protective reconciliation request is unavailable")
            risk_record = {**record, "request": persisted["request"]}
        return await reconciler.reconcile_risk_record(
            risk_record,
            user_id=context.user_id,
            condition_id=str(record["condition_id"]),
            token_id=str(record["asset"]),
            confirm_fill=reconciler.confirm_fill,
            pnl_tracker=context.pnl_tracker,
            get_position=context.positions_repo.get_position,
            logger=logger,
            startup_recovery=startup_recovery,
        )
    return await reconciler.reconcile_trade_record(
        record,
        side,
        confirm_fill=reconciler.confirm_fill,
        pnl_tracker=context.pnl_tracker,
        get_position=context.positions_repo.get_position,
        logger=logger,
        startup_recovery=startup_recovery,
        # Phase 4 — let the reconciler release the daily-cap reservation
        # for BUYs that terminally cancelled without filling.
        balance_regime=getattr(context, "balance_regime", None),
    )


async def reconcile_submitted_unconfirmed(
    context: TradingContext,
    *,
    max_age_seconds: float = 300.0,
    skip_locked_scopes: bool = False,
    startup_recovery: bool = False,
) -> int:
    """Poll exchange for BUY and SELL orders stuck at submitted_unconfirmed.

    BUY reconciliation: applies the fill and creates the local position so it
    becomes visible before the current batch of SELL signals is processed.

    SELL reconciliation: applies the fill to reduce/close the local position
    and records realized PnL. If the FOK was cancelled (no fill), the
    processed_trade row is deleted so the SELL can be retried next cycle.

    Called at the top of each execute_trades invocation.
    Returns the total number of orders reconciled (fills applied + retries unlocked).

    ``skip_locked_scopes`` — best-effort callers (the risk loop's pre-reconcile
    phase) pass True so a record whose scoped position lock is currently held
    by another consumer (execute_trades submit/confirm, manual sync, the
    standalone reconciler) is skipped instead of queued behind that holder's
    venue I/O; the lock holder converges the position, or the next sweep
    retries the row. The execute_trades path keeps the blocking default: BUY
    fills must become visible before the batch's SELL signals are processed.
    """
    if context.execution_service is None:
        return 0

    reconcile_user_id = str(getattr(context, "user_id", "") or "") or None
    buy_pending = await context.orders_repo.get_unconfirmed_buys(
        max_age_seconds=max_age_seconds,
        user_id=reconcile_user_id,
    )
    sell_pending = await context.orders_repo.get_unconfirmed_sells(
        max_age_seconds=max_age_seconds,
        user_id=reconcile_user_id,
    )
    exec_state = resolve_account_execution_state(context)
    pending_query = exec_state.pending_order_query_service
    if pending_query is None and hasattr(
        type(context.orders_repo),
        "get_unconfirmed_order_submissions",
    ):
        pending_query = PendingOrderReconciliationQueryService(
            context.orders_repo,
            max_batch_size=200,
        )
        exec_state.pending_order_query_service = pending_query
    if pending_query is not None:
        try:
            order_records = await pending_query.list_records(
                user_id=reconcile_user_id,
                limit=200,
            )
        except Exception as exc:
            logger.warning(
                "reconcile_orders_table_query_failed | error_type=%s",
                type(exc).__name__,
            )
        else:
            known_order_ids = {
                str(record.get("order_id") or "") for record in [*buy_pending, *sell_pending]
            }
            for side, record in order_records:
                order_id = str(record.get("order_id") or "")
                if order_id and order_id not in known_order_ids:
                    (buy_pending if side == "BUY" else sell_pending).append(record)
                    known_order_ids.add(order_id)
    if not buy_pending and not sell_pending:
        return 0

    sem = _reconcile_semaphore(context)

    async def _reconcile_batch(records: list[dict[str, Any]], side: str) -> int:
        async def _run(record: dict[str, Any]) -> bool:
            # Per-scoped-position action coordination (issue #28): serialize
            # the reconciliation state-apply against the same scoped position
            # so risk monitor / execute_trades cannot observe stale state in
            # the middle of the fill-apply critical section.
            try:
                _scope = PositionKey.from_reconciliation_record(record)
            except ValueError:
                # Legacy rows predating issue #67 may have NULL/empty
                # user_id, condition_id, or asset. Emit a hard metric so
                # ops can detect them, then fall back to a *per-row unique*
                # sentinel scope so the row still reconciles (else it stays
                # permanently stuck and skews equity).
                #
                # Each legacy row must get its OWN PositionKey — collapsing
                # unrelated rows under a single shared sentinel (`__legacy_*__`)
                # would serialize them through the same coordinator slot and
                # corrupt per-position attribution. We salt every missing
                # field with the order_id (falling back to trade_key, then a
                # uuid) so two unrelated legacy rows lock independently.
                _legacy_salt = (
                    str(record.get("order_id") or "").strip()
                    or str(record.get("trade_key") or "").strip()
                    or f"row-{uuid.uuid4().hex[:12]}"
                )
                metrics.inc(
                    "reconcile_missing_scope_total",
                    labels={"side": side},
                )
                logger.warning(
                    "reconcile_legacy_scope_fallback | order_id=%s trade_key=%s side=%s "
                    "user_id=%r condition_id=%r asset=%r salt=%s",
                    record.get("order_id"),
                    record.get("trade_key"),
                    side,
                    record.get("user_id"),
                    record.get("condition_id"),
                    record.get("asset") or record.get("token_id"),
                    _legacy_salt,
                )
                _scope = PositionKey(
                    user_id=record.get("user_id") or f"__legacy_user__{_legacy_salt}",
                    condition_id=(record.get("condition_id") or f"__legacy_cond__{_legacy_salt}"),
                    token_id=(
                        record.get("asset")
                        or record.get("token_id")
                        or f"__legacy_token__{_legacy_salt}"
                    ),
                )
            if skip_locked_scopes and context.position_coordinator.is_locked(_scope):
                # Another consumer is actively converging this scope and may
                # hold the lock through venue round-trips; queueing here would
                # burn the best-effort caller's wall-clock ceiling (live
                # symptom: "Risk pre-reconcile TIMEOUT ... after 12.0s" right
                # after manual_sync_run / a new BUY execution).
                metrics.inc(
                    "pre_reconcile_skipped_locked_total",
                    labels={"side": side},
                )
                logger.debug(
                    "reconcile_skip_locked_scope | side=%s order_id=%s",
                    side,
                    record.get("order_id"),
                )
                return False
            async with sem:
                if skip_locked_scopes and context.position_coordinator.is_locked(_scope):
                    metrics.inc(
                        "pre_reconcile_skipped_locked_total",
                        labels={"side": side},
                    )
                    return False
                async with context.position_coordinator.acquire(_scope):
                    return await _pipeline_facade_attr("_reconcile_one", _reconcile_one)(
                        context, record, side, startup_recovery=startup_recovery
                    )

        if not records:
            return 0
        results = await asyncio.gather(
            *(_run(record) for record in records), return_exceptions=True
        )
        batch_reconciled = 0
        for result in results:
            if isinstance(result, Exception):
                if startup_recovery:
                    raise result
                logger.warning("reconcile_batch_error | side=%s error=%s", side, repr(result))
                continue
            if result:
                batch_reconciled += 1
        return batch_reconciled

    # Reconcile BUY and SELL batches concurrently. The shared semaphore (`sem`,
    # RECONCILE_CONCURRENCY) still caps TOTAL in-flight reconciles, so peak
    # venue concurrency — and rate-limit exposure — is unchanged; this only
    # removes the artificial barrier that made SELL wait for the entire BUY
    # batch. Per-PositionKey serialization is preserved by
    # `position_coordinator.acquire` inside `_reconcile_one`, so a BUY and SELL
    # touching the same position still cannot interleave their state-apply.
    # "BUYs visible before SELL *signals*" ordering is unaffected: this whole
    # function still completes before signal normalization/execution downstream.
    buy_n, sell_n = await asyncio.gather(
        _reconcile_batch(buy_pending, "BUY"),
        _reconcile_batch(sell_pending, "SELL"),
    )
    reconciled = buy_n + sell_n

    if reconciled:
        metrics.inc("reconcile_unconfirmed_total", value=float(reconciled))
        _trade_telemetry(context).reconciliation_run(reconciled)

    return reconciled


def _decision_live_price(signal: Any, bundle: Any) -> float:
    """Return the copied fill price used for decision sizing.

    A supplied bundle is deliberately ignored. Copy FAK execution uses the
    leader's observed fill as its sole price, while dynamic venue metadata is
    fetched separately for sizing and signing safety.
    """
    del bundle
    try:
        return float(getattr(signal, "price", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True, slots=True)
class _SignalRef:
    """Minimal projection of one raw leader row for telemetry correlation.

    Attributes:
        side: Leader trade side.
        tx_hash: Leader transaction hash used as the copy correlation id.
    """

    side: str
    tx_hash: str


def _trade_telemetry(context: TradingContext) -> TradeTelemetryEmitter:
    """Return the context's copy-pipeline emitter, or a disabled null object.

    Args:
        context: Trading context, possibly a fake without the field.

    Returns:
        The injected emitter when present, else a total null object.
    """
    bundle = getattr(context, "telemetry", None)
    emitter = getattr(bundle, "trade", None)
    return emitter if isinstance(emitter, TradeTelemetryEmitter) else _DISABLED_TELEMETRY.trade


def _signals_by_trade_key(
    user_id: str,
    leader_id: str,
    signals: list[dict[str, Any]],
) -> dict[str, _SignalRef]:
    """Recover the side and correlation hash of each row by its copy identity.

    ``build_trade_key`` is pure and is exactly what the pipeline loop uses for
    every ``TradePipelineResult``, so re-deriving it here pairs each result row
    with the raw signal it came from. ``normalize_signals`` is deliberately not
    re-run: it prints an operator warning and logs for malformed rows, so a
    second pass would duplicate operator output for the same batch.

    Args:
        user_id: Follower account identity.
        leader_id: Copied leader identity.
        signals: The raw batch handed to the pipeline.

    Returns:
        Trade key -> minimal signal reference.
    """
    by_key: dict[str, _SignalRef] = {}
    for raw in signals:
        if not isinstance(raw, dict):
            continue
        tx_hash = ""
        for key in _TX_HASH_KEYS:
            value = raw.get(key)
            if value:
                tx_hash = str(value)
                break
        by_key[build_trade_key(user_id, leader_id, raw)] = _SignalRef(
            side=str(raw.get("side") or "").strip().upper(),
            tx_hash=tx_hash,
        )
    return by_key


def _project_pipeline_results(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    results: list[TradePipelineResult],
) -> None:
    """Publish bounded skip events for one finished pipeline batch.

    Runs strictly after the copy path has finished, so its cost is off the
    detection-to-submit path entirely. Building the correlation map reads
    caller-supplied dicts, which happens outside the emitter's own guard, so
    the whole projection is wrapped: a completed copy must never be turned
    into a raised exception by observability.

    Args:
        context: Trading context owning the emitter bundle.
        leader_id: Copied leader identity.
        signals: The raw batch handed to the pipeline.
        results: The rows the pipeline returned.
    """
    emitter = _trade_telemetry(context)
    if not emitter.enabled:
        return
    user_id = str(getattr(context, "user_id", "") or "")
    try:
        emitter.pipeline_results(
            user_id,
            leader_id,
            results,
            _signals_by_trade_key(user_id, leader_id, signals),
        )
    except Exception as exc:
        # Defence in depth around the correlation map: the emitter itself never
        # raises, but this projection also touches raw provider dicts. The drop
        # is visible at DEBUG; the copy outcome is already decided.
        logger.debug("pipeline_telemetry_projection_failed | error=%r", exc)


