"""Market-data prefetch, entry price planning, reconciliation factory, and coordinator submission."""

from __future__ import annotations

import asyncio
import sys
import time
from decimal import Decimal
from typing import Any, Callable

from pmcopy.application.copy_trade_service import CopyTradeIdentityBuilder
from pmcopy.application.runtime_state import (
    resolve_account_execution_state,
    resolve_accounted_fill_reader,
    resolve_clob_gateway,
    resolve_leader_sell_target_service,
    resolve_reservation_service,
    resolve_task_budget_settlement,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.config.app_config import ExecutionConfig
from pmcopy.domain.execution import LeaderPriceBand
from pmcopy.domain.position_ownership import PositionOwnership
from pmcopy.services.execution.reconciliation_service import ReconciliationService
from pmcopy.services.observability.metrics import COPY_LATENCY_BUCKETS, METRIC_COPY_LATENCY, metrics
from pmcopy.services.telemetry import ExecutionTelemetryEmitter, TelemetryEmitters
from pmcopy.services.trading.order_sizing import MinimumOrderPolicy
from pmcopy.storage.postgres.positions_repo import PositionRecord
from pmcopy.utils.logger import get_logger

logger = get_logger("TradePipeline")
_COPY_TRADE_IDENTITY = CopyTradeIdentityBuilder()
_DISABLED_TELEMETRY = TelemetryEmitters.disabled()
_TX_HASH_KEYS = ("transactionHash", "txHash", "transaction_hash", "tx_hash")
_LEADER_SELL_MINIMUM_POLICY = MinimumOrderPolicy()


def _pipeline_facade_attr(name: str, default: Any) -> Any:
    facade = sys.modules.get("pmcopy.orchestrators.pipeline.trade_pipeline")
    return getattr(facade, name, default) if facade is not None else default


def _copy_entry_price_plan(
    context: TradingContext,
    signal: Any,
    side: str,
    *,
    leader_id: str = "",
) -> tuple[Decimal, dict[str, str]]:
    """Build one tolerance-bounded FAK limit from the leader fill price.

    The configured tolerance is applied once to the copied price without a
    book read, reprice, or retry. The global BUY ceiling remains authoritative.

    The tolerance is resolved per leader: a leader who sweeps the top of the
    book leaves the next resting ask well above their own fill, so the band
    that keeps a copied FAK marketable is a property of the leader rather than
    one global constant.

    Args:
        context: Trading context containing execution tolerance and price caps.
        signal: Normalized leader signal containing the copied fill price.
        side: ``"BUY"`` or ``"SELL"``.
        leader_id: Wallet whose per-leader band applies. An empty value, or a
            config that predates per-leader bands, falls back to the execution
            layer.

    Returns:
        A ``(price_limit, recovery_metadata)`` pair for one FAK submission.
    """
    leader_price = Decimal(str(signal.price or 0))
    app_config = getattr(context, "config", None)
    execution_config = getattr(app_config, "execution", None)
    execution_mode = getattr(execution_config, "trading_mode", "unknown")
    drift_pct = Decimal(str(_resolve_entry_drift_pct(app_config, leader_id)))
    band = LeaderPriceBand(leader_price=leader_price, drift_pct=drift_pct)
    unclamped_price_limit = band.limit_for(side)
    price_limit = unclamped_price_limit
    metadata = {
        "source": "copy_trade",
        "order_type": "FAK",
        "leader_price": str(leader_price),
        "price_drift_pct": str(drift_pct),
        "unclamped_price_limit": str(unclamped_price_limit),
        "execution_mode": execution_mode if execution_mode in {"paper", "live"} else "unknown",
    }
    if side.upper() == "BUY":
        max_share_price = Decimal(
            str(
                getattr(
                    execution_config,
                    "max_share_price",
                    ExecutionConfig.max_share_price,
                )
            )
        )
        if max_share_price > 0:
            price_limit = min(price_limit, max_share_price)
        metadata["max_share_price"] = str(max_share_price)
    metadata["price_limit"] = str(price_limit)
    metadata["leader_id"] = str(leader_id)
    source_action_id = getattr(signal, "source_action_id", None)
    if source_action_id is not None:
        metadata["source_action_id"] = source_action_id
    return price_limit, metadata


def _resolve_entry_drift_pct(app_config: Any, leader_id: str) -> float:
    """Resolve the copy-entry FAK band percentage for one leader.

    Args:
        app_config: The loaded application configuration, or any fake exposing
            an ``execution`` section.
        leader_id: Wallet whose band applies; may be empty.

    Returns:
        The per-leader band when the configuration can resolve one, otherwise
        the execution-layer value, otherwise the typed default.
    """
    resolver: Callable[[str], float] | None = getattr(
        app_config, "entry_price_drift_pct_for_leader", None
    )
    if callable(resolver) and leader_id:
        return float(resolver(leader_id))
    return float(
        getattr(
            getattr(app_config, "execution", None),
            "max_entry_price_drift_pct",
            ExecutionConfig.max_entry_price_drift_pct,
        )
    )


def _observe_copy_latency(
    raw_trade: dict[str, Any],
    labels: dict[str, str],
    submission_state: str,
) -> None:
    detected = raw_trade.get("_copy_detected_monotonic")
    if not isinstance(detected, (int, float)):
        return
    metrics.observe(
        METRIC_COPY_LATENCY,
        max(0.0, time.monotonic() - float(detected)),
        buckets=COPY_LATENCY_BUCKETS,
        labels={**labels, "state": submission_state},
    )


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(1, value)


def _analysis_semaphore(context: TradingContext) -> asyncio.Semaphore:
    exec_state = resolve_account_execution_state(context)
    semaphore = exec_state.trade_analysis_semaphore
    if semaphore is None:
        config_val = context.config.execution.trade_analysis_concurrency if context.config else 6
        semaphore = asyncio.Semaphore(_positive_int(config_val, 6))
        exec_state.trade_analysis_semaphore = semaphore
    return semaphore


def _reconcile_semaphore(context: TradingContext) -> asyncio.Semaphore:
    exec_state = resolve_account_execution_state(context)
    semaphore = exec_state.reconcile_unconfirmed_semaphore
    if semaphore is None:
        config_val = context.config.execution.reconcile_concurrency if context.config else 4
        semaphore = asyncio.Semaphore(_positive_int(config_val, 4))
        exec_state.reconcile_unconfirmed_semaphore = semaphore
    return semaphore


def _execution_telemetry(context: TradingContext) -> ExecutionTelemetryEmitter:
    """Return the context's execution emitter, or a disabled null object.

    Args:
        context: Trading context, possibly a fake without the field.

    Returns:
        The injected emitter when present, else a total null object.
    """
    bundle = getattr(context, "telemetry", None)
    emitter = getattr(bundle, "execution", None)
    if isinstance(emitter, ExecutionTelemetryEmitter):
        return emitter
    return _DISABLED_TELEMETRY.execution


def get_reconciliation_service(context: TradingContext) -> ReconciliationService:
    """Return the shared account-scoped reconciler bound to ``context``.

    The reconciler is the canonical owner of unconfirmed-order settlement
    (issue #71). One instance per follower account keeps batch reconciles
    and poll backoffs consistent across all leader bindings and risk paths.
    """
    exec_state = resolve_account_execution_state(context)
    svc = exec_state.reconciliation_service
    task_budget_settlement = resolve_task_budget_settlement(context)
    reservation_service = resolve_reservation_service(context)
    target_service = resolve_leader_sell_target_service(context)
    if svc is None:
        from pmcopy.services.execution.recovery_inventory import CompleteRecoveryInventoryReader

        holdings_reader = getattr(context, "holdings_reader", None)
        recovery_inventory = (
            CompleteRecoveryInventoryReader(
                holdings_reader,
                wallet_by_user={context.user_id: context.wallet_address},
            )
            if holdings_reader is not None
            else None
        )
        reconciler_cls = _pipeline_facade_attr("ReconciliationService", ReconciliationService)
        svc = reconciler_cls(
            orders_repo=context.orders_repo,
            fills_repo=context.fills_repo,
            positions_repo=context.positions_repo,
            risk_blocks_repo=getattr(context, "risk_blocks_repo", None),
            risk_triggers_repo=getattr(context, "risk_triggers_repo", None),
            clob_gateway=resolve_clob_gateway(context),
            audit_logger=getattr(context, "audit_logger", None),
            reservation_service=reservation_service,
            # Funder wallet is the dedup key for the ManualSync write-back.
            # Falls back to context.wallet_address (the user's primary wallet)
            # when no explicit funder is set.
            funder_address=getattr(context, "wallet_address", None),
            order_confirmation_cfg=(
                getattr(
                    getattr(getattr(context, "config", None), "reconciliation", None),
                    "order_confirmation",
                    None,
                )
                if getattr(context, "config", None) is not None
                else None
            ),
            task_budget_settlement=task_budget_settlement,
            telemetry=_execution_telemetry(context),
            leader_sell_target_service=target_service,
            accounted_fill_reader=resolve_accounted_fill_reader(context),
            recovery_inventory=recovery_inventory,
        )
        exec_state.reconciliation_service = svc
    elif getattr(svc, "reservation_service", None) is None and reservation_service is not None:
        svc.reservation_service = reservation_service
    if task_budget_settlement is not None:
        svc.task_budget_settlement = task_budget_settlement
    if target_service is not None:
        svc.leader_sell_target_service = target_service
    return svc


async def _submit_copy_through_coordinator(
    context: TradingContext,
    exec_req,
    signal,
    *,
    leader_id: str,
):
    """Route copy BUY/SELL through ExecutionCoordinator when wired.

    Live mode requires the coordinator: a missing coordinator fails closed
    with status=``execution_coordinator_missing`` so we never silently bypass
    ReservationService + PositionActionCoordinator. Paper / dry-run / test
    contexts fall back to the ExecutionService primitive (via ``getattr``
    alias) so existing tests that don't wire the coordinator keep working.
    """
    from pmcopy.services.execution.coordinator import is_live_mode

    coordinator = getattr(context, "execution_coordinator", None)
    if coordinator is None:
        if is_live_mode(getattr(context, "config", None)):
            logger.error(
                "execution_coordinator_missing | live mode requires coordinator | "
                "user=%s leader=%s cond=%s token=%s side=%s",
                context.user_id,
                leader_id,
                signal.condition_id,
                signal.token_id,
                exec_req.side,
                extra={
                    "type": "execution_coordinator_missing",
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "condition_id": signal.condition_id,
                    "token_id": signal.token_id,
                    "side": exec_req.side,
                },
            )
            try:
                from pmcopy.services.observability.metrics import (
                    METRIC_COORDINATOR_MISSING_SKIP,
                    metrics,
                )

                metrics.inc_sync(
                    METRIC_COORDINATOR_MISSING_SKIP,
                    labels={"side": exec_req.side, "source": "copy_trade"},
                )
            except Exception:
                pass
            from pmcopy.services.execution.execution_service import ExecutionResult as _ER

            return _ER(
                accepted=False,
                order_id=None,
                status="execution_coordinator_missing",
                requested_amount=Decimal(str(exec_req.amount)),
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="ExecutionCoordinator not wired in live mode",
            )
        execution_service = context.execution_service
        if execution_service is None:
            raise RuntimeError("TradingContext.execution_service is required")
        submitter = execution_service.execute
        return await submitter(exec_req)

    from pmcopy.services.execution.coordinator import ExecutionIntent

    side = (exec_req.side or "BUY").upper()
    try:
        price = Decimal(str(signal.price or 0))
    except Exception:
        price = Decimal("0")

    # ``exec_req.amount`` is the risk-sized follower quantity produced by the
    # sizing + decision + regime gates: pUSD spend for BUY, outcome shares for
    # SELL. ExecutionCoordinator re-derives the BUY ExecutionRequest.amount
    # from ``notional_usdc`` and reserves ``size * price``, so both fields must
    # be projections of that same follower amount. The leader's own notional
    # (``signal.usdc_size``) is reference data and must never size a submit.
    amount = Decimal(str(exec_req.amount))
    if side == "BUY":
        notional = amount
        size = amount / price if price > 0 else Decimal("0")
    else:
        size = amount
        notional = size * price

    protected_limit = Decimal(str(getattr(exec_req, "price_limit", None) or price))
    logger.info(
        "coordinator_intent_built | side=%s size=%s notional_usdc=%s "
        "leader_price=%s protected_limit=%s",
        side,
        size,
        notional,
        price,
        protected_limit,
        extra={
            "type": "coordinator_intent_built",
            "user_id": context.user_id,
            "leader_id": leader_id,
            "condition_id": exec_req.condition_id or signal.condition_id,
            "side": side,
            "size": str(size),
            "notional_usdc": str(notional),
            "price": str(protected_limit),
            "leader_price": str(price),
            "protected_limit": str(protected_limit),
        },
    )

    intent = ExecutionIntent(
        user_id=context.user_id,
        condition_id=exec_req.condition_id or signal.condition_id,
        token_id=exec_req.token_id,
        side=side,
        size=size,
        price=price,
        notional_usdc=notional,
        idempotency_key=signal.tx_hash,
        source="copy_trade",
        signal_id=exec_req.signal_id or signal.tx_hash,
        leader_wallet=leader_id,
        max_slippage_bps=int(exec_req.max_slippage_bps),
        reason=exec_req.reason or "copy_trade",
        recovery_metadata=exec_req.recovery_metadata,
        order_type=getattr(exec_req, "order_type", None),
        price_limit=getattr(exec_req, "price_limit", None),
        tick_size=getattr(exec_req, "tick_size", None),
        min_order_size=getattr(exec_req, "min_order_size", None),
        neg_risk=getattr(exec_req, "neg_risk", None),
        leader_sell_fraction=getattr(exec_req, "leader_sell_fraction", None),
        live_position_size=getattr(exec_req, "live_position_size", None),
        sub_min_sell_policy=getattr(exec_req, "sub_min_sell_policy", None),
    )
    if side == "SELL":
        target_service = resolve_leader_sell_target_service(context)
        if target_service is not None:
            try:
                intent = await target_service.record_fresh_target(intent)
            except Exception as exc:
                logger.error(
                    "leader_sell_target_record_failed | user=%s leader=%s token=%s error_type=%s",
                    context.user_id,
                    leader_id,
                    exec_req.token_id,
                    type(exc).__name__,
                )
    coord_result = await coordinator.submit_copy_trade(intent)
    raw = coord_result.raw_result
    if raw is not None:
        return raw
    # Build a minimal ExecutionResult so downstream code (which reads status,
    # filled_amount, etc.) continues to work.
    from pmcopy.services.execution.execution_service import ExecutionResult

    skip_status = coord_result.skip_code or coord_result.status or "rejected"
    return ExecutionResult(
        accepted=coord_result.accepted,
        order_id=coord_result.order_id,
        status=skip_status,
        requested_amount=Decimal(str(exec_req.amount)),
        filled_amount=Decimal("0"),
        avg_price=None,
        fees_paid=Decimal("0"),
        error=coord_result.reason,
    )


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_response(response: dict[str, Any] | None) -> Decimal:
    if not isinstance(response, dict):
        return Decimal("0")
    return _decimal_or_zero(
        response.get("size_matched") or response.get("filled_size") or response.get("size") or 0
    )


_MDS_RETRY_INTERVAL_S = 0.2
_MDS_DEFAULT_TIMEOUT_S = 2.0


def _market_data_subscribe_timeout(context: TradingContext) -> float:
    market_data = getattr(getattr(context, "config", None), "market_data", None)
    value = float(getattr(market_data, "subscribe_timeout_seconds", _MDS_DEFAULT_TIMEOUT_S))
    return max(0.2, value)


def _market_data_primary_source(context: TradingContext) -> str:
    cfg = getattr(context, "config", None)
    md_cfg = getattr(cfg, "market_data", None) if cfg is not None else None
    return str(getattr(md_cfg, "primary_source", "rest_snapshot")).lower()


#: Strong references to in-flight market-data warm-up tasks. A bare
#: ``asyncio.create_task`` result is only weakly held by the event loop, so a
#: warm-up could be garbage-collected mid-flight; the set is drained by the
#: task's own done-callback, so it stays bounded by the number of unique
#: tokens currently warming.
_market_data_warmup_tasks: set[asyncio.Task] = set()


def _spawn_market_data_warmup(context: TradingContext, token_ids: set[str]) -> None:
    """Warm the market-data cache for ``token_ids`` without blocking the copy.

    Metadata-only prefetch is the copy entry gate; WSS subscription remains a
    background concern for later cache consumers. Warm-up failures are logged
    and swallowed because required execution metadata fails closed separately.

    Args:
        context: Trading context carrying the market-data runtime.
        token_ids: Tokens to subscribe to the market WSS.
    """
    if not token_ids or getattr(context, "market_data_runtime", None) is None:
        return

    async def _warm() -> None:
        for token_id in sorted(token_ids):
            await _ensure_token_subscribed(context, token_id)

    task = asyncio.create_task(_warm(), name="market_data_warmup")
    _market_data_warmup_tasks.add(task)

    def _on_done(finished: asyncio.Task) -> None:
        _market_data_warmup_tasks.discard(finished)
        if finished.cancelled():
            return
        error = finished.exception()
        if error is not None:  # pragma: no cover - _warm swallows its own errors
            logger.warning("trade_pipeline_market_data_warmup_failed | error=%r", error)

    task.add_done_callback(_on_done)


async def _ensure_token_subscribed(context: TradingContext, token_id: str) -> None:
    """Subscribe ``token_id`` to the market WSS if a runtime is wired.

    Best-effort: a missing runtime (e.g. test contexts) is not fatal — sizing
    will still attempt to read MDS, which fails closed if data is unavailable.
    """
    runtime = getattr(context, "market_data_runtime", None)
    if runtime is None:
        return
    try:
        await runtime.subscribe_tokens([token_id])
    except Exception as exc:
        logger.warning(
            "trade_pipeline_subscribe_failed | token=%s error=%s",
            token_id,
            repr(exc),
        )


async def _resolve_market_bundle(
    context: TradingContext,
    token_id: str,
    *,
    timeout_seconds: float,
):
    """Wait up to ``timeout_seconds`` for fresh executable market data.

    Returns a ``MarketDataBundle`` or ``None`` if the token stays stale past
    the timeout. ``None`` triggers a per-signal ``market_data_stale`` skip in
    the caller; the trade is never placed off stale data.
    """
    mds = getattr(context, "market_data_service", None)
    if mds is None:
        return None

    # Imported lazily to keep the module independent of the MDS error types
    # at import time (and to avoid circular imports through trade_pipeline).
    from pmcopy.services.market_data.market_data_service import (  # noqa: WPS433  local import
        MarketDataError,
        MarketDataNotReady,
        StaleDataError,
    )

    deadline = time.monotonic() + max(0.0, timeout_seconds)
    last_error: Exception | None = None

    while True:
        try:
            return await mds.require_fresh_data(token_id)
        except MarketDataNotReady as exc:
            # Resolved markets / not-executable states will not recover within
            # the wait window. Fail closed immediately.
            last_error = exc
            break
        except StaleDataError as exc:
            last_error = exc
        except MarketDataError as exc:
            last_error = exc
            break
        except Exception as exc:  # pragma: no cover - defensive
            last_error = exc
            break

        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(_MDS_RETRY_INTERVAL_S)

    logger.info(
        "trade_pipeline_market_data_stale | token=%s reason=%s",
        token_id,
        repr(last_error) if last_error else "timeout",
    )
    return None


async def _prefetch_market_data_for_signals(
    context: TradingContext,
    normalized: list,
    *,
    metadata_out: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], set[str], dict[tuple[str, str], str]]:
    """Resolve the execution inputs required for each signal token.

    In REST-primary mode this reads only dynamic execution metadata into the
    batch-local ``metadata_out`` mapping (eliminating shared context mutation
    across concurrent batches). It never fetches ``/book``, never gates on
    spread/depth, and never substitutes a book quote for the copied price.
    WSS is warmed in the background.

    In WSS primary mode the legacy path is used: subscribe -> wait for
    ``ensure_token_ready_or_bootstrapped`` -> ``require_fresh_data`` retry loop.
    """
    if not normalized:
        return {}, set(), {}

    primary_source = _market_data_primary_source(context)

    if primary_source == "rest_snapshot":
        return await _prefetch_copy_execution_metadata(
            context,
            normalized,
            metadata_out=metadata_out,
        )

    return await _prefetch_market_data_wss(context, normalized)


async def _prefetch_copy_execution_metadata(
    context: TradingContext,
    normalized: list,
    *,
    metadata_out: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], set[str], dict[tuple[str, str], str]]:
    """Fetch copy sizing metadata without reading or gating an order book.

    The copied signal price remains the sole FAK price. This prefetch obtains
    only dynamic tick size, share minimum, and negative-risk metadata, then
    warms WSS subscriptions in the background for non-execution consumers.
    """
    if metadata_out is not None:
        metadata_out.clear()
    else:
        resolve_account_execution_state(context).last_prefetched_metadata_by_token = {}
    warm_tokens = {
        token_id for signal, _ in normalized if (token_id := getattr(signal, "token_id", None))
    }
    prefetch_started = time.monotonic()
    logger.info(
        "trade_pipeline_copy_metadata_prefetch | tokens=%d",
        len(warm_tokens),
    )
    _spawn_market_data_warmup(context, warm_tokens)

    mds = getattr(context, "market_data_service", None)
    if mds is None:
        unavailable_codes = {
            (
                str(getattr(signal, "token_id", "") or ""),
                str(getattr(signal, "side", "") or "").upper(),
            ): "market_metadata_unavailable"
            for signal, _ in normalized
            if getattr(signal, "token_id", None)
        }
        return (
            {},
            {token_id for token_id, _side in unavailable_codes},
            unavailable_codes,
        )

    unique_tokens: dict[str, str | None] = {}
    sides_by_token: dict[str, set[str]] = {}
    for signal, _ in normalized:
        token_id = str(getattr(signal, "token_id", "") or "")
        if not token_id:
            continue
        unique_tokens.setdefault(token_id, getattr(signal, "condition_id", None))
        sides_by_token.setdefault(token_id, set()).add(
            str(getattr(signal, "side", "") or "").upper()
        )

    analysis_sem = _analysis_semaphore(context)

    async def _resolve_metadata(
        token_id: str,
        condition_id: str | None,
    ) -> tuple[str, Any | None, str | None]:
        async with analysis_sem:
            try:
                value = await mds.require_execution_metadata(
                    token_id,
                    condition_id=condition_id,
                )
                return token_id, value, None
            except Exception as exc:
                skip_code = getattr(exc, "skip_code", None) or "market_metadata_unavailable"
                logger.warning(
                    "trade_pipeline_execution_metadata_failed | token=%s error_type=%s",
                    token_id,
                    type(exc).__name__,
                )
                return token_id, None, str(skip_code)

    resolved = await asyncio.gather(
        *(
            _resolve_metadata(token_id, condition_id)
            for token_id, condition_id in unique_tokens.items()
        )
    )
    metadata_by_token: dict[str, Any] = {}
    stale_tokens: set[str] = set()
    stale_codes: dict[tuple[str, str], str] = {}
    for token_id, metadata, skip_code in resolved:
        if metadata is not None:
            metadata_by_token[token_id] = metadata
            continue
        stale_tokens.add(token_id)
        for side in sides_by_token.get(token_id, {""}):
            stale_codes[(token_id, side)] = skip_code or "market_metadata_unavailable"
    if metadata_out is not None:
        metadata_out.update(metadata_by_token)
    else:
        resolve_account_execution_state(
            context
        ).last_prefetched_metadata_by_token = metadata_by_token
    # This prefetch is the largest single block of the detection-to-submit
    # budget (two CLOB round trips per token), and the copy race it feeds is
    # decided in 1-2 seconds. Report it so the cost is visible next to the
    # token count rather than inferred from log timestamps, and so the effect
    # of ``execution.venue_metadata_ttl_seconds`` is measurable.
    prefetch_ms = (time.monotonic() - prefetch_started) * 1000.0
    logger.info(
        "trade_pipeline_copy_metadata_prefetch_done | tokens=%d resolved=%d "
        "stale=%d elapsed_ms=%.0f",
        len(warm_tokens),
        len(metadata_by_token),
        len(stale_tokens),
        prefetch_ms,
        extra={
            "type": "copy_metadata_prefetch",
            "tokens": len(warm_tokens),
            "resolved": len(metadata_by_token),
            "stale": len(stale_tokens),
            "copy_latency_stage_ms": round(prefetch_ms, 1),
            "stage": "metadata_prefetch",
        },
    )
    return {}, stale_tokens, stale_codes


async def _prefetch_market_data_wss(
    context: TradingContext,
    normalized: list,
) -> tuple[dict[str, Any], set[str], dict[tuple[str, str], str]]:
    """Legacy WSS-primary path. Subscribe → bootstrap → require_fresh_data."""
    unique_tokens: list[str] = []
    seen: set[str] = set()
    for signal, _ in normalized:
        token_id = signal.token_id
        if token_id and token_id not in seen:
            seen.add(token_id)
            unique_tokens.append(token_id)

    if not unique_tokens:
        return {}, set(), {}

    timeout_seconds = _market_data_subscribe_timeout(context)
    bundles_by_token: dict[str, Any] = {}
    stale_tokens: set[str] = set()
    stale_codes: dict[tuple[str, str], str] = {}

    analysis_sem = _analysis_semaphore(context)

    async def _resolve_one(token_id: str) -> tuple[str, Any]:
        async with analysis_sem:
            runtime = getattr(context, "market_data_runtime", None)

            logger.info(
                "trade_pipeline_market_data_prefetch_start | token_id=%s",
                token_id,
            )

            if runtime is None:
                await _ensure_token_subscribed(context, token_id)
                logger.warning(
                    "trade_pipeline_market_data_prefetch_no_runtime | token_id=%s",
                    token_id,
                )
            else:
                try:
                    ready = await runtime.ensure_token_ready_or_bootstrapped(token_id)
                    if ready:
                        logger.info(
                            "trade_pipeline_market_data_prefetch_ready | token_id=%s",
                            token_id,
                        )
                    else:
                        logger.info(
                            "trade_pipeline_market_data_prefetch_timeout | token_id=%s",
                            token_id,
                        )
                        return token_id, None
                except Exception as exc:
                    logger.warning(
                        "trade_pipeline_market_data_prefetch_timeout | token_id=%s error=%r",
                        token_id,
                        exc,
                    )
                    return token_id, None

            bundle = await _resolve_market_bundle(
                context,
                token_id,
                timeout_seconds=timeout_seconds,
            )
            return token_id, bundle

    results = await asyncio.gather(
        *(_resolve_one(token_id) for token_id in unique_tokens),
        return_exceptions=True,
    )
    sides_by_token: dict[str, set[str]] = {}
    for signal, _ in normalized:
        side = str(getattr(signal, "side", "") or "").upper()
        if signal.token_id and side:
            sides_by_token.setdefault(signal.token_id, set()).add(side)

    for entry in results:
        if isinstance(entry, BaseException):
            logger.warning(
                "trade_pipeline_market_data_prefetch_error | error=%s",
                repr(entry),
            )
            continue
        token_id, bundle = entry
        if bundle is None:
            stale_tokens.add(token_id)
            for side in sides_by_token.get(token_id, {""}):
                stale_codes[(token_id, side)] = "market_data_stale"
        else:
            bundles_by_token[token_id] = bundle
            bundle_source = getattr(getattr(bundle, "source", None), "value", None) or str(
                getattr(bundle, "source", "")
            )
            if bundle_source == "rest_degraded":
                logger.warning(
                    "market_data_rest_degraded_used | token_id=%s source=%s",
                    token_id,
                    bundle_source,
                )
    return bundles_by_token, stale_tokens, stale_codes


async def _fetch_live_position_size(
    context: TradingContext,
    *,
    token_id: str,
) -> Decimal | None:
    if context.holdings_reader is None:
        return None
    return await context.holdings_reader.shares(context.wallet_address, token_id)


async def _sync_sell_position_from_live_holdings(
    context: TradingContext,
    *,
    leader_id: str,
    condition_id: str,
    token_id: str,
) -> Decimal | None:
    live_size = await _pipeline_facade_attr(
        "_fetch_live_position_size", _fetch_live_position_size
    )(context, token_id=token_id)
    if live_size is None:
        return None

    existing = await context.positions_repo.get_position(
        context.user_id,
        condition_id,
        token_id=token_id,
    )
    avg_entry_price = (
        getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")
    )

    if live_size <= Decimal("1e-12"):
        await context.positions_repo.delete_position(
            context.user_id, condition_id, token_id=token_id
        )
        logger.info(
            "live_position_sync_deleted_local_position | user_id=%s leader_id=%s condition_id=%s token=%s",
            context.user_id,
            leader_id,
            condition_id,
            token_id,
        )
        return Decimal("0")

    await context.positions_repo.upsert_position(
        PositionRecord(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=PositionOwnership.after_snapshot(
                existing.leader_id if existing is not None else None,
                existing.size_tokens if existing is not None else Decimal(0),
                live_size,
            ),
            size_tokens=live_size,
            avg_entry_price=Decimal(str(avg_entry_price or 0)),
        )
    )
    logger.info(
        "live_position_sync_updated_local_position | user_id=%s leader_id=%s condition_id=%s token=%s live_size=%s",
        context.user_id,
        leader_id,
        condition_id,
        token_id,
        str(live_size),
    )
    return live_size


