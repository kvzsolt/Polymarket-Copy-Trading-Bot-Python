"""Batch trade-pipeline execution orchestration, submission, and fill persistence."""

from __future__ import annotations

import asyncio
import contextlib
import time
import traceback as _tb
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg

from pmcopy.application.runtime_state import (
    resolve_leader_sell_scope_guard,
    resolve_reservation_service,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.cost_basis import PositionCostBasis
from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.domain.models import TradeSignal
from pmcopy.domain.position_key import PositionKey
from pmcopy.domain.source_time import SourceTimestampParser
from pmcopy.orchestrators.pipeline.pipeline_buy_stage import _prepare_buy_execution_request
from pmcopy.orchestrators.pipeline.pipeline_market_data import (
    _decimal_or_zero,
    _fetch_live_position_size as _default_fetch_live_position_size,
    _observe_copy_latency,
    _pipeline_facade_attr,
    _prefetch_market_data_for_signals as _default_prefetch_market_data_for_signals,
    _submit_copy_through_coordinator,
    get_reconciliation_service as _default_get_reconciliation_service,
)
from pmcopy.orchestrators.pipeline.pipeline_reconcile import (
    _decision_live_price,
    _project_pipeline_results,
    _trade_telemetry,
    reconcile_submitted_unconfirmed as _default_reconcile_submitted_unconfirmed,
)
from pmcopy.orchestrators.pipeline.pipeline_sell_stage import _prepare_sell_execution_request
from pmcopy.orchestrators.pipeline.pipeline_sizing_and_caps import (
    TradePipelineResult,
    _max_non_terminal_failure_attempts,
    _resolve_leader_tier,
    _retry_attempt_from_status,
    _trade_config,
    all_tx_hashes_for_trade,
    build_trade_key,
    normalize_signals,
)
from pmcopy.services.copy_tasks.source_freshness import SourceFreshnessPolicy
from pmcopy.services.execution.types import ExecutionAmountProjection
from pmcopy.services.observability.metrics import metrics
from pmcopy.services.trading.entry_permission_service import EntryDecision, EntryPermissionService
from pmcopy.services.trading.leader_performance import FollowerLeaderKey
from pmcopy.storage.postgres.fills_repo import FillRecord
from pmcopy.storage.postgres.orders_repo import _RETRYABLE_FAILED_STATUS
from pmcopy.utils.logger import (
    get_logger,
    log_trade_execution,
    log_trade_skip as _default_log_trade_skip,
    log_trade_submitted as _default_log_trade_submitted,
    print_executing_strategy,
    print_skip_reason as _default_print_skip_reason,
    print_trade_result as _default_print_trade_result,
    shorten_addr,
)

logger = get_logger("TradePipeline")


async def execute_trades(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    now_ts: float,
) -> list[TradePipelineResult]:
    """Execute one leader batch and project its outcome onto operator events.

    This wrapper is the pipeline's single telemetry choke point. It performs
    **zero** work before ``_execute_trades_impl`` runs, so the detection-to-submit
    path is unchanged, and projects the returned rows afterwards. An escaped
    exception is reported as a worker failure and then re-raised unchanged;
    ``asyncio.CancelledError`` is a shutdown signal and passes straight through.

    Args:
        context: Trading context for one follower.
        leader_id: Copied leader identity.
        signals: Raw leader rows detected for this batch.
        now_ts: Wall-clock timestamp of the batch.

    Returns:
        One ``TradePipelineResult`` per admitted signal, unchanged.

    Raises:
        Exception: Whatever ``_execute_trades_impl`` raised.
    """
    try:
        results = await _pipeline_facade_attr("_execute_trades_impl", _execute_trades_impl)(
            context, leader_id, signals, now_ts
        )
    except Exception as exc:
        _trade_telemetry(context).pipeline_failed(
            str(getattr(context, "user_id", "") or ""),
            leader_id,
            exc,
        )
        raise
    _project_pipeline_results(context, leader_id, signals, results)
    return results


async def _execute_trades_impl(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    now_ts: float,
) -> list[TradePipelineResult]:
    """Execute normalized signals through admission, risk, and reservation gates."""
    reconcile_submitted_unconfirmed = _pipeline_facade_attr(
        "reconcile_submitted_unconfirmed", _default_reconcile_submitted_unconfirmed
    )
    _prefetch_market_data_for_signals = _pipeline_facade_attr(
        "_prefetch_market_data_for_signals", _default_prefetch_market_data_for_signals
    )
    get_reconciliation_service = _pipeline_facade_attr(
        "get_reconciliation_service", _default_get_reconciliation_service
    )
    _fetch_live_position_size = _pipeline_facade_attr(
        "_fetch_live_position_size", _default_fetch_live_position_size
    )
    print_skip_reason = _pipeline_facade_attr("print_skip_reason", _default_print_skip_reason)
    print_trade_result = _pipeline_facade_attr("print_trade_result", _default_print_trade_result)
    log_trade_skip = _pipeline_facade_attr("log_trade_skip", _default_log_trade_skip)
    log_trade_submitted = _pipeline_facade_attr("log_trade_submitted", _default_log_trade_submitted)
    if context.execution_service is None:
        raise RuntimeError("TradingContext.execution_service is required")
    leader_scorer = context.leader_scorer
    if leader_scorer is None:
        raise RuntimeError("TradingContext.leader_scorer is required")
    copy_decision_svc = context.copy_decision_service
    if copy_decision_svc is None:
        raise RuntimeError("TradingContext.copy_decision_service is required")

    # Reconcile any BUYs from previous cycles whose fill confirmation was delayed.
    # This must happen before SELL processing so that positions are visible.
    try:
        n = await reconcile_submitted_unconfirmed(context)
        if n:
            logger.info("reconcile_unconfirmed_batch | count=%d", n)
    except Exception as exc:
        logger.warning("reconcile_unconfirmed_error | %s", repr(exc))

    normalized = normalize_signals(signals, leader_id)
    results: list[TradePipelineResult] = []
    freshness = SourceFreshnessPolicy(
        _trade_config(context).copy_trading.copy_max_signal_age_seconds
    )
    processing_time = datetime.fromtimestamp(now_ts, timezone.utc)
    admitted: list[tuple[TradeSignal, dict[str, Any]]] = []
    for signal, raw in normalized:
        source_time = SourceTimestampParser.parse(signal.event_ts, unit="seconds")
        reason = (
            freshness.entry_rejection(source_time, processing_time)
            if signal.side == "BUY"
            else None
        )
        if reason is None:
            admitted.append((signal, raw))
        else:
            results.append(
                TradePipelineResult(
                    trade_key=build_trade_key(context.user_id, leader_id, raw),
                    status="skipped",
                    reason=reason,
                    order_id=None,
                )
            )
    normalized = admitted

    leader_tier_resolved = await _resolve_leader_tier(context, leader_id)
    if _trade_config(context).execution.paper_trade_only and leader_tier_resolved == "live_follow":
        logger.info(
            "paper_trade_only_override_all",
            extra={
                "user_id": context.user_id,
                "leader_id": leader_id,
                "original_tier": leader_tier_resolved,
            },
        )
        leader_tier_resolved = "paper_follow"

    analytics: dict[str, object] = {}
    if context.leader_performance is not None:
        stats = await context.leader_performance.get_closed_episode_stats(
            FollowerLeaderKey(context.user_id, leader_id)
        )
        win_fraction = stats.win_rate_fraction
        analytics = {
            "net_pnl": stats.realized_pnl if stats.closed_episodes else None,
            "win_rate": None if win_fraction is None else win_fraction * Decimal(100),
            "sample_size": stats.closed_episodes,
        }
    leader_score = leader_scorer.score_leader(
        leader_id,
        analytics=analytics,
        recent_trade_count=len(signals),
    )

    market_min_size_by_token: dict[str, Decimal] = {}
    tick_size_by_token: dict[str, Decimal] = {}
    neg_risk_by_token: dict[str, bool] = {}
    market_data_stale_tokens: set[str] = set()
    market_data_stale_codes: dict[tuple[str, str], str] = {}
    metadata_by_token: dict[str, Any] = {}
    # The compatibility source name ``rest_snapshot`` now selects metadata-only
    # copy prefetch; WSS mode retains its subscribe/bootstrap/bundle path. Both
    # modes must run before sizing so absent dynamic metadata fails closed.
    if normalized:
        (
            bundles_by_token,
            market_data_stale_tokens,
            market_data_stale_codes,
        ) = await _prefetch_market_data_for_signals(
            context,
            normalized,
            metadata_out=metadata_by_token,
        )
        for token_id, metadata in metadata_by_token.items():
            try:
                tick_size = Decimal(str(metadata.tick_size))
                min_order_size = Decimal(str(metadata.min_order_size))
                neg_risk = metadata.neg_risk
                if tick_size <= 0 or min_order_size <= 0 or not isinstance(neg_risk, bool):
                    raise ValueError("invalid execution metadata")
                tick_size_by_token[str(token_id)] = tick_size
                market_min_size_by_token[str(token_id)] = min_order_size
                neg_risk_by_token[str(token_id)] = neg_risk
            except (AttributeError, TypeError, ValueError):
                market_data_stale_tokens.add(str(token_id))
        for token_id, bundle in bundles_by_token.items():
            try:
                market_min_size_by_token[token_id] = Decimal(str(bundle.tick.min_order_size))
                tick_size_by_token[token_id] = Decimal(str(bundle.tick.tick_size))
                neg_risk = bundle.tick.neg_risk
                if not isinstance(neg_risk, bool):
                    raise ValueError("invalid neg_risk metadata")
                neg_risk_by_token[token_id] = neg_risk
            except (TypeError, ValueError, AttributeError):
                # Defensive: a malformed bundle should not crash the loop.
                market_data_stale_tokens.add(token_id)
                market_min_size_by_token.pop(token_id, None)
                tick_size_by_token.pop(token_id, None)
                neg_risk_by_token.pop(token_id, None)
                for sig, _raw in normalized:
                    if sig.token_id == token_id:
                        side = str(getattr(sig, "side", "") or "").upper()
                        market_data_stale_codes.setdefault((token_id, side), "market_data_stale")

    for signal, raw in normalized:
        labels = {"user_id": context.user_id, "leader_id": leader_id}
        sell_entry_price: Decimal | None = None
        metrics.inc("trades_detected_total", labels=labels)

        trade_key = build_trade_key(context.user_id, leader_id, raw)
        # Phase 3: atomic claim replaces the prior non-atomic
        # has_processed_trade → mark_processed_trade check-then-set. A
        # concurrent worker that races on the same trade_key sees the row
        # already inserted and gets False, so only one pipeline run can
        # advance past this point per trade_key.
        _raw_tx_hash = raw.get("transactionHash") or raw.get("txHash") or ""
        _raw_side = str(raw.get("side") or "").strip().upper()
        _raw_asset = raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or ""
        _raw_condition = raw.get("conditionId") or raw.get("condition_id") or ""
        if not await context.orders_repo.try_claim_processed_trade(
            trade_key=trade_key,
            user_id=context.user_id,
            leader_id=leader_id,
            tx_hash=str(_raw_tx_hash),
            side=_raw_side,
            asset=str(_raw_asset),
            condition_id=str(_raw_condition),
            status="in_progress",
        ):
            metrics.inc("duplicate_order_prevented_total", labels=labels)
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason="already_processed",
                    order_id=None,
                )
            )
            continue

        # Phase 2.10 (H6) — claim ALL constituent tx hashes for an aggregated
        # signal so a second detection channel (on-chain WS) cannot re-trigger
        # an already-processed trade via a different constituent hash. The
        # primary claim above is the atomic gate; these are best-effort marks
        # for the remaining constituents (a concurrent claim on a constituent
        # just means that channel already saw it).
        for _constituent_hash in all_tx_hashes_for_trade(raw):
            if _constituent_hash and _constituent_hash != str(_raw_tx_hash):
                _constituent_key = build_trade_key(
                    context.user_id,
                    leader_id,
                    {
                        "transactionHash": _constituent_hash,
                        "side": _raw_side,
                        "asset": _raw_asset,
                    },
                )
                try:
                    await context.orders_repo.try_claim_processed_trade(
                        trade_key=_constituent_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=_constituent_hash,
                        side=_raw_side,
                        asset=str(_raw_asset),
                        condition_id=str(_raw_condition),
                        status="in_progress",
                    )
                except Exception:
                    logger.debug(
                        "constituent_tx_hash_claim_failed | hash=%s",
                        _constituent_hash[:16],
                        exc_info=True,
                    )

        if str(signal.side or "").upper() == "BUY":
            _sell_scope_guard = resolve_leader_sell_scope_guard(context)
            if _sell_scope_guard is not None:
                try:
                    _sell_scope_paused = await _sell_scope_guard.is_buy_paused(
                        leader_id,
                        signal.token_id,
                    )
                except Exception as exc:
                    _sell_scope_paused = True
                    logger.error(
                        "leader_sell_scope_guard_unavailable | leader=%s token=%s error_type=%s",
                        leader_id,
                        signal.token_id,
                        type(exc).__name__,
                    )
                if _sell_scope_paused:
                    skip_code = "leader_sell_fraction_scope_degraded"
                    print_skip_reason(
                        "Leader SELL sizing degraded",
                        "BUY blocked for this leader/outcome until an authoritative "
                        "leader-position source recovers",
                    )
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price) if signal.price else None,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason="emergency leader SELL fraction is still active",
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": skip_code},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_code}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason=skip_code,
                            order_id=None,
                        )
                    )
                    continue

        # ------------------------------------------------------------------
        # Market-data freshness gate — fail closed when WSS is not ready
        # ------------------------------------------------------------------
        # MarketDataService is the sole authority for executable market data.
        # If subscription + WSS handshake did not converge inside the
        # configured window, skip the signal rather than trade on stale or
        # Fail closed when either metadata-only or WSS input resolution failed.
        _signal_side_norm = str(getattr(signal, "side", "") or "").upper()
        _stale_pair_key = (signal.token_id, _signal_side_norm)
        _bundle_for_pair = None
        if _stale_pair_key in market_data_stale_codes or signal.token_id in market_data_stale_tokens:
            skip_code = market_data_stale_codes.get(_stale_pair_key, "market_data_stale")
            print_skip_reason(
                "Execution inputs unavailable",
                f"{signal.side} {signal.title or shorten_addr(signal.token_id, 8, 6)} "
                f"— {skip_code} (fail-closed: required metadata unavailable)",
            )
            log_trade_skip(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(signal.size) if signal.size else None,
                price=float(signal.price) if signal.price else None,
                usdc_value=float(signal.usdc_size),
                skip_code=skip_code,
                reason="execution inputs unavailable: " + skip_code,
                event_slug=signal.event_slug,
                meta={"user_id": context.user_id, "leader_id": leader_id},
            )
            metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
            await context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status=f"skipped_{skip_code}",
            )
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="skipped",
                    reason=skip_code,
                    order_id=None,
                )
            )
            continue

        # ------------------------------------------------------------------
        # Per-scoped-position action coordination (issue #28 / #67)
        # ------------------------------------------------------------------
        # Serialize all observe-then-mutate work on this scoped position
        # against the risk monitor's protective SELL path and any concurrent
        # reconciliation state-apply on the same (condition_id, token_id).
        # Unrelated positions remain fully concurrent.
        _scope = PositionKey.from_signal_event(signal, context.user_id)
        async with context.position_coordinator.acquire(_scope):
            logger.debug("trade_pipeline.signal_scope_acquired", extra=_scope.log_fields())
            # ------------------------------------------------------------------
            # Entry-permission gate — single authoritative BUY re-entry check
            # ------------------------------------------------------------------
            if signal.side == "BUY":
                _eps: EntryPermissionService | None = context.entry_permission_service
                if _eps is None:
                    logger.error(
                        "entry_permission_unavailable | user=%s leader=%s condition=%s token=%s",
                        context.user_id,
                        leader_id,
                        signal.condition_id,
                        signal.token_id,
                    )
                    skip_reason = "entry_policy_unavailable"
                    print_skip_reason(
                        "Entry policy unavailable",
                        "BUY blocked fail-closed — entry-permission service is "
                        "not wired; cannot evaluate re-entry safety",
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_reason})
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason=skip_reason,
                            order_id=None,
                        )
                    )
                    continue

                # NOTE: ``leader_notional_usdc`` MUST be the leader's CURRENT
                # total position notional in this market — not the size of a
                # single signal. Passing ``signal.usdc_size`` here causes the
                # ``fresh_leader_buy_after_net_flat`` clear rule to mis-fire on
                # any incremental BUY whose notional is < 10% of the trigger
                # notional, prematurely clearing the risk block. We do not
                # currently fetch the leader's live position on the BUY path,
                # so we pass ``None`` to disable that rule. The
                # ``leader_changed`` rule still applies. (Greptile P1 PR #63.)
                _decision: EntryDecision | None = await _eps.check_entry(
                    user_id=context.user_id,
                    condition_id=signal.condition_id,
                    token_id=signal.token_id,
                    leader_id=leader_id,
                    leader_notional_usdc=None,
                )

                if _decision is not None and not _decision.allowed:
                    skip_reason = "market_risk_quarantined"
                    _quarantine_detail = f"entry denied by policy [{_decision.reason}]" + (
                        f": {_decision.deny_detail}" if _decision.deny_detail else ""
                    )
                    print_skip_reason("Market quarantined by risk monitor", _quarantine_detail)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price or 0),
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_reason,
                        reason=_quarantine_detail,
                        event_slug=signal.event_slug,
                        meta={
                            "user_id": context.user_id,
                            "leader_id": leader_id,
                            "policy_reason": _decision.reason,
                        },
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_reason})
                    metrics.inc(
                        "entry_denied_total",
                        labels={**labels, "policy_reason": _decision.reason},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{skip_reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason=skip_reason,
                            order_id=None,
                        )
                    )
                    continue

                if _decision is not None and _decision.clear_rule_applied:
                    # Block was auto-cleared — emit a metric so we can track clear-rule firing
                    metrics.inc(
                        "entry_block_auto_cleared_total",
                        labels={**labels, "clear_rule": _decision.clear_rule_applied},
                    )

            # print_new_trade_detected removed: tradeMonitor already prints on detection

            # --- Tier gate: check follow tier before costly decision/execution ---
            leader_tier = leader_tier_resolved

            if leader_tier == "watchlist":
                config = _trade_config(context)
                if config.copy_trading.enable_tier_gate_watchlist:
                    if config.copy_trading.enable_tier_gate_watchlist_log:
                        logger.info(
                            "tier_gate_watchlist",
                            extra={
                                "user_id": context.user_id,
                                "leader_id": leader_id,
                                "trade_key": trade_key,
                                "side": signal.side,
                                "condition_id": signal.condition_id,
                            },
                        )
                    print_skip_reason(
                        "Tier gate: watchlist",
                        "Leader is on watchlist — observing only",
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": "tier_watchlist"},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="skipped_tier_watchlist",
                    )
                    results.append(
                        TradePipelineResult(trade_key, "skipped", "tier_watchlist", None)
                    )
                    continue
            if leader_tier == "paper_follow":
                logger.info(
                    "tier_gate_paper",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "trade_key": trade_key,
                        "side": signal.side,
                        "condition_id": signal.condition_id,
                        "usdc_size": float(signal.usdc_size),
                    },
                )
                print_skip_reason(
                    "Tier gate: paper follow",
                    "Paper-tracking only — no live order submitted",
                )
                metrics.inc("trades_paper_total", labels=labels)
                await context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="paper_follow",
                )
                results.append(TradePipelineResult(trade_key, "paper", "tier_paper_follow", None))
                continue

            # --- Decision gate: score the leader and this specific trade ---
            # The copied fill is the sole price for sizing and the one-shot FAK.
            live_price = _decision_live_price(signal, _bundle_for_pair)
            market_min_size_decimal = market_min_size_by_token.get(
                signal.token_id,
                Decimal("0"),
            )
            market_tick_size_decimal = tick_size_by_token.get(
                signal.token_id,
                Decimal("0"),
            )
            market_min_size = float(market_min_size_decimal)
            market_tick_size = float(market_tick_size_decimal)

            if signal.side == "BUY":
                exec_req = await _prepare_buy_execution_request(
                    context=context,
                    signal=signal,
                    raw=raw,
                    leader_id=leader_id,
                    trade_key=trade_key,
                    labels=labels,
                    results=results,
                    copy_decision_svc=copy_decision_svc,
                    leader_score=leader_score,
                    live_price=live_price,
                    _bundle_for_pair=_bundle_for_pair,
                    market_min_size=market_min_size,
                    market_min_size_decimal=market_min_size_decimal,
                    market_tick_size=market_tick_size,
                    market_tick_size_decimal=market_tick_size_decimal,
                    neg_risk_by_token=neg_risk_by_token,
                )
                if exec_req is None:
                    continue
            else:
                exec_req, sell_entry_price = await _prepare_sell_execution_request(
                    context=context,
                    signal=signal,
                    raw=raw,
                    leader_id=leader_id,
                    trade_key=trade_key,
                    labels=labels,
                    results=results,
                    live_price=live_price,
                    market_min_size=market_min_size,
                    market_min_size_decimal=market_min_size_decimal,
                    market_tick_size_decimal=market_tick_size_decimal,
                    neg_risk_by_token=neg_risk_by_token,
                )
                if exec_req is None:
                    continue

            print_executing_strategy(signal.side)
            try:
                exec_result = await _submit_copy_through_coordinator(
                    context, exec_req, signal, leader_id=leader_id
                )
            except BaseException as exc:
                is_reconcile_required = isinstance(
                    exc, RuntimeError
                ) and "Submission outcome requires reconciliation" in str(exc)
                if is_reconcile_required:
                    if signal.side == "BUY":
                        _regime = getattr(context, "balance_regime", None)
                        if _regime is not None:
                            with contextlib.suppress(Exception):
                                await asyncio.shield(_regime.commit(order_id=trade_key))
                    with contextlib.suppress(Exception):
                        await asyncio.shield(
                            context.orders_repo.mark_processed_trade(
                                trade_key=trade_key,
                                user_id=context.user_id,
                                leader_id=leader_id,
                                tx_hash=signal.tx_hash,
                                side=signal.side,
                                asset=signal.token_id,
                                condition_id=signal.condition_id,
                                status="submitted_unconfirmed",
                            )
                        )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="submitted_unconfirmed",
                            reason="coordinator_reconciliation_required",
                            order_id=None,
                        )
                    )
                    continue
                if signal.side == "BUY":
                    _regime = getattr(context, "balance_regime", None)
                    if _regime is not None:
                        with contextlib.suppress(Exception):
                            await asyncio.shield(_regime.release(order_id=trade_key))
                raise

            if exec_result.status == "duplicate_ignored":
                print_skip_reason(
                    "Duplicate ignored",
                    exec_result.error or "an in-flight order already covers this trade",
                )
                metrics.inc("duplicate_order_prevented_total", labels=labels)
                # Greptile P1: roll back the regime reservation taken at the
                # pre-submit ``reserve()`` so the daily cap is not double-
                # consumed when the coordinator dedup-fires.
                if signal.side == "BUY":
                    _regime = getattr(context, "balance_regime", None)
                    if _regime is not None:
                        try:
                            await _regime.release(order_id=trade_key)
                        except Exception:  # pragma: no cover — defensive
                            pass
                await context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="duplicate_ignored",
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="skipped",
                        reason="duplicate_ignored",
                        order_id=exec_result.order_id,
                    )
                )
                continue

            # Phase 4 — commit the BUY reservation held by the pre-submit
            # ``reserve()`` against the rolling 24h spend ceiling. Fires on
            # BOTH the immediate-fill path and the accepted-no-fill path so
            # every submitted BUY consumes the daily cap; ``commit`` with
            # ``actual_filled_usdc`` returns any unfilled remainder. A
            # rejection (``not exec_result.accepted``) instead RELEASES the
            # reservation a few lines below. SELLs do not consume the
            # daily-spend budget.
            if exec_result.accepted and signal.side == "BUY":
                regime = getattr(context, "balance_regime", None)
                if regime is not None:
                    try:
                        _filled_amt = Decimal(str(exec_result.filled_amount or 0))
                        _avg_px = (
                            Decimal(str(exec_result.avg_price))
                            if exec_result.avg_price is not None
                            else Decimal("0")
                        )
                        if _filled_amt > 0 and _avg_px > 0:
                            await regime.commit(
                                order_id=trade_key,
                                actual_filled_usdc=_filled_amt * _avg_px,
                            )
                        else:
                            # Accepted with unknown fill price or deferred fill — keep the
                            # held reservation intact until reconciliation resolves actual spend.
                            await regime.commit(order_id=trade_key)
                    except Exception:  # pragma: no cover — defensive
                        pass

            if exec_result.accepted and _decimal_or_zero(exec_result.filled_amount) <= 0:
                # Execution is settlement-deferred by design: a FAK that the
                # venue accepts returns filled_amount=0 / submitted_unconfirmed
                # and the reconciler owns final accounting. This is NOT a "price
                # moved past limit" kill (a genuine kill returns accepted=False);
                # the order may well have filled. Report it as pending, not dead.
                # Copy-scoped display values. ``exec_req`` carries the COPY size,
                # not the leader's ``signal.size``: for a BUY the request amount
                # is USDC notional (tokens ≈ notional / limit price); for a SELL
                # it is shares directly.
                _submitted_amount = getattr(exec_result, "requested_amount", None)
                _copy_amount = _decimal_or_zero(
                    _submitted_amount
                    if _submitted_amount is not None
                    else getattr(exec_req, "amount", 0)
                )
                _limit_px = _decimal_or_zero(
                    getattr(exec_result, "effective_price", None)
                    or getattr(exec_req, "price_limit", None)
                    or signal.price
                    or 0
                )
                _copy_tokens: Decimal | None
                _copy_usdc: Decimal | None
                if exec_req.side == "BUY":
                    _copy_usdc = _copy_amount
                    _copy_tokens = (_copy_amount / _limit_px) if _limit_px > 0 else None
                else:
                    _copy_tokens = _copy_amount
                    _copy_usdc = (_copy_amount * _limit_px) if _limit_px > 0 else None
                await context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="submitted_unconfirmed",
                    order_id=exec_result.order_id,
                )
                log_trade_submitted(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(_copy_tokens) if _copy_tokens is not None else None,
                    price=float(_limit_px) if _limit_px > 0 else None,
                    usdc_value=float(_copy_usdc) if _copy_usdc is not None else None,
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "submissionState": "submitted_unconfirmed",
                        "copyOrderId": exec_result.order_id,
                    },
                )
                _observe_copy_latency(raw, labels, "submitted_unconfirmed")
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="submitted_unconfirmed",
                        reason="accepted_no_fill_confirmed",
                        order_id=exec_result.order_id,
                    )
                )
                continue

            if not exec_result.accepted:
                # Greptile P1: roll back the regime reservation taken at the
                # pre-submit ``reserve()`` so a rejected BUY does not consume
                # the daily cap. SELLs never reserved against the cap so the
                # release is BUY-only.
                if signal.side == "BUY":
                    _regime = getattr(context, "balance_regime", None)
                    if _regime is not None:
                        try:
                            await _regime.release(order_id=trade_key)
                        except Exception:  # pragma: no cover — defensive
                            pass
                metrics.inc(
                    "execution_failures_total",
                    labels={**labels, "reason": exec_result.status},
                )
                failure_price = Decimal(
                    str(getattr(exec_req, "price_limit", None) or signal.price or 0)
                )
                failure_amounts = ExecutionAmountProjection.from_request(
                    side=signal.side,
                    amount=Decimal(str(exec_req.amount)),
                    price=failure_price,
                )
                log_trade_skip(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=(
                        float(failure_amounts.token_size)
                        if failure_amounts.token_size is not None
                        else None
                    ),
                    price=float(failure_price) if failure_price > 0 else None,
                    usdc_value=(
                        float(failure_amounts.usdc_value)
                        if failure_amounts.usdc_value is not None
                        else None
                    ),
                    skip_code=exec_result.status,
                    reason=exec_result.error or exec_result.status,
                    status="failed",
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "order_id": exec_result.order_id,
                        "sizeEstimated": failure_amounts.token_size_estimated,
                        "requestAmount": str(failure_amounts.request_amount),
                        "requestAmountUnit": failure_amounts.request_amount_unit,
                    },
                )
                is_terminal = exec_result.status == "rejected" or (
                    getattr(exec_req, "order_type", None) is ExecutionOrderType.FAK
                    and exec_result.status in {"canceled", "cancelled", "expired"}
                )
                max_non_terminal_attempts = _max_non_terminal_failure_attempts(context)
                prev_trade = await context.orders_repo.get_processed_trade(trade_key) or {}
                prev_retry_attempt = _retry_attempt_from_status(
                    prev_trade.get("status"), prev_trade.get("retry_attempt")
                )
                next_retry_attempt = prev_retry_attempt + 1
                non_terminal_retry_exhausted = (
                    not is_terminal and next_retry_attempt >= max_non_terminal_attempts
                )
                status_to_persist = (
                    "terminal_failed"
                    if is_terminal
                    else (
                        "terminal_failed"
                        if non_terminal_retry_exhausted
                        else _RETRYABLE_FAILED_STATUS
                    )
                )
                await context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status=status_to_persist,
                    retry_attempt=next_retry_attempt,
                )
                fail_reason = exec_result.error or exec_result.status
                live_synced_size: Decimal | None = None
                if signal.side == "SELL" and "insufficient_balance" in str(fail_reason).lower():
                    # CLAUDE.md non-negotiable: SELL drift reconciliation must
                    # release the stranded SELL reservation in addition to
                    # syncing the local position. Pre-fix this branch called
                    # _sync_sell_position_from_live_holdings, which updates
                    # the position table but does NOT release any active
                    # reservation — leaving the SELL reservation "active"
                    # forever and blocking all future SELLs on the scope.
                    #
                    # Route through ReconciliationService.reconcile_position_drift
                    # which owns both the position update AND the
                    # reservation release on its drift-deleted path.
                    drift_reconciler = get_reconciliation_service(context)
                    drift_result = await drift_reconciler.reconcile_position_drift(
                        user_id=context.user_id,
                        condition_id=signal.condition_id,
                        token_id=signal.token_id,
                        leader_id=leader_id,
                        live_position_fetcher=lambda _token_id=signal.token_id: (
                            _fetch_live_position_size(context, token_id=_token_id)
                        ),
                        reservation_service=resolve_reservation_service(context),
                    )
                    live_synced_size = drift_result.get("live_size")
                    if drift_result.get("reconciled"):
                        fail_reason = (
                            f"{fail_reason} | drift_reconciled action={drift_result.get('action')} "
                            f"live_size={live_synced_size}"
                        )
                    elif drift_result.get("action") == "no_drift":
                        fail_reason = f"{fail_reason} | no_local_drift_detected"
                    # ReconciliationService emits the manual_position_drift_*
                    # audit events itself; no second emit here.
                if non_terminal_retry_exhausted:
                    fail_reason = (
                        f"{fail_reason} "
                        f"(retry budget exhausted after {next_retry_attempt} attempts)"
                    )
                print_trade_result(
                    success=False,
                    side=signal.side,
                    reason=fail_reason,
                    title=signal.title,
                    submarket=signal.submarket,
                    outcome=signal.outcome,
                )
                logger.warning(
                    "trade_copy_failed",
                    extra={
                        "type": "trade_copy_failed",
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "side": signal.side,
                        "status": exec_result.status,
                        "error": exec_result.error,
                        "title": signal.title,
                        "submarket": signal.submarket,
                        "outcome": signal.outcome,
                        "terminal": is_terminal,
                        "retry_exhausted": non_terminal_retry_exhausted,
                        "retry_attempt": next_retry_attempt,
                        "max_non_terminal_attempts": max_non_terminal_attempts,
                        "persisted_status": status_to_persist,
                    },
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="failed",
                        reason=fail_reason,
                        order_id=exec_result.order_id,
                    )
                )
                continue

            fill_price = exec_result.avg_price or signal.price
            fill_size = Decimal(str(exec_result.filled_amount))
            fee_paid = (
                None if exec_result.fees_paid is None else Decimal(str(exec_result.fees_paid))
            )
            try:
                # Atomic fill + position write — PR #108 finding: the
                # previous two-call sequence (append_fill auto-commit +
                # apply_fill in a separate pool connection) left a
                # crash-window where the fill was durably committed but
                # the position never updated. ``append_fill_and_apply``
                # serialises both in one ``conn.transaction()``; the
                # duplicate-fill short-circuit (fill_id == 0) prevents
                # double-counting when the ON CONFLICT DO NOTHING branch
                # fires.
                (
                    _fill_id,
                    updated_position,
                ) = await context.fills_repo.append_fill_and_apply(
                    FillRecord(
                        user_id=context.user_id,
                        leader_id=leader_id,
                        order_id=exec_result.order_id,
                        condition_id=signal.condition_id,
                        token_id=signal.token_id,
                        side=signal.side,
                        size_tokens=fill_size,
                        price=Decimal(str(fill_price or 0)),
                        fee=fee_paid,
                        fill_ts=int(now_ts),
                        source_action_id=signal.source_action_id,
                    ),
                    context.positions_repo,
                    raw_payload={
                        "signal_tx_hash": signal.tx_hash,
                        "copy_order_id": exec_result.order_id,
                        "status": exec_result.status,
                    },
                )
            except asyncpg.exceptions.UniqueViolationError as ie:
                error_msg = str(ie)
                if "idx_fills_unique" in error_msg or "fills" in error_msg.lower():
                    # Duplicate fill — idempotent, skip gracefully
                    logger.info(
                        "trade_pipeline_duplicate_fill_ignored",
                        extra={
                            "user_id": context.user_id,
                            "leader_id": leader_id,
                            "condition_id": signal.condition_id,
                            "order_id": exec_result.order_id,
                        },
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="duplicate_ignored",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key=trade_key,
                            status="skipped",
                            reason="duplicate_ignored",
                            order_id=exec_result.order_id,
                        )
                    )
                    continue
                # Non-duplicate integrity error — log and re-raise
                logger.error(
                    "trade_pipeline_integrity_error",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "signal_tx_hash": signal.tx_hash,
                        "order_id": exec_result.order_id,
                        "traceback": _tb.format_exc(),
                    },
                )
                raise

            if _fill_id == 0:
                # ON CONFLICT DO NOTHING fired — fill already persisted by a
                # prior call. Position, risk-block, and PnL were updated in
                # that call; replaying them here would double-count.
                logger.info(
                    "trade_pipeline_duplicate_fill_ignored",
                    extra={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "condition_id": signal.condition_id,
                        "order_id": exec_result.order_id,
                        "source": "on_conflict",
                    },
                )
                await context.orders_repo.mark_processed_trade(
                    trade_key=trade_key,
                    user_id=context.user_id,
                    leader_id=leader_id,
                    tx_hash=signal.tx_hash,
                    side=signal.side,
                    asset=signal.token_id,
                    condition_id=signal.condition_id,
                    status="duplicate_ignored",
                )
                results.append(
                    TradePipelineResult(
                        trade_key=trade_key,
                        status="skipped",
                        reason="duplicate_ignored",
                        order_id=exec_result.order_id,
                    )
                )
                continue

            if signal.side == "BUY":
                # Phase 4 — count the BUY against the rolling 24h spend
                # ceiling on the immediately-filled path too. The submitted-
                # unconfirmed branch (above) only fires when filled_amount<=0,
                # so without this hook fast fills bypass the daily ledger.
                if context.pnl_tracker is not None:
                    context.pnl_tracker.record_buy(
                        condition_id=signal.condition_id,
                        price=Decimal(str(fill_price)),
                        size_tokens=fill_size,
                        usdc_value=fill_size * Decimal(str(fill_price)),
                        leader_wallet=leader_id,
                        event_slug=signal.event_slug,
                        title=signal.title,
                    )
            else:
                updated_basis = PositionCostBasis.from_value(
                    getattr(updated_position, "avg_entry_price", None)
                    if updated_position is not None
                    else None
                )
                basis = PositionCostBasis(
                    sell_entry_price if sell_entry_price is not None else updated_basis.entry_price
                )
                entry_price = basis.entry_price
                realized = basis.gross_realized_pnl(Decimal(str(fill_price)), fill_size)
                if context.pnl_tracker is not None:
                    context.pnl_tracker.record_sell(
                        condition_id=signal.condition_id,
                        price=Decimal(str(fill_price)),
                        size_tokens=fill_size,
                        usdc_value=fill_size * Decimal(str(fill_price)),
                        entry_price=entry_price,
                        realized_pnl=realized,
                        exit_reason="leader_sell",
                        leader_wallet=leader_id,
                        event_slug=signal.event_slug,
                        title=signal.title,
                    )

            metrics.inc("trades_copied_total", labels=labels)

            log_trade_submitted(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(fill_size),
                price=float(fill_price or 0),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "copyOrderId": exec_result.order_id,
                    "order_id": exec_result.order_id,
                    "fee_paid": None if fee_paid is None else float(fee_paid),
                    "title": signal.title,
                    "submarket": signal.submarket,
                    "outcome": signal.outcome,
                },
            )
            _observe_copy_latency(raw, labels, "submitted")
            log_trade_execution(
                logger,
                side=signal.side,
                condition_id=signal.condition_id,
                asset=signal.token_id,
                size=float(fill_size),
                price=float(fill_price or 0),
                usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                tx_hash=signal.tx_hash,
                event_slug=signal.event_slug,
                meta={
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "copyOrderId": exec_result.order_id,
                    "order_id": exec_result.order_id,
                    "fee_paid": None if fee_paid is None else float(fee_paid),
                    "status": exec_result.status,
                    "title": signal.title,
                    "submarket": signal.submarket,
                    "outcome": signal.outcome,
                },
            )
            print_trade_result(
                success=True,
                side=signal.side,
                leader_tx=signal.tx_hash,
                copy_tx=exec_result.order_id,
                amount=float(fill_size * Decimal(str(fill_price or 0))),
                price=float(fill_price or 0),
                title=signal.title,
                submarket=signal.submarket,
                outcome=signal.outcome,
            )

            await context.orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=context.user_id,
                leader_id=leader_id,
                tx_hash=signal.tx_hash,
                side=signal.side,
                asset=signal.token_id,
                condition_id=signal.condition_id,
                status="executed",
            )

            tx_hashes = raw.get("_txHashes") or raw.get("txHashes") or []
            if isinstance(tx_hashes, list):
                for txh in tx_hashes:
                    tx_key = build_trade_key(
                        context.user_id,
                        leader_id,
                        {
                            "transactionHash": txh,
                            "side": signal.side,
                            "asset": signal.token_id,
                        },
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=tx_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=str(txh),
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="executed",
                    )

            context.readiness.update(last_successful_order_ts=int(time.time()))
            results.append(
                TradePipelineResult(
                    trade_key=trade_key,
                    status="executed",
                    reason=None,
                    order_id=exec_result.order_id,
                )
            )

    return results
