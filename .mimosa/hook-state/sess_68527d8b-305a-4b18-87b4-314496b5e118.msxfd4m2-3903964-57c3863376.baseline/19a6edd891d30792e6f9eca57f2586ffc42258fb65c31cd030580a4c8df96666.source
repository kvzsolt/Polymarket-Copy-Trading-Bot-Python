"""Normalize leader signals and execute them through the guarded pipeline."""

from __future__ import annotations

import asyncio
import time
import traceback as _tb
import uuid

import asyncpg
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any

from config.app_config import AppConfig
from application.copy_trade_service import CopyTradeIdentityBuilder
from domain.execution import ExecutionOrderType
from domain.models import TradeSignal
from domain.units import BASIS_POINTS_PER_UNIT, UNIX_MILLISECONDS_THRESHOLD
from application.trading_context import TradingContext
from services.entryPermissionService import EntryDecision, EntryPermissionService
from services.execution.reconciliation_service import ReconciliationService
from services.execution.pending_order_reconciliation import (
    PendingOrderReconciliationQueryService,
)
from services.executionService import ExecutionRequest
from services.metrics import COPY_LATENCY_BUCKETS, METRIC_COPY_LATENCY, metrics
from domain.position_key import PositionKey
from services.risk.risk_engine import headroom_for_buy
from storage.postgres.fills_repo import FillRecord
from storage.postgres.positions_repo import PositionRecord
from utils import is_valid_clob_token_id
from utils.dayEquity import get_day_start_equity
from utils.fetchData import fetch_data
from utils.getMyBalance import get_my_balance
from utils.logger import (
    get_logger,
    log_trade_execution,
    log_trade_submitted,
    log_trade_decision,
    log_trade_skip,
    print_executing_strategy,
    print_skip_reason,
    print_trade_result,
    print_warning,
    shorten_addr,
)

logger = get_logger("TradePipeline")
_COPY_TRADE_IDENTITY = CopyTradeIdentityBuilder()


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
    semaphore = context.extra.get("_trade_analysis_semaphore")
    if semaphore is None:
        config_val = context.config.execution.trade_analysis_concurrency if context.config else 6
        semaphore = asyncio.Semaphore(_positive_int(config_val, 6))
        context.extra["_trade_analysis_semaphore"] = semaphore
    return semaphore


def _reconcile_semaphore(context: TradingContext) -> asyncio.Semaphore:
    semaphore = context.extra.get("_reconcile_unconfirmed_semaphore")
    if semaphore is None:
        config_val = context.config.execution.reconcile_concurrency if context.config else 4
        semaphore = asyncio.Semaphore(_positive_int(config_val, 4))
        context.extra["_reconcile_unconfirmed_semaphore"] = semaphore
    return semaphore


def get_reconciliation_service(context: TradingContext) -> ReconciliationService:
    """Return the shared reconciler bound to this trading context.

    The reconciler is the canonical owner of unconfirmed-order settlement
    (issue #71). One instance per ``TradingContext`` keeps batch reconciles
    consistent across trade and risk paths.
    """
    svc = context.extra.get("_reconciliation_service")
    task_budget_settlement = context.extra.get("task_budget_settlement")
    reservation_service = context.extra.get("reservation_service")
    if reservation_service is None:
        coordinator = getattr(context, "execution_coordinator", None)
        reservation_service = getattr(coordinator, "reservation_service", None)
        if reservation_service is not None:
            context.extra["reservation_service"] = reservation_service
    if svc is None:
        svc = ReconciliationService(
            orders_repo=context.orders_repo,
            fills_repo=context.fills_repo,
            positions_repo=context.positions_repo,
            risk_blocks_repo=getattr(context, "risk_blocks_repo", None),
            risk_triggers_repo=getattr(context, "risk_triggers_repo", None),
            clob_gateway=context.extra.get("clob_gateway"),
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
        )
        context.extra["_reconciliation_service"] = svc
    elif getattr(svc, "reservation_service", None) is None and reservation_service is not None:
        svc.reservation_service = reservation_service
    if task_budget_settlement is not None:
        svc.task_budget_settlement = task_budget_settlement
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
    from services.execution.coordinator import is_live_mode

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
                from services.metrics import (
                    METRIC_COORDINATOR_MISSING_SKIP,
                    metrics,
                )

                metrics.inc_sync(
                    METRIC_COORDINATOR_MISSING_SKIP,
                    labels={"side": exec_req.side, "source": "copy_trade"},
                )
            except Exception:
                pass
            from services.executionService import ExecutionResult as _ER

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

    from services.execution.coordinator import ExecutionIntent

    side = (exec_req.side or "BUY").upper()
    try:
        price = Decimal(str(signal.price or 0))
    except Exception:
        price = Decimal("0")
    try:
        notional = Decimal(str(signal.usdc_size or 0))
    except Exception:
        notional = exec_req.amount * price if price > 0 else Decimal("0")

    intent = ExecutionIntent(
        user_id=context.user_id,
        condition_id=exec_req.condition_id or signal.condition_id,
        token_id=exec_req.token_id,
        side=side,
        size=Decimal(str(exec_req.amount)),
        price=price,
        notional_usdc=notional,
        idempotency_key=signal.tx_hash,
        source="copy_trade",
        signal_id=exec_req.signal_id or signal.tx_hash,
        leader_wallet=leader_id,
        max_slippage_bps=int(exec_req.max_slippage_bps),
        reason=exec_req.reason or "copy_trade",
        recovery_metadata=exec_req.recovery_metadata,
    )
    coord_result = await coordinator.submit_copy_trade(intent)
    raw = coord_result.raw_result
    if raw is not None:
        return raw
    # Build a minimal ExecutionResult so downstream code (which reads status,
    # filled_amount, etc.) continues to work.
    from services.executionService import ExecutionResult

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
    from services.market_data_service import (  # noqa: WPS433  local import
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
) -> tuple[dict[str, Any], set[str], dict[tuple[str, str], str]]:
    """Resolve a fresh executable market bundle for each signal token.

    In REST primary mode (``MARKET_DATA_PRIMARY_SOURCE=rest_snapshot``) this
    calls :meth:`MarketDataService.require_execution_snapshot` once per unique
    ``(token_id, side)`` pair, immediately before sizing. After the REST
    snapshot has seeded ``MarketStateStore``, the token is best-effort
    subscribed to market WSS so later book/quote updates can overwrite the
    REST source in the same store. No WSS readiness wait, no retry loop — one
    fresh REST `/book` per pair, gated against the millisecond budgets
    configured on :class:`MarketDataPolicy`.

    In WSS primary mode the legacy path is used: subscribe → wait for
    ``ensure_token_ready_or_bootstrapped`` → ``require_fresh_data`` retry loop.

    Returns:
        bundles_by_token: token_id -> MarketDataBundle for any token that
            resolved successfully. Used by the per-signal loop to extract
            ``tick_size`` / ``min_order_size`` for sizing.
        stale_tokens: token_ids whose primary-mode lookup failed at least
            once. Retained for back-compat with callers that key off the
            token only.
        stale_skip_codes: ``(token_id, side)`` -> ``skip_code`` for each
            failed lookup. Used by the per-signal loop to surface a
            specific reason (``market_data_stale``,
            ``market_data_timeout``, ``book_no_liquidity``,
            ``book_spread_wide``, ``market_resolved``).
    """
    if not normalized:
        return {}, set(), {}

    primary_source = _market_data_primary_source(context)

    if primary_source == "rest_snapshot":
        return await _prefetch_market_data_rest_snapshot(context, normalized)

    return await _prefetch_market_data_wss(context, normalized)


async def _prefetch_market_data_rest_snapshot(
    context: TradingContext,
    normalized: list,
) -> tuple[dict[str, Any], set[str], dict[tuple[str, str], str]]:
    """REST-primary path. One fresh `/book` per unique (token_id, side).

    This path intentionally does not wait for WSS readiness before sizing, but
    it still subscribes the token after the REST snapshot write. That preserves
    the intended handoff: REST seeds ``MarketStateStore`` immediately; WSS then
    owns ongoing quote/book updates in the same token-keyed store.
    """
    mds = getattr(context, "market_data_service", None)
    if mds is None:
        # No MDS → fail closed: every signal will skip with market_data_stale.
        stale_tokens: set[str] = set()
        stale_codes: dict[tuple[str, str], str] = {}
        for signal, _ in normalized:
            token_id = getattr(signal, "token_id", None)
            side = str(getattr(signal, "side", "") or "").upper()
            if not token_id:
                continue
            stale_tokens.add(token_id)
            stale_codes[(token_id, side)] = "market_data_stale"
        return {}, stale_tokens, stale_codes

    from services.market_data_service import (  # noqa: WPS433  local import
        MarketDataError,
        MarketDataNotReady,
        StaleDataError,
    )

    unique_pairs: list[tuple[str, str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    for signal, _ in normalized:
        token_id = getattr(signal, "token_id", None)
        side = str(getattr(signal, "side", "") or "").upper()
        condition_id = getattr(signal, "condition_id", None)
        if not token_id or not side:
            continue
        key = (token_id, side)
        if key in seen:
            continue
        seen.add(key)
        unique_pairs.append((token_id, side, condition_id))

    if not unique_pairs:
        return {}, set(), {}

    analysis_sem = _analysis_semaphore(context)
    bundles_by_token: dict[str, Any] = {}
    bundles_by_pair: dict[tuple[str, str], Any] = {}
    stale_tokens: set[str] = set()
    stale_codes: dict[tuple[str, str], str] = {}

    async def _resolve_one(
        token_id: str,
        side: str,
        condition_id: str | None,
    ) -> tuple[str, str, Any, str | None]:
        async with analysis_sem:
            logger.info(
                "trade_pipeline_market_data_snapshot_start | token_id=%s side=%s cond=%s",
                token_id,
                side,
                condition_id,
            )
            try:
                bundle = await mds.require_execution_snapshot(
                    token_id,
                    side=side,
                    condition_id=condition_id,
                )
            except MarketDataNotReady as exc:
                code = exc.skip_code or "market_resolved"
                logger.info(
                    "trade_pipeline_market_data_snapshot_rejected | token_id=%s side=%s reason=%s",
                    token_id,
                    side,
                    code,
                )
                return token_id, side, None, code
            except StaleDataError as exc:
                code = exc.skip_code or "market_data_stale"
                if code == "market_data_timeout":
                    logger.info(
                        "trade_pipeline_market_data_snapshot_timeout | token_id=%s side=%s",
                        token_id,
                        side,
                    )
                logger.info(
                    "trade_pipeline_market_data_snapshot_rejected | token_id=%s side=%s reason=%s",
                    token_id,
                    side,
                    code,
                )
                return token_id, side, None, code
            except MarketDataError as exc:
                code = getattr(exc, "skip_code", None) or "market_data_stale"
                logger.info(
                    "trade_pipeline_market_data_snapshot_rejected | token_id=%s side=%s reason=%s",
                    token_id,
                    side,
                    code,
                )
                return token_id, side, None, code
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "trade_pipeline_market_data_snapshot_rejected | token_id=%s side=%s reason=unexpected error=%r",
                    token_id,
                    side,
                    exc,
                )
                return token_id, side, None, "market_data_stale"

            quote = getattr(bundle, "quote", None)
            book = getattr(bundle, "book", None)
            best_bid = getattr(quote, "best_bid", None) if quote is not None else None
            best_ask = getattr(quote, "best_ask", None) if quote is not None else None
            spread_bps: int | None = None
            if best_bid is not None and best_ask is not None and best_ask > 0:
                try:
                    spread_bps = int(((best_ask - best_bid) / best_ask) * BASIS_POINTS_PER_UNIT)
                except Exception:
                    spread_bps = None
            bid_count = len(getattr(book, "bids", {}) or {}) if book is not None else 0
            ask_count = len(getattr(book, "asks", {}) or {}) if book is not None else 0
            logger.info(
                "trade_pipeline_market_data_snapshot_ready | token_id=%s side=%s source=rest_snapshot"
                " bid_count=%d ask_count=%d spread_bps=%s",
                token_id,
                side,
                bid_count,
                ask_count,
                spread_bps,
            )
            return token_id, side, bundle, None

    results = await asyncio.gather(
        *(
            _resolve_one(token_id, side, condition_id)
            for token_id, side, condition_id in unique_pairs
        ),
        return_exceptions=True,
    )
    for entry in results:
        if isinstance(entry, BaseException):
            logger.warning(
                "trade_pipeline_market_data_snapshot_rejected | reason=gather_error error=%r",
                entry,
            )
            continue
        token_id, side, bundle, skip_code = entry
        if bundle is None:
            stale_tokens.add(token_id)
            stale_codes[(token_id, side)] = skip_code or "market_data_stale"
            continue
        bundles_by_pair[(token_id, side)] = bundle
        # Any per-token sizing data (tick/min_order_size) is side-independent;
        # keep one bundle keyed by token for downstream sizing inputs.
        bundles_by_token.setdefault(token_id, bundle)

    # Attach per-pair bundles on the context so the per-signal loop can pull
    # the exact side's bundle when constructing decision/execute metadata.
    # SHP-09: ``context.extra`` is the canonical extension point — direct
    # ``__dict__`` writes bypass the dataclass field set and would break
    # under a future ``frozen=True`` / ``__slots__`` switch.
    context.extra["_md_bundles_by_pair"] = bundles_by_pair

    # REST-primary execution seeds the store from `/book`, then immediately
    # hands the token to WSS for ongoing updates. Do not call
    # ensure_token_ready_or_bootstrapped() here: that would reintroduce a WSS
    # readiness wait on the latency-critical copy-trade path.
    for token_id in {token_id for token_id, _side, _condition_id in unique_pairs}:
        await _ensure_token_subscribed(context, token_id)

    return bundles_by_token, stale_tokens, stale_codes


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
    wallet = str(getattr(context, "wallet_address", "") or "").strip().lower()
    if not wallet:
        return None

    try:
        positions = await fetch_data(
            f"https://data-api.polymarket.com/positions?user={wallet}",
            silent=True,
        )
    except Exception as exc:
        logger.warning(
            "live_position_sync_fetch_failed | user_id=%s token=%s error=%s",
            context.user_id,
            token_id,
            repr(exc),
        )
        return None

    live_size = Decimal("0")
    if isinstance(positions, list):
        for position in positions:
            if not isinstance(position, dict):
                continue
            live_token_id = str(
                position.get("asset") or position.get("tokenID") or position.get("token_id") or ""
            ).strip()
            if live_token_id != token_id:
                continue
            try:
                live_size = Decimal(str(position.get("size") or "0"))
            except Exception:
                live_size = Decimal("0")
            break
    return live_size


async def _sync_sell_position_from_live_holdings(
    context: TradingContext,
    *,
    leader_id: str,
    condition_id: str,
    token_id: str,
) -> Decimal | None:
    live_size = await _fetch_live_position_size(context, token_id=token_id)
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
            leader_id=leader_id,
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


@dataclass
class TradePipelineResult:
    """Describe one leader-signal pipeline outcome."""

    trade_key: str
    status: str
    reason: str | None
    order_id: str | None


@dataclass(frozen=True)
class _BuyRiskCap:
    allow: bool
    reason: str
    policy_name: str
    cap_notional_usdc: Decimal


_POSITION_PCT_CAP_BALANCE_KEY = "_max_position_pct_balance_snapshot_usdc"
_POSITION_PCT_CAP_BALANCE_AT_KEY = "_max_position_pct_balance_snapshot_at"
_POSITION_PCT_CAP_BALANCE_WALLET_KEY = "_max_position_pct_balance_snapshot_wallet"
_POSITION_PCT_CAP_REFRESH_SECONDS = 24 * 60 * 60


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _first_text(raw: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_trade_outcome(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "outcome",
        "outcomeName",
        "outcome_name",
        "tokenOutcome",
        "token_outcome",
        "selection",
        "selectionName",
        "choice",
    )


def _extract_trade_submarket(raw: dict[str, Any]) -> str:
    return _first_text(
        raw,
        "submarket",
        "subMarket",
        "sub_market",
        "marketSubtype",
        "market_subtype",
        "marketType",
        "market_type",
        "lineType",
        "line_type",
        "betType",
        "bet_type",
        "subMarketName",
        "groupItemTitle",
        "groupTitle",
    )


async def _resolve_leader_tier(context: TradingContext, leader_id: str) -> str:
    """Look up the follow tier for a leader.

    P0-2: fail-closed. An absent repo, a lookup error, or an unknown/missing
    tier resolves to ``watchlist`` (the non-live, no-submission tier), never
    ``live_follow``. A DB glitch or a misconfiguration must not auto-promote a
    leader to live copy trading. (The global LIVE_TRADING master switch already
    forces paper unless explicitly enabled; this is defense-in-depth within
    live mode.)
    """
    if not hasattr(context, "leaders_repo") or context.leaders_repo is None:
        return "watchlist"
    try:
        rec = await context.leaders_repo.get_tier(leader_id)
        tier = rec.follow_tier if rec and rec.follow_tier else None
    except Exception:
        return "watchlist"
    if not tier or tier not in ("watchlist", "paper_follow", "live_follow"):
        return "watchlist"
    return tier


def build_trade_key(user_id: str, leader_id: str, trade: dict[str, Any]) -> str:
    """Delegate canonical economic identity construction."""

    return _COPY_TRADE_IDENTITY.build(user_id, leader_id, trade)


def all_tx_hashes_for_trade(trade: dict[str, Any]) -> list[str]:
    """Return every tx hash associated with a trade, deduped, order-preserved.

    Phase 2.10 (H6) — aggregated signals carry constituent hashes in
    ``_txHashes`` / ``txHashes``. Dedup must consider ALL of them, not just
    the primary ``transactionHash``, so a second detection channel (e.g.
    on-chain WS) cannot re-trigger an already-processed aggregated signal.
    The primary hash is first; constituents follow in their list order.
    """
    primary = (
        trade.get("transactionHash")
        or trade.get("txHash")
        or trade.get("transaction_hash")
        or trade.get("tx_hash")
        or ""
    )
    seen: set[str] = set()
    out: list[str] = []
    if primary:
        out.append(str(primary))
        seen.add(str(primary))
    constituents = trade.get("_txHashes") or trade.get("txHashes") or []
    if isinstance(constituents, (list, tuple)):
        for txh in constituents:
            if not txh:
                continue
            h = str(txh)
            if h not in seen:
                out.append(h)
                seen.add(h)
    return out


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def seed_position_pct_cap_snapshot(
    context: TradingContext,
    balance_usdc: Any,
    *,
    timestamp: float | None = None,
) -> None:
    """Seed the MAX_POSITION_PCT cap basis from a wallet balance snapshot.

    The position percentage cap is intentionally based on a coarse wallet
    snapshot: startup balance, then a refresh at most once per 24 hours. Other
    balance policies may still use live free cash.
    """
    balance = max(Decimal("0"), _to_decimal(balance_usdc))
    context.extra[_POSITION_PCT_CAP_BALANCE_KEY] = balance
    context.extra[_POSITION_PCT_CAP_BALANCE_AT_KEY] = (
        float(timestamp) if timestamp is not None else time.time()
    )
    context.extra[_POSITION_PCT_CAP_BALANCE_WALLET_KEY] = _funder_wallet_address(context)


def _position_pct_cap_snapshot_is_fresh(
    context: TradingContext,
    *,
    now: float | None = None,
) -> bool:
    wallet = _funder_wallet_address(context)
    cached_wallet = str(context.extra.get(_POSITION_PCT_CAP_BALANCE_WALLET_KEY) or "").strip()
    try:
        cached_at_raw = context.extra.get(_POSITION_PCT_CAP_BALANCE_AT_KEY)
        if cached_at_raw is None:
            return False
        cached_at = float(cached_at_raw)
    except (TypeError, ValueError):
        return False
    cached_balance = _to_decimal(
        context.extra.get(_POSITION_PCT_CAP_BALANCE_KEY),
        default="-1",
    )
    wallet_matches = not cached_wallet or cached_wallet.lower() == wallet.lower()
    age_seconds = max(0.0, (time.time() if now is None else now) - cached_at)
    return (
        cached_balance >= 0 and wallet_matches and age_seconds < _POSITION_PCT_CAP_REFRESH_SECONDS
    )


def _seed_position_pct_cap_snapshot_if_stale(
    context: TradingContext,
    balance_usdc: Any,
) -> None:
    if _position_pct_cap_snapshot_is_fresh(context):
        return
    seed_position_pct_cap_snapshot(context, balance_usdc)


def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _max_non_terminal_failure_attempts(context: TradingContext) -> int:
    raw = _trade_config(context).execution.non_terminal_failure_max_attempts
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 3
    return max(1, value)


_RETRYABLE_FAILED_STATUS_PREFIX = "retryable_failed_"
_RETRYABLE_FAILED_STATUS = "retryable_failed"
_LEGACY_RETRY_PREFIX = "retry_pending_"


def _retry_attempt_from_status(status: str | None, retry_attempt: int | None = None) -> int:
    """Return the retry attempt number.

    Prefers the explicit ``retry_attempt`` column value when available.
    Falls back to parsing legacy status strings (``retryable_failed_<n>``
    or ``retry_pending_<n>``) for rows written before the column existed.
    """
    if retry_attempt is not None and retry_attempt > 0:
        return retry_attempt
    text = str(status or "").strip().lower()
    for prefix in (_RETRYABLE_FAILED_STATUS_PREFIX, _LEGACY_RETRY_PREFIX):
        if text.startswith(prefix):
            suffix = text.replace(prefix, "", 1)
            try:
                return int(suffix)
            except (TypeError, ValueError):
                return 0
    return 0


def _account_cap_policy_enabled(context: TradingContext) -> bool:
    return bool(_trade_config(context).risk.enable_balance_tiered_policy)


def _funder_wallet_address(context: TradingContext) -> str:
    config = getattr(context, "config", None)
    configured = str(getattr(getattr(config, "wallet", None), "funder_address", "") or "").strip()
    return configured or str(getattr(context, "wallet_address", "") or "").strip()


def _trade_config(context: TradingContext) -> AppConfig:
    """Return the AppConfig for this context.

    ``TracingContext.config`` is typed ``AppConfig | None`` and may be unset
    by integration tests or callers that don't run the full ``load_config()``
    startup path. Callers that *require* a real config (live tier gating)
    must inject one — the function raises when it is absent.
    """
    config = context.config
    if config is None:
        raise RuntimeError("TradingContext.config is required for live tier gating")
    return config


def _position_notional_usdc(position: Any) -> Decimal:
    size_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    avg_entry_price = _to_decimal(getattr(position, "avg_entry_price", 0))
    notional = size_tokens * avg_entry_price
    return notional if notional > 0 else Decimal("0")


def _extract_leader_sell_fraction(raw_trade: dict[str, Any]) -> Decimal | None:
    raw_fraction = raw_trade.get("_leader_sell_fraction")
    if raw_fraction is None:
        raw_fraction = raw_trade.get("leaderSellFraction")
    if raw_fraction is None:
        return None
    try:
        fraction = Decimal(str(raw_fraction))
    except Exception:
        return None
    if fraction <= 0:
        return Decimal("0")
    if fraction > 1:
        return Decimal("1")
    return fraction


def _derive_sell_tokens(
    signal: TradeSignal,
    position: Any,
    raw_trade: dict[str, Any],
    *,
    missing_policy: str = "skip",
) -> Decimal | None:
    """Derive the copied sell size from the leader's sell fraction.

    Preferred rule: sell the same fraction of *our* held shares as the leader
    sold from *their* position using the trade monitor's
    ``_leader_sell_fraction`` annotation.

    When the annotation is missing (legacy feed, or the leader positions
    fetch failed — see ``_leader_sell_fraction_unavailable``), the configured
    ``execution.sell_fraction_missing_policy`` decides:
    ``skip`` returns ``None`` (fail closed; caller must treat it as a
    retryable skip), ``full_close`` preserves the legacy behaviour of
    dumping the whole held position.
    """
    leader_fraction = _extract_leader_sell_fraction(raw_trade)
    if leader_fraction is None:
        if missing_policy != "full_close":
            logger.warning(
                "sell_fraction_missing_skipped | cond=%s leader=%s unavailable=%s "
                "— skipping copy sell (fail closed)",
                signal.condition_id[:12],
                raw_trade.get("_leader_id", "")[:12],
                bool(raw_trade.get("_leader_sell_fraction_unavailable")),
                extra={
                    "type": "sell_fraction_missing_skipped",
                    "condition_id": signal.condition_id,
                },
            )
            return None
        logger.info(
            "sell_fraction_missing | cond=%s leader=%s — defaulting to full close",
            signal.condition_id[:12],
            raw_trade.get("_leader_id", "")[:12],
            extra={
                "type": "sell_fraction_missing",
                "condition_id": signal.condition_id,
            },
        )
        leader_fraction = Decimal("1")
    held_tokens = _to_decimal(getattr(position, "size_tokens", 0))
    derived = held_tokens * leader_fraction
    return derived if derived > 0 else Decimal("0")


def _resolve_leader_sell_tokens(
    *,
    requested_sell_tokens: Decimal,
    max_sell_tokens: Decimal,
    market_min_size: float,
    sub_min_sell_policy: str = "round_up",
) -> Decimal:
    """Resolve leader-follow SELL size without decision-gate scoring.

    Leader SELLs are authoritative exits: copy the same fraction of our held
    position, capped by what we can actually sell. ``sub_min_sell_policy``
    controls what happens when that fraction is below the venue minimum:

    - ``round_up`` (legacy): sell the minimum when possible; otherwise sell
      the largest remaining sellable amount. A leader's small nibble becomes
      a near-full exit on small positions.
    - ``skip`` (fail-closed, Phase 2.8): drop the copy sell (return 0) rather
      than overshoot the leader's intent. The signal is retried on the next
      poll when the leader's remaining fraction crosses the minimum.

    Risk-management exits may still use their own logic, but leader SELLs
    should not be blocked by copy-decision scoring.
    """
    max_sell_tokens = max(Decimal("0"), max_sell_tokens)
    target = min(max(Decimal("0"), requested_sell_tokens), max_sell_tokens)
    if target <= 0:
        return Decimal("0")

    min_tokens = _to_decimal(market_min_size)
    if min_tokens > 0 and target < min_tokens:
        if str(sub_min_sell_policy).strip().lower() == "skip":
            # Fail-closed: do not overshoot a sub-minimum proportional sell.
            return Decimal("0")
        return min(max_sell_tokens, min_tokens)
    return target


class _BalanceSnapshot:
    """Cached balance + exposure data shared across cap resolvers."""

    __slots__ = (
        "free_cash_usdc",
        "equity_usdc",
        "total_exposure_usdc",
        "market_exposure_usdc",
        "leader_exposure_usdc",
        "open_positions_count",
    )

    def __init__(
        self,
        free_cash_usdc: Decimal,
        total_exposure_usdc: Decimal,
        market_exposure_usdc: Decimal,
        leader_exposure_usdc: Decimal,
        open_positions_count: int,
    ) -> None:
        self.free_cash_usdc = free_cash_usdc
        self.total_exposure_usdc = total_exposure_usdc
        self.market_exposure_usdc = market_exposure_usdc
        self.leader_exposure_usdc = leader_exposure_usdc
        self.equity_usdc = free_cash_usdc + total_exposure_usdc
        self.open_positions_count = open_positions_count


async def _fetch_balance_snapshot(
    context: TradingContext,
    condition_id: str,
    leader_id: str,
) -> _BalanceSnapshot | None:
    """Single balance + positions fetch shared by both cap resolvers."""
    balance_usdc = await get_my_balance(_funder_wallet_address(context))
    if balance_usdc is None:
        return None

    free_cash_usdc = Decimal(str(balance_usdc))
    _seed_position_pct_cap_snapshot_if_stale(context, free_cash_usdc)
    open_positions = await context.positions_repo.list_open_positions(context.user_id)
    total_exposure_usdc = Decimal("0")
    market_exposure_usdc = Decimal("0")
    leader_exposure_usdc = Decimal("0")
    for position in open_positions:
        position_notional = _position_notional_usdc(position)
        total_exposure_usdc += position_notional
        if str(getattr(position, "condition_id", "") or "") == condition_id:
            market_exposure_usdc += position_notional
        if str(getattr(position, "leader_id", "") or "") == leader_id:
            leader_exposure_usdc += position_notional

    return _BalanceSnapshot(
        free_cash_usdc=free_cash_usdc,
        total_exposure_usdc=total_exposure_usdc,
        market_exposure_usdc=market_exposure_usdc,
        leader_exposure_usdc=leader_exposure_usdc,
        open_positions_count=len(open_positions),
    )


async def _position_pct_cap_balance_usdc(context: TradingContext) -> Decimal | None:
    now = time.time()
    cached_balance = _to_decimal(
        context.extra.get(_POSITION_PCT_CAP_BALANCE_KEY),
        default="-1",
    )
    if _position_pct_cap_snapshot_is_fresh(context, now=now):
        return cached_balance

    wallet = _funder_wallet_address(context)
    refreshed = await get_my_balance(wallet)
    if refreshed is None:
        return None
    seed_position_pct_cap_snapshot(context, refreshed, timestamp=now)
    return _to_decimal(refreshed)


async def _resolve_position_pct_cap(
    context: TradingContext,
    signal: TradeSignal,
    snapshot: _BalanceSnapshot,
) -> _BuyRiskCap:
    max_position_pct = Decimal(str(_trade_config(context).sizing.max_position_pct or 0))
    if max_position_pct <= 0:
        # MAX_POSITION_PCT <= 0 means the cap is intentionally disabled by the
        # operator. Returning ``allow=False`` here would silently block every
        # BUY signal — that was a regression introduced when the legacy
        # ``_BuyRiskCap(True, "disabled", ...)`` branch was rewritten. Restore
        # the disabled-cap semantics so BUYs continue to flow uncapped.
        return _BuyRiskCap(
            allow=True,
            reason="disabled",
            policy_name="max_position_pct",
            cap_notional_usdc=snapshot.free_cash_usdc,
        )

    cap_balance_usdc = await _position_pct_cap_balance_usdc(context)
    if cap_balance_usdc is None:
        return _BuyRiskCap(
            allow=False,
            reason="position_pct_cap_balance_unavailable",
            policy_name="max_position_pct",
            cap_notional_usdc=Decimal("0"),
        )

    # Coarse wallet-snapshot per-BUY cap. Existing position exposure is
    # enforced by the market/account exposure policies; MAX_POSITION_PCT only
    # limits how much of the startup/24h wallet snapshot this new BUY may
    # consume.
    cap_notional_usdc = max(Decimal("0"), cap_balance_usdc * max_position_pct)
    if cap_notional_usdc <= 0:
        return _BuyRiskCap(
            allow=False,
            reason="max_position_pct_reached",
            policy_name="max_position_pct",
            cap_notional_usdc=Decimal("0"),
        )

    return _BuyRiskCap(
        allow=True,
        reason="ok",
        policy_name="max_position_pct",
        cap_notional_usdc=cap_notional_usdc,
    )


async def _resolve_buy_risk_cap(
    context: TradingContext,
    leader_id: str,
    signal: TradeSignal,
    snapshot: _BalanceSnapshot,
) -> _BuyRiskCap:
    equity_usdc = snapshot.equity_usdc
    free_cash_usdc = snapshot.free_cash_usdc

    day_start_equity = Decimal(str(_trade_config(context).risk.day_start_equity_usdc or 0))
    if day_start_equity <= 0:
        day_start_equity = get_day_start_equity(equity_usdc)
    daily_loss_usdc = max(Decimal("0"), day_start_equity - equity_usdc)

    # The active pipeline does not yet persist per-position categories, so
    # category headroom cannot be reconstructed reliably here.
    headroom = headroom_for_buy(
        equity_usdc=equity_usdc,
        free_cash_usdc=free_cash_usdc,
        open_positions_count=snapshot.open_positions_count,
        market_exposure_usdc=snapshot.market_exposure_usdc,
        category_exposure_usdc=Decimal("0"),
        leader_exposure_usdc=snapshot.leader_exposure_usdc,
        daily_loss_usdc=daily_loss_usdc,
        day_start_equity_usdc=day_start_equity,
        config=_trade_config(context),
    )
    return _BuyRiskCap(
        allow=headroom.allow,
        reason=headroom.reason,
        policy_name=headroom.policy_name,
        cap_notional_usdc=headroom.cap_notional_usdc,
    )


def _buy_required_cash_with_fees(
    notional_usdc: Decimal,
    fee_bps: int,
) -> Decimal:
    """Cash a BUY actually consumes once venue fees are added (P2-e).

    A BUY that is affordable on notional alone can still be unaffordable once
    fees are charged on top. ``required_cash = notional * (1 + fee_bps/10_000)``
    in Decimal so the headroom/affordability comparison never silently lets a
    BUY through that the wallet cannot cover net-of-fees. Fees affect ONLY this
    affordability gate — never order sizing or the effective-idempotency key.
    """
    if notional_usdc <= 0:
        return Decimal("0")
    fee_multiplier = Decimal("1") + (Decimal(int(fee_bps)) / BASIS_POINTS_PER_UNIT)
    return notional_usdc * fee_multiplier


async def _buy_fee_bps_or_none(context: TradingContext, token_id: str) -> int | None:
    """Best-effort venue fee in bps for the BUY affordability gate.

    Returns ``None`` when no fee source is wired (configless/legacy/test paths)
    so the caller can keep the pre-existing no-fee cash gates authoritative
    instead of regressing previously-allowed trades. ``FeeService.get_fee_bps``
    is itself cached and never raises (it falls back to its default bps), so a
    non-None result is always a usable integer.
    """
    exec_service = getattr(context, "execution_service", None)
    fee_service = getattr(exec_service, "fee_service", None)
    getter = getattr(fee_service, "get_fee_bps", None)
    if getter is None:
        return None
    try:
        fee_bps = await getter(token_id)
    except Exception as exc:  # defensive — get_fee_bps should not raise
        logger.warning(
            "buy_fee_lookup_failed | token=%s error=%s",
            shorten_addr(token_id, 8, 6),
            repr(exc),
        )
        return None
    try:
        return int(fee_bps)
    except (TypeError, ValueError):
        return None


def normalize_signals(
    raw_signals: list[dict[str, Any]], leader_id: str
) -> list[tuple[TradeSignal, dict[str, Any]]]:
    """Normalize raw leader rows into immutable execution signals."""
    out: list[tuple[TradeSignal, dict[str, Any]]] = []
    for raw in raw_signals:
        side = str(raw.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            continue

        condition_id = str(raw.get("conditionId") or raw.get("condition_id") or "")
        token_id = str(raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or "")
        tx_hash = str(
            raw.get("transactionHash")
            or raw.get("txHash")
            or raw.get("transaction_hash")
            or raw.get("tx_hash")
            or ""
        )
        if not condition_id or not token_id or not tx_hash:
            # A detected TRADE that is missing core market identifiers cannot be
            # copied. Surface a concise line so the operator is not left with a
            # silent "NEW TRADE DETECTED" followed by nothing.
            print_warning(
                f"Ignored — missing market identifiers "
                f"({side} {raw.get('title') or raw.get('slug') or 'unknown market'})"
            )
            continue
        if not is_valid_clob_token_id(token_id):
            logger.warning("normalize_signals: skipping signal with invalid token_id=%r", token_id)
            continue

        size = _to_decimal(raw.get("size") or 0)
        price = _to_decimal(raw.get("price") or 0)
        usdc_size = _to_decimal(raw.get("usdcSize") or 0)
        if usdc_size <= 0 and size > 0 and price > 0:
            usdc_size = size * price

        ts_raw = raw.get("timestamp") or int(time.time())
        ts = (
            int(float(ts_raw) / 1000)
            if float(ts_raw) > UNIX_MILLISECONDS_THRESHOLD
            else int(float(ts_raw))
        )

        signal = TradeSignal(
            leader_id=leader_id,
            condition_id=condition_id,
            token_id=token_id,
            side=side,
            size=size,
            price=price,
            usdc_size=usdc_size,
            tx_hash=tx_hash,
            event_ts=ts,
            title=str(raw.get("title") or ""),
            event_slug=str(raw.get("eventSlug") or raw.get("slug") or ""),
            outcome=_extract_trade_outcome(raw),
            submarket=_extract_trade_submarket(raw),
        )
        out.append((signal, raw))
    return out


async def _reconcile_one(
    context: TradingContext,
    record: dict,
    side: str,
) -> bool:
    """Reconcile a single unconfirmed BUY/SELL via the canonical reconciler.

    Issue #71 — terminal settlement is owned by ``ReconciliationService``;
    this wrapper supplies the per-context dependencies (fill confirmation,
    position lookup, PnL tracker) and preserves caller-facing logging tags.
    """
    if context.execution_service is None:
        return False

    reconciler = get_reconciliation_service(context)
    return await reconciler.reconcile_trade_record(
        record,
        side,
        confirm_fill=reconciler.confirm_fill,
        pnl_tracker=context.pnl_tracker,
        get_position=context.positions_repo.get_position,
        logger=logger,
        # Phase 4 — let the reconciler release the daily-cap reservation
        # for BUYs that terminally cancelled without filling.
        balance_regime=getattr(context, "balance_regime", None),
    )


async def reconcile_submitted_unconfirmed(
    context: TradingContext,
    *,
    max_age_seconds: float = 300.0,
    skip_locked_scopes: bool = False,
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
    extra = getattr(context, "extra", {})
    pending_query = extra.get("_pending_order_query_service")
    if pending_query is None and hasattr(
        type(context.orders_repo),
        "get_unconfirmed_order_submissions",
    ):
        pending_query = PendingOrderReconciliationQueryService(
            context.orders_repo,
            max_batch_size=200,
        )
        extra["_pending_order_query_service"] = pending_query
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
                async with context.position_coordinator.acquire(_scope):
                    return await _reconcile_one(context, record, side)

        if not records:
            return 0
        results = await asyncio.gather(
            *(_run(record) for record in records), return_exceptions=True
        )
        batch_reconciled = 0
        for result in results:
            if isinstance(result, Exception):
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

    return reconciled


def _decision_live_price(signal: Any, bundle: Any) -> float:
    """Price used to score the copy decision (drift/edge gates + BUY sizing).

    P1-c: this must be the FRESH executable quote — ``best_ask`` for a BUY,
    ``best_bid`` for a SELL — not the stale leader signal price. Scoring against
    ``signal.price`` made ``price_drift_pct`` / ``edge_decay_pct`` identically
    zero (the signal compared against itself) and sized BUYs off the old price,
    leaving the protective drift gate inert on every copy. Falls back to the
    signal price when no fresh quote is available; the degraded/REST path is
    gated separately upstream.
    """
    fallback = float(getattr(signal, "price", 0) or 0)
    quote = getattr(bundle, "quote", None) if bundle is not None else None
    if quote is None:
        return fallback
    side = str(getattr(signal, "side", "") or "").upper()
    fresh = getattr(quote, "best_ask", None) if side == "BUY" else getattr(quote, "best_bid", None)
    try:
        fresh_f = float(fresh) if fresh is not None else 0.0
    except (TypeError, ValueError):
        return fallback
    return fresh_f if fresh_f > 0 else fallback


async def execute_trades(
    context: TradingContext,
    leader_id: str,
    signals: list[dict[str, Any]],
    now_ts: float,
) -> list[TradePipelineResult]:
    """Execute normalized signals through admission, risk, and reservation gates."""
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

    pnl_metrics = (
        context.pnl_tracker.metrics()
        if context.pnl_tracker is not None and hasattr(context.pnl_tracker, "metrics")
        else {}
    )
    leader_score = leader_scorer.score_leader(
        leader_id,
        analytics={
            "net_pnl": float(pnl_metrics.get("net_pnl", 0.0) or 0.0),
            "win_rate": float(pnl_metrics.get("win_rate", 50.0) or 50.0),
        },
        recent_trade_count=len(signals),
    )

    market_min_size_by_token: dict[str, float] = {}
    tick_size_by_token: dict[str, float] = {}
    market_data_stale_tokens: set[str] = set()
    market_data_stale_codes: dict[tuple[str, str], str] = {}
    # ``_prefetch_market_data_for_signals`` dispatches on the primary source
    # internally (rest_snapshot -> REST /book prefetch; otherwise the WSS
    # subscribe/bootstrap/require_fresh_data path), so this gate must run for
    # BOTH modes — restricting it to one mode disables the other's pre-loop
    # bundling and its per-signal freshness gate. (2dea338 inverted this gate
    # to ``== rest_snapshot``, which silently dropped WSS-mode stale gating.)
    if normalized:
        (
            bundles_by_token,
            market_data_stale_tokens,
            market_data_stale_codes,
        ) = await _prefetch_market_data_for_signals(context, normalized)
        for token_id, bundle in bundles_by_token.items():
            try:
                market_min_size_by_token[token_id] = float(bundle.tick.min_order_size)
                tick_size_by_token[token_id] = float(bundle.tick.tick_size)
            except (TypeError, ValueError, AttributeError):
                # Defensive: a malformed bundle should not crash the loop.
                market_data_stale_tokens.add(token_id)
                market_min_size_by_token.pop(token_id, None)
                tick_size_by_token.pop(token_id, None)
                for sig, _raw in normalized:
                    if sig.token_id == token_id:
                        side = str(getattr(sig, "side", "") or "").upper()
                        market_data_stale_codes.setdefault((token_id, side), "market_data_stale")
    else:
        context.extra["_md_bundles_by_pair"] = {}

    for signal, raw in normalized:
        labels = {"user_id": context.user_id, "leader_id": leader_id}
        sell_entry_price: float | None = None
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

        # ------------------------------------------------------------------
        # Market-data freshness gate — fail closed when WSS is not ready
        # ------------------------------------------------------------------
        # MarketDataService is the sole authority for executable market data.
        # If subscription + WSS handshake did not converge inside the
        # configured window, skip the signal rather than trade on stale or
        # REST-only prices/books.
        _signal_side_norm = str(getattr(signal, "side", "") or "").upper()
        _stale_pair_key = (signal.token_id, _signal_side_norm)
        _bundle_for_pair = None
        if normalized:
            # SHP-09: read from the canonical ``context.extra`` extension
            # point — matches the write at the top of this module.
            _bundle_for_pair = context.extra.get("_md_bundles_by_pair", {}).get(_stale_pair_key)
        if _stale_pair_key in market_data_stale_codes or (
            _bundle_for_pair is None and signal.token_id in market_data_stale_tokens
        ):
            skip_code = market_data_stale_codes.get(_stale_pair_key, "market_data_stale")
            print_skip_reason(
                "Market data stale",
                f"{signal.side} {signal.title or shorten_addr(signal.token_id, 8, 6)} "
                f"— {skip_code} (fail-closed: no fresh quote/book)",
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
                reason="REST snapshot not executable: " + skip_code,
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
            # Fetch live price for decision scoring from the FRESH quote
            # (best_ask for BUY / best_bid for SELL), not the stale leader
            # signal price — see _decision_live_price (P1-c).
            live_price = _decision_live_price(signal, _bundle_for_pair)
            market_min_size = market_min_size_by_token.get(signal.token_id, 0.0)
            market_tick_size = tick_size_by_token.get(signal.token_id, 0.0)

            if signal.side == "BUY":
                sizing_result = context.copy_notional(signal.usdc_size)
                target_notional = sizing_result.final_notional_usdc
                configured_buy_max = Decimal(
                    str(getattr(context, "buy_max_amount", target_notional) or target_notional)
                )
                # BUY_MAX_AMOUNT is the hard ceiling for decision-layer upsizing.
                # Keep base_notional_usdc as the strategy target below, but let
                # CopyDecisionService apply trust sizing up to this ceiling before
                # we further clamp by always-on cash/position caps.
                max_notional_for_decision = configured_buy_max

                snapshot = await _fetch_balance_snapshot(
                    context,
                    signal.condition_id,
                    leader_id,
                )
                if snapshot is None:
                    reason = "balance_unavailable"
                    print_skip_reason("Balance unavailable", reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=reason,
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": reason},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{reason}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", reason, None))
                    continue

                position_pct_cap = await _resolve_position_pct_cap(context, signal, snapshot)
                if not position_pct_cap.allow:
                    reason = (
                        f"position cap [{position_pct_cap.policy_name}] blocked BUY: "
                        f"{position_pct_cap.reason}"
                    )
                    print_skip_reason(
                        f"Position cap [{position_pct_cap.policy_name}] blocked BUY",
                        position_pct_cap.reason,
                    )
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=position_pct_cap.reason,
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": position_pct_cap.reason},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{position_pct_cap.reason}",
                    )
                    results.append(
                        TradePipelineResult(
                            trade_key,
                            "skipped",
                            position_pct_cap.reason,
                            None,
                        )
                    )
                    continue

                max_notional_for_decision = min(
                    max_notional_for_decision,
                    position_pct_cap.cap_notional_usdc,
                )

                buy_risk_cap = None
                if _account_cap_policy_enabled(context):
                    buy_risk_cap = await _resolve_buy_risk_cap(context, leader_id, signal, snapshot)
                    if not buy_risk_cap.allow:
                        reason = (
                            f"account-cap policy [{buy_risk_cap.policy_name}] headroom: "
                            f"{buy_risk_cap.reason}"
                        )
                        print_skip_reason(
                            f"Account-cap policy [{buy_risk_cap.policy_name}] headroom blocked BUY",
                            buy_risk_cap.reason,
                        )
                        log_trade_skip(
                            logger,
                            side=signal.side,
                            condition_id=signal.condition_id,
                            asset=signal.token_id,
                            size=float(signal.size) if signal.size else None,
                            price=live_price,
                            usdc_value=float(signal.usdc_size),
                            skip_code=buy_risk_cap.reason,
                            reason=reason,
                            event_slug=signal.event_slug,
                            meta={"user_id": context.user_id, "leader_id": leader_id},
                        )
                        metrics.inc(
                            "trades_skipped_total",
                            labels={**labels, "reason": buy_risk_cap.reason},
                        )
                        await context.orders_repo.mark_processed_trade(
                            trade_key=trade_key,
                            user_id=context.user_id,
                            leader_id=leader_id,
                            tx_hash=signal.tx_hash,
                            side=signal.side,
                            asset=signal.token_id,
                            condition_id=signal.condition_id,
                            status=f"skipped_{buy_risk_cap.reason}",
                        )
                        results.append(
                            TradePipelineResult(
                                trade_key,
                                "skipped",
                                buy_risk_cap.reason,
                                None,
                            )
                        )
                        continue

                    max_notional_for_decision = min(
                        max_notional_for_decision,
                        buy_risk_cap.cap_notional_usdc,
                    )

                policy_enabled = _account_cap_policy_enabled(context)

                # --- Sizing audit: unified log showing both sizing sources ---
                policy_binding = (
                    buy_risk_cap is not None and buy_risk_cap.cap_notional_usdc < target_notional
                )
                logger.info(
                    "sizing_audit | cond=%s leader=%s "
                    "leader_notional=%.2f strategy=%s sizing_target=%.4f "
                    "policy_enabled=%s policy_cap=%s policy_binding=%s "
                    "max_for_decision=%.4f | %s",
                    signal.condition_id[:12],
                    leader_id[:12],
                    float(signal.usdc_size),
                    sizing_result.strategy,
                    float(target_notional),
                    policy_enabled,
                    f"{float(buy_risk_cap.cap_notional_usdc):.4f}[{buy_risk_cap.policy_name}]"
                    if buy_risk_cap
                    else "n/a",
                    policy_binding,
                    float(max_notional_for_decision),
                    sizing_result.reasoning,
                    extra={
                        "type": "sizing_audit",
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                        "leader_notional": float(signal.usdc_size),
                        "strategy": sizing_result.strategy,
                        "sizing_target": float(target_notional),
                        "policy_enabled": policy_enabled,
                        "policy_cap": float(buy_risk_cap.cap_notional_usdc)
                        if buy_risk_cap
                        else None,
                        "policy_binding": policy_binding,
                        "max_for_decision": float(max_notional_for_decision),
                    },
                )

                # Run the copy decision gate
                decision = copy_decision_svc.decide_buy(
                    trade=raw,
                    leader_score=leader_score,
                    live_price=live_price,
                    base_notional_usdc=float(target_notional),
                    max_notional_usdc=float(max_notional_for_decision),
                    market_min_size=market_min_size,
                    tick_size=market_tick_size,
                )

                _decision_meta: dict[str, Any] = {
                    "user_id": context.user_id,
                    "leader_id": leader_id,
                    "leader_score": leader_score.score,
                    "trade_score": decision.trade_score,
                    "price_drift_pct": decision.price_drift_pct,
                    "edge_decay_pct": decision.edge_decay_pct,
                    "leader_notional": float(signal.usdc_size),
                    "tx_hash": signal.tx_hash,
                }
                if _bundle_for_pair is not None:
                    _bsrc = getattr(_bundle_for_pair, "source", None)
                    _decision_meta["market_data_source"] = getattr(_bsrc, "value", None) or str(
                        _bsrc or ""
                    )
                    _q = getattr(_bundle_for_pair, "quote", None)
                    _b = getattr(_bundle_for_pair, "book", None)
                    if _q is not None:
                        _decision_meta["quote_source"] = getattr(_q, "source", None)
                    if _b is not None:
                        _decision_meta["book_source"] = getattr(_b, "source", None)
                log_trade_decision(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    decision_score=decision.decision_score,
                    allow=decision.allow,
                    reason=decision.reason,
                    event_slug=signal.event_slug,
                    meta=_decision_meta,
                )

                if not decision.allow:
                    if decision.skip_code == "signal_too_old":
                        skip_title = "Signal too old"
                    elif decision.skip_code in {
                        "risk_cap_too_low_for_min_order",
                        "buy_below_minimum_order_requirements",
                        "buy_cap_blocks_upsize_to_venue_min",
                    }:
                        skip_title = "BUY blocked by execution minimums/risk cap"
                    else:
                        skip_title = "Decision gate rejected BUY"
                    print_skip_reason(skip_title, decision.reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=decision.skip_code,
                        reason=decision.reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": decision.skip_code},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=f"skipped_{decision.skip_code}",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", decision.reason, None))
                    continue

                # Use decision-gated notional (may be adjusted by scoring)
                gated_notional = (
                    Decimal(str(decision.target_notional_usdc))
                    if decision.target_notional_usdc > 0
                    else target_notional
                )

                # --- P2-e: fee-aware affordability gate ---
                # The upstream cash gates (headroom_for_buy / balance regime)
                # compare against the BARE notional and ignore venue fees, so a
                # BUY affordable on notional alone but NOT once fees are added
                # could slip through. Compare required_cash = notional*(1+fee)
                # against live free cash and fail closed if it does not fit. This
                # runs BEFORE the regime reservation so a rejection never leaks a
                # held reservation, and it does NOT alter sizing — the order
                # amount stays exactly ``gated_notional``.
                buy_fee_bps = await _buy_fee_bps_or_none(context, signal.token_id)
                if buy_fee_bps is not None and buy_fee_bps > 0:
                    required_cash_with_fees = _buy_required_cash_with_fees(
                        gated_notional, buy_fee_bps
                    )
                    if required_cash_with_fees > snapshot.free_cash_usdc:
                        reason = (
                            f"insufficient cash for BUY net of fees: "
                            f"need {required_cash_with_fees} "
                            f"(notional {gated_notional} + {buy_fee_bps}bps), "
                            f"free {snapshot.free_cash_usdc}"
                        )
                        skip_code = "insufficient_cash_with_fees"
                        print_skip_reason("Insufficient cash (fees)", reason)
                        logger.warning(
                            "buy_insufficient_cash_with_fees | cond=%s leader=%s "
                            "notional=%s fee_bps=%d required=%s free_cash=%s",
                            signal.condition_id[:12],
                            leader_id[:12],
                            str(gated_notional),
                            buy_fee_bps,
                            str(required_cash_with_fees),
                            str(snapshot.free_cash_usdc),
                            extra={
                                "type": skip_code,
                                "user_id": context.user_id,
                                "condition_id": signal.condition_id,
                                "leader_id": leader_id,
                                "notional_usdc": str(gated_notional),
                                "fee_bps": buy_fee_bps,
                                "required_cash_usdc": str(required_cash_with_fees),
                                "free_cash_usdc": str(snapshot.free_cash_usdc),
                            },
                        )
                        log_trade_skip(
                            logger,
                            side=signal.side,
                            condition_id=signal.condition_id,
                            asset=signal.token_id,
                            size=float(signal.size) if signal.size else None,
                            price=live_price,
                            usdc_value=float(gated_notional),
                            skip_code=skip_code,
                            reason=reason,
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
                        results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                        continue

                # --- Phase 4 balance regime gate ---
                regime = getattr(context, "balance_regime", None)
                if regime is not None:
                    # Greptile P1 (PR #106 review): replace the non-atomic
                    # ``check_buy → submit → record_buy`` triple with the
                    # atomic ``await regime.reserve(...)`` so two concurrent
                    # BUYs cannot both pass the cap against the same
                    # ``daily_spent`` snapshot. ``reserve`` atomically checks
                    # the cap AND records the hold under ``order_id=trade_key``;
                    # the post-submit branches below either ``commit`` (on
                    # accepted submission) or ``release`` (on
                    # duplicate_ignored / not accepted / exception paths) the
                    # held reservation. The proven atomic semantics live in
                    # ``tests/test_balance_regime_atomic_reserve.py``.
                    regime_decision = await regime.reserve(
                        order_id=trade_key,
                        notional_usdc=gated_notional,
                        free_cash_usdc=snapshot.free_cash_usdc,
                    )
                    if not regime_decision.allow:
                        reason = (
                            f"balance regime [{regime_decision.policy_name}] blocked BUY: "
                            f"{regime_decision.reason}"
                        )
                        print_skip_reason(
                            f"Balance regime [{regime_decision.policy_name}] blocked BUY",
                            regime_decision.reason,
                        )
                        log_trade_skip(
                            logger,
                            side=signal.side,
                            condition_id=signal.condition_id,
                            asset=signal.token_id,
                            size=float(signal.size) if signal.size else None,
                            price=live_price,
                            usdc_value=float(gated_notional),
                            skip_code=regime_decision.reason,
                            reason=reason,
                            event_slug=signal.event_slug,
                            meta={
                                "user_id": context.user_id,
                                "leader_id": leader_id,
                                "requested_usdc": str(regime_decision.requested_usdc),
                                "daily_spent_usdc": str(regime_decision.daily_spent_usdc),
                                "daily_remaining_usdc": str(regime_decision.daily_remaining_usdc),
                                "headroom_usdc": str(regime_decision.headroom_usdc),
                            },
                        )
                        metrics.inc(
                            "trades_skipped_total",
                            labels={**labels, "reason": regime_decision.reason},
                        )
                        await context.orders_repo.mark_processed_trade(
                            trade_key=trade_key,
                            user_id=context.user_id,
                            leader_id=leader_id,
                            tx_hash=signal.tx_hash,
                            side=signal.side,
                            asset=signal.token_id,
                            condition_id=signal.condition_id,
                            status=f"skipped_{regime_decision.reason}",
                        )
                        results.append(
                            TradePipelineResult(trade_key, "skipped", regime_decision.reason, None)
                        )
                        continue

                # Log final execution sizing if decision gate changed the amount
                if float(gated_notional) != float(target_notional):
                    logger.info(
                        "sizing_final | cond=%s decision_gate adjusted %.4f → %.4f "
                        "(trust_mult=%.3f)",
                        signal.condition_id[:12],
                        float(target_notional),
                        float(gated_notional),
                        decision.details.get("trust_multiplier", 0),
                        extra={
                            "type": "sizing_final",
                            "condition_id": signal.condition_id,
                            "leader_id": leader_id,
                            "original_notional": float(target_notional),
                            "gated_notional": float(gated_notional),
                        },
                    )

                exec_req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    token_id=signal.token_id,
                    side="BUY",
                    amount=gated_notional,
                    max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                    reason="copy_trade",
                    signal_id=signal.tx_hash,
                    condition_id=signal.condition_id,
                    recovery_metadata={
                        "source": "copy_trade",
                        "order_type": "FAK",
                        "price_limit": str(signal.price),
                    },
                    order_type=ExecutionOrderType.FAK,
                    price_limit=Decimal(str(signal.price)),
                )
            else:
                pos = await context.positions_repo.get_position(
                    context.user_id, signal.condition_id, token_id=signal.token_id
                )
                if pos is None:
                    # No exact (condition_id, token_id) match — check if we hold the *other* outcome token.
                    candidates = await context.positions_repo.list_positions_for_condition(
                        context.user_id, signal.condition_id
                    )
                    wrong_token_held = any(p.token_id != signal.token_id for p in candidates)
                    if wrong_token_held:
                        skip_detail = (
                            f"wrong token held — leader sells "
                            f"{shorten_addr(signal.token_id, 8, 6)} but we hold a different outcome"
                        )
                        skip_code = "wrong_token_held"
                    else:
                        skip_detail = "never held — we never entered this market"
                        skip_code = "never_held"
                    print_skip_reason("SELL skipped", skip_detail)
                    logger.debug(
                        "sell_skip",
                        extra={
                            "reason": skip_code,
                            "condition_id": signal.condition_id,
                            "token_id": signal.token_id,
                            "leader_id": leader_id,
                        },
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price) if signal.price else None,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=skip_detail,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
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
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue

                if pos.size_tokens <= Decimal("0"):
                    skip_detail = "already closed — position size is zero"
                    skip_code = "already_closed"
                    print_skip_reason("SELL skipped", skip_detail)
                    logger.debug(
                        "sell_skip",
                        extra={
                            "reason": skip_code,
                            "condition_id": signal.condition_id,
                            "leader_id": leader_id,
                        },
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(signal.size) if signal.size else None,
                        price=float(signal.price) if signal.price else None,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=skip_detail,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
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
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue

                # Capture entry price before apply_fill mutates or closes/deletes the position.
                sell_entry_price = _to_optional_float(getattr(pos, "avg_entry_price", None))

                sell_missing_policy = (
                    context.config.execution.sell_fraction_missing_policy
                    if context.config
                    else "skip"
                )
                requested_sell_tokens = _derive_sell_tokens(
                    signal, pos, raw, missing_policy=sell_missing_policy
                )
                if requested_sell_tokens is None:
                    # Fraction unknown (positions API outage) and policy=skip.
                    # Retryable, not terminal: the next monitor cycle re-fetches
                    # and re-annotates the leader trade once the data API
                    # recovers, so the exit is copied late rather than never.
                    reason = "leader sell fraction unknown — cannot size copy sell"
                    skip_code = "sell_fraction_missing_skipped"
                    print_skip_reason("SELL skipped", reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=0.0,
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=reason,
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
                        status=_RETRYABLE_FAILED_STATUS,
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue
                max_sell_tokens = pos.size_tokens

                live_sellable_tokens = await _fetch_live_position_size(
                    context,
                    token_id=signal.token_id,
                )
                if live_sellable_tokens is not None:
                    venue_sell_cap = live_sellable_tokens.quantize(
                        Decimal("0.01"), rounding=ROUND_DOWN
                    )
                    if venue_sell_cap <= Decimal("0") and live_sellable_tokens > Decimal("0"):
                        venue_sell_cap = live_sellable_tokens
                    if venue_sell_cap < max_sell_tokens:
                        logger.info(
                            "sell_size_clamped_to_live_balance | cond=%s leader=%s "
                            "requested=%.4f live_balance=%.4f clamped=%.4f",
                            signal.condition_id[:12],
                            leader_id[:12],
                            float(requested_sell_tokens),
                            float(live_sellable_tokens),
                            float(venue_sell_cap),
                            extra={
                                "type": "sell_size_clamped_to_live_balance",
                                "condition_id": signal.condition_id,
                                "leader_id": leader_id,
                                "requested_sell_tokens": float(requested_sell_tokens),
                                "live_sellable_tokens": float(live_sellable_tokens),
                                "clamped_sell_tokens": float(venue_sell_cap),
                            },
                        )
                        if venue_sell_cap <= Decimal("0") and live_sellable_tokens <= Decimal("0"):
                            await context.positions_repo.delete_position(
                                context.user_id,
                                signal.condition_id,
                                token_id=signal.token_id,
                            )
                        max_sell_tokens = venue_sell_cap
                else:
                    # P2-m: live-holdings fetch returned None (transport error /
                    # unknown). We cannot determine the on-chain sellable size,
                    # so selling against the (possibly stale) DB position size is
                    # unsafe — it can oversell or sell shares we no longer hold.
                    # Fail closed: skip this SELL. ``None`` (unknown) is distinct
                    # from ``Decimal("0")`` (known-zero), which the clamp above
                    # already handles as a close/delete.
                    reason = "live holdings unavailable — cannot confirm sellable size"
                    skip_code = "sell_clamp_live_holdings_unavailable"
                    print_skip_reason("SELL skipped", reason)
                    logger.warning(
                        "sell_clamp_live_holdings_unavailable | cond=%s leader=%s token=%s db_size=%.4f",
                        signal.condition_id[:12],
                        leader_id[:12],
                        shorten_addr(signal.token_id, 8, 6),
                        float(max_sell_tokens),
                        extra={
                            "type": skip_code,
                            "user_id": context.user_id,
                            "condition_id": signal.condition_id,
                            "leader_id": leader_id,
                            "token_id": signal.token_id,
                            "db_position_size_tokens": float(max_sell_tokens),
                        },
                    )
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(requested_sell_tokens),
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code=skip_code,
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc("trades_skipped_total", labels={**labels, "reason": skip_code})
                    # Transient/unknown — persist a RETRYABLE status (not a
                    # terminal one). A missing live-holdings read is not a final
                    # decision; the atomic claim above already inserted an
                    # ``in_progress`` row, so we must downgrade it to
                    # ``retryable_failed`` to release the claim and let the next
                    # poll retry the protective SELL once the venue/data-API
                    # recovers (same retryable status the transient venue-failure
                    # path uses).
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status=_RETRYABLE_FAILED_STATUS,
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", skip_code, None))
                    continue

                leader_sell_fraction = _extract_leader_sell_fraction(raw) or Decimal("1")
                sell_tokens = _resolve_leader_sell_tokens(
                    requested_sell_tokens=requested_sell_tokens,
                    max_sell_tokens=max_sell_tokens,
                    market_min_size=market_min_size,
                    sub_min_sell_policy=str(
                        getattr(_trade_config(context).execution, "sub_min_sell_policy", "round_up")
                    ),
                )

                if sell_tokens <= Decimal("0"):
                    reason = "SELL size is zero"
                    print_skip_reason("SELL skipped", reason)
                    log_trade_skip(
                        logger,
                        side=signal.side,
                        condition_id=signal.condition_id,
                        asset=signal.token_id,
                        size=float(sell_tokens),
                        price=live_price,
                        usdc_value=float(signal.usdc_size),
                        skip_code="sell_size_zero",
                        reason=reason,
                        event_slug=signal.event_slug,
                        meta={"user_id": context.user_id, "leader_id": leader_id},
                    )
                    metrics.inc(
                        "trades_skipped_total",
                        labels={**labels, "reason": "sell_size_zero"},
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=trade_key,
                        user_id=context.user_id,
                        leader_id=leader_id,
                        tx_hash=signal.tx_hash,
                        side=signal.side,
                        asset=signal.token_id,
                        condition_id=signal.condition_id,
                        status="skipped_sell_size_zero",
                    )
                    results.append(TradePipelineResult(trade_key, "skipped", reason, None))
                    continue

                if requested_sell_tokens + Decimal("1e-12") < sell_tokens:
                    logger.info(
                        "sell_size_bumped_to_min | requested=%.4f bumped_to=%.4f held=%.4f",
                        float(requested_sell_tokens),
                        float(sell_tokens),
                        float(pos.size_tokens),
                    )

                logger.info(
                    "sell_ratio_applied | cond=%s leader=%s held=%.4f leader_fraction=%.6f sell_tokens=%.4f",
                    signal.condition_id[:12],
                    leader_id[:12],
                    float(pos.size_tokens),
                    float(leader_sell_fraction),
                    float(sell_tokens),
                    extra={
                        "type": "sell_ratio_applied",
                        "condition_id": signal.condition_id,
                        "leader_id": leader_id,
                        "position_size": float(pos.size_tokens),
                        "leader_sell_fraction": float(leader_sell_fraction),
                        "requested_sell_tokens": float(requested_sell_tokens),
                        "max_sell_tokens": float(max_sell_tokens),
                        "market_min_size": float(market_min_size),
                        "sell_tokens": float(sell_tokens),
                    },
                )

                exec_req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    token_id=signal.token_id,
                    side="SELL",
                    amount=sell_tokens,
                    max_slippage_bps=int(float(context.extra.get("slippage_tolerance_bps", 200))),
                    reason="leader_sell",
                    signal_id=signal.tx_hash,
                    condition_id=signal.condition_id,
                    recovery_metadata={
                        "source": "copy_trade",
                        "order_type": "FAK",
                        "price_limit": str(signal.price),
                    },
                    order_type=ExecutionOrderType.FAK,
                    price_limit=Decimal(str(signal.price)),
                )

            print_executing_strategy(signal.side)
            exec_result = await _submit_copy_through_coordinator(
                context, exec_req, signal, leader_id=leader_id
            )

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
                        if _filled_amt > 0:
                            _fill_px = Decimal(str(exec_result.avg_price or signal.price or 0))
                            await regime.commit(
                                order_id=trade_key,
                                actual_filled_usdc=_filled_amt * _fill_px,
                            )
                        else:
                            # Accepted but no immediate fill — keep the full
                            # reservation in place (commit with None preserves
                            # the held notional until a later reconciliation
                            # path resolves the order).
                            await regime.commit(order_id=trade_key)
                    except Exception:  # pragma: no cover — defensive
                        pass

            if exec_result.accepted and _decimal_or_zero(exec_result.filled_amount) <= 0:
                # Execution is settlement-deferred by design: a FAK that the
                # venue accepts returns filled_amount=0 / submitted_unconfirmed
                # and the reconciler owns final accounting. This is NOT a "price
                # moved past limit" kill (a genuine kill returns accepted=False);
                # the order may well have filled. Report it as pending, not dead.
                print_skip_reason(
                    "Order submitted — awaiting fill confirmation",
                    f"{signal.side} FAK accepted at limit {signal.price}; fill "
                    f"not yet confirmed by the venue — reconciliation will settle it",
                )
                # Copy-scoped display values. ``exec_req`` carries the COPY size,
                # not the leader's ``signal.size``: for a BUY the request amount
                # is USDC notional (tokens ≈ notional / limit price); for a SELL
                # it is shares directly.
                _copy_amount = _decimal_or_zero(getattr(exec_req, "amount", 0))
                _limit_px = _decimal_or_zero(
                    getattr(exec_req, "price_limit", None) or signal.price or 0
                )
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
                print_skip_reason(
                    f"Copy not accepted: {exec_result.status}",
                    exec_result.error or exec_result.status,
                )
                log_trade_skip(
                    logger,
                    side=signal.side,
                    condition_id=signal.condition_id,
                    asset=signal.token_id,
                    size=float(signal.size) if signal.size else None,
                    price=float(signal.price) if signal.price else None,
                    usdc_value=float(signal.usdc_size),
                    skip_code=exec_result.status,
                    reason=exec_result.error or exec_result.status,
                    status="failed",
                    event_slug=signal.event_slug,
                    meta={
                        "user_id": context.user_id,
                        "leader_id": leader_id,
                        "order_id": exec_result.order_id,
                    },
                )
                is_terminal = exec_result.status == "rejected"
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
                        reservation_service=context.extra.get("reservation_service"),
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
            fee_paid = Decimal(str(exec_result.fees_paid or 0))
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
                        price=float(fill_price or 0),
                        size_tokens=float(fill_size),
                        usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
                        leader_wallet=leader_id,
                        event_slug=signal.event_slug,
                        title=signal.title,
                    )
            else:
                updated_entry_price = _to_optional_float(
                    getattr(updated_position, "avg_entry_price", None)
                    if updated_position is not None
                    else None
                )
                signal_entry_price = _to_optional_float(signal.price)
                entry_price = next(
                    (
                        candidate
                        for candidate in (
                            sell_entry_price,
                            updated_entry_price,
                            signal_entry_price,
                        )
                        if candidate is not None
                    ),
                    0.0,
                )
                if entry_price == 0.0 and all(
                    candidate is None
                    for candidate in (
                        sell_entry_price,
                        updated_entry_price,
                        signal_entry_price,
                    )
                ):
                    logger.warning(
                        "trade_sell_entry_price_missing",
                        extra={
                            "user_id": context.user_id,
                            "leader_id": leader_id,
                            "condition_id": signal.condition_id,
                            "order_id": exec_result.order_id,
                        },
                    )
                realized = (float(fill_price or 0) - entry_price) * float(fill_size)
                if context.pnl_tracker is not None:
                    context.pnl_tracker.record_sell(
                        condition_id=signal.condition_id,
                        price=float(fill_price or 0),
                        size_tokens=float(fill_size),
                        usdc_value=float(fill_size * Decimal(str(fill_price or 0))),
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
                    "fee_paid": float(fee_paid),
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
                    "fee_paid": float(fee_paid),
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
