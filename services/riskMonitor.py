"""Risk monitor — background task that checks open positions for advanced exit triggers."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from domain.trading_context import TradingContext
from services.accountingService import AccountingService, PositionLot
from services.executionService import ExecutionRequest, ExecutionService
from services.feeService import FeeService
from services.healthServer import ReadinessTracker
from services.marketMetaService import MarketMetaService
from services.metrics import metrics
from services.orderRepository import OrderRepository
from services.positionActionCoordinator import PositionScope
from services.riskEngine import active_policy
from storage.sqlite.fills_repo import FillRecord
from utils import is_valid_clob_token_id

# ---------------------------------------------------------------------------
# Venue circuit breaker — suppresses price polls during exchange outages.
# ---------------------------------------------------------------------------
_DEFAULT_CB_THRESHOLD = 3  # consecutive failures to open
_DEFAULT_CB_COOLDOWN = 60.0  # seconds before half-open retry


class VenueCircuitBreaker:
    """Tracks consecutive 404 / venue-unavailable failures across all tokens.

    When the threshold is exceeded the breaker opens and all price fetches
    are suppressed for *cooldown* seconds, preventing log spam and wasted
    API calls during a Polymarket outage.
    """

    def __init__(
        self,
        *,
        threshold: int = _DEFAULT_CB_THRESHOLD,
        cooldown: float = _DEFAULT_CB_COOLDOWN,
    ) -> None:
        self._threshold = max(1, threshold)
        self._cooldown = max(5.0, cooldown)
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @property
    def state(self) -> str:
        """``closed`` | ``open`` | ``half_open``"""
        if self._opened_at is None:
            return "closed"
        if time.time() - self._opened_at >= self._cooldown:
            return "half_open"
        return "open"

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold and self._opened_at is None:
            self._opened_at = time.time()
            logger.warning(
                "venue_circuit_breaker_opened | failures=%d cooldown=%.0fs",
                self._consecutive_failures,
                self._cooldown,
            )

    def should_suppress(self) -> bool:
        """Return *True* when the breaker is open (polls should be skipped)."""
        state = self.state
        if state == "closed":
            return False
        if state == "half_open":
            return False  # allow one probe
        return True  # open

    def reset(self) -> None:
        self._consecutive_failures = 0
        self._opened_at = None
from utils.logger import get_logger, log_risk_exit

logger = get_logger("RiskMonitor")

# Track token_ids we've already warned about to avoid log spam every cycle.
_warned_invalid_tokens: set[str] = set()
_DEFAULT_RISK_PRICE_SEMAPHORE = 8
_MIN_RISK_CACHE_STALE_SECONDS = 0.75


@dataclass
class CachedRiskPrice:
    price: Decimal
    size: Decimal | None
    fetched_at: float


class RiskPriceCache:
    def __init__(self) -> None:
        self._entries: dict[str, CachedRiskPrice] = {}

    def get_fresh(self, token_id: str, *, stale_after: float, now: float | None = None) -> CachedRiskPrice | None:
        entry = self._entries.get(token_id)
        if entry is None:
            return None
        current_ts = now if now is not None else time.time()
        if current_ts - entry.fetched_at > stale_after:
            return None
        return entry

    def set(self, token_id: str, *, price: Decimal, size: Decimal | None = None, fetched_at: float | None = None) -> None:
        self._entries[token_id] = CachedRiskPrice(
            price=price,
            size=size,
            fetched_at=fetched_at if fetched_at is not None else time.time(),
        )


def _risk_cache_stale_after(refresh_interval: float) -> float:
    return max(_MIN_RISK_CACHE_STALE_SECONDS, float(refresh_interval) * 3.0)


def _ensure_context_venue_circuit_breaker(context: TradingContext) -> VenueCircuitBreaker:
    breaker = context.extra.get("venue_circuit_breaker")
    if isinstance(breaker, VenueCircuitBreaker):
        return breaker
    breaker = VenueCircuitBreaker()
    context.extra["venue_circuit_breaker"] = breaker
    return breaker


async def _fetch_and_store_price(
    client,
    token_id: str,
    cache: RiskPriceCache,
    semaphore: asyncio.Semaphore,
    *,
    circuit_breaker: VenueCircuitBreaker | None = None,
) -> None:
    if circuit_breaker is not None and circuit_breaker.should_suppress():
        return
    async with semaphore:
        try:
            price_data = await client.get_price(token_id, "SELL")
        except Exception as exc:
            if circuit_breaker is not None:
                circuit_breaker.record_failure()
            raise
    if not price_data or price_data.get("price") is None:
        if circuit_breaker is not None:
            circuit_breaker.record_failure()
        return

    try:
        price = Decimal(str(price_data["price"]).strip())
    except Exception:
        if circuit_breaker is not None:
            circuit_breaker.record_failure()
        return
    if price <= 0:
        if circuit_breaker is not None:
            circuit_breaker.record_failure()
        return

    if circuit_breaker is not None:
        circuit_breaker.record_success()

    size_raw = price_data.get("size")
    size: Decimal | None = None
    if size_raw is not None:
        try:
            size = Decimal(str(size_raw).strip())
        except Exception:
            size = None

    cache.set(token_id, price=price, size=size)


async def _refresh_price_cache_for_tokens(
    client,
    token_ids: list[str],
    cache: RiskPriceCache,
    *,
    max_concurrency: int = _DEFAULT_RISK_PRICE_SEMAPHORE,
    circuit_breaker: VenueCircuitBreaker | None = None,
) -> None:
    unique_token_ids = [token_id for token_id in dict.fromkeys(token_ids) if token_id]
    if not unique_token_ids:
        return

    semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
    await asyncio.gather(
        *[
            _fetch_and_store_price(client, token_id, cache, semaphore, circuit_breaker=circuit_breaker)
            for token_id in unique_token_ids
        ],
        return_exceptions=True,
    )


def _ensure_context_price_cache(context: TradingContext) -> RiskPriceCache:
    cache = context.extra.get("risk_price_cache")
    if isinstance(cache, RiskPriceCache):
        return cache
    cache = RiskPriceCache()
    context.extra["risk_price_cache"] = cache
    return cache


async def _refresh_context_price_cache(
    context: TradingContext,
    *,
    refresh_interval: float,
    max_concurrency: int = _DEFAULT_RISK_PRICE_SEMAPHORE,
) -> None:
    token_ids = [
        pos.token_id
        for pos in context.positions_repo.list_open_positions(context.user_id)
        if is_valid_clob_token_id(pos.token_id)
    ]
    if not token_ids:
        return
    cache = _ensure_context_price_cache(context)
    breaker = _ensure_context_venue_circuit_breaker(context)
    if breaker.should_suppress():
        return
    await _refresh_price_cache_for_tokens(
        context.client,
        token_ids,
        cache,
        max_concurrency=max_concurrency,
        circuit_breaker=breaker,
    )


async def _get_context_cached_price(
    context: TradingContext,
    token_id: str,
    *,
    refresh_interval: float,
) -> Decimal | None:
    cache = _ensure_context_price_cache(context)
    breaker = _ensure_context_venue_circuit_breaker(context)
    if breaker.should_suppress():
        return None
    stale_after = _risk_cache_stale_after(refresh_interval)
    entry = cache.get_fresh(token_id, stale_after=stale_after)
    if entry is not None:
        return entry.price

    await _refresh_price_cache_for_tokens(
        context.client,
        [token_id],
        cache,
        circuit_breaker=breaker,
    )
    refreshed = cache.get_fresh(token_id, stale_after=stale_after)
    return refreshed.price if refreshed is not None else None


def _exit_monitor_enabled(config: AppConfig | None) -> bool:
    if config is None:
        return True
    return bool(getattr(config.risk, "enable_risk_mgmt", False))


async def risk_monitor_loop(
    clob_client,
    bot_params: dict,
    position_manager,
    pnl_tracker,
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig | None = None,
):
    """Continuously check open positions against advanced exit engines.

    Runs as a background asyncio task alongside the trade monitor.

    Legacy compatibility path: active runtime uses
    `risk_monitor_loop_for_contexts` with persisted position repositories.
    """
    pm = position_manager

    if config is not None:
        interval = config.polling.risk_check_interval
        enabled = _exit_monitor_enabled(config)
        slippage_bps = int(config.execution.slippage_tolerance * 100)
        funder_address = config.wallet.funder_address
        enable_tiered = config.risk.enable_balance_tiered_policy
        fee_bps = int(config.execution.transaction_fee_bps)
    else:
        interval = 15
        enabled = True
        slippage_bps = 200
        funder_address = ""
        enable_tiered = False
        fee_bps = 20

    execution_service = ExecutionService(
        clob_client=clob_client,
        market_meta_service=MarketMetaService(clob_client),
        fee_service=FeeService(clob_client, default_fee_bps=fee_bps),
        order_repo=OrderRepository(),
    )
    accounting = AccountingService()

    if not enabled:
        logger.info("Global risk management disabled — monitor will not run")
        return
    if not funder_address:
        logger.error("Risk monitor disabled: missing FUNDER_ADDRESS")
        return

    logger.info(
        "Risk monitor started: advanced exit engines active "
        f"interval={interval}s"
    )

    price_cache = RiskPriceCache()
    venue_breaker = VenueCircuitBreaker()

    while True:
        try:
            readiness_tracker.update(last_risk_check_ts=int(time.time()))
            positions = pm.open_positions
            valid_assets = [pos.asset for pos in positions.values() if is_valid_clob_token_id(pos.asset)]
            await _refresh_price_cache_for_tokens(
                clob_client,
                valid_assets,
                price_cache,
                circuit_breaker=venue_breaker,
            )
            for runtime_key, pos in positions.items():
                condition_id = str(pos.condition_id or "")
                token_id = str(pos.asset or "")
                if not is_valid_clob_token_id(token_id):
                    logger.warning("Legacy risk monitor: skipping invalid asset=%r cid=%s", token_id, condition_id)
                    continue
                try:
                    cached_price = price_cache.get_fresh(
                        token_id,
                        stale_after=_risk_cache_stale_after(interval),
                    )
                    if cached_price is None:
                        continue
                    current_price = float(cached_price.price)
                    if current_price <= 0:
                        continue

                    action = pm.evaluate_risk_action(condition_id, token_id, current_price)
                    if action:
                        trigger = str(action.get("reason") or "risk_exit")
                        fraction = float(action.get("size_fraction", 1.0) or 1.0)

                        # Simplified account-cap policy: partial exits are a single global choice.
                        if enable_tiered and fraction < 1.0:
                            policy = active_policy()
                            if not policy.allow_partial_exits:
                                logger.info(
                                    "Balance policy [%s] upgrades partial exit to full for %s…",
                                    policy.name,
                                    condition_id[:12],
                                )
                                fraction = 1.0

                        sell_size = pos.size_tokens * max(0.0, min(1.0, fraction))
                        if sell_size <= 0:
                            continue
                        logger.info(
                            f"Executing risk {trigger} for {condition_id[:12]}…/{token_id[:12]} "
                            f"tokens={sell_size:.4f}"
                        )
                        if not funder_address:
                            logger.error(
                                "Risk monitor cannot build ExecutionRequest without FUNDER_ADDRESS"
                            )
                            continue
                        exec_req = ExecutionRequest(
                            user_id=funder_address,
                            leader_id=pos.leader_wallet or "risk-monitor",
                            token_id=token_id,
                            side="SELL",
                            amount=Decimal(str(sell_size)),
                            max_slippage_bps=slippage_bps,
                            reason=trigger,
                            signal_id=f"risk:{trigger}:{condition_id}:{int(time.time())}",
                            condition_id=condition_id,
                            recovery_metadata={
                                "leader_notional_at_trigger": float(pos.size_tokens * Decimal(str(current_price))),
                                "copied_position_notional_at_trigger": float(pos.size_tokens * Decimal(str(current_price))),
                                "trigger_price": float(current_price),
                                "trigger_size_tokens": float(pos.size_tokens),
                                "trigger_fraction": fraction,
                            },
                        )
                        result = await execution_service.execute(exec_req)
                        if result.accepted and result.filled_amount > 0:
                            fill_tokens = float(result.filled_amount)
                            fill_price = float(result.avg_price or current_price)
                            usdc_value = fill_price * fill_tokens
                            tracker = pnl_tracker
                            tracked = pm.get_position(condition_id, token_id)
                            entry_price = tracked.entry_price if tracked else pos.entry_price
                            pnl = (fill_price - entry_price) * fill_tokens
                            log_risk_exit(
                                logger,
                                condition_id=condition_id,
                                asset=token_id,
                                size=fill_tokens,
                                price=fill_price,
                                usdc_value=usdc_value,
                                reason=trigger,
                                event_slug=pos.event_slug,
                                meta={
                                    "copyOrderId": result.order_id,
                                    "leaderWallet": pos.leader_wallet,
                                    "runtimeKey": runtime_key,
                                },
                            )
                            closed_pos = pm.track_sell(condition_id, token_id, fill_price, fill_tokens, reason=trigger)
                            if closed_pos is not None and hasattr(context, "position_coordinator"):
                                try:
                                    context.position_coordinator.release_scope(
                                        PositionScope(
                                            user_id=context.user_id,
                                            condition_id=condition_id,
                                            token_id=token_id,
                                        )
                                    )
                                except Exception:
                                    pass
                            tracker.record_sell(
                                condition_id=condition_id,
                                price=fill_price,
                                size_tokens=fill_tokens,
                                usdc_value=usdc_value,
                                entry_price=entry_price,
                                realized_pnl=pnl,
                                exit_reason=trigger,
                                leader_wallet=pos.leader_wallet,
                                event_slug=pos.event_slug,
                                title=pos.title,
                            )
                            user_id = str(
                                bot_params.get("proxyWallet")
                                or funder_address
                            )
                            accounting.book_fill(
                                user_id=user_id,
                                fill_doc={
                                    "order_id": result.order_id,
                                    "condition_id": condition_id,
                                    "token_id": token_id,
                                    "side": "SELL",
                                    "size_tokens": str(fill_tokens),
                                    "price": str(fill_price),
                                    "usdc_value": str(usdc_value),
                                    "leader_id": pos.leader_wallet,
                                    "category": str(pos.category or "unknown"),
                                    "reason": trigger,
                                },
                            )
                            remaining = pm.get_position(condition_id, token_id)
                            if remaining is None:
                                accounting.close_position(user_id=user_id, condition_id=condition_id)
                            else:
                                accounting.upsert_position(
                                    PositionLot(
                                        user_id=user_id,
                                        condition_id=condition_id,
                                        token_id=str(remaining.asset or token_id),
                                        size_tokens=Decimal(str(remaining.size_tokens)),
                                        avg_entry_price=Decimal(str(remaining.entry_price)),
                                        leader_id=remaining.leader_wallet or pos.leader_wallet,
                                        category=str(remaining.category or pos.category or "unknown"),
                                    )
                                )
                except Exception as e:
                    logger.error(f"Risk check error for {condition_id[:12]}…/{token_id[:12]}: {e}")

        except Exception as e:
            logger.error(f"Risk monitor loop error: {e}")

        await asyncio.sleep(interval)


def rehydrate_position_manager(context: TradingContext) -> None:
    """Load persisted open positions into the context position manager."""
    pm = context.position_manager
    if not hasattr(pm, "track_buy"):
        return

    for pos in context.positions_repo.list_open_positions(context.user_id):
        existing = None
        if hasattr(pm, "get_position"):
            try:
                existing = pm.get_position(pos.condition_id, pos.token_id)
            except Exception:
                existing = None
        if existing is not None:
            continue
        try:
            pm.track_buy(
                condition_id=pos.condition_id,
                asset=pos.token_id,
                entry_price=float(pos.avg_entry_price),
                size_tokens=float(pos.size_tokens),
                usdc_cost=float(pos.size_tokens * pos.avg_entry_price),
                leader_wallet=pos.leader_id or "",
            )
        except Exception:
            continue


def _fallback_risk_decision(
    entry_price: Decimal,
    current_price: Decimal,
    *,
    config: AppConfig | None = None,
) -> dict | None:
    """No implicit fallback exits. Advanced exit decisions live in PositionManager."""
    return None


def _extract_risk_condition_id(request: dict[str, object] | None) -> str | None:
    if not isinstance(request, dict):
        return None
    condition_id = str(request.get("condition_id") or "").strip()
    if condition_id:
        return condition_id
    signal_id = str(request.get("signal_id") or "").strip()
    if not signal_id.startswith("risk:"):
        return None
    parts = signal_id.split(":", 3)
    if len(parts) < 3:
        return None
    derived = str(parts[2] or "").strip()
    return derived or None


def _decimal_or_zero(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_payload(payload: dict[str, object] | None) -> Decimal:
    if not isinstance(payload, dict):
        return Decimal("0")
    return _decimal_or_zero(
        payload.get("size_matched")
        or payload.get("filled_size")
        or payload.get("size")
        or 0
    )


def _risk_exit_fully_closed_position(updated_pos: object | None) -> bool:
    """Return True only when the protective SELL fully closed the position.

    Partial fills are real and must still update fills/positions/PnL, but they
    must NOT activate a durable re-entry block.  The block represents a
    completed protective exit regime, not merely a partial de-risking step.
    """
    return updated_pos is None


async def reconcile_unconfirmed_risk_orders(context: TradingContext) -> int:
    """Converge risk-monitor-originated submitted_unconfirmed SELLs.

    These orders live in the durable ``orders`` table but do not create a
    ``processed_trades`` row, so trade-pipeline reconciliation alone cannot
    recover them after a restart. This helper reuses the persisted submission
    metadata to resolve the venue outcome deterministically before the risk
    monitor evaluates positions again.
    """
    if not context.execution_service:
        return 0

    records = context.orders_repo.get_unconfirmed_order_submissions(
        user_id=context.user_id,
        side="SELL",
    )
    reconciled = 0
    terminal_no_fill_statuses = {"cancelled", "canceled", "expired", "rejected"}
    final_fill_statuses = {"filled", "matched", ""}

    for record in records:
        request = record.get("request") or {}
        signal_id = str(request.get("signal_id") or "")
        if not signal_id.startswith("risk:"):
            continue

        order_id = str(record.get("order_id") or "").strip()
        token_id = str(request.get("token_id") or "").strip()
        condition_id = _extract_risk_condition_id(request)
        if not order_id or not token_id or not condition_id or not is_valid_clob_token_id(token_id):
            continue

        scope = PositionScope(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
        )
        async with context.position_coordinator.acquire(scope):
            try:
                fill_info = await context.execution_service._confirm_fill(order_id)
            except Exception as exc:
                logger.warning(
                    "risk_unconfirmed_reconcile_poll_error | user=%s order_id=%s error=%s",
                    context.user_id,
                    order_id,
                    repr(exc),
                )
                continue

            venue_status = str(fill_info.get("status", "")).lower()
            current_matched = _matched_amount_from_payload(fill_info)
            previous_matched = _matched_amount_from_payload(record.get("response"))
            delta_matched = current_matched - previous_matched
            if delta_matched < 0:
                delta_matched = Decimal("0")

            if current_matched <= 0 and venue_status in terminal_no_fill_statuses:
                context.orders_repo.update_submission_status(
                    order_id=order_id,
                    status=venue_status,
                    response_patch=fill_info,
                )
                reconciled += 1
                continue

            is_final_fill = venue_status in final_fill_statuses or (
                venue_status in terminal_no_fill_statuses and current_matched > 0
            )
            if delta_matched <= 0 and not is_final_fill:
                continue
            if delta_matched <= 0 and is_final_fill:
                context.orders_repo.update_submission_status(
                    order_id=order_id,
                    status="filled_reconciled",
                    response_patch={
                        **fill_info,
                        "reconciled_from": "risk_submitted_unconfirmed",
                        "matched_previous": str(previous_matched),
                        "matched_delta": str(delta_matched),
                    },
                )
                reconciled += 1
                continue

            avg_price_raw = (
                fill_info.get("avg_price")
                or fill_info.get("price")
                or fill_info.get("average_price")
                or 0
            )
            try:
                avg_price = Decimal(str(avg_price_raw))
            except Exception:
                avg_price = Decimal("0")
            if avg_price <= 0:
                continue

            reason = str(request.get("reason") or "risk_exit")
            leader_id = str(request.get("leader_id") or "risk-monitor")
            existing_pos = context.positions_repo.get_position(
                context.user_id,
                condition_id,
                token_id=token_id,
            )
            entry_price = float(getattr(existing_pos, "avg_entry_price", avg_price)) if existing_pos else float(avg_price)

            try:
                context.fills_repo.append_fill(
                    FillRecord(
                        user_id=context.user_id,
                        leader_id=leader_id,
                        order_id=order_id,
                        condition_id=condition_id,
                        token_id=token_id,
                        side="SELL",
                        size_tokens=delta_matched,
                        price=avg_price,
                        fee=Decimal("0"),
                        fill_ts=int(time.time()),
                    ),
                    raw_payload={
                        "reconciled_from": "risk_submitted_unconfirmed",
                        "copy_order_id": order_id,
                        "status": "reconciled",
                        "side": "SELL",
                    },
                )
                updated_pos = context.positions_repo.apply_fill(
                    user_id=context.user_id,
                    leader_id=leader_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    side="SELL",
                    fill_size=delta_matched,
                    fill_price=avg_price,
                )
            except Exception as exc:
                logger.warning(
                    "risk_unconfirmed_reconcile_apply_error | user=%s order_id=%s error=%s",
                    context.user_id,
                    order_id,
                    repr(exc),
                )
                continue

            recovery_metadata = request.get("recovery_metadata")
            block_metadata = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
            block_metadata["fill_price"] = float(avg_price)
            block_metadata["fill_size_tokens"] = float(delta_matched)
            block_metadata["fill_order_id"] = order_id
            if is_final_fill and _risk_exit_fully_closed_position(updated_pos):
                context.risk_blocks_repo.mark_risk_block(
                    user_id=context.user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    reason=reason,
                    source="risk_monitor",
                    leader_anchor=leader_id,
                    metadata=block_metadata,
                )
            else:
                logger.info(
                    "risk_unconfirmed_partial_fill_no_block | user=%s order_id=%s condition=%s token=%s remaining=%s",
                    context.user_id,
                    order_id,
                    condition_id,
                    token_id,
                    getattr(updated_pos, "size_tokens", None),
                )

            if hasattr(context.pnl_tracker, "record_sell"):
                realized = (float(avg_price) - entry_price) * float(delta_matched)
                context.pnl_tracker.record_sell(
                    condition_id=condition_id,
                    price=float(avg_price),
                    size_tokens=float(delta_matched),
                    usdc_value=float(delta_matched * avg_price),
                    entry_price=entry_price,
                    realized_pnl=realized,
                    exit_reason=reason,
                    leader_wallet=leader_id,
                )

            submission_status = "filled_reconciled" if is_final_fill else "submitted_unconfirmed"
            context.orders_repo.update_submission_status(
                order_id=order_id,
                status=submission_status,
                response_patch={
                    **fill_info,
                    "reconciled_from": "risk_submitted_unconfirmed",
                    "matched_previous": str(previous_matched),
                    "matched_delta": str(delta_matched),
                },
            )
            reconciled += 1

    return reconciled


async def run_risk_check_for_context(context: TradingContext) -> None:
    if not context.execution_service:
        return

    # Issue #29 — reconcile unconfirmed orders BEFORE rehydration / position
    # evaluation.  Without this, on restart the risk monitor observes stale
    # position state: unconfirmed SELLs that already filled on-exchange still
    # appear as open positions, leading to duplicate protective SELLs; and
    # unconfirmed BUYs that filled are invisible, so the monitor cannot
    # protect them.  The trade pipeline already does this at the top of
    # execute_trades; the risk monitor must converge the same way.
    try:
        from orchestrators.trade_pipeline import reconcile_submitted_unconfirmed
        trade_n = await reconcile_submitted_unconfirmed(context)
        risk_n = await reconcile_unconfirmed_risk_orders(context)
        total_n = trade_n + risk_n
        if total_n:
            logger.info(
                "risk_monitor_pre_reconcile | reconciled=%d unconfirmed orders (trade=%d risk=%d)",
                total_n,
                trade_n,
                risk_n,
            )
    except Exception as exc:
        logger.warning("risk_monitor_pre_reconcile_error | %s", repr(exc))

    rehydrate_position_manager(context)
    slippage_bps = int(float(context.extra.get("slippage_tolerance_bps", 200)))
    refresh_interval = float(context.extra.get("risk_check_interval_seconds", 0.25) or 0.25)

    # Read enable_tiered from context extra (set by bootstrap from config)
    enable_tiered = str(context.extra.get("enable_balance_tiered_policy", "false")).lower() in ("1", "true", "yes")

    for pos in context.positions_repo.list_open_positions(context.user_id):
        if not is_valid_clob_token_id(pos.token_id):
            if pos.token_id not in _warned_invalid_tokens:
                _warned_invalid_tokens.add(pos.token_id)
                logger.warning(
                    "Skipping position with invalid token_id=%r (condition=%s) — "
                    "likely stale placeholder; close or delete manually",
                    pos.token_id, pos.condition_id,
                )
            continue
        # --- Per-scoped-position action coordination (issue #28) ---
        _scope = PositionScope(
            user_id=context.user_id,
            condition_id=pos.condition_id,
            token_id=pos.token_id,
        )
        async with context.position_coordinator.acquire(_scope):
            try:
                current_price = await _get_context_cached_price(
                    context,
                    pos.token_id,
                    refresh_interval=refresh_interval,
                )
                if current_price is None or current_price <= 0:
                    continue

                action = None
                if hasattr(context.position_manager, "evaluate_risk_action"):
                    try:
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            pos.token_id,
                            float(current_price),
                        )
                    except TypeError:
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            float(current_price),
                        )
                if not action:
                    action = _fallback_risk_decision(pos.avg_entry_price, current_price)
                if not action:
                    continue

                fraction = max(0.0, min(1.0, float(action.get("size_fraction", 1.0) or 1.0)))
                reason = str(action.get("reason") or "risk_exit")
                trigger_notional = float((pos.size_tokens * Decimal(str(current_price))).quantize(Decimal("0.00000001")))

                # Capture block metadata at trigger time — but defer persisting the
                # risk block until after the protective SELL actually fills.  Writing
                # the block before execution creates a durable re-entry ban even when
                # the SELL fails, which is the core issue #27 lifecycle bug.
                _block_metadata = {
                    "leader_notional_at_trigger": trigger_notional,
                    "copied_position_notional_at_trigger": trigger_notional,
                    "trigger_price": float(current_price),
                    "trigger_size_tokens": float(pos.size_tokens),
                    "trigger_fraction": fraction,
                }

                # Simplified account-cap policy
                if fraction < 1.0 and enable_tiered:
                    policy = active_policy()
                    if not policy.allow_partial_exits:
                        logger.info(
                            "Balance policy [%s] upgrades partial exit → full for %s (was %.0f%%)",
                            policy.name, pos.condition_id[:12], fraction * 100,
                        )
                        fraction = 1.0

                sell_size = (pos.size_tokens * Decimal(str(fraction))).quantize(Decimal("0.00000001"))
                if sell_size <= 0:
                    continue

                req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=pos.leader_id or "risk-monitor",
                    token_id=pos.token_id,
                    side="SELL",
                    amount=sell_size,
                    max_slippage_bps=slippage_bps,
                    reason=reason,
                    signal_id=f"risk:{reason}:{pos.condition_id}:{int(time.time())}",
                    condition_id=pos.condition_id,
                    recovery_metadata=_block_metadata,
                )
                result = await context.execution_service.execute(req)
                if not result.accepted or result.filled_amount <= 0:
                    logger.info(
                        "risk_exit_sell_not_filled | cond=%s token=%s reason=%s status=%s error=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.status,
                        result.error,
                    )
                    continue

                fill_size = Decimal(str(result.filled_amount))
                fill_price = Decimal(str(result.avg_price or current_price))

                # Persist risk block only when the protective SELL fully closes
                # the scoped position. A partial fill is real and should be
                # accounted for, but it must not create a durable re-entry ban.
                _block_metadata["fill_price"] = float(fill_price)
                _block_metadata["fill_size_tokens"] = float(fill_size)
                _block_metadata["fill_order_id"] = result.order_id

                context.fills_repo.append_fill(
                    FillRecord(
                        user_id=context.user_id,
                        leader_id=pos.leader_id,
                        order_id=result.order_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        side="SELL",
                        size_tokens=fill_size,
                        price=fill_price,
                        fee=Decimal(str(result.fees_paid or 0)),
                        fill_ts=int(time.time()),
                    ),
                )
                updated_pos = context.positions_repo.apply_fill(
                    user_id=context.user_id,
                    leader_id=pos.leader_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                    side="SELL",
                    fill_size=fill_size,
                    fill_price=fill_price,
                )
                if _risk_exit_fully_closed_position(updated_pos):
                    context.risk_blocks_repo.mark_risk_block(
                        user_id=context.user_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        reason=reason,
                        source="risk_monitor",
                        leader_anchor=pos.leader_id,
                        metadata=_block_metadata,
                    )
                else:
                    logger.info(
                        "risk_exit_partial_fill_no_block | user=%s condition=%s token=%s remaining=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        getattr(updated_pos, "size_tokens", None),
                    )
                if hasattr(context.pnl_tracker, "record_sell"):
                    entry_price = float(pos.avg_entry_price)
                    realized = (float(fill_price) - entry_price) * float(fill_size)
                    context.pnl_tracker.record_sell(
                        condition_id=pos.condition_id,
                        price=float(fill_price),
                        size_tokens=float(fill_size),
                        usdc_value=float(fill_size * fill_price),
                        entry_price=entry_price,
                        realized_pnl=realized,
                        exit_reason=reason,
                        leader_wallet=pos.leader_id or "",
                    )
                await metrics.inc(
                    "risk_exits_total",
                    labels={
                        "user_id": context.user_id,
                        "leader_id": str(pos.leader_id or ""),
                        "reason": reason,
                    },
                )
                if hasattr(context.position_manager, "track_sell"):
                    try:
                        closed_pos = context.position_manager.track_sell(
                            pos.condition_id,
                            pos.token_id,
                            float(fill_price),
                            float(fill_size),
                            reason=reason,
                        )
                    except TypeError:
                        closed_pos = context.position_manager.track_sell(
                            pos.condition_id,
                            float(fill_price),
                            float(fill_size),
                            reason=reason,
                        )
                    if closed_pos is not None and hasattr(context, "position_coordinator"):
                        try:
                            context.position_coordinator.release_scope(
                                PositionScope(
                                    user_id=context.user_id,
                                    condition_id=pos.condition_id,
                                    token_id=getattr(pos, "token_id", getattr(pos, "asset", "")),
                                )
                            )
                        except Exception:
                            pass
            except Exception as exc:
                logger.error(f"Risk context check failed user={context.user_id} condition={pos.condition_id}: {exc}")


async def risk_monitor_loop_for_contexts(
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig | None = None,
    interval: float | None = None,
) -> None:
    """Restart-safe risk monitor using persisted positions as the source of truth."""
    if not contexts:
        logger.warning("Risk monitor started with no contexts")
        return

    if config is not None:
        enabled = _exit_monitor_enabled(config)
        poll_interval = float(interval or config.polling.risk_check_interval)
    else:
        enabled = True
        poll_interval = float(interval or 15)

    if not enabled:
        logger.info("Global risk management disabled — monitor will not run")
        return

    for context in contexts:
        context.extra["risk_check_interval_seconds"] = poll_interval

    while True:
        readiness_tracker.update(last_risk_check_ts=int(time.time()))
        refresh_tasks = [
            _refresh_context_price_cache(context, refresh_interval=poll_interval)
            for context in contexts
        ]
        refresh_results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
        for context, result in zip(contexts, refresh_results, strict=False):
            if isinstance(result, Exception):
                logger.error(f"Risk price cache refresh failed user={context.user_id}: {result}")
        for context in contexts:
            try:
                await run_risk_check_for_context(context)
            except Exception as exc:
                logger.error(f"Risk loop error user={context.user_id}: {exc}")
        await asyncio.sleep(poll_interval)
