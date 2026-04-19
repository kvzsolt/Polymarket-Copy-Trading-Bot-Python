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
from services.riskEngine import active_policy
from storage.sqlite.fills_repo import FillRecord
from utils import is_valid_clob_token_id
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


async def _fetch_and_store_price(
    client,
    token_id: str,
    cache: RiskPriceCache,
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        price_data = await client.get_price(token_id, "SELL")
    if not price_data or price_data.get("price") is None:
        return

    try:
        price = Decimal(str(price_data["price"]).strip())
    except Exception:
        return
    if price <= 0:
        return

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
) -> None:
    unique_token_ids = [token_id for token_id in dict.fromkeys(token_ids) if token_id]
    if not unique_token_ids:
        return

    semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
    await asyncio.gather(
        *[
            _fetch_and_store_price(client, token_id, cache, semaphore)
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
    await _refresh_price_cache_for_tokens(
        context.client,
        token_ids,
        cache,
        max_concurrency=max_concurrency,
    )


async def _get_context_cached_price(
    context: TradingContext,
    token_id: str,
    *,
    refresh_interval: float,
) -> Decimal | None:
    cache = _ensure_context_price_cache(context)
    stale_after = _risk_cache_stale_after(refresh_interval)
    entry = cache.get_fresh(token_id, stale_after=stale_after)
    if entry is not None:
        return entry.price

    await _refresh_price_cache_for_tokens(context.client, [token_id], cache)
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

    logger.info(
        "Risk monitor started: advanced exit engines active "
        f"interval={interval}s"
    )

    price_cache = RiskPriceCache()

    while True:
        try:
            readiness_tracker.update(last_risk_check_ts=int(time.time()))
            positions = pm.open_positions
            valid_assets = [pos.asset for pos in positions.values() if is_valid_clob_token_id(pos.asset)]
            await _refresh_price_cache_for_tokens(clob_client, valid_assets, price_cache)
            for cid, pos in positions.items():
                if not is_valid_clob_token_id(pos.asset):
                    logger.warning("Legacy risk monitor: skipping invalid asset=%r cid=%s", pos.asset, cid)
                    continue
                try:
                    cached_price = price_cache.get_fresh(
                        pos.asset,
                        stale_after=_risk_cache_stale_after(interval),
                    )
                    if cached_price is None:
                        continue
                    current_price = float(cached_price.price)
                    if current_price <= 0:
                        continue

                    action = pm.evaluate_risk_action(cid, current_price)
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
                                    cid[:12],
                                )
                                fraction = 1.0

                        sell_size = pos.size_tokens * max(0.0, min(1.0, fraction))
                        if sell_size <= 0:
                            continue
                        logger.info(
                            f"Executing risk {trigger} for {cid[:12]}… "
                            f"tokens={sell_size:.4f}"
                        )
                        exec_req = ExecutionRequest(
                            user_id=funder_address,
                            leader_id=pos.leader_wallet or "risk-monitor",
                            token_id=pos.asset,
                            side="SELL",
                            amount=Decimal(str(sell_size)),
                            max_slippage_bps=slippage_bps,
                            reason=trigger,
                            signal_id=f"risk:{trigger}:{cid}:{int(time.time())}",
                        )
                        result = await execution_service.execute(exec_req)
                        if result.accepted and result.filled_amount > 0:
                            fill_tokens = float(result.filled_amount)
                            fill_price = float(result.avg_price or current_price)
                            usdc_value = fill_price * fill_tokens
                            tracker = pnl_tracker
                            tracked = pm.get_position(cid)
                            entry_price = tracked.entry_price if tracked else pos.entry_price
                            pnl = (fill_price - entry_price) * fill_tokens
                            log_risk_exit(
                                logger,
                                condition_id=cid,
                                asset=pos.asset,
                                size=fill_tokens,
                                price=fill_price,
                                usdc_value=usdc_value,
                                reason=trigger,
                                event_slug=pos.event_slug,
                                meta={
                                    "copyOrderId": result.order_id,
                                    "leaderWallet": pos.leader_wallet,
                                },
                            )
                            pm.track_sell(cid, fill_price, fill_tokens, reason=trigger)
                            tracker.record_sell(
                                condition_id=cid,
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
                                    "condition_id": cid,
                                    "token_id": pos.asset,
                                    "side": "SELL",
                                    "size_tokens": str(fill_tokens),
                                    "price": str(fill_price),
                                    "usdc_value": str(usdc_value),
                                    "leader_id": pos.leader_wallet,
                                    "category": str(pos.category or "unknown"),
                                    "reason": trigger,
                                },
                            )
                            remaining = pm.get_position(cid)
                            if remaining is None:
                                accounting.close_position(user_id=user_id, condition_id=str(cid or ""))
                            else:
                                accounting.upsert_position(
                                    PositionLot(
                                        user_id=user_id,
                                        condition_id=str(cid or ""),
                                        token_id=str(remaining.asset or pos.asset),
                                        size_tokens=Decimal(str(remaining.size_tokens)),
                                        avg_entry_price=Decimal(str(remaining.entry_price)),
                                        leader_id=remaining.leader_wallet or pos.leader_wallet,
                                        category=str(remaining.category or pos.category or "unknown"),
                                    )
                                )
                except Exception as e:
                    logger.error(f"Risk check error for {cid[:12]}…: {e}")

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
                existing = pm.get_position(pos.condition_id)
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


async def run_risk_check_for_context(context: TradingContext) -> None:
    if not context.execution_service:
        return

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
                action = context.position_manager.evaluate_risk_action(pos.condition_id, float(current_price))
            if not action:
                action = _fallback_risk_decision(pos.avg_entry_price, current_price)
            if not action:
                continue

            fraction = max(0.0, min(1.0, float(action.get("size_fraction", 1.0) or 1.0)))
            reason = str(action.get("reason") or "risk_exit")

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
            )
            result = await context.execution_service.execute(req)
            if not result.accepted or result.filled_amount <= 0:
                continue

            fill_size = Decimal(str(result.filled_amount))
            fill_price = Decimal(str(result.avg_price or current_price))
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
                context.position_manager.track_sell(
                    pos.condition_id,
                    float(fill_price),
                    float(fill_size),
                    reason=reason,
                )
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
