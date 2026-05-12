"""Near-one exit — replaces on-chain CTF redemption.

When a copied position's token quote stays at or above ``threshold`` for at
least ``dwell_seconds``, NearOneExitService submits a SELL for the entire
held size through the existing ExecutionService pipeline. The bot exits to
USDC at near-par instead of waiting for venue resolution and burning gas on
``CTF.redeemPositions``.

Reuses the same sizing (services/order_sizing.size_sell_order), reservation,
idempotency, and reconciliation surfaces as risk-monitor protective exits;
no new venue or settlement code is introduced.

Freshness: quotes come from MarketDataService.get_quote_or_none() which
already falls back to REST when WSS is stale. If neither WSS nor REST can
return a quote, the dwell timer never advances — NearOne is fail-closed on
no-data.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from services.execution.types import ExecutionRequest
from services.order_sizing import size_sell_order
from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import AppConfig
    from cache.market_state_store import MarketStateStore
    from services.executionService import ExecutionService
    from services.healthServer import ReadinessTracker
    from services.market_data_service import MarketDataService
    from services.positionManager import PositionManager

logger = get_logger("NearOneExit")


@dataclass
class _DwellState:
    """Per-token dwell tracking.

    ``first_seen_above_ts`` is the monotonic timestamp the price first hit
    threshold and has stayed at or above it since. ``None`` means below.
    """

    first_seen_above_ts: float | None = None


class NearOneExitService:
    """Sustained-near-one exit owner.

    The loop body is intentionally minimal: it reads the latest quote per
    open position, advances or clears each dwell timer, and triggers a SELL
    via ExecutionService when dwell exceeds ``dwell_seconds``. All
    book/tick/sizing/idempotency concerns flow through the same plumbing
    used by copy SELL and protective-exit SELL paths.
    """

    def __init__(
        self,
        *,
        position_manager: "PositionManager",
        execution_service: "ExecutionService",
        cache: "MarketStateStore",
        market_data_service: "MarketDataService",
        config: "AppConfig",
        readiness: "ReadinessTracker",
        user_id: str,
        funder_address: str,
    ) -> None:
        self._pm = position_manager
        self._exec = execution_service
        self._cache = cache
        self._mds = market_data_service
        self._config = config
        self._readiness = readiness
        self._user_id = user_id
        self._funder = funder_address
        self._dwell: dict[str, _DwellState] = {}
        # Tokens already firing this session — prevents re-emit while the
        # SELL is in-flight or partially filled. Cleared on terminal close
        # (position deleted from PositionManager).
        self._firing: set[str] = set()

    # ------------------------------------------------------------------ observers
    def observe_price(self, token_id: str, price: Decimal, ts: float) -> None:
        """External hook for WSS / cache to push price updates without RPC.

        Currently optional — the loop pulls via MarketDataService — but a
        future MDS hook can call this for sub-second responsiveness.
        """
        threshold = self._config.near_one_exit.threshold
        state = self._dwell.setdefault(token_id, _DwellState())
        if price >= threshold:
            if state.first_seen_above_ts is None:
                state.first_seen_above_ts = ts
        else:
            state.first_seen_above_ts = None

    # ------------------------------------------------------------------ loop
    async def loop(self) -> None:
        cfg = self._config.near_one_exit
        if not cfg.enable_near_one_exit:
            logger.info("near_one_exit_disabled")
            return
        if cfg.check_interval <= 0:
            logger.error("near_one_exit_check_interval_invalid interval=%s", cfg.check_interval)
            return

        logger.info(
            "near_one_exit_started threshold=%s dwell_seconds=%s check_interval=%s",
            cfg.threshold,
            cfg.dwell_seconds,
            cfg.check_interval,
        )
        slippage_bps = int(self._config.execution.slippage_tolerance * 100)
        while True:
            try:
                await self._tick(slippage_bps)
            except asyncio.CancelledError:
                logger.info("near_one_exit_cancelled")
                raise
            except Exception as exc:
                logger.warning("near_one_exit_tick_error error=%r", exc)
            await asyncio.sleep(cfg.check_interval)

    async def _tick(self, slippage_bps: int) -> None:
        cfg = self._config.near_one_exit
        positions = self._pm.open_positions
        now = time.monotonic()
        live_tokens: set[str] = set()
        for pos in positions.values():
            token_id = str(pos.asset or "")
            condition_id = str(pos.condition_id or "")
            if not token_id or not condition_id:
                continue
            live_tokens.add(token_id)

            if token_id in self._firing:
                # Already submitted a SELL on this token; let
                # reconciliation close it before we consider re-emit.
                continue

            quote = await self._mds.get_quote_or_none(token_id)
            if quote is None or quote.best_bid is None or quote.best_ask is None:
                # Fail-closed: do not advance timer on missing/stale data.
                continue
            price = (quote.best_bid + quote.best_ask) / 2

            state = self._dwell.setdefault(token_id, _DwellState())
            if price >= cfg.threshold:
                if state.first_seen_above_ts is None:
                    state.first_seen_above_ts = now
                    logger.info(
                        "near_one_exit_threshold_arm token=%s price=%s threshold=%s",
                        token_id[:12],
                        price,
                        cfg.threshold,
                    )
                dwell_elapsed = now - state.first_seen_above_ts
                if dwell_elapsed >= cfg.dwell_seconds:
                    await self._fire(
                        condition_id=condition_id,
                        token_id=token_id,
                        size_tokens=float(pos.size_tokens),
                        live_price=float(price),
                        dwell_elapsed=dwell_elapsed,
                        slippage_bps=slippage_bps,
                    )
            else:
                if state.first_seen_above_ts is not None:
                    logger.info(
                        "near_one_exit_threshold_clear token=%s price=%s",
                        token_id[:12],
                        price,
                    )
                state.first_seen_above_ts = None

        # Drop dwell state for tokens no longer in the open-position set.
        # Firing set is cleared once the position closes (size goes to zero).
        stale_dwell = set(self._dwell) - live_tokens
        for tok in stale_dwell:
            self._dwell.pop(tok, None)
        stale_firing = self._firing - live_tokens
        for tok in stale_firing:
            self._firing.discard(tok)

    async def _fire(
        self,
        *,
        condition_id: str,
        token_id: str,
        size_tokens: float,
        live_price: float,
        dwell_elapsed: float,
        slippage_bps: int,
    ) -> None:
        # Pull venue min-size from cache; size_sell_order enforces both
        # min-size and held-shares clamps consistently with risk exits.
        tick = self._cache.get_tick(token_id)
        venue_min = float(tick.min_order_size) if tick else 0.0

        decision = size_sell_order(
            sell_tokens=size_tokens,
            held_size=size_tokens,
            live_price=live_price,
            market_min_size=venue_min,
        )
        if not decision.accept:
            logger.info(
                "near_one_exit_sized_skip token=%s reason=%s held=%s venue_min=%s",
                token_id[:12],
                decision.reason,
                size_tokens,
                venue_min,
            )
            return

        signal_id = f"near_one_exit:{condition_id}:{int(time.time())}"
        req = ExecutionRequest(
            user_id=self._funder,
            leader_id="near-one-exit",
            token_id=token_id,
            side="SELL",
            amount=decision.effective_size,
            max_slippage_bps=slippage_bps,
            reason="near_one_exit",
            signal_id=signal_id,
            condition_id=condition_id,
            recovery_metadata={
                "trigger_price": live_price,
                "dwell_seconds": float(dwell_elapsed),
                "threshold": str(self._config.near_one_exit.threshold),
                "trigger_size_tokens": size_tokens,
            },
        )
        self._firing.add(token_id)
        logger.info(
            "near_one_exit_fire token=%s condition=%s size=%s price=%s dwell=%.1fs",
            token_id[:12],
            condition_id[:12],
            decision.effective_size,
            live_price,
            dwell_elapsed,
        )
        try:
            result = await self._exec.execute(req)
        except Exception as exc:
            logger.warning(
                "near_one_exit_execute_error token=%s error=%r",
                token_id[:12],
                exc,
            )
            self._firing.discard(token_id)
            return

        if not result.accepted:
            # Allow re-arm after a venue-side reject (e.g. insufficient
            # balance after manual sell). Reservations release via the
            # normal SELL path.
            logger.info(
                "near_one_exit_rejected token=%s reason=%s error=%s",
                token_id[:12],
                getattr(result, "rejection_reason", "?"),
                getattr(result, "error", None),
            )
            self._firing.discard(token_id)
