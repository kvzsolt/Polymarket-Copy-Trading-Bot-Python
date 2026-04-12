"""Position manager — tracks open positions with advanced exit intelligence.

Exit priority (highest first):
1. Hard stop-loss
2. Near-max-price exit (95c default)
3. +70% profit lock (sell fraction + break-even trailing)
4. Staged take-profit (30%→25%, 50%→35%)
5. Trailing stop (activate after +20%, trail by 0.05)
6. Leader divergence (external trigger)
7. Legacy single take-profit
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from config.env import ENV
from config.settings import RiskSettings, load_settings
from utils.logger import get_logger

logger = get_logger("PositionManager")


def _parse_csv_floats(raw: str) -> list[float]:
    """Parse comma-separated float values."""
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


@dataclass
class PositionExitState:
    """Per-position state tracking for advanced exits."""
    # Trailing stop
    trailing_active: bool = False
    trailing_high_price: float = 0.0
    # Profit lock
    profit_lock_triggered: bool = False
    profit_lock_sold: bool = False
    # Staged TP — set of level indices already triggered
    staged_tp_triggered: set[int] = field(default_factory=set)
    # Near-max
    near_max_warn_done: bool = False
    # Leader divergence
    leader_divergence_pending: bool = False


@dataclass
class TrackedPosition:
    condition_id: str
    asset: str
    side: str                 # BUY
    entry_price: float
    size_tokens: float
    usdc_cost: float
    entry_time: float = field(default_factory=time.time)
    leader_wallet: str = ""
    event_slug: str = ""
    title: str = ""
    category: str = "unknown"
    peak_price: float = 0.0
    # Advanced exit state
    exit_state: PositionExitState = field(default_factory=PositionExitState)
    # Legacy compat fields
    profit_lock_armed: bool = False
    profit_lock_held_logged: bool = False
    near_max_warn_trim_done: bool = False
    # Filled when position is closed
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    exit_reason: Optional[str] = None
    realized_pnl: Optional[float] = None


class PositionManager:
    """In-memory position tracker with advanced exit logic."""

    def __init__(self, risk_settings: RiskSettings | None = None):
        self._positions: dict[str, TrackedPosition] = {}
        self._closed: list[TrackedPosition] = []

        # Load typed settings (caller can inject for testing)
        if risk_settings is None:
            risk_settings = load_settings(env=ENV).risk
        rs = risk_settings

        # Basic SL/TP — ratios already in [0,1], no /100 needed
        self._sl_pct = rs.stop_loss_ratio
        self._tp_pct = rs.take_profit_ratio
        self._enabled = ENV.get('ENABLE_RISK_MGMT', True)

        # Profit lock
        # There are 2 modes for backward compatibility:
        # - legacy (default when ENABLE_PROFIT_LOCK flag is absent): arm above trigger,
        #   then exit on fallback below trigger.
        # - advanced (when ENABLE_PROFIT_LOCK is present+true): immediately sell a
        #   fraction at trigger and trail the remainder.
        if "ENABLE_PROFIT_LOCK" in ENV:
            self._profit_lock_mode = "advanced" if bool(ENV.get("ENABLE_PROFIT_LOCK")) else "disabled"
        else:
            self._profit_lock_mode = "legacy"

        self._profit_lock_trigger = rs.profit_lock_trigger_ratio
        self._profit_lock_sell_fraction = rs.profit_lock_sell_ratio

        # Near-max exit
        self._enable_near_max = ENV.get('ENABLE_NEAR_MAX_EXIT', True)
        self._near_max_exit = rs.near_max_price_level
        self._near_max_warn = rs.near_max_warn_level
        self._near_max_warn_ratio = max(0.0, min(1.0, float(ENV.get("NEAR_MAX_PRICE_WARN_TRIM_RATIO", 0.50) or 0.50)))

        # Trailing stop (default OFF when flag is absent; ON when configured in ENV)
        self._enable_trailing = bool(ENV.get('ENABLE_TRAILING_STOP')) if "ENABLE_TRAILING_STOP" in ENV else False
        self._trailing_activation_pct = rs.trailing_trigger_ratio
        self._trailing_distance = rs.trailing_distance_ratio

        # Staged TP (default OFF when flag is absent; ON when configured in ENV)
        self._enable_staged_tp = bool(ENV.get('ENABLE_STAGED_TP')) if "ENABLE_STAGED_TP" in ENV else False
        self._staged_levels = _parse_csv_floats(str(ENV.get("STAGED_TP_LEVELS", "30,50")))
        self._staged_fractions = _parse_csv_floats(str(ENV.get("STAGED_TP_FRACTIONS", "0.25,0.35")))
        # Convert % levels to decimals if > 1
        self._staged_levels = [l / 100.0 if l > 1.0 else l for l in self._staged_levels]

        # Leader divergence (default OFF when flag is absent; ON when configured in ENV)
        self._enable_leader_divergence = bool(ENV.get('ENABLE_LEADER_DIVERGENCE_EXIT')) if "ENABLE_LEADER_DIVERGENCE_EXIT" in ENV else False

    @property
    def open_positions(self) -> dict[str, TrackedPosition]:
        return dict(self._positions)

    @property
    def closed_positions(self) -> list[TrackedPosition]:
        return list(self._closed)

    def track_buy(
        self,
        condition_id: str,
        asset: str,
        entry_price: float,
        size_tokens: float,
        usdc_cost: float,
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
        category: str = "unknown",
    ) -> TrackedPosition:
        """Register a new buy position for risk monitoring."""
        d_entry = Decimal(str(entry_price))
        d_size = Decimal(str(size_tokens))
        d_cost = Decimal(str(usdc_cost))

        pos = TrackedPosition(
            condition_id=condition_id,
            asset=asset,
            side="BUY",
            entry_price=entry_price,
            size_tokens=size_tokens,
            usdc_cost=usdc_cost,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
            category=category,
            peak_price=entry_price,
        )
        existing = self._positions.get(condition_id)
        if existing:
            total_tokens = Decimal(str(existing.size_tokens)) + d_size
            if total_tokens > 0:
                existing.entry_price = float(
                    (Decimal(str(existing.entry_price)) * Decimal(str(existing.size_tokens)) + d_entry * d_size)
                    / total_tokens
                )
            existing.size_tokens = float(total_tokens)
            existing.usdc_cost += usdc_cost
            existing.peak_price = max(existing.peak_price, entry_price)
            existing.category = category or existing.category
            logger.info(
                f"Position averaged: {condition_id[:12]}… "
                f"avg_entry={existing.entry_price:.4f} total_tokens={existing.size_tokens:.4f}"
            )
            return existing

        self._positions[condition_id] = pos
        logger.info(
            f"Position opened: {condition_id[:12]}… "
            f"entry={entry_price:.4f} tokens={size_tokens:.4f} cost=${usdc_cost:.2f}"
        )
        return pos

    def track_sell(
        self,
        condition_id: str,
        exit_price: float,
        size_tokens: float,
        reason: str = "leader_sell",
    ) -> Optional[TrackedPosition]:
        """Record a (partial or full) sell. Returns the position if fully closed."""
        pos = self._positions.get(condition_id)
        if not pos:
            return None

        # Clamp sell amount to held amount to prevent PnL inflation on oversells
        effective_size = min(Decimal(str(size_tokens)), Decimal(str(pos.size_tokens)))
        pos.size_tokens = float(Decimal(str(pos.size_tokens)) - effective_size)
        realized = float((Decimal(str(exit_price)) - Decimal(str(pos.entry_price))) * effective_size)
        if pos.realized_pnl is None:
            pos.realized_pnl = 0.0
        pos.realized_pnl += realized

        if pos.size_tokens <= 0.001:
            pos.exit_price = exit_price
            pos.exit_time = time.time()
            pos.exit_reason = reason
            pos.size_tokens = 0.0
            del self._positions[condition_id]
            self._closed.append(pos)
            logger.info(
                f"Position closed ({reason}): {condition_id[:12]}… "
                f"exit={exit_price:.4f} pnl=${pos.realized_pnl:.2f}"
            )
            return pos

        logger.info(
            f"Position partially sold: {condition_id[:12]}… "
            f"remaining={pos.size_tokens:.4f} partial_pnl=${realized:.2f}"
        )
        return None

    def signal_leader_divergence(self, condition_id: str) -> None:
        """External signal: the copied leader reduced or exited this position."""
        pos = self._positions.get(condition_id)
        if pos:
            pos.exit_state.leader_divergence_pending = True
            logger.info(f"🤝 Leader divergence signaled: {condition_id[:12]}…")

    def evaluate_risk_action(self, condition_id: str, current_price: float) -> Optional[dict]:
        """Return risk action dict with reason + size_fraction, or None.

        Follows exit priority:
        1. Hard stop-loss
        2. Near-max-price exit (95c)
        3. +70% profit lock
        4. Staged take-profit
        5. Trailing stop
        6. Leader divergence
        7. Legacy take-profit (only if staged TP disabled)
        """
        if not self._enabled:
            return None
        pos = self._positions.get(condition_id)
        if not pos or pos.entry_price <= 0:
            return None

        es = pos.exit_state
        pct_change = float((Decimal(str(current_price)) - Decimal(str(pos.entry_price))) / Decimal(str(pos.entry_price)))

        # Update peak price for trailing
        if current_price > pos.peak_price:
            pos.peak_price = current_price
        if current_price > es.trailing_high_price:
            es.trailing_high_price = current_price

        # ── Priority 1: HARD STOP-LOSS ──
        if pct_change <= -self._sl_pct:
            logger.warning(
                f"🛑 STOP-LOSS: {condition_id[:12]}… "
                f"entry={pos.entry_price:.4f} current={current_price:.4f} "
                f"change={pct_change*100:.1f}%"
            )
            return {"reason": "stop_loss", "size_fraction": 1.0}

        # ── Priority 2: NEAR-MAX-PRICE EXIT ──
        if self._enable_near_max and 0 < current_price <= 1.0:
            if current_price >= self._near_max_exit:
                logger.info(
                    f"🏁 NEAR-MAX full exit: {condition_id[:12]}… "
                    f"price={current_price:.4f} >= {self._near_max_exit}"
                )
                return {"reason": "near_max_full_exit", "size_fraction": 1.0}

            if (
                current_price >= self._near_max_warn
                and not es.near_max_warn_done
                and self._near_max_warn_ratio > 0
            ):
                es.near_max_warn_done = True
                logger.info(
                    f"✂️ NEAR-MAX warn trim: {condition_id[:12]}… "
                    f"price={current_price:.4f} >= {self._near_max_warn}"
                )
                return {"reason": "near_max_partial_trim", "size_fraction": self._near_max_warn_ratio}

        # ── Priority 3: PROFIT LOCK ──
        # Legacy mode: arm above trigger, exit on fallback below trigger.
        if self._profit_lock_mode == "legacy":
            if pct_change >= self._profit_lock_trigger and not pos.profit_lock_armed:
                pos.profit_lock_armed = True
                # do not exit immediately
                return None
            if pos.profit_lock_armed and pct_change >= self._profit_lock_trigger and not pos.profit_lock_held_logged:
                pos.profit_lock_held_logged = True
                return None
            if pos.profit_lock_armed and pct_change < self._profit_lock_trigger:
                # Fallback below trigger: exit fully.
                pos.profit_lock_armed = False
                return {"reason": "profit_lock_fallback", "size_fraction": 1.0}

        # Advanced mode: partial sell at trigger then trail remainder.
        if self._profit_lock_mode == "advanced" and pct_change >= self._profit_lock_trigger:
            if not es.profit_lock_sold:
                es.profit_lock_triggered = True
                es.profit_lock_sold = True
                # Activate trailing on remainder
                es.trailing_active = True
                es.trailing_high_price = current_price
                logger.info(
                    f"🔒 PROFIT LOCK: {condition_id[:12]}… "
                    f"profit={pct_change*100:.1f}% >= {self._profit_lock_trigger*100:.0f}% "
                    f"selling {self._profit_lock_sell_fraction*100:.0f}% + trailing remainder"
                )
                return {"reason": "profit_lock", "size_fraction": self._profit_lock_sell_fraction}

        # ── Priority 4: STAGED TAKE-PROFIT ──
        if self._enable_staged_tp and self._staged_levels and self._staged_fractions:
            for i, (level, fraction) in enumerate(zip(self._staged_levels, self._staged_fractions)):
                if i in es.staged_tp_triggered:
                    continue
                if pct_change >= level:
                    es.staged_tp_triggered.add(i)
                    # Activate trailing after first staged TP
                    if not es.trailing_active:
                        es.trailing_active = True
                        es.trailing_high_price = current_price
                    logger.info(
                        f"📊 STAGED TP level {i+1}: {condition_id[:12]}… "
                        f"profit={pct_change*100:.1f}% >= {level*100:.0f}% "
                        f"selling {fraction*100:.0f}%"
                    )
                    return {"reason": f"staged_tp_level_{i+1}", "size_fraction": fraction}

        # ── Priority 5: TRAILING STOP ──
        if self._enable_trailing:
            # Auto-activate trailing after activation threshold
            if not es.trailing_active and pct_change >= self._trailing_activation_pct:
                es.trailing_active = True
                es.trailing_high_price = current_price
                logger.info(
                    f"📈 TRAILING STOP activated: {condition_id[:12]}… "
                    f"profit={pct_change*100:.1f}% peak={current_price:.4f}"
                )

            if es.trailing_active and es.trailing_high_price > 0:
                trail_trigger = es.trailing_high_price - self._trailing_distance
                if current_price <= trail_trigger and current_price > pos.entry_price:
                    logger.info(
                        f"📉 TRAILING STOP triggered: {condition_id[:12]}… "
                        f"price={current_price:.4f} <= trail={trail_trigger:.4f} "
                        f"(peak={es.trailing_high_price:.4f} - {self._trailing_distance})"
                    )
                    return {"reason": "trailing_stop", "size_fraction": 1.0}

        # ── Priority 6: LEADER DIVERGENCE ──
        if self._enable_leader_divergence and es.leader_divergence_pending:
            es.leader_divergence_pending = False
            logger.info(
                f"🤝 LEADER DIVERGENCE exit: {condition_id[:12]}… "
                f"selling 50% (leader reduced/exited)"
            )
            return {"reason": "leader_divergence", "size_fraction": 0.50}

        # ── Priority 7: LEGACY TAKE-PROFIT (only if staged TP disabled) ──
        if not self._enable_staged_tp and pct_change >= self._tp_pct:
            logger.info(
                f"🎯 TAKE-PROFIT: {condition_id[:12]}… "
                f"profit={pct_change*100:.1f}%"
            )
            return {"reason": "take_profit", "size_fraction": 1.0}

        return None

    def check_risk_triggers(self, condition_id: str, current_price: float) -> Optional[str]:
        """Backwards-compatible risk trigger API (returns reason string only)."""
        action = self.evaluate_risk_action(condition_id, current_price)
        if not action:
            return None
        return str(action.get("reason"))

    def get_position(self, condition_id: str) -> Optional[TrackedPosition]:
        return self._positions.get(condition_id)

    def summary(self) -> dict:
        """Return portfolio summary stats."""
        total_invested = sum(p.usdc_cost for p in self._positions.values())
        total_realized = sum(p.realized_pnl or 0 for p in self._closed)
        wins = sum(1 for p in self._closed if (p.realized_pnl or 0) > 0)
        losses = sum(1 for p in self._closed if (p.realized_pnl or 0) <= 0)
        return {
            "open_positions": len(self._positions),
            "closed_positions": len(self._closed),
            "total_invested": round(total_invested, 2),
            "total_realized_pnl": round(total_realized, 2),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
        }


# Singleton instance
_manager: Optional[PositionManager] = None


def get_position_manager() -> PositionManager:
    global _manager
    if _manager is None:
        _manager = PositionManager()
    return _manager
