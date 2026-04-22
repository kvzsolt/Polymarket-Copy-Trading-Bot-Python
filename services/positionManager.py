"""Position manager — tracks open positions with advanced exit intelligence.

Exit priority (highest first):
0. Stop-loss (absolute price floor, suppressed inside grace period)
1. Near-max-price exit
2. Profit lock
3. Staged take-profit
4. Trailing stop
5. Leader divergence

Basic legacy single take-profit has been retired from the live risk system.
The remaining engines are the more context-aware exits.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from utils.logger import get_logger

logger = get_logger("PositionManager")


def _as_bool(value: object, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    return default


def _as_float(value: object, default: float) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except Exception:
        return default


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
    """In-memory position tracker with advanced exit logic.

    Canonical active-runtime identity is `(condition_id, asset)`.
    The internal map key is `condition_id::asset` so runtime state matches
    token-aware persistence and execution paths.

    Legacy condition-only lookups remain for compatibility, but only resolve
    when exactly one token-scoped position exists for that condition.
    """

    @staticmethod
    def _position_key(condition_id: str, asset: str) -> str:
        return f"{condition_id}::{asset}"

    @classmethod
    def _decode_position_key(cls, raw_key: str) -> tuple[str, str | None]:
        if "::" not in raw_key:
            return raw_key, None
        condition_id, asset = raw_key.split("::", 1)
        return condition_id, asset or None

    def _resolve_lookup_key(self, condition_id: str, asset: str | None = None) -> str | None:
        if asset:
            direct_key = self._position_key(condition_id, asset)
            if direct_key in self._positions:
                return direct_key
            return None

        if condition_id in self._positions:
            return condition_id

        matches = [
            key for key, pos in self._positions.items()
            if pos.condition_id == condition_id
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    def __init__(self, config: AppConfig | None = None, *, risk_settings=None):
        self._positions: dict[str, TrackedPosition] = {}
        self._closed: list[TrackedPosition] = []

        if config is not None:
            rc = config.risk
            fc = config.features

            self._risk_master_enabled = _as_bool(getattr(rc, "enable_risk_mgmt", True), True)

            # Stop-loss
            self._enable_stop_loss = _as_bool(getattr(fc, "enable_stop_loss", False), False)
            self._stop_loss_threshold = _as_float(getattr(rc, "stop_loss_threshold", 0.15), 0.15)
            self._stop_loss_grace_period_seconds = _as_float(
                getattr(rc, "stop_loss_grace_period_seconds", 180.0),
                180.0,
            )

            # Profit lock
            self._profit_lock_mode = (
                "advanced" if _as_bool(getattr(fc, "enable_profit_lock", False), False) else "disabled"
            )
            self._profit_lock_trigger = _as_float(getattr(rc, "profit_lock_trigger_pct", 70.0), 70.0) / 100.0
            self._profit_lock_sell_fraction = _as_float(getattr(rc, "profit_lock_sell_fraction", 0.5), 0.5)

            # Near-max exit
            self._enable_near_max = _as_bool(getattr(fc, "enable_near_max_exit", False), False)
            self._near_max_exit = _as_float(getattr(rc, "near_max_price_exit", 0.95), 0.95)
            self._near_max_warn = _as_float(getattr(rc, "near_max_price_warn", 0.92), 0.92)
            self._near_max_warn_ratio = max(
                0.0,
                min(1.0, _as_float(getattr(rc, "near_max_price_warn_trim_ratio", 0.5), 0.5)),
            )

            # Trailing stop
            self._enable_trailing = _as_bool(getattr(fc, "enable_trailing_stop", False), False)
            self._trailing_activation_pct = _as_float(getattr(rc, "trailing_activation_pct", 20.0), 20.0) / 100.0
            self._trailing_distance = _as_float(getattr(rc, "trailing_distance", 0.05), 0.05)

            # Staged TP
            self._enable_staged_tp = _as_bool(getattr(fc, "enable_staged_tp", False), False)
            staged_levels_raw = getattr(rc, "staged_tp_levels", "")
            staged_fractions_raw = getattr(rc, "staged_tp_fractions", "")
            self._staged_levels = _parse_csv_floats(staged_levels_raw) if staged_levels_raw else [0.30, 0.50]
            self._staged_fractions = _parse_csv_floats(staged_fractions_raw) if staged_fractions_raw else [0.25, 0.35]
            self._staged_levels = [l / 100.0 if l >= 100.0 else l for l in self._staged_levels]

            # Leader divergence
            self._enable_leader_divergence = _as_bool(getattr(fc, "enable_leader_divergence_exit", False), False)
            self._enabled = self._risk_master_enabled

        elif risk_settings is not None:
            # Legacy test-compatibility path: accept RiskSettings directly
            rs = risk_settings
            self._risk_master_enabled = True
            self._enable_stop_loss = False
            self._stop_loss_threshold = 0.15
            self._stop_loss_grace_period_seconds = 180.0
            self._profit_lock_mode = "legacy"
            self._profit_lock_trigger = rs.profit_lock_trigger_ratio
            self._profit_lock_sell_fraction = rs.profit_lock_sell_ratio
            self._enable_near_max = True
            self._near_max_exit = rs.near_max_price_level
            self._near_max_warn = rs.near_max_warn_level
            self._near_max_warn_ratio = 0.50
            self._enable_trailing = False
            self._trailing_activation_pct = rs.trailing_trigger_ratio
            self._trailing_distance = rs.trailing_distance_ratio
            self._enable_staged_tp = False
            self._staged_levels = [0.30, 0.50]
            self._staged_fractions = [0.25, 0.35]
            self._enable_leader_divergence = False
            self._enabled = self._risk_master_enabled

        else:
            # Bare defaults for backward compat
            self._risk_master_enabled = True
            self._enable_stop_loss = False
            self._stop_loss_threshold = 0.15
            self._stop_loss_grace_period_seconds = 180.0
            self._profit_lock_mode = "legacy"
            self._profit_lock_trigger = 0.70
            self._profit_lock_sell_fraction = 0.50
            self._enable_near_max = True
            self._near_max_exit = 0.95
            self._near_max_warn = 0.92
            self._near_max_warn_ratio = 0.50
            self._enable_trailing = False
            self._trailing_activation_pct = 0.20
            self._trailing_distance = 0.05
            self._enable_staged_tp = False
            self._staged_levels = [0.30, 0.50]
            self._staged_fractions = [0.25, 0.35]
            self._enable_leader_divergence = False
            self._enabled = self._risk_master_enabled

    def _risk_management_enabled(self) -> bool:
        return bool(getattr(self, "_risk_master_enabled", getattr(self, "_enabled", False)))

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
        key = self._position_key(condition_id, asset)
        existing = self._positions.get(key)
        if existing:
            total_tokens = Decimal(str(existing.size_tokens)) + d_size
            if total_tokens > 0:
                existing.entry_price = float(
                    (Decimal(str(existing.entry_price)) * Decimal(str(existing.size_tokens)) + d_entry * d_size)
                    / total_tokens
                )
            existing.size_tokens = float(total_tokens)
            existing.usdc_cost = float(Decimal(str(existing.usdc_cost)) + d_cost)
            existing.peak_price = max(existing.peak_price, entry_price)
            existing.category = category or existing.category
            logger.info(
                f"Position averaged: {condition_id[:12]}…/{asset[:12]} "
                f"avg_entry={existing.entry_price:.4f} total_tokens={existing.size_tokens:.4f}"
            )
            return existing

        self._positions[key] = pos
        logger.info(
            f"Position opened: {condition_id[:12]}…/{asset[:12]} "
            f"entry={entry_price:.4f} tokens={size_tokens:.4f} cost=${usdc_cost:.2f}"
        )
        return pos

    def track_sell(
        self,
        condition_id: str,
        asset_or_exit_price: str | float,
        exit_price_or_size_tokens: float,
        size_tokens: float | None = None,
        reason: str = "leader_sell",
    ) -> Optional[TrackedPosition]:
        """Record a (partial or full) sell. Returns the position if fully closed."""
        asset: str | None
        exit_price: float
        sell_size: float

        if isinstance(asset_or_exit_price, str):
            asset = asset_or_exit_price
            exit_price = float(exit_price_or_size_tokens)
            if size_tokens is None:
                raise ValueError(
                    "track_sell(condition_id, asset, exit_price, size_tokens, ...) requires size_tokens"
                )
            sell_size = float(size_tokens)
        else:
            asset = None
            exit_price = float(asset_or_exit_price)
            sell_size = float(exit_price_or_size_tokens)

        if sell_size <= 0:
            raise ValueError(
                f"track_sell requires positive size_tokens, got {sell_size} for condition_id={condition_id}"
            )

        resolved_key = self._resolve_lookup_key(condition_id, asset)
        if resolved_key is None:
            return None

        pos = self._positions.get(resolved_key)
        if not pos:
            return None

        # Clamp sell amount to held amount to prevent PnL inflation on oversells
        effective_size = min(Decimal(str(sell_size)), Decimal(str(pos.size_tokens)))
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
            del self._positions[resolved_key]
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

    def signal_leader_divergence(self, condition_id: str, asset: str | None = None) -> None:
        """External signal: the copied leader reduced or exited this position."""
        resolved_key = self._resolve_lookup_key(condition_id, asset)
        pos = self._positions.get(resolved_key) if resolved_key is not None else None
        if pos:
            pos.exit_state.leader_divergence_pending = True
            logger.info(f"🤝 Leader divergence signaled: {condition_id[:12]}…")

    def evaluate_risk_action(
        self,
        condition_id: str,
        asset_or_current_price: str | float,
        current_price: float | None = None,
    ) -> Optional[dict]:
        """Return risk action dict with reason + size_fraction, or None.

        Follows exit priority:
        1. Near-max-price exit
        2. Profit lock
        3. Staged take-profit
        4. Trailing stop
        5. Leader divergence
        """
        if not self._risk_management_enabled():
            return None

        if isinstance(asset_or_current_price, str):
            asset = asset_or_current_price
            current_price = float(current_price) if current_price is not None else None
        else:
            asset = None
            current_price = float(asset_or_current_price)

        if current_price is None:
            return None

        resolved_key = self._resolve_lookup_key(condition_id, asset)
        pos = self._positions.get(resolved_key) if resolved_key is not None else None
        if not pos or pos.entry_price <= 0:
            return None

        es = pos.exit_state
        pct_change = float((Decimal(str(current_price)) - Decimal(str(pos.entry_price))) / Decimal(str(pos.entry_price)))

        # Update peak price for trailing
        if current_price > pos.peak_price:
            pos.peak_price = current_price
        if current_price > es.trailing_high_price:
            es.trailing_high_price = current_price

        # ── Priority 0: STOP-LOSS (absolute price floor, grace-period protected) ──
        if getattr(self, "_enable_stop_loss", False):
            sl_threshold = getattr(self, "_stop_loss_threshold", 0.15)
            grace = getattr(self, "_stop_loss_grace_period_seconds", 180.0)
            age_seconds = time.time() - pos.entry_time
            if current_price <= sl_threshold and age_seconds > grace:
                logger.info(
                    f"STOP-LOSS triggered: {condition_id[:12]}… "
                    f"price={current_price:.4f} <= threshold={sl_threshold:.4f} "
                    f"age={age_seconds:.0f}s > grace={grace:.0f}s"
                )
                return {"reason": "stop_loss", "size_fraction": 1.0}

        # ── Priority 1: NEAR-MAX-PRICE EXIT ──
        if self._enable_near_max and current_price > 0:
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

        # ── Priority 2: PROFIT LOCK ──
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

        # ── Priority 3: STAGED TAKE-PROFIT ──
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

        # ── Priority 4: TRAILING STOP ──
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

        # ── Priority 5: LEADER DIVERGENCE ──
        if self._enable_leader_divergence and es.leader_divergence_pending:
            es.leader_divergence_pending = False
            logger.info(
                f"🤝 LEADER DIVERGENCE exit: {condition_id[:12]}… "
                f"selling 50% (leader reduced/exited)"
            )
            return {"reason": "leader_divergence", "size_fraction": 0.50}

        return None

    def check_risk_triggers(
        self,
        condition_id: str,
        asset_or_current_price: str | float,
        current_price: float | None = None,
    ) -> Optional[str]:
        """Backwards-compatible risk trigger API (returns reason string only)."""
        action = self.evaluate_risk_action(condition_id, asset_or_current_price, current_price)
        if not action:
            return None
        return str(action.get("reason"))

    def get_position(self, condition_id: str, asset: str | None = None) -> Optional[TrackedPosition]:
        resolved_key = self._resolve_lookup_key(condition_id, asset)
        if resolved_key is None:
            return None
        return self._positions.get(resolved_key)

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
