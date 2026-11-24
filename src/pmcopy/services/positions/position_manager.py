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
    from pmcopy.config.app_config import AppConfig

from pmcopy.utils.logger import get_logger

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


def _as_pct_ratio(value: object, default: float) -> float:
    raw = _as_float(value, default)
    ratio = raw / 100.0 if raw > 1.0 else raw
    return max(0.0, min(1.0, ratio))


def _to_dec(value: object, default: Decimal = Decimal("0")) -> Decimal:
    """Normalise any numeric input to Decimal at the position-manager boundary.

    CLAUDE.md non-negotiable: position state must not flow through float
    arithmetic. Callers may still pass float / int from legacy paths;
    we convert via ``str()`` to avoid IEEE-754 representation noise.

    Review fix (cubic / greptile P1 on PR #110): on conversion failure we
    now raise ``InvalidOperation`` instead of silently returning the
    default. The prior broad ``except Exception: return default`` was
    fail-open — a non-numeric ``entry_price`` slid through as
    ``Decimal("0")`` and then either crashed the risk monitor with
    ``DivisionByZero`` or produced a full-exit-price × size PnL on the
    next sell. ``None`` is still treated as "no value" and returns the
    default (kept for the evaluate_risk_action ``current_price=None``
    short-circuit). Anything else that is not coercible to Decimal must
    fail at the call site.
    """
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal(int(value))
    # ``Decimal(str(value))`` raises ``decimal.InvalidOperation`` (a
    # subclass of ArithmeticError) on garbage — let it propagate.
    return Decimal(str(value))


def _parse_csv_floats(raw: str) -> list[float]:
    """Parse comma-separated float values."""
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


def _parse_staged_tp_levels(raw: str) -> list[float]:
    """Parse staged TP levels, preserving legacy integer-percent syntax."""
    levels: list[float] = []
    for token in (x.strip() for x in raw.split(",") if x.strip()):
        level = float(token)
        if "." not in token and "e" not in token.lower() and level > 1.0:
            level /= 100.0
        levels.append(level)
    return levels


@dataclass
class PositionExitState:
    """Per-position state tracking for advanced exits."""

    # Trailing stop
    trailing_active: bool = False
    trailing_high_price: Decimal = field(default_factory=lambda: Decimal("0"))
    # Profit lock
    profit_lock_triggered: bool = False
    profit_lock_sold: bool = False
    # Staged TP — set of level indices already triggered
    staged_tp_triggered: set[int] = field(default_factory=set)
    # Near-max
    near_max_warn_done: bool = False
    # Leader divergence
    leader_divergence_pending: bool = False
    # Steady-state full-exit log de-dup. The risk loop re-evaluates every
    # ~0.25s; a position parked at/above near-max (or under stop-loss / trailing
    # trigger) would otherwise emit the same INFO line every tick. We log INFO
    # once per state transition and demote repeats to DEBUG. A key is discarded
    # when its branch condition clears so a genuine re-entry logs again.
    logged_steady_exits: set[str] = field(default_factory=set)


@dataclass
class TrackedPosition:
    condition_id: str
    asset: str
    side: str  # BUY
    entry_price: Decimal
    size_tokens: Decimal
    usdc_cost: Decimal
    entry_time: float = field(default_factory=time.time)
    leader_wallet: str = ""
    event_slug: str = ""
    title: str = ""
    category: str = "unknown"
    peak_price: Decimal = field(default_factory=lambda: Decimal("0"))
    # Advanced exit state
    exit_state: PositionExitState = field(default_factory=PositionExitState)
    # Legacy compat fields
    profit_lock_armed: bool = False
    profit_lock_held_logged: bool = False
    near_max_warn_trim_done: bool = False
    # Filled when position is closed
    exit_price: Optional[Decimal] = None
    exit_time: Optional[float] = None
    exit_reason: Optional[str] = None
    realized_pnl: Optional[Decimal] = None


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

        matches = [key for key, pos in self._positions.items() if pos.condition_id == condition_id]
        if len(matches) == 1:
            return matches[0]
        return None

    def __init__(self, config: AppConfig | None = None):
        self._positions: dict[str, TrackedPosition] = {}
        self._closed: list[TrackedPosition] = []

        if config is not None:
            rc = config.risk
            # Exit-lifecycle thresholds and feature switches are owned by the
            # protective_exits service section (formerly risk.* + features.*).
            pe = config.protective_exits
            fc = pe

            self._risk_master_enabled = _as_bool(getattr(rc, "enable_risk_mgmt", True), True)

            # Stop-loss
            self._enable_stop_loss = _as_bool(getattr(fc, "enable_stop_loss", False), False)
            self._stop_loss_threshold = _as_float(getattr(pe, "stop_loss_threshold", 0.15), 0.15)
            self._stop_loss_grace_period_seconds = _as_float(
                getattr(pe, "stop_loss_grace_period_seconds", 180.0),
                180.0,
            )

            # Profit lock
            self._profit_lock_mode = (
                "advanced"
                if _as_bool(getattr(fc, "enable_profit_lock", False), False)
                else "disabled"
            )
            self._profit_lock_trigger = (
                _as_float(getattr(pe, "profit_lock_trigger_pct", 70.0), 70.0) / 100.0
            )
            self._profit_lock_sell_fraction = _as_float(
                getattr(pe, "profit_lock_sell_fraction", 0.5), 0.5
            )

            # Near-max exit
            self._enable_near_max = _as_bool(getattr(fc, "enable_near_max_exit", False), False)
            self._near_max_exit = _as_float(getattr(pe, "near_max_price_exit", 0.95), 0.95)
            self._near_max_warn = _as_float(getattr(pe, "near_max_price_warn", 0.92), 0.92)
            self._near_max_warn_ratio = max(
                0.0,
                min(1.0, _as_float(getattr(pe, "near_max_price_warn_trim_ratio", 0.5), 0.5)),
            )
            # Phase 2.8 — min-hold grace for the near-max partial trim. A
            # fresh BUY at the ask with the risk mark at the best bid satisfies
            # the trim condition at the instant of the fill, selling back
            # across the spread for an instant loss. The full exit at the exit
            # threshold is NOT grace-protected (it caps a position that has
            # already reached near-resolution). 0 disables the grace.
            self._near_max_grace_period_seconds = _as_float(
                getattr(pe, "near_max_grace_period_seconds", 0.0), 0.0
            )

            # Trailing stop
            self._enable_trailing = _as_bool(getattr(fc, "enable_trailing_stop", False), False)
            trailing_activation_raw = getattr(pe, "trailing_activation_pct", 20.0)
            self._trailing_activation_auto = trailing_activation_raw is None or (
                isinstance(trailing_activation_raw, str)
                and trailing_activation_raw.strip().lower() in ("", "auto")
            )
            self._trailing_activation_pct = (
                0.0
                if self._trailing_activation_auto
                else _as_float(trailing_activation_raw, 20.0) / 100.0
            )
            self._trailing_distance = _as_pct_ratio(getattr(pe, "trailing_distance", 0.05), 0.05)

            # Staged TP
            self._enable_staged_tp = _as_bool(getattr(fc, "enable_staged_tp", False), False)
            staged_levels_raw = getattr(pe, "staged_tp_levels", "")
            staged_fractions_raw = getattr(pe, "staged_tp_fractions", "")
            self._staged_levels = (
                _parse_staged_tp_levels(staged_levels_raw) if staged_levels_raw else [0.30, 0.50]
            )
            self._staged_fractions = (
                _parse_csv_floats(staged_fractions_raw) if staged_fractions_raw else [0.25, 0.35]
            )

            # Leader divergence
            self._enable_leader_divergence = _as_bool(
                getattr(fc, "enable_leader_divergence_exit", False), False
            )
            self._enabled = self._risk_master_enabled

        else:
            # Bare defaults for tests that build a PositionManager() without
            # a typed config. Profit-lock evaluation requires an explicit
            # ``enable_profit_lock`` opt-in via AppConfig.features, so the
            # no-config path leaves it disabled. The legacy "armed-then-
            # retrace" profit-lock strategy was removed as part of the
            # nasty-rework slice (issue #77); the trailing-stop priority
            # (Priority 4 below) covers the same exit-on-retrace use case
            # when configured via AppConfig.
            self._risk_master_enabled = True
            self._enable_stop_loss = False
            self._stop_loss_threshold = 0.15
            self._stop_loss_grace_period_seconds = 180.0
            self._profit_lock_mode = "disabled"
            self._profit_lock_trigger = 0.70
            self._profit_lock_sell_fraction = 0.50
            self._enable_near_max = True
            self._near_max_exit = 0.95
            self._near_max_warn = 0.92
            self._near_max_warn_ratio = 0.50
            self._enable_trailing = False
            self._trailing_activation_auto = False
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

    def retain_active_positions(self, keys: set[tuple[str, str]]) -> None:
        """Remove tracking outside authoritative active inventory without inventing fills.

        Args:
            keys: Complete set of active condition and outcome identities.
        """
        self._positions = {
            key: position
            for key, position in self._positions.items()
            if (position.condition_id, position.asset) in keys
        }

    def track_buy(
        self,
        condition_id: str,
        asset: str,
        entry_price,
        size_tokens,
        usdc_cost,
        leader_wallet: str = "",
        event_slug: str = "",
        title: str = "",
        category: str = "unknown",
    ) -> TrackedPosition:
        """Register a new buy position for risk monitoring.

        Money-shaped inputs (``entry_price``, ``size_tokens``, ``usdc_cost``)
        accept any numeric type; they are normalised to ``Decimal`` at this
        boundary so the position store never holds float-typed money.
        """
        d_entry = _to_dec(entry_price)
        d_size = _to_dec(size_tokens)
        d_cost = _to_dec(usdc_cost)
        # Defense-in-depth (cubic P1 on PR #110): even after the broad
        # except removal in ``_to_dec`` above, a caller passing literal
        # zero or a negative slips through as a valid Decimal. A zero
        # ``entry_price`` then crashes ``evaluate_risk_action`` via
        # ``pct_change = (cp - entry) / entry`` (DivisionByZero) and a
        # zero ``size_tokens`` poisons ``track_sell``'s ``effective_size``
        # clamp. Fail closed here.
        if d_entry <= 0 or d_size <= 0 or d_cost <= 0:
            raise ValueError(
                "track_buy requires positive entry_price/size_tokens/usdc_cost, "
                f"got entry={entry_price!r}, size={size_tokens!r}, cost={usdc_cost!r}"
            )

        pos = TrackedPosition(
            condition_id=condition_id,
            asset=asset,
            side="BUY",
            entry_price=d_entry,
            size_tokens=d_size,
            usdc_cost=d_cost,
            leader_wallet=leader_wallet,
            event_slug=event_slug,
            title=title,
            category=category,
            peak_price=d_entry,
        )
        key = self._position_key(condition_id, asset)
        existing = self._positions.get(key)
        if existing:
            total_tokens = existing.size_tokens + d_size
            if total_tokens > 0:
                existing.entry_price = (
                    existing.entry_price * existing.size_tokens + d_entry * d_size
                ) / total_tokens
            existing.size_tokens = total_tokens
            existing.usdc_cost = existing.usdc_cost + d_cost
            existing.peak_price = max(existing.peak_price, d_entry)
            existing.category = category or existing.category
            logger.info(
                f"Position averaged: {condition_id[:12]}…/{asset[:12]} "
                f"avg_entry={float(existing.entry_price):.4f} "
                f"total_tokens={float(existing.size_tokens):.4f}"
            )
            return existing

        self._positions[key] = pos
        logger.info(
            f"Position opened: {condition_id[:12]}…/{asset[:12]} "
            f"entry={float(d_entry):.4f} tokens={float(d_size):.4f} "
            f"cost=${float(d_cost):.2f}"
        )
        return pos

    def track_sell(
        self,
        condition_id: str,
        exit_price,
        size_tokens,
        *,
        asset: str | None = None,
        reason: str = "leader_sell",
    ) -> Optional[TrackedPosition]:
        """Record a (partial or full) sell. Returns the position if fully closed.

        Pass ``asset=<token_id>`` to disambiguate when multiple positions share
        the same ``condition_id`` (binary-outcome markets). Without ``asset``
        the manager falls back to the first matching key — fine for tests and
        single-token markets.
        """
        # NOTE: This used to dispatch on ``isinstance(asset_or_exit_price, str)``
        # which silently routed any unexpected type to the wrong path. The
        # keyword-only ``asset`` parameter makes the asset-aware form
        # unambiguous. (Greptile P2 PR #63.)
        sell_size = _to_dec(size_tokens)
        exit_price_d = _to_dec(exit_price)

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
        effective_size = min(sell_size, pos.size_tokens)
        pos.size_tokens = pos.size_tokens - effective_size
        realized = (exit_price_d - pos.entry_price) * effective_size
        if pos.realized_pnl is None:
            pos.realized_pnl = Decimal("0")
        pos.realized_pnl += realized

        # 0.001 token == residual dust threshold; matches the pre-Decimal
        # comparison in the float path.
        if pos.size_tokens <= Decimal("0.001"):
            pos.exit_price = exit_price_d
            pos.exit_time = time.time()
            pos.exit_reason = reason
            pos.size_tokens = Decimal("0")
            del self._positions[resolved_key]
            self._closed.append(pos)
            logger.info(
                f"Position closed ({reason}): {condition_id[:12]}… "
                f"exit={float(exit_price_d):.4f} pnl=${float(pos.realized_pnl):.2f}"
            )
            return pos

        logger.info(
            f"Position partially sold: {condition_id[:12]}… "
            f"remaining={float(pos.size_tokens):.4f} partial_pnl=${float(realized):.2f}"
        )
        return None

    def signal_leader_divergence(self, condition_id: str, asset: str | None = None) -> None:
        """External signal: the copied leader reduced or exited this position."""
        resolved_key = self._resolve_lookup_key(condition_id, asset)
        pos = self._positions.get(resolved_key) if resolved_key is not None else None
        if pos:
            pos.exit_state.leader_divergence_pending = True
            logger.info(f"🤝 Leader divergence signaled: {condition_id[:12]}…")

    @staticmethod
    def _log_steady_exit(es: "PositionExitState", key: str, message: str) -> None:
        """Log a steady-state full-exit trigger at INFO once per transition.

        Repeats (the risk loop re-evaluates every tick while the position stays
        parked at/above the exit threshold) drop to DEBUG so the operator sees
        the trigger once, not hundreds of identical lines. ``_clear_steady_exit``
        resets the key when the branch condition clears.
        """
        if key in es.logged_steady_exits:
            logger.debug(message)
        else:
            es.logged_steady_exits.add(key)
            logger.info(message)

    @staticmethod
    def _clear_steady_exit(es: "PositionExitState", key: str) -> None:
        es.logged_steady_exits.discard(key)

    def _trailing_activation_threshold(self, pos: TrackedPosition) -> Decimal:
        if not getattr(self, "_trailing_activation_auto", False):
            return _to_dec(getattr(self, "_trailing_activation_pct", 0.20))

        # Polymarket winning outcome tokens settle at 1 USDC per token. The
        # size term cancels out, so the activation scales by payout multiple.
        max_payout_multiple = Decimal("1") / pos.entry_price
        activation_value_multiple = Decimal("0.68") * max_payout_multiple + Decimal("0.28")
        activation_value_multiple = max(Decimal("1"), activation_value_multiple)
        return activation_value_multiple - Decimal("1")

    def evaluate_risk_action(
        self,
        condition_id: str,
        asset_or_current_price,
        current_price=None,
    ) -> Optional[dict]:
        """Return risk action dict with reason + size_fraction, or None.

        Follows exit priority:
        1. Near-max-price exit
        2. Profit lock
        3. Staged take-profit
        4. Trailing stop
        5. Leader divergence

        ``current_price`` accepts any numeric (float / int / Decimal) and is
        normalised to ``Decimal`` at the boundary. All internal money
        arithmetic stays in ``Decimal``; config thresholds are wrapped via
        ``_to_dec`` at each comparison so the no-config defaults stay
        float-typed without polluting downstream state.
        """
        if not self._risk_management_enabled():
            return None

        if isinstance(asset_or_current_price, str):
            asset = asset_or_current_price
            cp = _to_dec(current_price) if current_price is not None else None
        else:
            asset = None
            cp = _to_dec(asset_or_current_price)

        if cp is None:
            return None

        resolved_key = self._resolve_lookup_key(condition_id, asset)
        pos = self._positions.get(resolved_key) if resolved_key is not None else None
        if not pos or pos.entry_price <= 0:
            return None

        es = pos.exit_state
        pct_change = (cp - pos.entry_price) / pos.entry_price

        # Update peak price for trailing
        if cp > pos.peak_price:
            pos.peak_price = cp
        if cp > es.trailing_high_price:
            es.trailing_high_price = cp

        # ── Priority 0: STOP-LOSS (absolute price floor, grace-period protected) ──
        if getattr(self, "_enable_stop_loss", False):
            sl_threshold = _to_dec(getattr(self, "_stop_loss_threshold", 0.15))
            grace = getattr(self, "_stop_loss_grace_period_seconds", 180.0)
            age_seconds = time.time() - pos.entry_time
            if cp <= sl_threshold and age_seconds > grace:
                self._log_steady_exit(
                    es,
                    "stop_loss",
                    f"STOP-LOSS triggered: {condition_id[:12]}… "
                    f"price={float(cp):.4f} <= threshold={float(sl_threshold):.4f} "
                    f"age={age_seconds:.0f}s > grace={grace:.0f}s",
                )
                return {"reason": "stop_loss", "size_fraction": 1.0}
            self._clear_steady_exit(es, "stop_loss")

        # ── Priority 1: NEAR-MAX-PRICE EXIT ──
        if self._enable_near_max and cp > 0:
            near_max_exit_d = _to_dec(self._near_max_exit)
            near_max_warn_d = _to_dec(self._near_max_warn)
            if cp >= near_max_exit_d:
                self._log_steady_exit(
                    es,
                    "near_max_full_exit",
                    f"🏁 NEAR-MAX full exit: {condition_id[:12]}… "
                    f"price={float(cp):.4f} >= {self._near_max_exit}",
                )
                return {"reason": "near_max_full_exit", "size_fraction": 1.0}
            self._clear_steady_exit(es, "near_max_full_exit")

            if (
                cp >= near_max_warn_d
                and not es.near_max_warn_done
                and self._near_max_warn_ratio > 0
            ):
                # Phase 2.8 — grace-protect the partial trim so a freshly
                # entered position is not trimmed before it settles (the
                # bid/ask spread asymmetry would sell back across the spread
                # for an instant loss). The full exit above is NOT grace-
                # protected. 0 grace disables the check (legacy behavior).
                grace = getattr(self, "_near_max_grace_period_seconds", 0.0) or 0.0
                age_seconds = time.time() - pos.entry_time
                if grace > 0 and age_seconds <= grace:
                    self._clear_steady_exit(es, "near_max_partial_trim")
                    # Fall through to lower-priority exits (the position is
                    # still held; trailing/profit-lock may still act).
                else:
                    es.near_max_warn_done = True
                    logger.info(
                        f"✂️ NEAR-MAX warn trim: {condition_id[:12]}… "
                        f"price={float(cp):.4f} >= {self._near_max_warn}"
                    )
                    return {
                        "reason": "near_max_partial_trim",
                        "size_fraction": self._near_max_warn_ratio,
                    }

        # ── Priority 2: PROFIT LOCK ──
        # Advanced mode: partial sell at trigger then trail remainder.
        # (The historical "legacy" armed-then-retrace strategy was removed in
        # issue #77; the trailing-stop priority below covers exit-on-retrace.)
        if self._profit_lock_mode == "advanced" and pct_change >= _to_dec(
            self._profit_lock_trigger
        ):
            if not es.profit_lock_sold:
                es.profit_lock_triggered = True
                es.profit_lock_sold = True
                # Activate trailing on remainder
                es.trailing_active = True
                es.trailing_high_price = cp
                logger.info(
                    f"🔒 PROFIT LOCK: {condition_id[:12]}… "
                    f"profit={float(pct_change) * 100:.1f}% >= {self._profit_lock_trigger * 100:.0f}% "
                    f"selling {self._profit_lock_sell_fraction * 100:.0f}% + trailing remainder"
                )
                return {"reason": "profit_lock", "size_fraction": self._profit_lock_sell_fraction}

        # ── Priority 3: STAGED TAKE-PROFIT ──
        if self._enable_staged_tp and self._staged_levels and self._staged_fractions:
            # strict=False preserved: operator config may supply unequal-length
            # level/fraction lists; truncation to the shorter is long-standing behavior.
            for i, (level, fraction) in enumerate(
                zip(self._staged_levels, self._staged_fractions, strict=False)
            ):
                if i in es.staged_tp_triggered:
                    continue
                if pct_change >= _to_dec(level):
                    es.staged_tp_triggered.add(i)
                    # Activate trailing after first staged TP
                    if not es.trailing_active:
                        es.trailing_active = True
                        es.trailing_high_price = cp
                    logger.info(
                        f"📊 STAGED TP level {i + 1}: {condition_id[:12]}… "
                        f"profit={float(pct_change) * 100:.1f}% >= {level * 100:.0f}% "
                        f"selling {fraction * 100:.0f}%"
                    )
                    return {"reason": f"staged_tp_level_{i + 1}", "size_fraction": fraction}

        # ── Priority 4: TRAILING STOP ──
        if self._enable_trailing:
            # Auto-activate trailing after activation threshold
            activation_threshold = self._trailing_activation_threshold(pos)
            if not es.trailing_active and pct_change >= activation_threshold:
                es.trailing_active = True
                es.trailing_high_price = cp
                activation_mode = (
                    "dynamic" if getattr(self, "_trailing_activation_auto", False) else "fixed"
                )
                logger.info(
                    f"📈 TRAILING STOP activated: {condition_id[:12]}… "
                    f"profit={float(pct_change) * 100:.1f}% >= {float(activation_threshold) * 100:.1f}% "
                    f"({activation_mode}) peak={float(cp):.4f}"
                )

            if es.trailing_active and es.trailing_high_price > 0:
                trail_distance_d = _to_dec(self._trailing_distance)
                trail_trigger = es.trailing_high_price * (Decimal("1") - trail_distance_d)
                # P1-e: fire on the retrace from the in-profit peak regardless
                # of entry. The old ``and cp > pos.entry_price`` clause silently
                # suppressed the trailing stop once price fell through entry,
                # letting a position round-trip the whole move (and then some)
                # with only the opt-in absolute stop-loss as a backstop.
                if cp <= trail_trigger:
                    self._log_steady_exit(
                        es,
                        "trailing_stop",
                        f"📉 TRAILING STOP triggered: {condition_id[:12]}… "
                        f"price={float(cp):.4f} <= trail={float(trail_trigger):.4f} "
                        f"(peak={float(es.trailing_high_price):.4f}, distance={self._trailing_distance * 100:.2f}%)",
                    )
                    return {"reason": "trailing_stop", "size_fraction": 1.0}
                self._clear_steady_exit(es, "trailing_stop")

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

    def get_position(
        self, condition_id: str, asset: str | None = None
    ) -> Optional[TrackedPosition]:
        resolved_key = self._resolve_lookup_key(condition_id, asset)
        if resolved_key is None:
            return None
        return self._positions.get(resolved_key)

    def summary(self) -> dict:
        """Return portfolio summary stats."""
        total_invested = sum(
            (p.usdc_cost for p in self._positions.values()),
            start=Decimal("0"),
        )
        total_realized = sum(
            ((p.realized_pnl or Decimal("0")) for p in self._closed),
            start=Decimal("0"),
        )
        wins = sum(1 for p in self._closed if (p.realized_pnl or Decimal("0")) > 0)
        losses = sum(1 for p in self._closed if (p.realized_pnl or Decimal("0")) <= 0)
        # Truncate to 2dp via quantize so the dict value stays Decimal —
        # callers that need a float for display can cast at the boundary.
        return {
            "open_positions": len(self._positions),
            "closed_positions": len(self._closed),
            "total_invested": total_invested.quantize(Decimal("0.01")),
            "total_realized_pnl": total_realized.quantize(Decimal("0.01")),
            "wins": wins,
            "losses": losses,
            "win_rate": round(wins / max(wins + losses, 1) * 100, 1),
        }
