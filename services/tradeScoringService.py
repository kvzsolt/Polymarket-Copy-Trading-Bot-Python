"""Optional entry-quality scoring with explicit time and price evidence."""

from __future__ import annotations

import time
import math
from collections.abc import Callable
from decimal import Decimal

from domain.performance import PerformanceUnits
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import AppConfig


logger = get_logger("TradeScoringService")


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class TradeScore:
    score: float
    price_drift_pct: float
    edge_decay_pct: float
    age_seconds: float | None
    liquidity_score: float
    hard_fail_reason: str | None
    details: dict[str, Any]


class TradeScoringService:
    """Score trade quality before copy execution.

    Production code MUST pass typed ``AppConfig``. The ``config=None``
    fallback exists only for legacy test fixtures that still construct
    the service without DI; it loads the same scoring constants from
    ``env_mod.ENV``. Hitting that branch in a live process emits a
    once-per-process WARNING so operators see the misconfigured
    bootstrap in logs rather than silently running on env defaults.
    """

    def __init__(
        self, config: AppConfig | None = None, *, clock: Callable[[], float] = time.time
    ) -> None:
        """Bind the existing quality policy and a deterministic wall clock."""
        self._clock = clock
        if config is None:
            from config.app_config import load_config

            config = load_config()
        self._config = config
        self._max_age_seconds = config.copy_trading.copy_max_signal_age_seconds
        self._max_drift_pct = config.copy_trading.copy_max_price_drift_pct
        self._max_edge_decay = config.copy_trading.copy_max_edge_decay_pct
        self._liquidity_target = config.copy_trading.copy_liquidity_target_usdc
        self._near_res_enabled = config.execution.enable_near_resolution_gate
        self._near_res_price = config.execution.near_resolution_gate_price
        self._near_res_max_age = config.execution.near_resolution_max_age_seconds

    def score_buy_trade(
        self,
        trade: dict,
        *,
        live_price: Decimal | float,
        leader_score: float,
    ) -> TradeScore:
        """Score a BUY; legacy edge/liquidity fields mean upside decay and notional strength."""
        try:
            reference = PerformanceUnits.finite(trade.get("price"))
            executable = PerformanceUnits.finite(live_price)
        except ValueError:
            reference, executable = None, None
        if (
            reference is None
            or executable is None
            or not Decimal(0) < reference < Decimal(1)
            or not Decimal(0) < executable < Decimal(1)
        ):
            return TradeScore(
                score=0.0,
                price_drift_pct=1.0,
                edge_decay_pct=1.0,
                age_seconds=0.0,
                liquidity_score=0.0,
                hard_fail_reason="invalid_reference_or_live_price",
                details={"reference_price": str(reference), "live_price": str(executable)},
            )

        now = self._clock()
        trade_ts = self._timestamp_seconds(trade.get("timestamp"))
        age_seconds = now - trade_ts if trade_ts is not None else None
        max_age_seconds = float(self._max_age_seconds)
        age_factor = 0.0
        if age_seconds is not None and age_seconds >= 0:
            age_factor = 1.0 if max_age_seconds <= 0 else _clamp(1 - age_seconds / max_age_seconds)

        price_drift_pct = float(max(Decimal(0), (executable - reference) / reference))
        max_drift_pct = self._max_drift_pct
        initial_upside = Decimal(1) - reference
        remaining_upside = Decimal(1) - executable
        edge_decay_pct = _clamp(float((initial_upside - remaining_upside) / initial_upside))
        max_edge_decay = self._max_edge_decay

        leader_notional = PerformanceUnits.finite(trade.get("usdcSize"))
        if leader_notional is None or leader_notional <= 0:
            shares = PerformanceUnits.finite(trade.get("size"))
            leader_notional = (shares or Decimal(0)) * reference
        notional_target = Decimal(str(self._liquidity_target))
        liquidity_score = (
            1.0 if notional_target <= 0 else _clamp(float(leader_notional / notional_target))
        )

        drift_penalty = _clamp(price_drift_pct / max(max_drift_pct / 100.0, 1e-9))
        edge_penalty = _clamp(edge_decay_pct / max(max_edge_decay / 100.0, 1e-9))
        quality = (
            leader_score
            * (1.0 - 0.55 * drift_penalty)
            * (1.0 - 0.35 * edge_penalty)
            * (0.60 + 0.40 * age_factor)
            * (0.65 + 0.35 * liquidity_score)
        )
        quality = _clamp(quality)

        near_res_enabled = self._near_res_enabled
        near_res_price = self._near_res_price
        near_res_max_age = self._near_res_max_age

        hard_fail_reason = None
        if age_seconds is None:
            hard_fail_reason = "signal_age_unknown"
        elif age_seconds < 0:
            hard_fail_reason = "signal_timestamp_future"
        elif price_drift_pct > max_drift_pct / 100.0:
            hard_fail_reason = "price_drift_exceeded"
        elif edge_decay_pct > max_edge_decay / 100.0:
            hard_fail_reason = "edge_decay_exceeded"
        elif max_age_seconds > 0 and age_seconds > max_age_seconds:
            hard_fail_reason = "signal_too_old"
        else:
            # Near-resolution gate: if the live price is very close to 1.0 (market
            # about to resolve YES), only allow the signal if it is very fresh.
            # A leader's last-second bet at 0.95+ cannot be safely copied because
            # the market may resolve before our order fills.
            # Set ENABLE_NEAR_RESOLUTION_GATE=false (or NEAR_RESOLUTION_GATE_PRICE=0)
            # to disable this gate.
            if (
                near_res_enabled
                and near_res_price > 0
                and executable >= Decimal(str(near_res_price))
                and age_seconds is not None
                and age_seconds > near_res_max_age
            ):
                hard_fail_reason = "near_resolution_too_stale"

        return TradeScore(
            score=quality,
            price_drift_pct=price_drift_pct,
            edge_decay_pct=edge_decay_pct,
            age_seconds=age_seconds,
            liquidity_score=liquidity_score,
            hard_fail_reason=hard_fail_reason,
            details={
                "reference_price": str(reference),
                "live_price": str(executable),
                "leader_notional": str(leader_notional),
                "upside_decay_fraction": edge_decay_pct,
                "leader_notional_strength": liquidity_score,
                "max_age_seconds": max_age_seconds,
                "max_drift_pct": max_drift_pct,
                "max_edge_decay": max_edge_decay,
                "near_resolution_gate_enabled": near_res_enabled,
                "near_resolution_gate_price": near_res_price,
                "near_resolution_max_age_seconds": near_res_max_age,
            },
        )

    @staticmethod
    def _timestamp_seconds(value: object) -> float | None:
        if isinstance(value, bool) or value is None:
            return None
        try:
            timestamp = float(str(value))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(timestamp) or timestamp <= 0:
            return None
        return timestamp / 1000 if timestamp > 1e12 else timestamp
