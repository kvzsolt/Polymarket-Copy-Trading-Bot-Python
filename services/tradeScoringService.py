from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import AppConfig


logger = get_logger("TradeScoringService")


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _to_seconds(ts: Any) -> float:
    try:
        value = float(ts or 0)
    except (TypeError, ValueError):
        return 0.0
    if value > 1e12:
        return value / 1000.0
    return value


@dataclass(frozen=True)
class TradeScore:
    score: float
    price_drift_pct: float
    edge_decay_pct: float
    age_seconds: float
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

    def __init__(self, config: AppConfig | None = None) -> None:
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
        live_price: float,
        leader_score: float,
    ) -> TradeScore:
        ref_price = float(trade.get("price", 0) or 0)
        if ref_price <= 0 or live_price <= 0:
            return TradeScore(
                score=0.0,
                price_drift_pct=1.0,
                edge_decay_pct=1.0,
                age_seconds=0.0,
                liquidity_score=0.0,
                hard_fail_reason="invalid_reference_or_live_price",
                details={"reference_price": ref_price, "live_price": live_price},
            )

        now = time.time()
        trade_ts = _to_seconds(trade.get("timestamp"))
        age_seconds = max(0.0, now - trade_ts) if trade_ts > 0 else 0.0
        max_age_seconds = float(self._max_age_seconds)
        age_factor = 1.0 if max_age_seconds <= 0 else _clamp(1.0 - (age_seconds / max_age_seconds))

        price_drift_pct = max(0.0, (live_price - ref_price) / ref_price)
        max_drift_pct = self._max_drift_pct

        initial_edge = max(0.0, 1.0 - ref_price)
        live_edge = max(0.0, 1.0 - live_price)
        if initial_edge <= 0:
            edge_decay_pct = 1.0
        else:
            edge_decay_pct = _clamp((initial_edge - live_edge) / initial_edge)
        max_edge_decay = self._max_edge_decay

        leader_notional = float(trade.get("usdcSize", 0) or 0)
        if leader_notional <= 0:
            size = float(trade.get("size", 0) or 0)
            leader_notional = size * ref_price
        liquidity_target = float(self._liquidity_target)
        liquidity_score = 1.0 if liquidity_target <= 0 else _clamp(leader_notional / liquidity_target)

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
        if price_drift_pct > max_drift_pct / 100.0:
            hard_fail_reason = "price_drift_exceeded"
        elif edge_decay_pct > max_edge_decay / 100.0:
            hard_fail_reason = "edge_decay_exceeded"
        elif trade_ts > 0 and max_age_seconds > 0 and age_seconds > max_age_seconds:
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
                and live_price >= near_res_price
                and trade_ts > 0
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
                "reference_price": ref_price,
                "live_price": live_price,
                "leader_notional": leader_notional,
                "max_age_seconds": max_age_seconds,
                "max_drift_pct": max_drift_pct,
                "max_edge_decay": max_edge_decay,
                "near_resolution_gate_enabled": near_res_enabled,
                "near_resolution_gate_price": near_res_price,
                "near_resolution_max_age_seconds": near_res_max_age,
            },
        )
