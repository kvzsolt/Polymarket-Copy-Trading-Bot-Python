from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from config.env import ENV


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
    """Score trade quality before copy execution."""

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
        max_age_seconds = float(ENV.get("COPY_MAX_SIGNAL_AGE_SECONDS", 120) or 120)
        age_factor = 1.0 if max_age_seconds <= 0 else _clamp(1.0 - (age_seconds / max_age_seconds))

        price_drift_pct = max(0.0, (live_price - ref_price) / ref_price)
        max_drift_pct = float(ENV.get("COPY_MAX_PRICE_DRIFT_PCT", 6.0) or 6.0) / 100.0

        initial_edge = max(0.0, 1.0 - ref_price)
        live_edge = max(0.0, 1.0 - live_price)
        if initial_edge <= 0:
            edge_decay_pct = 1.0
        else:
            edge_decay_pct = _clamp((initial_edge - live_edge) / initial_edge)
        max_edge_decay = float(ENV.get("COPY_MAX_EDGE_DECAY_PCT", 70.0) or 70.0) / 100.0

        leader_notional = float(trade.get("usdcSize", 0) or 0)
        if leader_notional <= 0:
            size = float(trade.get("size", 0) or 0)
            leader_notional = size * ref_price
        liquidity_target = float(ENV.get("COPY_LIQUIDITY_TARGET_USDC", 50.0) or 50.0)
        liquidity_score = 1.0 if liquidity_target <= 0 else _clamp(leader_notional / liquidity_target)

        drift_penalty = _clamp(price_drift_pct / max(max_drift_pct, 1e-9))
        edge_penalty = _clamp(edge_decay_pct / max(max_edge_decay, 1e-9))
        quality = (
            leader_score
            * (1.0 - 0.55 * drift_penalty)
            * (1.0 - 0.35 * edge_penalty)
            * (0.60 + 0.40 * age_factor)
            * (0.65 + 0.35 * liquidity_score)
        )
        quality = _clamp(quality)

        hard_fail_reason = None
        if price_drift_pct > max_drift_pct:
            hard_fail_reason = "price_drift_exceeded"
        elif edge_decay_pct > max_edge_decay:
            hard_fail_reason = "edge_decay_exceeded"
        elif trade_ts > 0 and max_age_seconds > 0 and age_seconds > max_age_seconds:
            hard_fail_reason = "signal_too_old"
        else:
            # Near-resolution gate: if the live price is very close to 1.0 (market
            # about to resolve YES), only allow the signal if it is very fresh.
            # A leader's last-second bet at 0.95+ cannot be safely copied because
            # the market may resolve before our order fills.
            # Set NEAR_RESOLUTION_GATE_PRICE=0 to disable this gate entirely.
            near_res_price_raw = ENV.get("NEAR_RESOLUTION_GATE_PRICE", 0.95)
            near_res_price = float(near_res_price_raw) if near_res_price_raw is not None else 0.95
            near_res_max_age = float(ENV.get("NEAR_RESOLUTION_MAX_AGE_SECONDS", 15) or 15)
            if (
                near_res_price > 0
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
            },
        )

