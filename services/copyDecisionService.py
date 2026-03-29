from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from config.env import ENV
from services.leaderScoringService import LeaderScore, LeaderScoringService
from services.tradeScoringService import TradeScore, TradeScoringService


@dataclass(frozen=True)
class CopyDecision:
    allow: bool
    skip_code: str
    reason: str
    side: str
    decision_score: float
    leader_score: float
    trade_score: float
    live_price: float
    reference_price: float
    target_notional_usdc: float
    target_size_tokens: float
    price_drift_pct: float
    edge_decay_pct: float
    details: dict[str, Any] = field(default_factory=dict)


class CopyDecisionService:
    """Decision-first gate for copy trading signals."""

    def __init__(
        self,
        *,
        leader_scorer: LeaderScoringService | None = None,
        trade_scorer: TradeScoringService | None = None,
    ):
        self.leader_scorer = leader_scorer or LeaderScoringService()
        self.trade_scorer = trade_scorer or TradeScoringService()

    @staticmethod
    def _ceil(value: float, decimals: int) -> float:
        if value <= 0:
            return 0.0
        factor = 10 ** max(decimals, 0)
        return math.ceil(value * factor) / factor

    @staticmethod
    def _floor(value: float, decimals: int) -> float:
        if value <= 0:
            return 0.0
        factor = 10 ** max(decimals, 0)
        return math.floor(value * factor) / factor

    def decide_buy(
        self,
        *,
        trade: dict,
        leader_score: LeaderScore,
        live_price: float,
        base_notional_usdc: float,
        max_notional_usdc: float,
        market_min_size: float,
    ) -> CopyDecision:
        ref_price = float(trade.get("price", 0) or 0)
        if live_price <= 0 or ref_price <= 0:
            return CopyDecision(
                allow=False,
                skip_code="invalid_price",
                reason="invalid reference/live price",
                side="BUY",
                decision_score=0.0,
                leader_score=leader_score.score,
                trade_score=0.0,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
            )

        if base_notional_usdc <= 0 or max_notional_usdc <= 0:
            return CopyDecision(
                allow=False,
                skip_code="insufficient_notional",
                reason="computed order notional is non-positive",
                side="BUY",
                decision_score=0.0,
                leader_score=leader_score.score,
                trade_score=0.0,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
            )

        trust_mult = self.leader_scorer.trust_multiplier(leader_score.score)
        desired_notional = min(base_notional_usdc * trust_mult, max_notional_usdc)

        trade_score = self.trade_scorer.score_buy_trade(
            trade,
            live_price=live_price,
            leader_score=leader_score.score,
        )

        if trade_score.hard_fail_reason:
            return CopyDecision(
                allow=False,
                skip_code=trade_score.hard_fail_reason,
                reason=f"decision gate rejected BUY: {trade_score.hard_fail_reason}",
                side="BUY",
                decision_score=trade_score.score,
                leader_score=leader_score.score,
                trade_score=trade_score.score,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=trade_score.price_drift_pct,
                edge_decay_pct=trade_score.edge_decay_pct,
                details=trade_score.details,
            )

        min_decision_score = float(ENV.get("COPY_DECISION_MIN_SCORE", 0.32) or 0.32)
        if trade_score.score < min_decision_score:
            return CopyDecision(
                allow=False,
                skip_code="low_trade_score",
                reason=(
                    f"trade trust score {trade_score.score:.3f} below threshold "
                    f"{min_decision_score:.3f}"
                ),
                side="BUY",
                decision_score=trade_score.score,
                leader_score=leader_score.score,
                trade_score=trade_score.score,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=trade_score.price_drift_pct,
                edge_decay_pct=trade_score.edge_decay_pct,
                details=trade_score.details,
            )

        min_shares = float(ENV.get("COPY_MIN_SHARES", 5.0) or 5.0)
        min_notional = float(ENV.get("COPY_MIN_NOTIONAL_USDC", 1.0) or 1.0)
        decimals = int(ENV.get("COPY_SIZE_DECIMALS", 4) or 4)

        target_size_tokens = desired_notional / live_price
        if 0 < target_size_tokens < min_shares:
            target_size_tokens = min_shares
        if target_size_tokens * live_price < min_notional:
            target_size_tokens = min_notional / live_price
        if market_min_size > 0:
            target_size_tokens = max(target_size_tokens, market_min_size)

        target_size_tokens = self._ceil(target_size_tokens, decimals)
        target_notional_usdc = target_size_tokens * live_price

        if target_notional_usdc > max_notional_usdc + 1e-9:
            floored_size_tokens = self._floor(max_notional_usdc / live_price, decimals)
            floored_notional_usdc = floored_size_tokens * live_price
            floored_meets_mins = (
                floored_size_tokens > 0
                and floored_size_tokens + 1e-12 >= min_shares
                and floored_notional_usdc + 1e-9 >= min_notional
                and (market_min_size <= 0 or floored_size_tokens + 1e-12 >= market_min_size)
            )

            if floored_meets_mins:
                target_size_tokens = floored_size_tokens
                target_notional_usdc = floored_notional_usdc
            else:
                return CopyDecision(
                    allow=False,
                    skip_code="risk_cap_too_low_for_min_order",
                    reason=(
                        "minimum executable BUY size exceeds available/risk-capped notional "
                        f"(required={target_notional_usdc:.4f}, cap={max_notional_usdc:.4f})"
                    ),
                    side="BUY",
                    decision_score=trade_score.score,
                    leader_score=leader_score.score,
                    trade_score=trade_score.score,
                    live_price=live_price,
                    reference_price=ref_price,
                    target_notional_usdc=target_notional_usdc,
                    target_size_tokens=target_size_tokens,
                    price_drift_pct=trade_score.price_drift_pct,
                    edge_decay_pct=trade_score.edge_decay_pct,
                )

        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason=(
                f"decision-approved BUY; trust={leader_score.score:.3f}, "
                f"trade_score={trade_score.score:.3f}"
            ),
            side="BUY",
            decision_score=trade_score.score,
            leader_score=leader_score.score,
            trade_score=trade_score.score,
            live_price=live_price,
            reference_price=ref_price,
            target_notional_usdc=target_notional_usdc,
            target_size_tokens=target_size_tokens,
            price_drift_pct=trade_score.price_drift_pct,
            edge_decay_pct=trade_score.edge_decay_pct,
            details={
                **trade_score.details,
                "trust_multiplier": trust_mult,
                "base_notional_usdc": base_notional_usdc,
                "max_notional_usdc": max_notional_usdc,
                "market_min_size": market_min_size,
            },
        )

    def decide_sell(
        self,
        *,
        trade: dict,
        sell_tokens: float,
        live_price: float,
        market_min_size: float,
        max_sell_tokens: float,
    ) -> CopyDecision:
        ref_price = float(trade.get("price", 0) or 0)
        decimals = int(ENV.get("COPY_SIZE_DECIMALS", 4) or 4)
        if sell_tokens <= 0:
            return CopyDecision(
                allow=False,
                skip_code="sell_size_zero",
                reason="SELL size is zero",
                side="SELL",
                decision_score=1.0,
                leader_score=1.0,
                trade_score=1.0,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=0.0,
                target_size_tokens=0.0,
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
            )

        target = min(sell_tokens, max_sell_tokens)
        if market_min_size > 0 and target < market_min_size:
            if max_sell_tokens < market_min_size:
                return CopyDecision(
                    allow=False,
                    skip_code="sell_below_market_min",
                    reason=(
                        f"SELL size below market minimum ({target:.4f} < {market_min_size:.4f}) "
                        "and position too small to satisfy minimum"
                    ),
                    side="SELL",
                    decision_score=1.0,
                    leader_score=1.0,
                    trade_score=1.0,
                    live_price=live_price,
                    reference_price=ref_price,
                    target_notional_usdc=target * max(live_price, 0.0),
                    target_size_tokens=target,
                    price_drift_pct=0.0,
                    edge_decay_pct=0.0,
                )
            target = market_min_size

        target = self._ceil(target, decimals)
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason="decision-approved SELL",
            side="SELL",
            decision_score=1.0,
            leader_score=1.0,
            trade_score=1.0,
            live_price=live_price,
            reference_price=ref_price,
            target_notional_usdc=target * max(live_price, 0.0),
            target_size_tokens=target,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
            details={"market_min_size": market_min_size, "max_sell_tokens": max_sell_tokens},
        )

