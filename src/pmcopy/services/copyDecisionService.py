from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, TYPE_CHECKING

from pmcopy.services.order_sizing import SizingDecision as _SizingDecision
from pmcopy.services.order_sizing import size_buy_order as _size_buy_order
from pmcopy.services.order_sizing import size_sell_order as _size_sell_order
from pmcopy.services.idempotency import (
    compute_effective_key as _compute_effective_key,
)
from pmcopy.services.leaderScoringService import LeaderScore, LeaderScoringService
from pmcopy.services.tradeScoringService import TradeScore, TradeScoringService
from pmcopy.utils.logger import get_logger

if TYPE_CHECKING:
    from pmcopy.config.app_config import AppConfig


logger = get_logger("CopyDecisionService")


# Re-export SizingDecision and effective_intent_key for backward compatibility.
# New code should import directly from services.order_sizing and services.idempotency.
SizingDecision = _SizingDecision


def effective_intent_key(
    *,
    user: str,
    condition_id: str,
    token_id: str,
    side: str,
    effective_size: Decimal,
    effective_price: Decimal,
    tick_size: Decimal,
    intent_id: str = "",
    policy_version: str = "",
) -> str:
    """Compute SHA256 idempotency key from the **effective adjusted** request.

    Delegates to :func:`services.idempotency.compute_effective_key`.
    Added ``policy_version`` parameter (defaults to empty for backward compat).
    """
    return _compute_effective_key(
        user=user,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        effective_size=effective_size,
        effective_price=effective_price,
        tick_size=tick_size,
        intent_id=intent_id,
        policy_version=policy_version,
    )


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
    sizing: SizingDecision | None = None
    details: dict[str, Any] = field(default_factory=dict)


class CopyDecisionService:
    """Decision-first gate for copy trading signals."""

    def __init__(
        self,
        config: AppConfig,
        *,
        leader_scorer: LeaderScoringService | None = None,
        trade_scorer: TradeScoringService | None = None,
    ):
        self._config = config
        self.leader_scorer = leader_scorer or LeaderScoringService(config=config)
        self.trade_scorer = trade_scorer or TradeScoringService(config=config)

        self._decision_gate_enabled = config.execution.enable_copy_decision_gate
        self._min_decision_score = config.copy_trading.copy_decision_min_score

    def decide_buy(
        self,
        *,
        trade: dict,
        leader_score: LeaderScore,
        live_price: float,
        base_notional_usdc: float,
        max_notional_usdc: float,
        market_min_size: float,
        tick_size: float = 0.0,
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

        decision_gate_enabled = self._decision_gate_enabled
        trust_mult = (
            self.leader_scorer.trust_multiplier(leader_score.score)
            if decision_gate_enabled
            else 1.0
        )
        unconstrained_notional = base_notional_usdc * trust_mult

        if decision_gate_enabled:
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

            min_decision_score = self._min_decision_score
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
        else:
            trade_score = TradeScore(
                score=1.0,
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
                age_seconds=0.0,
                liquidity_score=1.0,
                hard_fail_reason=None,
                details={"decision_gate_enabled": False},
            )

        sizing = _size_buy_order(
            live_price=live_price,
            base_notional_usdc=unconstrained_notional,
            max_notional_usdc=max_notional_usdc,
            market_min_size=market_min_size,
            tick_size=tick_size,
            slippage_ratio=Decimal("0"),
        )

        if not sizing.accept:
            return CopyDecision(
                allow=False,
                skip_code=sizing.reason,
                reason=f"BUY rejected by sizing: {sizing.reason}",
                side="BUY",
                decision_score=trade_score.score,
                leader_score=leader_score.score,
                trade_score=trade_score.score,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=float(sizing.worst_notional),
                target_size_tokens=float(sizing.effective_size),
                price_drift_pct=trade_score.price_drift_pct,
                edge_decay_pct=trade_score.edge_decay_pct,
                sizing=sizing,
                details={
                    **trade_score.details,
                    "decision_gate_enabled": decision_gate_enabled,
                    "minimum_order_guard_enabled": True,
                    "trust_multiplier": trust_mult,
                    "base_notional_usdc": base_notional_usdc,
                    "max_notional_usdc": max_notional_usdc,
                    "market_min_size": market_min_size,
                    "cap_binding": max_notional_usdc + 1e-9 < unconstrained_notional,
                },
            )

        target_size_tokens = float(sizing.effective_size)
        live_notional_usdc = target_size_tokens * live_price
        required_worst_notional = float(sizing.worst_notional)

        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason=(
                f"decision-approved BUY; trust={leader_score.score:.3f}, "
                f"trade_score={trade_score.score:.3f}"
                if decision_gate_enabled
                else "decision gate disabled by config"
            ),
            side="BUY",
            decision_score=trade_score.score,
            leader_score=leader_score.score,
            trade_score=trade_score.score,
            live_price=live_price,
            reference_price=ref_price,
            target_notional_usdc=required_worst_notional,
            target_size_tokens=target_size_tokens,
            price_drift_pct=trade_score.price_drift_pct,
            edge_decay_pct=trade_score.edge_decay_pct,
            details={
                **trade_score.details,
                "decision_gate_enabled": decision_gate_enabled,
                "minimum_order_guard_enabled": True,
                "trust_multiplier": trust_mult,
                "base_notional_usdc": base_notional_usdc,
                "unconstrained_notional_usdc": unconstrained_notional,
                "max_notional_usdc": max_notional_usdc,
                "market_min_size": market_min_size,
                "cap_binding": max_notional_usdc + 1e-9 < unconstrained_notional,
                "live_notional_usdc": live_notional_usdc,
                "required_worst_notional_usdc": required_worst_notional,
            },
            sizing=sizing,
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
        sizing = _size_sell_order(
            sell_tokens=sell_tokens,
            held_size=max_sell_tokens,
            live_price=live_price,
            market_min_size=market_min_size,
        )

        if not sizing.accept:
            return CopyDecision(
                allow=False,
                skip_code=sizing.reason,
                reason=f"SELL rejected by sizing: {sizing.reason}",
                side="SELL",
                decision_score=1.0,
                leader_score=1.0,
                trade_score=1.0,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=float(sizing.worst_notional),
                target_size_tokens=float(sizing.effective_size),
                price_drift_pct=0.0,
                edge_decay_pct=0.0,
                sizing=sizing,
                details={
                    "decision_gate_enabled": self._decision_gate_enabled,
                    "minimum_order_guard_enabled": True,
                    "market_min_size": market_min_size,
                    "max_sell_tokens": max_sell_tokens,
                },
            )

        target = float(sizing.effective_size)
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason=(
                "decision gate disabled by config"
                if not self._decision_gate_enabled
                else "decision-approved SELL"
            ),
            side="SELL",
            decision_score=1.0,
            leader_score=1.0,
            trade_score=1.0,
            live_price=live_price,
            reference_price=ref_price,
            target_notional_usdc=float(sizing.worst_notional),
            target_size_tokens=target,
            sizing=sizing,
            price_drift_pct=0.0,
            edge_decay_pct=0.0,
            details={
                "decision_gate_enabled": self._decision_gate_enabled,
                "minimum_order_guard_enabled": True,
                "market_min_size": market_min_size,
                "max_sell_tokens": max_sell_tokens,
            },
        )
