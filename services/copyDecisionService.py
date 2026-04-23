from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from services.leaderScoringService import LeaderScore, LeaderScoringService
from services.tradeScoringService import TradeScore, TradeScoringService

if TYPE_CHECKING:
    from config.app_config import AppConfig


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
        config: AppConfig | None = None,
        *,
        leader_scorer: LeaderScoringService | None = None,
        trade_scorer: TradeScoringService | None = None,
    ):
        self._config = config
        self.leader_scorer = leader_scorer or LeaderScoringService(config=config)
        self.trade_scorer = trade_scorer or TradeScoringService(config=config)

        if config is not None:
            self._decision_gate_enabled = config.execution.enable_copy_decision_gate
            self._minimum_order_guard_enabled = config.execution.enable_executable_order_guard
            self._minimum_order_buffer_notional = config.execution.executable_order_buffer_notional_usdc
            self._min_decision_score = config.leader.copy_decision_min_score
            self._copy_min_shares = config.sizing.copy_min_shares
            self._copy_min_notional = config.sizing.copy_min_notional_usdc
            self._copy_size_decimals = config.sizing.copy_size_decimals
            self._slippage_ratio = config.execution.slippage_tolerance / 100.0
        else:
            self._decision_gate_enabled = False
            self._minimum_order_guard_enabled = os.getenv("ENABLE_EXECUTABLE_ORDER_GUARD", "true").strip().lower() in ("1", "true", "yes")
            self._minimum_order_buffer_notional = float(os.getenv("EXECUTABLE_ORDER_BUFFER_NOTIONAL_USDC", "1.05") or 1.05)
            self._min_decision_score = 0.0
            self._copy_min_shares = float(os.getenv("COPY_MIN_SHARES", "5") or 5.0)
            self._copy_min_notional = float(os.getenv("COPY_MIN_NOTIONAL_USDC", "1") or 1.0)
            self._copy_size_decimals = int(os.getenv("COPY_SIZE_DECIMALS", "4") or 4)
            self._slippage_ratio = float(os.getenv("SLIPPAGE_TOLERANCE", "2.0") or 2.0) / 100.0

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

        decision_gate_enabled = self._decision_gate_enabled
        trust_mult = (
            self.leader_scorer.trust_multiplier(leader_score.score)
            if decision_gate_enabled
            else 1.0
        )
        desired_notional = min(base_notional_usdc * trust_mult, max_notional_usdc)

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

        # Hard safety floors
        min_shares = self._copy_min_shares
        min_notional = self._copy_min_notional
        decimals = self._copy_size_decimals

        # Worst-case price for cap compatibility (slippage-adjusted).
        slippage_ratio = self._slippage_ratio
        worst_price = live_price * (1.0 + slippage_ratio)

        target_size_tokens = desired_notional / live_price
        required_min_size_tokens = (
            max(
                min_shares,
                (min_notional / live_price) if live_price > 0 else 0.0,
                market_min_size if market_min_size > 0 else 0.0,
            )
            if self._minimum_order_guard_enabled
            else 0.0
        )

        if 0 < target_size_tokens + 1e-12 < required_min_size_tokens:
            return CopyDecision(
                allow=False,
                skip_code="buy_below_minimum_order_requirements",
                reason=(
                    "copied BUY below minimum executable venue size "
                    f"(target={target_size_tokens:.4f}, required={required_min_size_tokens:.4f}, live_price={live_price:.4f})"
                ),
                side="BUY",
                decision_score=trade_score.score,
                leader_score=leader_score.score,
                trade_score=trade_score.score,
                live_price=live_price,
                reference_price=ref_price,
                target_notional_usdc=target_size_tokens * worst_price,
                target_size_tokens=target_size_tokens,
                price_drift_pct=trade_score.price_drift_pct,
                edge_decay_pct=trade_score.edge_decay_pct,
                details={
                    **trade_score.details,
                    "decision_gate_enabled": decision_gate_enabled,
                    "minimum_order_guard_enabled": self._minimum_order_guard_enabled,
                    "trust_multiplier": trust_mult,
                    "base_notional_usdc": base_notional_usdc,
                    "max_notional_usdc": max_notional_usdc,
                    "market_min_size": market_min_size,
                    "required_min_size_tokens": required_min_size_tokens,
                    "required_min_live_notional_usdc": required_min_size_tokens * live_price,
                },
            )

        target_size_tokens = self._ceil(target_size_tokens, decimals)

        # For BUY execution the downstream order amount is USDC notional, not
        # token count. To reliably buy `target_size_tokens` under slippage
        # protection, we must carry the worst-case protected notional forward,
        # not just the live-price notional. Otherwise the decision layer can
        # approve 5 shares while execution receives too little USDC to buy 5
        # shares at the protected price and rejects it as below min_order_size.
        live_notional_usdc = target_size_tokens * live_price
        required_worst_notional = target_size_tokens * worst_price
        target_notional_usdc = required_worst_notional

        # Cap check must use worst-case notional (what we might actually pay).

        if required_worst_notional > max_notional_usdc + 1e-9:
            # Try to floor size to fit cap at worst price.
            floored_size_tokens = self._floor(max_notional_usdc / worst_price, decimals)
            floored_live_notional_usdc = floored_size_tokens * live_price
            floored_required_worst = floored_size_tokens * worst_price

            floored_meets_mins = (
                floored_size_tokens > 0
                and (
                    not self._minimum_order_guard_enabled
                    or (
                        floored_size_tokens + 1e-12 >= min_shares
                        and floored_required_worst + 1e-9 >= min_notional
                        and (market_min_size <= 0 or floored_size_tokens + 1e-12 >= market_min_size)
                    )
                )
            )

            if floored_meets_mins:
                target_size_tokens = floored_size_tokens
                live_notional_usdc = floored_live_notional_usdc
                target_notional_usdc = floored_required_worst
            else:
                return CopyDecision(
                    allow=False,
                    skip_code="risk_cap_too_low_for_min_order",
                    reason=(
                        "minimum executable BUY size exceeds available/risk-capped notional "
                        f"(required_worst={required_worst_notional:.4f}, cap={max_notional_usdc:.4f})"
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
                if decision_gate_enabled
                else "decision gate disabled by config"
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
                "decision_gate_enabled": decision_gate_enabled,
                "minimum_order_guard_enabled": self._minimum_order_guard_enabled,
                "trust_multiplier": trust_mult,
                "base_notional_usdc": base_notional_usdc,
                "max_notional_usdc": max_notional_usdc,
                "market_min_size": market_min_size,
                "required_min_size_tokens": required_min_size_tokens,
                "live_notional_usdc": live_notional_usdc,
                "required_worst_notional_usdc": required_worst_notional,
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
        decimals = self._copy_size_decimals
        decision_gate_enabled = self._decision_gate_enabled
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
        minimum_guard_enabled = self._minimum_order_guard_enabled
        if minimum_guard_enabled:
            # SELLs should be constrained by the venue minimum and the held
            # position size, not by the copy-sizing floor used on BUYs.
            required_min_tokens = market_min_size if market_min_size > 0 else 0.0
            if target < required_min_tokens:
                if max_sell_tokens < required_min_tokens:
                    return CopyDecision(
                        allow=False,
                        skip_code="sell_below_market_min",
                        reason=(
                            f"SELL size below exchange minimum ({target:.4f} < {required_min_tokens:.4f}) "
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
                        details={
                            "decision_gate_enabled": decision_gate_enabled,
                            "minimum_order_guard_enabled": minimum_guard_enabled,
                            "market_min_size": market_min_size,
                            "required_min_size_tokens": required_min_tokens,
                            "max_sell_tokens": max_sell_tokens,
                        },
                    )
                target = required_min_tokens

        target = self._ceil(target, decimals)
        return CopyDecision(
            allow=True,
            skip_code="ok",
            reason=(
                "decision gate disabled by config"
                if not decision_gate_enabled
                else "decision-approved SELL"
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
            details={
                "decision_gate_enabled": decision_gate_enabled,
                "minimum_order_guard_enabled": minimum_guard_enabled,
                "market_min_size": market_min_size,
                "max_sell_tokens": max_sell_tokens,
            },
        )
