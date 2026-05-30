from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from services.copyStrategy import calculate_copy_notional


@dataclass(frozen=True)
class SignalReplayResult:
    signal_id: str
    copyable: bool
    reason: str
    leader_notional: Decimal
    target_notional: Decimal
    effective_notional: Decimal
    ts: int
    condition_id: str
    token_id: str
    side: str


@dataclass(frozen=True)
class CopyabilityReplayResult:
    wallet: str
    total_signals: int
    meaningful_signals: int
    copyable_signals: int
    total_leader_notional: Decimal
    copyable_leader_notional: Decimal
    target_notional_sum: Decimal
    copyable_target_notional_sum: Decimal
    copyable_signal_ratio: float
    copyable_notional_ratio: float
    max_concurrent_open_positions: int
    estimated_required_daily_budget: Decimal
    signal_results: tuple[SignalReplayResult, ...] = field(default_factory=tuple)
    details: dict[str, Any] = field(default_factory=dict)


class StaticBalanceProvider:
    def __init__(self, balance_usdc: Decimal | str | int | float):
        self._balance_usdc = Decimal(str(balance_usdc))

    async def get_available_balance_usdc(self) -> Decimal:
        return self._balance_usdc


class CopyabilityReplayService:
    def __init__(self, *, config, balance_provider):
        self.config = config
        self.balance_provider = balance_provider

    async def replay_wallet(self, wallet: str, signals: list) -> CopyabilityReplayResult:
        available_balance = Decimal(
            str(await self.balance_provider.get_available_balance_usdc())
        )
        simulated_free_cash = available_balance
        open_positions: set[tuple[str, str]] = set()
        open_position_notional: dict[tuple[str, str], Decimal] = {}
        max_open_positions = 0

        results: list[SignalReplayResult] = []
        total_leader_notional = Decimal("0")
        copyable_leader_notional = Decimal("0")
        target_sum = Decimal("0")
        copyable_target_sum = Decimal("0")

        for signal in sorted(
            signals, key=lambda s: (s.first_ts, s.condition_id, s.token_id, s.side)
        ):
            if signal.side == "SELL":
                key = (signal.condition_id, signal.token_id)
                released = open_position_notional.pop(key, Decimal("0"))
                simulated_free_cash += released
                open_positions.discard(key)
                continue

            leader_notional = Decimal(str(signal.total_usdc_size))
            if leader_notional < Decimal(
                str(self.config.discovery.discovery_min_group_notional_usdc)
            ):
                continue

            total_leader_notional += leader_notional

            sizing = calculate_copy_notional(
                strategy=str(self.config.sizing.mode or "proportional"),
                leader_notional_usdc=leader_notional,
                copy_percentage=Decimal(str(self.config.sizing.percentage)),
                fixed_amount=self.config.sizing.fixed_amount,
                min_order_usdc=self.config.sizing.buy_min_amount,
                max_order_usdc=self.config.sizing.buy_max_amount,
                trade_multiplier=self.config.sizing.trade_multiplier,
                tiered_multipliers=self.config.sizing.tiered_trade_multipliers,
                adaptive_basis=str(self.config.sizing.adaptive_basis or "trade_size"),
                adaptive_min_percent=self.config.sizing.adaptive_min_percent,
                adaptive_max_percent=self.config.sizing.adaptive_max_percent,
                adaptive_threshold_usdc=self.config.sizing.adaptive_threshold_usdc,
            )

            target = Decimal(str(sizing.final_notional_usdc))
            target_sum += target
            copyable, reason, effective = self._check_caps(
                target=target,
                simulated_free_cash=simulated_free_cash,
                open_positions_count=len(open_positions),
            )

            if copyable:
                simulated_free_cash -= effective
                key = (signal.condition_id, signal.token_id)
                open_positions.add(key)
                open_position_notional[key] = (
                    open_position_notional.get(key, Decimal("0")) + effective
                )
                max_open_positions = max(max_open_positions, len(open_positions))
                copyable_leader_notional += leader_notional
                copyable_target_sum += effective

            results.append(
                SignalReplayResult(
                    signal_id=(
                        f"{signal.wallet}|{signal.condition_id}|{signal.token_id}|"
                        f"{signal.side}|{signal.first_ts}"
                    ),
                    copyable=copyable,
                    reason=reason,
                    leader_notional=leader_notional,
                    target_notional=target,
                    effective_notional=effective,
                    ts=signal.first_ts,
                    condition_id=signal.condition_id,
                    token_id=signal.token_id,
                    side=signal.side,
                )
            )

        meaningful = len(results)
        copyable_count = sum(1 for r in results if r.copyable)
        signal_ratio = copyable_count / meaningful if meaningful else 0.0
        notional_ratio = (
            float(copyable_leader_notional / total_leader_notional)
            if total_leader_notional > 0
            else 0.0
        )

        return CopyabilityReplayResult(
            wallet=wallet.lower(),
            total_signals=len(signals),
            meaningful_signals=meaningful,
            copyable_signals=copyable_count,
            total_leader_notional=total_leader_notional,
            copyable_leader_notional=copyable_leader_notional,
            target_notional_sum=target_sum,
            copyable_target_notional_sum=copyable_target_sum,
            copyable_signal_ratio=signal_ratio,
            copyable_notional_ratio=notional_ratio,
            max_concurrent_open_positions=max_open_positions,
            estimated_required_daily_budget=target_sum,
            signal_results=tuple(results),
        )

    def _check_caps(
        self,
        *,
        target: Decimal,
        simulated_free_cash: Decimal,
        open_positions_count: int,
    ):
        if target <= 0:
            return False, "target_notional_non_positive", Decimal("0")

        if target < Decimal(str(self.config.sizing.buy_min_amount)):
            return False, "below_buy_min_amount", Decimal("0")

        effective = min(target, Decimal(str(self.config.sizing.buy_max_amount)))
        max_position_size = Decimal(
            str(self.config.execution.max_position_size_usdc or 0)
        )
        if max_position_size > 0:
            effective = min(effective, max_position_size)

        min_headroom = Decimal(
            str(self.config.execution.min_balance_headroom_usdc or 0)
        )
        if simulated_free_cash - effective < min_headroom:
            return False, "insufficient_balance_headroom", Decimal("0")

        if open_positions_count >= int(self.config.risk.max_open_positions):
            return False, "max_open_positions_reached", Decimal("0")

        if effective <= 0:
            return False, "effective_notional_zero", Decimal("0")

        return True, "ok", effective
