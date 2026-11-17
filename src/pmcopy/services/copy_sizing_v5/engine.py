"""Pure exact-decimal V5 sizing engine.

Mode-specific BUY intent calculation is delegated to narrow
:class:`~pmcopy.services.copy_sizing_v5.strategies.BuyIntentStrategy` objects.
Shared input validation, pUSD copy budgets, reservation deductions, position
caps, strict-mode affordability refusals, minimum-order checks, and audit
formatting remain centralized here.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation

from pmcopy.services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy
from pmcopy.services.copy_sizing_v5.strategies import (
    DEFAULT_BUY_INTENT_STRATEGIES,
    BuyIntentContext,
    BuyIntentStrategy,
    SizingStrategyInputError,
    finite_sizing_decimal as _finite,
    parse_tier_multiplier as _parse_tier_multiplier,
)

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")


def _percent(numerator: Decimal, denominator: Decimal | None) -> Decimal | None:
    if denominator is None or denominator <= 0:
        return None
    return numerator / denominator * _HUNDRED


def _audit_decimal(value: object, *, default: Decimal = _ZERO) -> Decimal:
    """Best-effort decimal conversion used only for rejected-decision audit fields."""
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default
    return result if result.is_finite() else default


def _min_positive(values: Iterable[tuple[str, Decimal | None]]) -> tuple[Decimal, tuple[str, ...]]:
    usable = [(name, value) for name, value in values if value is not None and value >= 0]
    if not usable:
        return _ZERO, ()
    minimum = min(value for _, value in usable)
    names = tuple(name for name, value in usable if value == minimum)
    return minimum, names


class CopySizingEngine:
    """Pure exact-decimal sizing engine for follower BUY intents.

    Mode-specific intent calculation happens first via injected
    :class:`BuyIntentStrategy` collaborators. Shared pUSD, reservation,
    position, and configured min/max caps are applied afterwards. Fixed and
    exact modes are strict by default: if the requested amount is unaffordable
    the trade is skipped rather than silently changing the selected sizing
    semantics.
    """

    def __init__(
        self,
        policy: CopySizingPolicy,
        *,
        strategies: Mapping[str, BuyIntentStrategy] = DEFAULT_BUY_INTENT_STRATEGIES,
    ) -> None:
        self.policy = policy
        self._strategies = strategies

    def reject(self, request: CopySizingRequest, reason: str) -> CopySizingDecision:
        """Create an auditable fail-closed decision at an external boundary."""
        return self._reject(request, self.policy.mode, reason)

    def size(self, request: CopySizingRequest) -> CopySizingDecision:
        """Return the canonical BUY sizing decision for one request.

        Args:
            request: Immutable sizing request. ``side`` must be ``BUY``; SELL
                sizing remains the existing exit-reconstruction path.

        Returns:
            Immutable decision with final notional, audit fields, and the
            cap names that bound the result.
        """
        mode = self.policy.mode
        if str(request.side).upper() != "BUY":
            return self._reject(request, mode, "sell_sizing_remains_existing_exit_path")

        strategy = self._strategies.get(mode)
        if strategy is None:
            return self._reject(request, mode, "unsupported_sizing_mode")

        try:
            leader_trade = _finite(
                request.leader_trade_notional,
                name="leader_trade_notional",
                positive=True,
            )
            follower_balance = _finite(
                request.follower_pusd_balance,
                name="follower_pusd_balance",
                positive=True,
            )
            reserved = _finite(
                request.follower_reserved_notional,
                name="follower_reserved_notional",
            )
            market_exposure = _finite(
                request.follower_market_exposure,
                name="follower_market_exposure",
            )
        except ValueError as exc:
            return self._reject(request, mode, str(exc))

        leader_balance: Decimal | None = None
        if request.leader_pusd_balance is not None:
            try:
                leader_balance = _finite(
                    request.leader_pusd_balance,
                    name="leader_pusd_balance",
                    positive=True,
                )
            except ValueError as exc:
                return self._reject(request, mode, str(exc))

        copy_budget = follower_balance * self.policy.copyable_balance_fraction
        available_budget = max(_ZERO, copy_budget - reserved)
        position_room = max(
            _ZERO,
            follower_balance * self.policy.max_position_pct - market_exposure,
        )
        balance_ratio_pct = _percent(follower_balance, leader_balance)
        balance_adjusted_pct = (
            balance_ratio_pct * self.policy.copyable_balance_fraction
            if balance_ratio_pct is not None
            else None
        )

        if (
            self.policy.position_threshold > 0
            and request.current_open_positions >= self.policy.position_threshold
            and not request.is_existing_market
        ):
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason="position_threshold_reached",
                requested=_ZERO,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=self.policy.percentage,
                effective_pct=None,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
            )

        ctx = BuyIntentContext(
            policy=self.policy,
            request=request,
            leader_trade_notional=leader_trade,
            follower_pusd_balance=follower_balance,
            leader_pusd_balance=leader_balance,
            market_exposure=market_exposure,
            copy_budget=copy_budget,
            balance_adjusted_pct=balance_adjusted_pct,
        )

        try:
            outcome = strategy.evaluate(ctx)
        except SizingStrategyInputError as exc:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason=f"invalid_sizing_input:{exc}",
                requested=_ZERO,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=exc.configured_percentage,
                effective_pct=exc.effective_percentage,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=exc.market_target_notional,
                multiplier=exc.combined_multiplier,
                flags=exc.flags,
            )
        except ValueError as exc:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason=f"invalid_sizing_input:{exc}",
                requested=_ZERO,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=None,
                effective_pct=None,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
            )

        if outcome.rejection_reason is not None:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason=outcome.rejection_reason,
                requested=_ZERO,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=outcome.configured_percentage,
                effective_pct=outcome.effective_percentage,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=outcome.market_target_notional,
                multiplier=outcome.combined_multiplier,
                flags=outcome.flags,
            )

        requested = max(_ZERO, outcome.requested_notional)
        flags = list(outcome.flags)
        buy_max = self.policy.buy_max_amount if self.policy.buy_max_amount > 0 else None
        cap, cap_names = _min_positive(
            (
                ("wallet_copy_budget_cap", available_budget),
                ("market_position_cap", position_room),
                ("buy_max_amount_cap", buy_max),
            )
        )
        if outcome.strict_budget_cap and requested > cap:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason="strict_mode_exceeds_available_budget",
                requested=requested,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=outcome.configured_percentage,
                effective_pct=outcome.effective_percentage,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=outcome.market_target_notional,
                multiplier=outcome.combined_multiplier,
                flags=tuple(sorted(set(cap_names))),
            )

        final_notional = min(requested, cap)
        if final_notional < requested:
            for name in cap_names:
                if name not in flags:
                    flags.append(name)
        if final_notional < self.policy.buy_min_amount:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason="sized_trade_below_buy_minimum",
                requested=requested,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=outcome.configured_percentage,
                effective_pct=outcome.effective_percentage,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=outcome.market_target_notional,
                multiplier=outcome.combined_multiplier,
                flags=tuple(flags),
            )
        return self._decision(
            request=request,
            mode=mode,
            allowed=True,
            reason="sized",
            requested=requested,
            final=final_notional,
            copy_budget=copy_budget,
            available_budget=available_budget,
            configured_pct=outcome.configured_percentage,
            effective_pct=outcome.effective_percentage,
            balance_ratio_pct=balance_ratio_pct,
            balance_adjusted_pct=balance_adjusted_pct,
            market_target=outcome.market_target_notional,
            multiplier=outcome.combined_multiplier,
            flags=tuple(flags),
        )

    def _reject(self, request: CopySizingRequest, mode: str, reason: str) -> CopySizingDecision:
        follower = _audit_decimal(request.follower_pusd_balance)
        budget = follower * self.policy.copyable_balance_fraction if follower > 0 else _ZERO
        return self._decision(
            request=request,
            mode=mode,
            allowed=False,
            reason=reason,
            requested=_ZERO,
            final=_ZERO,
            copy_budget=budget,
            available_budget=_ZERO,
            configured_pct=self.policy.percentage
            if mode in {"proportional", "adaptive", "tiered"}
            else None,
            effective_pct=None,
            balance_ratio_pct=None,
            balance_adjusted_pct=None,
        )

    @staticmethod
    def _decision(
        *,
        request: CopySizingRequest,
        mode: str,
        allowed: bool,
        reason: str,
        requested: Decimal,
        final: Decimal,
        copy_budget: Decimal,
        available_budget: Decimal,
        configured_pct: Decimal | None,
        effective_pct: Decimal | None,
        balance_ratio_pct: Decimal | None,
        balance_adjusted_pct: Decimal | None,
        market_target: Decimal | None = None,
        multiplier: Decimal = _ONE,
        flags: tuple[str, ...] = (),
    ) -> CopySizingDecision:
        leader_trade = _audit_decimal(request.leader_trade_notional)
        follower = _audit_decimal(request.follower_pusd_balance)
        leader_balance = (
            _audit_decimal(request.leader_pusd_balance)
            if request.leader_pusd_balance is not None
            else None
        )
        return CopySizingDecision(
            allowed=allowed,
            mode=mode,
            reason=reason,
            requested_notional=requested,
            final_notional=final,
            copyable_wallet_budget=copy_budget,
            available_wallet_budget=available_budget,
            configured_percentage=configured_pct,
            effective_configured_percentage=effective_pct,
            balance_ratio_percent=balance_ratio_pct,
            balance_adjusted_copy_percent=balance_adjusted_pct,
            effective_leader_trade_percent=_percent(final, leader_trade),
            effective_leader_balance_percent=(
                _percent(leader_trade, leader_balance)
                if mode == "balance_weighted"
                else _percent(final, leader_balance)
            ),
            effective_follower_balance_percent=_percent(final, follower),
            market_target_notional=market_target,
            combined_multiplier=multiplier,
            flags=flags,
            audit={
                "leader_wallet": request.leader_wallet,
                "leader_trade_notional": str(leader_trade),
                "leader_pusd_balance": str(leader_balance) if leader_balance is not None else None,
                "follower_pusd_balance": str(follower),
                "copyable_balance_percent": str(copy_budget / follower * _HUNDRED)
                if follower > 0
                else None,
            },
        )


__all__ = ["CopySizingEngine", "_parse_tier_multiplier"]
