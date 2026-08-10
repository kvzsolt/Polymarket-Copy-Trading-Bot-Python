"""Pure exact-decimal V5 sizing engine.

All mode-specific intent calculation happens first. Shared pUSD, reservation,
position, and configured min/max caps are applied afterwards. Discovery niche
fields are deliberately ignored at runtime in V6. Fixed and
exact modes are strict by default: if the requested amount is unaffordable the
trade is skipped rather than silently changing the selected sizing semantics.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

from services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from services.copy_sizing_v5.policy import CopySizingPolicy

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")


def _finite(value: object, *, name: str, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{name} must be positive")
    if not positive and result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


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


def _parse_tier_multiplier(spec: str, notional: Decimal) -> Decimal:
    """Parse a compact ``threshold:multiplier`` list.

    Example: ``"100:0.75,500:1.0,1000:1.25"`` means use 0.75 below 100,
    1.0 from 100 through 499.99, and 1.25 from 500 upward. Empty config is 1.
    """

    text = str(spec or "").strip()
    if not text:
        return _ONE
    rows: list[tuple[Decimal, Decimal]] = []
    for raw in text.split(","):
        piece = raw.strip()
        if not piece:
            continue
        if ":" not in piece:
            raise ValueError("tiered_trade_multipliers entries must be threshold:multiplier")
        threshold_raw, multiplier_raw = piece.split(":", 1)
        threshold = _finite(threshold_raw.strip(), name="tier threshold")
        multiplier = _finite(multiplier_raw.strip(), name="tier multiplier", positive=True)
        rows.append((threshold, multiplier))
    if not rows:
        return _ONE
    rows.sort(key=lambda item: item[0])
    selected = rows[0][1]
    for threshold, multiplier in rows:
        if notional >= threshold:
            selected = multiplier
        else:
            break
    return selected


class CopySizingEngine:
    """Pure exact-decimal sizing engine for follower BUY intents.

    Mode-specific intent calculation happens first. Shared pUSD, reservation,
    position, and configured min/max caps are applied afterwards. Fixed and
    exact modes are strict by default: if the requested amount is unaffordable
    the trade is skipped rather than silently changing the selected sizing
    semantics.
    """

    def __init__(self, policy: CopySizingPolicy) -> None:
        self.policy = policy

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

        flags: list[str] = []
        configured_pct: Decimal | None = None
        effective_pct: Decimal | None = None
        market_target: Decimal | None = None
        multiplier = _ONE

        try:
            if mode == "fixed":
                requested = self.policy.fixed_amount
            elif mode == "exact_mirror":
                requested = leader_trade
            elif mode in {"proportional", "adaptive", "tiered"}:
                configured_pct = self._configured_percent(mode, request, leader_trade)
                multiplier = self._combined_multiplier(mode, request, leader_trade)
                intended_pct = configured_pct * multiplier
                effective_pct = intended_pct
                if self.policy.proportional_balance_cap_enabled:
                    if balance_adjusted_pct is None:
                        if self.policy.require_leader_balance_for_proportional:
                            return self._decision(
                                request=request,
                                mode=mode,
                                allowed=False,
                                reason="leader_pusd_balance_required",
                                requested=_ZERO,
                                final=_ZERO,
                                copy_budget=copy_budget,
                                available_budget=available_budget,
                                configured_pct=configured_pct,
                                effective_pct=None,
                                balance_ratio_pct=balance_ratio_pct,
                                balance_adjusted_pct=balance_adjusted_pct,
                            )
                    elif effective_pct > balance_adjusted_pct:
                        effective_pct = balance_adjusted_pct
                        flags.append("balance_adjusted_percentage_cap")
                requested = leader_trade * (effective_pct or _ZERO) / _HUNDRED
            elif mode == "balance_weighted":
                if leader_balance is None:
                    return self._decision(
                        request=request,
                        mode=mode,
                        allowed=False,
                        reason="leader_pusd_balance_required",
                        requested=_ZERO,
                        final=_ZERO,
                        copy_budget=copy_budget,
                        available_budget=available_budget,
                        configured_pct=None,
                        effective_pct=None,
                        balance_ratio_pct=balance_ratio_pct,
                        balance_adjusted_pct=balance_adjusted_pct,
                    )
                leader_fraction = leader_trade / leader_balance
                if leader_fraction > _ONE:
                    return self._decision(
                        request=request,
                        mode=mode,
                        allowed=False,
                        reason="leader_trade_exceeds_available_balance",
                        requested=_ZERO,
                        final=_ZERO,
                        copy_budget=copy_budget,
                        available_budget=available_budget,
                        configured_pct=None,
                        effective_pct=None,
                        balance_ratio_pct=balance_ratio_pct,
                        balance_adjusted_pct=balance_adjusted_pct,
                    )
                multiplier = self._combined_multiplier(mode, request, leader_trade)
                market_target = (
                    copy_budget
                    * leader_fraction
                    * self.policy.portfolio_weight_multiplier
                    * multiplier
                )
                requested = max(_ZERO, market_target)
            elif mode == "portfolio_weighted":
                weight = self._portfolio_weight(request)
                multiplier = self._combined_multiplier(mode, request, leader_trade)
                market_target = (
                    copy_budget * weight * self.policy.portfolio_weight_multiplier * multiplier
                )
                requested = max(_ZERO, market_target - market_exposure)
            else:  # pragma: no cover - policy already validates
                return self._reject(request, mode, "unsupported_sizing_mode")

            requested = max(_ZERO, requested)
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
                configured_pct=configured_pct,
                effective_pct=effective_pct,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=market_target,
                multiplier=multiplier,
                flags=tuple(flags),
            )

        strict_mode = (mode == "fixed" and self.policy.strict_fixed_amount) or (
            mode == "exact_mirror" and self.policy.strict_exact_mirror
        )
        buy_max = self.policy.buy_max_amount if self.policy.buy_max_amount > 0 else None
        cap, cap_names = _min_positive(
            (
                ("wallet_copy_budget_cap", available_budget),
                ("market_position_cap", position_room),
                ("buy_max_amount_cap", buy_max),
            )
        )
        if strict_mode and requested > cap:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason="strict_mode_exceeds_available_budget",
                requested=requested,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=configured_pct,
                effective_pct=effective_pct,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=market_target,
                multiplier=multiplier,
                flags=tuple(sorted(set(cap_names))),
            )

        final = min(requested, cap)
        if final < requested:
            for name in cap_names:
                if name not in flags:
                    flags.append(name)
        if final < self.policy.buy_min_amount:
            return self._decision(
                request=request,
                mode=mode,
                allowed=False,
                reason="sized_trade_below_buy_minimum",
                requested=requested,
                final=_ZERO,
                copy_budget=copy_budget,
                available_budget=available_budget,
                configured_pct=configured_pct,
                effective_pct=effective_pct,
                balance_ratio_pct=balance_ratio_pct,
                balance_adjusted_pct=balance_adjusted_pct,
                market_target=market_target,
                multiplier=multiplier,
                flags=tuple(flags),
            )
        return self._decision(
            request=request,
            mode=mode,
            allowed=True,
            reason="sized",
            requested=requested,
            final=final,
            copy_budget=copy_budget,
            available_budget=available_budget,
            configured_pct=configured_pct,
            effective_pct=effective_pct,
            balance_ratio_pct=balance_ratio_pct,
            balance_adjusted_pct=balance_adjusted_pct,
            market_target=market_target,
            multiplier=multiplier,
            flags=tuple(flags),
        )

    def _configured_percent(
        self,
        mode: str,
        request: CopySizingRequest,
        leader_trade: Decimal,
    ) -> Decimal:
        if mode == "proportional":
            return self.policy.percentage
        if mode == "tiered":
            tier_multiplier = _parse_tier_multiplier(
                self.policy.tiered_trade_multipliers,
                leader_trade,
            )
            return self.policy.percentage * tier_multiplier
        if self.policy.adaptive_max_percent <= 0:
            return self.policy.percentage
        if self.policy.adaptive_basis == "performance":
            signal = request.adaptive_signal or _ZERO
            signal = max(_ZERO, min(_ONE, _finite(signal, name="adaptive_signal")))
        else:
            threshold = max(Decimal("0.00000001"), self.policy.adaptive_threshold_usdc)
            signal = min(_ONE, leader_trade / threshold)
        return (
            self.policy.adaptive_min_percent
            + (self.policy.adaptive_max_percent - self.policy.adaptive_min_percent) * signal
        )

    def _combined_multiplier(
        self,
        mode: str,
        request: CopySizingRequest,
        leader_trade: Decimal,
    ) -> Decimal:
        multiplier = self.policy.trade_multiplier
        if mode == "tiered":
            # Tier adjustment is already applied to percentage to preserve the
            # configured percentage audit field.
            pass
        if self.policy.enable_trust_size_multipliers:
            trust = _finite(request.trust_multiplier, name="trust_multiplier", positive=True)
            trust = max(
                self.policy.trust_size_multiplier_min,
                min(self.policy.trust_size_multiplier_max, trust),
            )
            multiplier *= trust
        return multiplier

    @staticmethod
    def _portfolio_weight(request: CopySizingRequest) -> Decimal:
        if request.leader_market_weight is not None:
            weight = _finite(request.leader_market_weight, name="leader_market_weight")
        elif (
            request.leader_market_position_notional is not None
            and request.leader_total_portfolio_notional is not None
        ):
            market = _finite(
                request.leader_market_position_notional,
                name="leader_market_position_notional",
            )
            total = _finite(
                request.leader_total_portfolio_notional,
                name="leader_total_portfolio_notional",
                positive=True,
            )
            weight = market / total
        else:
            raise ValueError(
                "portfolio_weighted mode requires leader market weight or portfolio notionals"
            )
        if not _ZERO <= weight <= _ONE:
            raise ValueError("leader market weight must be in [0, 1]")
        return weight

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


__all__ = ["CopySizingEngine"]
