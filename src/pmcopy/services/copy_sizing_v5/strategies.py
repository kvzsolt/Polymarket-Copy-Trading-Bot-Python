"""Polymorphic BUY intent strategies for exact-Decimal V5 copy sizing (WP-D).

Each :class:`BuyIntentStrategy` owns **only** the mode-specific requested-quote
calculation and mode-specific input checks.  Shared request validation, wallet
copy budget, per-market room, multi-cap clamping, strict-mode budget refusal,
minimum-order rejection, and final audit formatting remain centralized in
:class:`~pmcopy.services.copy_sizing_v5.engine.CopySizingEngine`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import ClassVar, final, override

from pmcopy.services.copy_sizing_v5.domain import CopySizingRequest
from pmcopy.services.copy_sizing_v5.policy import CopySizingPolicy

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")


def finite_sizing_decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
    """Validate and coerce ``value`` to a finite non-negative (or positive) Decimal."""
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{name} must be positive")
    if not positive and result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def parse_tier_multiplier(spec: str, notional: Decimal) -> Decimal:
    """Parse a compact ``threshold:multiplier`` step list for V5 tiered sizing.

    Entries are sorted ascending by ``threshold``.  The multiplier for the
    greatest ``threshold <= notional`` is selected; when ``notional`` is below
    the lowest configured threshold, the lowest entry's multiplier acts as the
    floor step (e.g. ``"100:0.75,500:1.0,1000:1.25"`` selects ``0.75`` below
    ``500``, ``1.0`` on ``[500, 1000)``, and ``1.25`` from ``1000`` upward).
    An empty specification returns ``Decimal("1")``.
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
        threshold = finite_sizing_decimal(threshold_raw.strip(), name="tier threshold")
        multiplier = finite_sizing_decimal(
            multiplier_raw.strip(), name="tier multiplier", positive=True
        )
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


class SizingStrategyInputError(ValueError):
    """Strategy input validation error carrying partial audit state."""

    def __init__(
        self,
        message: str,
        *,
        configured_percentage: Decimal | None = None,
        effective_percentage: Decimal | None = None,
        market_target_notional: Decimal | None = None,
        combined_multiplier: Decimal = _ONE,
        flags: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.configured_percentage = configured_percentage
        self.effective_percentage = effective_percentage
        self.market_target_notional = market_target_notional
        self.combined_multiplier = combined_multiplier
        self.flags = flags


@dataclass(frozen=True, slots=True)
class BuyIntentContext:
    """Pre-validated finite domain inputs supplied to a :class:`BuyIntentStrategy`."""

    policy: CopySizingPolicy
    request: CopySizingRequest
    leader_trade_notional: Decimal
    follower_pusd_balance: Decimal
    leader_pusd_balance: Decimal | None
    market_exposure: Decimal
    copy_budget: Decimal
    balance_adjusted_pct: Decimal | None


@dataclass(frozen=True, slots=True)
class BuyIntentOutcome:
    """Intended quote notional (or mode-specific refusal) prior to shared caps."""

    requested_notional: Decimal
    rejection_reason: str | None = None
    configured_percentage: Decimal | None = None
    effective_percentage: Decimal | None = None
    market_target_notional: Decimal | None = None
    combined_multiplier: Decimal = _ONE
    flags: tuple[str, ...] = ()
    strict_budget_cap: bool = False


class BuyIntentStrategy(ABC):
    """Abstract base class for mode-specific BUY notional intent calculation.

    Concrete subclasses compute only the mode's intended quote notional (or
    mode-specific precondition refusal) and never apply shared wallet/position
    or min/max order caps.
    """

    mode: ClassVar[str]

    @abstractmethod
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        """Compute intended quote notional before shared policy caps."""
        raise NotImplementedError

    @staticmethod
    def _combined_multiplier(policy: CopySizingPolicy, request: CopySizingRequest) -> Decimal:
        multiplier = policy.trade_multiplier
        if policy.enable_trust_size_multipliers:
            trust = finite_sizing_decimal(
                request.trust_multiplier, name="trust_multiplier", positive=True
            )
            trust = max(
                policy.trust_size_multiplier_min,
                min(policy.trust_size_multiplier_max, trust),
            )
            multiplier *= trust
        return multiplier


@final
class FixedBuyIntentStrategy(BuyIntentStrategy):
    """Preserve fixed-quote BUY intent calculation."""

    mode: ClassVar[str] = "fixed"

    def requested_notional(
        self,
        *,
        policy: CopySizingPolicy,
        leader_trade_notional: Decimal,
    ) -> Decimal:
        del leader_trade_notional
        return policy.fixed_amount

    @override
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        return BuyIntentOutcome(
            requested_notional=max(
                _ZERO,
                self.requested_notional(
                    policy=ctx.policy,
                    leader_trade_notional=ctx.leader_trade_notional,
                ),
            ),
            strict_budget_cap=ctx.policy.strict_fixed_amount,
        )


@final
class ExactMirrorBuyIntentStrategy(BuyIntentStrategy):
    """Preserve 1:1 leader-notional mirror BUY intent calculation."""

    mode: ClassVar[str] = "exact_mirror"

    def requested_notional(
        self,
        *,
        policy: CopySizingPolicy,
        leader_trade_notional: Decimal,
    ) -> Decimal:
        del policy
        return leader_trade_notional

    @override
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        return BuyIntentOutcome(
            requested_notional=max(
                _ZERO,
                self.requested_notional(
                    policy=ctx.policy,
                    leader_trade_notional=ctx.leader_trade_notional,
                ),
            ),
            strict_budget_cap=ctx.policy.strict_exact_mirror,
        )


class PercentageFamilyBuyIntentStrategy(BuyIntentStrategy):
    """Template base for percentage-scaled BUY modes (`proportional`, `adaptive`, `tiered`)."""

    @abstractmethod
    def _configured_percentage(self, ctx: BuyIntentContext) -> Decimal:
        """Return the mode's pre-multiplier configured percentage."""
        raise NotImplementedError

    @final
    @override
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        configured_pct = self._configured_percentage(ctx)
        try:
            multiplier = self._combined_multiplier(ctx.policy, ctx.request)
        except ValueError as exc:
            raise SizingStrategyInputError(
                str(exc),
                configured_percentage=configured_pct,
            ) from exc

        effective_pct = configured_pct * multiplier
        flags: list[str] = []
        if ctx.policy.proportional_balance_cap_enabled:
            if ctx.balance_adjusted_pct is None:
                if ctx.policy.require_leader_balance_for_proportional:
                    return BuyIntentOutcome(
                        requested_notional=_ZERO,
                        rejection_reason="leader_pusd_balance_required",
                        configured_percentage=configured_pct,
                        effective_percentage=None,
                        combined_multiplier=multiplier,
                    )
            elif effective_pct > ctx.balance_adjusted_pct:
                effective_pct = ctx.balance_adjusted_pct
                flags.append("balance_adjusted_percentage_cap")

        requested = max(_ZERO, ctx.leader_trade_notional * effective_pct / _HUNDRED)
        return BuyIntentOutcome(
            requested_notional=requested,
            configured_percentage=configured_pct,
            effective_percentage=effective_pct,
            combined_multiplier=multiplier,
            flags=tuple(flags),
        )


@final
class ProportionalBuyIntentStrategy(PercentageFamilyBuyIntentStrategy):
    """Fixed configured percentage of leader trade notional."""

    mode: ClassVar[str] = "proportional"

    @override
    def _configured_percentage(self, ctx: BuyIntentContext) -> Decimal:
        return ctx.policy.percentage


@final
class AdaptiveBuyIntentStrategy(PercentageFamilyBuyIntentStrategy):
    """Interpolate configured percentage by trade size or performance signal."""

    mode: ClassVar[str] = "adaptive"

    @override
    def _configured_percentage(self, ctx: BuyIntentContext) -> Decimal:
        policy = ctx.policy
        if policy.adaptive_max_percent <= 0:
            return policy.percentage
        if policy.adaptive_basis == "performance":
            raw_signal = ctx.request.adaptive_signal or _ZERO
            signal = max(
                _ZERO,
                min(_ONE, finite_sizing_decimal(raw_signal, name="adaptive_signal")),
            )
        else:
            threshold = max(Decimal("0.00000001"), policy.adaptive_threshold_usdc)
            signal = min(_ONE, ctx.leader_trade_notional / threshold)
        return (
            policy.adaptive_min_percent
            + (policy.adaptive_max_percent - policy.adaptive_min_percent) * signal
        )


@final
class TieredBuyIntentStrategy(PercentageFamilyBuyIntentStrategy):
    """Scale configured percentage by threshold-stepped leader-notional multiplier."""

    mode: ClassVar[str] = "tiered"

    @override
    def _configured_percentage(self, ctx: BuyIntentContext) -> Decimal:
        tier_multiplier = parse_tier_multiplier(
            ctx.policy.tiered_trade_multipliers,
            ctx.leader_trade_notional,
        )
        return ctx.policy.percentage * tier_multiplier


@final
class BalanceWeightedBuyIntentStrategy(BuyIntentStrategy):
    """Match leader's trade fraction of leader collateral against follower copy budget."""

    mode: ClassVar[str] = "balance_weighted"

    @override
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        if ctx.leader_pusd_balance is None:
            return BuyIntentOutcome(
                requested_notional=_ZERO,
                rejection_reason="leader_pusd_balance_required",
            )
        leader_fraction = ctx.leader_trade_notional / ctx.leader_pusd_balance
        if leader_fraction > _ONE:
            return BuyIntentOutcome(
                requested_notional=_ZERO,
                rejection_reason="leader_trade_exceeds_available_balance",
            )
        multiplier = self._combined_multiplier(ctx.policy, ctx.request)
        market_target = (
            ctx.copy_budget
            * leader_fraction
            * ctx.policy.portfolio_weight_multiplier
            * multiplier
        )
        return BuyIntentOutcome(
            requested_notional=max(_ZERO, market_target),
            market_target_notional=market_target,
            combined_multiplier=multiplier,
        )


@final
class PortfolioWeightedBuyIntentStrategy(BuyIntentStrategy):
    """Target follower market exposure proportional to leader's portfolio weight."""

    mode: ClassVar[str] = "portfolio_weighted"

    @staticmethod
    def _portfolio_weight(request: CopySizingRequest) -> Decimal:
        if request.leader_market_weight is not None:
            weight = finite_sizing_decimal(
                request.leader_market_weight, name="leader_market_weight"
            )
        elif (
            request.leader_market_position_notional is not None
            and request.leader_total_portfolio_notional is not None
        ):
            market = finite_sizing_decimal(
                request.leader_market_position_notional,
                name="leader_market_position_notional",
            )
            total = finite_sizing_decimal(
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

    @override
    def evaluate(self, ctx: BuyIntentContext) -> BuyIntentOutcome:
        weight = self._portfolio_weight(ctx.request)
        multiplier = self._combined_multiplier(ctx.policy, ctx.request)
        market_target = (
            ctx.copy_budget * weight * ctx.policy.portfolio_weight_multiplier * multiplier
        )
        return BuyIntentOutcome(
            requested_notional=max(_ZERO, market_target - ctx.market_exposure),
            market_target_notional=market_target,
            combined_multiplier=multiplier,
        )


_BUILTIN_STRATEGIES: tuple[BuyIntentStrategy, ...] = (
    FixedBuyIntentStrategy(),
    ExactMirrorBuyIntentStrategy(),
    ProportionalBuyIntentStrategy(),
    AdaptiveBuyIntentStrategy(),
    TieredBuyIntentStrategy(),
    BalanceWeightedBuyIntentStrategy(),
    PortfolioWeightedBuyIntentStrategy(),
)

DEFAULT_BUY_INTENT_STRATEGIES: Mapping[str, BuyIntentStrategy] = MappingProxyType(
    {strategy.mode: strategy for strategy in _BUILTIN_STRATEGIES}
)

__all__ = [
    "AdaptiveBuyIntentStrategy",
    "BalanceWeightedBuyIntentStrategy",
    "BuyIntentContext",
    "BuyIntentOutcome",
    "BuyIntentStrategy",
    "DEFAULT_BUY_INTENT_STRATEGIES",
    "ExactMirrorBuyIntentStrategy",
    "FixedBuyIntentStrategy",
    "PercentageFamilyBuyIntentStrategy",
    "PortfolioWeightedBuyIntentStrategy",
    "ProportionalBuyIntentStrategy",
    "SizingStrategyInputError",
    "TieredBuyIntentStrategy",
    "finite_sizing_decimal",
    "parse_tier_multiplier",
]
