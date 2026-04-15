from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config.app_config import AppConfig

from utils.logger import get_logger

logger = get_logger("RiskEngine")


# ---------------------------------------------------------------------------
# Simplified account-cap policy
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AccountCapPolicy:
    name: str
    reserve_pct: Decimal
    reserve_floor: Decimal
    hard_cap_pct: Decimal
    hard_cap_ceiling: Decimal
    max_open_positions: int
    allow_partial_exits: bool
    allow_pyramiding: bool
    max_adds_per_position: int
    base_min_score: Decimal
    strategy_min_notional: Decimal
    dynamic_threshold_raise: bool = False


def _policy_from_config(config: AppConfig) -> AccountCapPolicy:
    """Build AccountCapPolicy from the typed AppConfig."""
    rc = config.risk
    return AccountCapPolicy(
        name="account_cap",
        reserve_pct=Decimal(str(rc.balance_policy_reserve_pct)),
        reserve_floor=Decimal(str(rc.balance_policy_reserve_floor)),
        hard_cap_pct=Decimal(str(rc.balance_policy_hard_cap_pct)),
        hard_cap_ceiling=Decimal(str(rc.balance_policy_hard_cap_ceiling)),
        max_open_positions=rc.balance_policy_max_open_positions,
        allow_partial_exits=rc.balance_policy_allow_partial_exits,
        allow_pyramiding=rc.balance_policy_allow_pyramiding,
        max_adds_per_position=rc.balance_policy_max_adds_per_position,
        base_min_score=Decimal(str(rc.balance_policy_base_min_score)),
        strategy_min_notional=Decimal(str(rc.balance_policy_min_notional_usdc)),
        dynamic_threshold_raise=rc.balance_policy_dynamic_threshold_raise,
    )


# Module-level cached policy (set via init_risk_engine)
_cached_config: AppConfig | None = None


def init_risk_engine(config: AppConfig) -> None:
    """Initialize the risk engine with an AppConfig. Call once at startup."""
    global _cached_config
    _cached_config = config


def active_policy() -> AccountCapPolicy:
    if _cached_config is not None:
        return _policy_from_config(_cached_config)
    # Fallback defaults when no config has been injected
    return AccountCapPolicy(
        name="account_cap",
        reserve_pct=Decimal("0.12"),
        reserve_floor=Decimal("7"),
        hard_cap_pct=Decimal("0.15"),
        hard_cap_ceiling=Decimal("8"),
        max_open_positions=12,
        allow_partial_exits=True,
        allow_pyramiding=False,
        max_adds_per_position=1,
        base_min_score=Decimal("0.22"),
        strategy_min_notional=Decimal("0.00001"),
        dynamic_threshold_raise=False,
    )


def policy_name_for_equity(equity_usdc: Decimal) -> str:
    _ = equity_usdc
    return active_policy().name


def get_active_policy(name: str = "") -> AccountCapPolicy:
    _ = name
    return active_policy()


get_policy = get_active_policy


def compute_reserve_cash(equity_usdc: Decimal, policy: AccountCapPolicy) -> Decimal:
    return max(policy.reserve_floor, equity_usdc * policy.reserve_pct)


def compute_hard_cap(equity_usdc: Decimal, tradable_cash: Decimal, policy: AccountCapPolicy) -> Decimal:
    return min(tradable_cash, equity_usdc * policy.hard_cap_pct, policy.hard_cap_ceiling)


def compute_soft_cap(
    hard_cap: Decimal,
    decision_score: Decimal,
    base_min_score: Decimal,
) -> Decimal:
    denom = max(Decimal("1e-9"), Decimal("1") - base_min_score)
    score_norm = max(Decimal("0"), min(Decimal("1"), (decision_score - base_min_score) / denom))
    return hard_cap * (Decimal("0.55") + Decimal("0.45") * score_norm)


# ---------------------------------------------------------------------------
# Risk headroom for BUY
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RiskHeadroom:
    allow: bool
    reason: str
    cap_notional_usdc: Decimal
    policy_name: str = "account_cap"


def headroom_for_buy(
    *,
    equity_usdc: Decimal,
    free_cash_usdc: Decimal,
    open_positions_count: int,
    market_exposure_usdc: Decimal = Decimal("0"),
    category_exposure_usdc: Decimal = Decimal("0"),
    leader_exposure_usdc: Decimal = Decimal("0"),
    daily_loss_usdc: Decimal = Decimal("0"),
    day_start_equity_usdc: Decimal | None = None,
    policy: AccountCapPolicy | None = None,
    config: AppConfig | None = None,
) -> RiskHeadroom:
    if policy is None:
        policy = active_policy()

    # Use injected config or fallback to cached module-level config
    _cfg = config or _cached_config

    if open_positions_count >= policy.max_open_positions:
        return RiskHeadroom(False, "max_open_positions_reached", Decimal("0"), policy.name)

    reserve = compute_reserve_cash(equity_usdc, policy)
    tradable = max(Decimal("0"), free_cash_usdc - reserve)

    if tradable <= 0:
        return RiskHeadroom(False, "no_tradable_cash_after_reserve", Decimal("0"), policy.name)

    hard_cap = compute_hard_cap(equity_usdc, tradable, policy)

    if _cfg is not None:
        max_market = Decimal(str(_cfg.risk.max_market_exposure_usdc))
        max_category = Decimal(str(_cfg.risk.max_category_exposure_usdc))
        max_leader = Decimal(str(_cfg.risk.max_leader_exposure_usdc))
        day_start_cfg = _cfg.risk.day_start_equity_usdc
        max_dd_pct = Decimal(str(_cfg.risk.max_daily_drawdown_pct)) / Decimal("100")
    else:
        max_market = Decimal("300")
        max_category = Decimal("600")
        max_leader = Decimal("500")
        day_start_cfg = 0.0
        max_dd_pct = Decimal("0.20")

    if day_start_equity_usdc is not None:
        day_start_eq = day_start_equity_usdc
    elif day_start_cfg > 0:
        day_start_eq = Decimal(str(day_start_cfg))
    else:
        day_start_eq = equity_usdc

    max_daily_loss = max(Decimal("2.5"), day_start_eq * max_dd_pct)

    market_headroom = max(Decimal("0"), max_market - market_exposure_usdc)
    category_headroom = max(Decimal("0"), max_category - category_exposure_usdc)
    leader_headroom = max(Decimal("0"), max_leader - leader_exposure_usdc)
    daily_headroom = max(Decimal("0"), max_daily_loss - daily_loss_usdc)

    cap = min(hard_cap, market_headroom, category_headroom, leader_headroom, daily_headroom)

    if cap <= 0:
        return RiskHeadroom(False, "no_buy_headroom", Decimal("0"), policy.name)

    return RiskHeadroom(True, "ok", cap, policy.name)


# ---------------------------------------------------------------------------
# Legacy policy/engine interface (kept for backward compat)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RiskPolicy:
    max_trade_usdc: Decimal
    max_portfolio_pct_per_trade: Decimal
    max_market_exposure_usdc: Decimal
    max_category_exposure_usdc: Decimal
    max_leader_exposure_usdc: Decimal
    max_open_positions: int
    max_daily_drawdown_pct: Decimal
    max_slippage_bps: int
    kill_switch: bool = False


@dataclass(frozen=True)
class ProposedTrade:
    user_id: str
    leader_id: str
    market_id: str
    category: str
    token_id: str
    side: str
    notional_usdc: Decimal


@dataclass(frozen=True)
class RiskDecision:
    allow: bool
    reason: str
    adjusted_notional_usdc: Decimal | None = None


@dataclass
class PortfolioSnapshot:
    equity_usdc: Decimal
    daily_drawdown_pct: Decimal
    open_positions_count: int
    market_exposures: dict[str, Decimal]
    category_exposures: dict[str, Decimal]
    leader_exposures: dict[str, Decimal]

    def market_exposure_usdc(self, market_id: str) -> Decimal:
        return self.market_exposures.get(market_id, Decimal("0"))

    def category_exposure_usdc(self, category: str) -> Decimal:
        return self.category_exposures.get(category, Decimal("0"))

    def leader_exposure_usdc(self, leader_id: str) -> Decimal:
        return self.leader_exposures.get(leader_id, Decimal("0"))


class RiskEngine:
    def evaluate(self, portfolio: PortfolioSnapshot, policy: RiskPolicy, trade: ProposedTrade) -> RiskDecision:
        if policy.kill_switch:
            return RiskDecision(False, "kill_switch_enabled")

        if trade.side.upper() == "SELL":
            return RiskDecision(True, "ok_sell_exit")

        if portfolio.daily_drawdown_pct >= policy.max_daily_drawdown_pct:
            return RiskDecision(False, "daily_drawdown_limit_hit")

        if trade.notional_usdc > policy.max_trade_usdc:
            return RiskDecision(False, "max_trade_usdc_exceeded")

        if portfolio.open_positions_count >= policy.max_open_positions:
            return RiskDecision(False, "max_open_positions_reached")

        max_notional_from_pct = portfolio.equity_usdc * policy.max_portfolio_pct_per_trade
        if max_notional_from_pct > 0 and trade.notional_usdc > max_notional_from_pct:
            return RiskDecision(False, "max_portfolio_pct_per_trade_exceeded")

        if portfolio.market_exposure_usdc(trade.market_id) + trade.notional_usdc > policy.max_market_exposure_usdc:
            return RiskDecision(False, "max_market_exposure_exceeded")

        if portfolio.category_exposure_usdc(trade.category) + trade.notional_usdc > policy.max_category_exposure_usdc:
            return RiskDecision(False, "max_category_exposure_exceeded")

        if portfolio.leader_exposure_usdc(trade.leader_id) + trade.notional_usdc > policy.max_leader_exposure_usdc:
            return RiskDecision(False, "max_leader_exposure_exceeded")

        return RiskDecision(True, "ok")


def policy_from_config(config: AppConfig) -> RiskPolicy:
    """Build a legacy RiskPolicy from the typed AppConfig."""
    rc = config.risk
    ec = config.execution
    sc = config.sizing
    return RiskPolicy(
        max_trade_usdc=Decimal(str(sc.buy_max_amount)),
        max_portfolio_pct_per_trade=Decimal(str(sc.max_position_pct)),
        max_market_exposure_usdc=Decimal(str(rc.max_market_exposure_usdc)),
        max_category_exposure_usdc=Decimal(str(rc.max_category_exposure_usdc)),
        max_leader_exposure_usdc=Decimal(str(rc.max_leader_exposure_usdc)),
        max_open_positions=rc.max_open_positions,
        max_daily_drawdown_pct=Decimal(str(rc.max_daily_drawdown_pct)),
        max_slippage_bps=int(ec.slippage_tolerance * 100),
        kill_switch=rc.kill_switch,
    )


# Legacy alias for backward compat
def policy_from_env() -> RiskPolicy:
    """DEPRECATED: Use policy_from_config() instead."""
    if _cached_config is not None:
        return policy_from_config(_cached_config)
    # Bare fallback
    return RiskPolicy(
        max_trade_usdc=Decimal("100"),
        max_portfolio_pct_per_trade=Decimal("0.45"),
        max_market_exposure_usdc=Decimal("300"),
        max_category_exposure_usdc=Decimal("600"),
        max_leader_exposure_usdc=Decimal("500"),
        max_open_positions=20,
        max_daily_drawdown_pct=Decimal("20"),
        max_slippage_bps=200,
        kill_switch=False,
    )
