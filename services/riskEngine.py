from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from config.env import ENV
from config.settings import load_settings
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


def _env_decimal(key: str, default: str) -> Decimal:
    value = ENV.get(key)
    if value not in (None, ""):
        return Decimal(str(value))
    return Decimal(default)


def _env_int(key: str, default: int) -> int:
    value = ENV.get(key)
    if value not in (None, ""):
        return int(value)
    return default


def _env_bool(key: str, default: bool) -> bool:
    value = ENV.get(key)
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes")


def active_policy() -> AccountCapPolicy:
    return AccountCapPolicy(
        name="account_cap",
        reserve_pct=_env_decimal("BALANCE_POLICY_RESERVE_PCT", default="0.12"),
        reserve_floor=_env_decimal("BALANCE_POLICY_RESERVE_FLOOR", default="7"),
        hard_cap_pct=_env_decimal("BALANCE_POLICY_HARD_CAP_PCT", default="0.15"),
        hard_cap_ceiling=_env_decimal("BALANCE_POLICY_HARD_CAP_CEILING", default="8"),
        max_open_positions=_env_int("BALANCE_POLICY_MAX_OPEN_POSITIONS", default=12),
        allow_partial_exits=_env_bool("BALANCE_POLICY_ALLOW_PARTIAL_EXITS", default=True),
        allow_pyramiding=_env_bool("BALANCE_POLICY_ALLOW_PYRAMIDING", default=False),
        max_adds_per_position=_env_int("BALANCE_POLICY_MAX_ADDS_PER_POSITION", default=1),
        base_min_score=_env_decimal("BALANCE_POLICY_BASE_MIN_SCORE", default="0.22"),
        strategy_min_notional=_env_decimal("BALANCE_POLICY_MIN_NOTIONAL_USDC", default="0.00001"),
        dynamic_threshold_raise=_env_bool("BALANCE_POLICY_DYNAMIC_THRESHOLD_RAISE", default=False),
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
) -> RiskHeadroom:
    if policy is None:
        policy = active_policy()

    if open_positions_count >= policy.max_open_positions:
        return RiskHeadroom(False, "max_open_positions_reached", Decimal("0"), policy.name)

    reserve = compute_reserve_cash(equity_usdc, policy)
    tradable = max(Decimal("0"), free_cash_usdc - reserve)

    if tradable <= 0:
        return RiskHeadroom(False, "no_tradable_cash_after_reserve", Decimal("0"), policy.name)

    hard_cap = compute_hard_cap(equity_usdc, tradable, policy)

    max_market = Decimal(str(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300)))
    max_category = Decimal(str(ENV.get("MAX_CATEGORY_EXPOSURE_USDC", 600)))
    max_leader = Decimal(str(ENV.get("MAX_LEADER_EXPOSURE_USDC", 500)))

    env_day_start = ENV.get("DAY_START_EQUITY_USDC")
    if day_start_equity_usdc is not None:
        day_start_eq = day_start_equity_usdc
    elif env_day_start not in (None, "") and Decimal(str(env_day_start)) > 0:
        day_start_eq = Decimal(str(env_day_start))
    else:
        day_start_eq = equity_usdc

    max_dd_pct = Decimal(str(ENV.get("MAX_DAILY_DRAWDOWN_PCT", 20) or 20)) / Decimal("100")
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


def policy_from_env() -> RiskPolicy:
    settings = load_settings(env=ENV)
    max_trade_usdc = Decimal(str(ENV.get("BUY_MAX_AMOUNT", 100)))
    return RiskPolicy(
        max_trade_usdc=max_trade_usdc,
        max_portfolio_pct_per_trade=Decimal(str(settings.sizing.max_position_ratio)),
        max_market_exposure_usdc=Decimal(str(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300))),
        max_category_exposure_usdc=Decimal(str(ENV.get("MAX_CATEGORY_EXPOSURE_USDC", 600))),
        max_leader_exposure_usdc=Decimal(str(ENV.get("MAX_LEADER_EXPOSURE_USDC", 500))),
        max_open_positions=int(ENV.get("MAX_OPEN_POSITIONS", 20)),
        max_daily_drawdown_pct=Decimal(str(ENV.get("MAX_DAILY_DRAWDOWN_PCT", 20))),
        # ratio -> bps: multiply by 10_000 (0.02 -> 200 bps)
        max_slippage_bps=int(settings.execution.slippage_tolerance_ratio * 10_000),
        kill_switch=bool(ENV.get("KILL_SWITCH", False)),
    )
