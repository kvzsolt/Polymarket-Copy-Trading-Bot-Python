from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from config.env import ENV
from utils.logger import get_logger

logger = get_logger("RiskEngine")


# ---------------------------------------------------------------------------
# Balance-tiered regime system
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BalanceRegime:
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


_REGIMES = {
    "micro": BalanceRegime(
        name="micro",
        reserve_pct=Decimal("0.20"), reserve_floor=Decimal("5"),
        hard_cap_pct=Decimal("0.18"), hard_cap_ceiling=Decimal("4.5"),
        max_open_positions=3, allow_partial_exits=False, allow_pyramiding=False,
        max_adds_per_position=0, base_min_score=Decimal("0.30"),
        strategy_min_notional=Decimal("1"), dynamic_threshold_raise=True,
    ),
    "small": BalanceRegime(
        name="small",
        reserve_pct=Decimal("0.15"), reserve_floor=Decimal("10"),
        hard_cap_pct=Decimal("0.15"), hard_cap_ceiling=Decimal("12"),
        max_open_positions=5, allow_partial_exits=True, allow_pyramiding=False,
        max_adds_per_position=0, base_min_score=Decimal("0.27"),
        strategy_min_notional=Decimal("2"), dynamic_threshold_raise=False,
    ),
    "medium": BalanceRegime(
        name="medium",
        reserve_pct=Decimal("0.10"), reserve_floor=Decimal("25"),
        hard_cap_pct=Decimal("0.12"), hard_cap_ceiling=Decimal("40"),
        max_open_positions=8, allow_partial_exits=True, allow_pyramiding=True,
        max_adds_per_position=1, base_min_score=Decimal("0.25"),
        strategy_min_notional=Decimal("3"), dynamic_threshold_raise=False,
    ),
    "large": BalanceRegime(
        name="large",
        reserve_pct=Decimal("0.05"), reserve_floor=Decimal("50"),
        hard_cap_pct=Decimal("0.10"), hard_cap_ceiling=Decimal("150"),
        max_open_positions=12, allow_partial_exits=True, allow_pyramiding=True,
        max_adds_per_position=2, base_min_score=Decimal("0.22"),
        strategy_min_notional=Decimal("5"), dynamic_threshold_raise=False,
    ),
}


def regime_for_equity(equity_usdc: Decimal) -> str:
    micro_max = Decimal(str(ENV.get("BALANCE_REGIME_MICRO_MAX", 75)))
    small_max = Decimal(str(ENV.get("BALANCE_REGIME_SMALL_MAX", 250)))
    medium_max = Decimal(str(ENV.get("BALANCE_REGIME_MEDIUM_MAX", 1000)))
    if equity_usdc < micro_max:
        return "micro"
    elif equity_usdc < small_max:
        return "small"
    elif equity_usdc < medium_max:
        return "medium"
    else:
        return "large"


def get_regime(name: str) -> BalanceRegime:
    return _REGIMES.get(name, _REGIMES["micro"])


def compute_reserve_cash(equity_usdc: Decimal, regime: BalanceRegime) -> Decimal:
    return max(regime.reserve_floor, equity_usdc * regime.reserve_pct)


def compute_hard_cap(equity_usdc: Decimal, tradable_cash: Decimal, regime: BalanceRegime) -> Decimal:
    return min(tradable_cash, equity_usdc * regime.hard_cap_pct, regime.hard_cap_ceiling)


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
    regime: str = "micro"


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
    regime: BalanceRegime | None = None,
) -> RiskHeadroom:
    if regime is None:
        rname = regime_for_equity(equity_usdc)
        regime = get_regime(rname)

    if open_positions_count >= regime.max_open_positions:
        return RiskHeadroom(False, "max_open_positions_reached", Decimal("0"), regime.name)

    reserve = compute_reserve_cash(equity_usdc, regime)
    tradable = max(Decimal("0"), free_cash_usdc - reserve)

    if tradable <= 0:
        return RiskHeadroom(False, "no_tradable_cash_after_reserve", Decimal("0"), regime.name)

    hard_cap = compute_hard_cap(equity_usdc, tradable, regime)

    max_market = Decimal(str(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300)))
    max_category = Decimal(str(ENV.get("MAX_CATEGORY_EXPOSURE_USDC", 600)))
    # Keep defaults consistent with policy_from_env and config/env.py.
    max_leader = Decimal(str(ENV.get("MAX_LEADER_EXPOSURE_USDC", 500)))

    day_start_eq = (
        day_start_equity_usdc
        if day_start_equity_usdc is not None
        else Decimal(str(ENV.get("DAY_START_EQUITY_USDC", equity_usdc)))
    )

    # Daily loss headroom should respect MAX_DAILY_DRAWDOWN_PCT.
    # Keep a small absolute floor to avoid micro-accounts getting immediately inert.
    max_dd_pct = Decimal(str(ENV.get("MAX_DAILY_DRAWDOWN_PCT", 20) or 20)) / Decimal("100")
    max_daily_loss = max(Decimal("2.5"), day_start_eq * max_dd_pct)

    market_headroom = max(Decimal("0"), max_market - market_exposure_usdc)
    category_headroom = max(Decimal("0"), max_category - category_exposure_usdc)
    leader_headroom = max(Decimal("0"), max_leader - leader_exposure_usdc)
    daily_headroom = max(Decimal("0"), max_daily_loss - daily_loss_usdc)

    cap = min(hard_cap, market_headroom, category_headroom, leader_headroom, daily_headroom)

    if cap <= 0:
        return RiskHeadroom(False, "no_buy_headroom", Decimal("0"), regime.name)

    return RiskHeadroom(True, "ok", cap, regime.name)


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
    max_trade_usdc = Decimal(str(ENV.get("BUY_MAX_AMOUNT", 100)))
    return RiskPolicy(
        max_trade_usdc=max_trade_usdc,
        max_portfolio_pct_per_trade=Decimal(str(ENV.get("MAX_POSITION_PCT", 0.25))),
        max_market_exposure_usdc=Decimal(str(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300))),
        max_category_exposure_usdc=Decimal(str(ENV.get("MAX_CATEGORY_EXPOSURE_USDC", 600))),
        max_leader_exposure_usdc=Decimal(str(ENV.get("MAX_LEADER_EXPOSURE_USDC", 500))),
        max_open_positions=int(ENV.get("MAX_OPEN_POSITIONS", 20)),
        max_daily_drawdown_pct=Decimal(str(ENV.get("MAX_DAILY_DRAWDOWN_PCT", 20))),
        max_slippage_bps=int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100),
        kill_switch=bool(ENV.get("KILL_SWITCH", False)),
    )
