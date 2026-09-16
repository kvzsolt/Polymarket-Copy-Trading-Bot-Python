"""Build account risk inputs with distinct historical cost and marked equity."""

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from domain.cost_basis import PositionCostBasis
from domain.performance import PerformanceUnits
from domain.position_lifecycle import PositionClassification, PositionLifecycle
from services.portfolio_valuation import CachedMarkPolicy, MarketQuoteSource


class RiskPosition(Protocol):
    """Expose only the durable inventory attributes required by account limits."""

    @property
    def user_id(self) -> str:
        """Return the owning follower account."""
        ...

    @property
    def condition_id(self) -> str:
        """Return the market condition identifier."""
        ...

    @property
    def token_id(self) -> str:
        """Return the raw outcome token identifier."""
        ...

    @property
    def leader_id(self) -> str | None:
        """Return leader attribution, or missing attribution."""
        ...

    @property
    def size_tokens(self) -> Decimal:
        """Return remaining shares."""
        ...

    @property
    def avg_entry_price(self) -> Decimal:
        """Return historical collateral per share; legacy zero means unknown."""
        ...


@dataclass(frozen=True, slots=True)
class AccountRiskSnapshot:
    """Separate cost-based limit inputs from current marked account equity.

    All monetary values use collateral units. Missing marks or historical cost
    remain None so admission can enforce each required evidence boundary.
    """

    free_cash_usdc: Decimal
    equity_usdc: Decimal | None
    total_exposure_usdc: Decimal | None
    market_exposure_usdc: Decimal | None
    leader_exposure_usdc: Decimal | None
    open_positions_count: int


class AccountRiskSnapshotBuilder:
    """Value one account from cached marks without venue or database I/O."""

    def __init__(self, quotes: MarketQuoteSource | None, *, max_age_seconds: float) -> None:
        """Bind a narrow quote reader and the existing configured freshness limit."""
        self._quotes = quotes
        self._marks = CachedMarkPolicy(max_age_seconds)

    def build(
        self,
        *,
        account: str,
        free_cash: Decimal,
        positions: Sequence[RiskPosition],
        condition_id: str,
        leader_id: str,
    ) -> AccountRiskSnapshot:
        """Build an account snapshot with explicit missing valuation evidence.

        Raises:
            ValueError: If balance, shares, or position ownership is invalid.
        """
        cash = PerformanceUnits.finite(free_cash)
        if cash is None or cash < 0:
            raise ValueError("Account cash must be finite and non-negative")
        equity = cash
        total_cost = market_cost = leader_cost = Decimal(0)
        active_position_count = 0
        marks_known = total_known = market_known = leader_known = True
        for position in positions:
            if position.user_id != account:
                raise ValueError("Position account does not match snapshot owner")
            shares = PerformanceUnits.finite(position.size_tokens)
            if shares is None or shares < 0:
                raise ValueError("Position shares must be finite and non-negative")
            if shares == 0:
                continue
            if (
                getattr(position, "lifecycle", PositionLifecycle.UNKNOWN)
                == PositionLifecycle.SETTLED
            ):
                settlement = PositionClassification(
                    PositionLifecycle.SETTLED, getattr(position, "settlement_price", None)
                ).settlement_price
                if settlement is None:
                    marks_known = False
                else:
                    equity += shares * settlement
                continue
            active_position_count += 1
            mark = self._marks.midpoint(self._quotes, position.token_id)
            if mark is None:
                marks_known = False
            else:
                equity += shares * mark
            entry = PositionCostBasis.from_value(position.avg_entry_price).entry_price
            if entry is None:
                total_known = False
                if position.condition_id == condition_id:
                    market_known = False
                if position.leader_id == leader_id:
                    leader_known = False
                continue
            cost = shares * entry
            total_cost += cost
            if position.condition_id == condition_id:
                market_cost += cost
            if position.leader_id == leader_id:
                leader_cost += cost
        return AccountRiskSnapshot(
            free_cash_usdc=cash,
            equity_usdc=equity if marks_known else None,
            total_exposure_usdc=total_cost if total_known else None,
            market_exposure_usdc=market_cost if market_known else None,
            leader_exposure_usdc=leader_cost if leader_known else None,
            open_positions_count=active_position_count,
        )
