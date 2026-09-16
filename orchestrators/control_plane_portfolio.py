"""Adapt follower runtime repositories into cached portfolio inputs."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from typing import Protocol

from domain.cost_basis import PositionCostBasis
from domain.position_lifecycle import PositionClassification, PositionLifecycle
from services.control_plane.valuation import CachedMarkPolicy, MarketQuoteSource

from services.control_plane.portfolio import (
    Clock,
    PortfolioAccountSnapshot,
    PortfolioRefreshService,
    PortfolioSnapshotCache,
)


class BalanceReader(Protocol):
    """Read current follower collateral through an existing adapter."""

    async def read(self, wallet: str) -> Decimal | None:
        """Return exact collateral or ``None`` when unavailable."""

        ...


class PositionRecord(Protocol):
    """Expose cost-basis fields from one durable open position."""

    size_tokens: Decimal
    avg_entry_price: Decimal
    token_id: str


class PositionReader(Protocol):
    """Read complete held inventory for one follower account."""

    async def list_held_positions(self, user_id: str) -> Sequence[PositionRecord]:
        """Return positive holdings including settled redemption claims."""

        ...


class ReservationReader(Protocol):
    """Read active local BUY collateral reservations."""

    async def sum_active_quote_for_user(self, user_id: str) -> Decimal:
        """Return exact currently reserved quote collateral."""

        ...


class TradingContextPortfolioSource:
    """Read one follower from existing balance and durable repository ports."""

    def __init__(
        self,
        *,
        follower_account: str,
        wallet: str,
        positions: PositionReader,
        reservations: ReservationReader,
        balance: BalanceReader,
        quotes: MarketQuoteSource | None = None,
        quote_max_age_seconds: float = 1,
    ) -> None:
        """Store one follower's narrow existing authorities.

        Args:
            follower_account: Canonical account/user scope.
            wallet: Bound collateral wallet alias.
            positions: Complete durable held-inventory reader.
            reservations: Durable active-reservation reader.
            balance: Existing collateral adapter.
        """

        self._follower_account = str(follower_account).strip()
        self._wallet = str(wallet).strip()
        if not self._follower_account or not self._wallet:
            raise ValueError("portfolio follower account and wallet are required")
        self._positions = positions
        self._reservations = reservations
        self._balance = balance
        self._quotes = quotes
        self._marks = CachedMarkPolicy(quote_max_age_seconds)

    async def read(self) -> PortfolioAccountSnapshot:
        """Return one exact cost-basis snapshot or fail without partial data."""

        cash_raw = await self._balance.read(self._wallet)
        if cash_raw is None:
            raise RuntimeError("follower collateral is unavailable")
        positions = tuple(await self._positions.list_held_positions(self._follower_account))
        reserved_raw = await self._reservations.sum_active_quote_for_user(self._follower_account)
        cash = self._decimal(cash_raw, name="cash")
        reserved = self._decimal(reserved_raw, name="reserved")
        positions_cost = Decimal(0)
        inventory_value = Decimal(0)
        settlement_value = Decimal(0)
        settlement_complete = True
        active_position_count = 0
        costs_complete = marks_complete = True
        for position in positions:
            shares = self._decimal(position.size_tokens, name="position size")
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
                    marks_complete = settlement_complete = False
                else:
                    value = shares * settlement
                    settlement_value += value
                    inventory_value += value
                continue
            active_position_count += 1
            entry = PositionCostBasis.from_value(position.avg_entry_price).entry_price
            mark = (
                self._marks.midpoint(self._quotes, position.token_id)
                if self._quotes is not None
                else None
            )
            if entry is None:
                costs_complete = False
            else:
                positions_cost += shares * entry
            if mark is None:
                marks_complete = False
            else:
                inventory_value += shares * mark
        return PortfolioAccountSnapshot(
            follower_account=self._follower_account,
            aliases=(self._wallet,),
            cash_usdc=cash,
            reserved_usdc=reserved,
            positions_cost_usdc=positions_cost if costs_complete else None,
            marked_inventory_usdc=inventory_value if marks_complete else None,
            settled_inventory_usdc=settlement_value if settlement_complete else None,
            position_count=active_position_count,
        )

    @staticmethod
    def _decimal(value: object, *, name: str) -> Decimal:
        """Return one non-negative finite exact decimal."""

        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"portfolio {name} must be decimal-like") from exc
        if not result.is_finite() or result < 0:
            raise ValueError(f"portfolio {name} must be non-negative and finite")
        return result


@dataclass(frozen=True, slots=True)
class PortfolioAccountBinding:
    """Bind one follower identity to its existing durable readers."""

    follower_account: str
    wallet: str
    positions: PositionReader
    reservations: ReservationReader
    quotes: MarketQuoteSource | None = None
    quote_max_age_seconds: float = 1

    def __post_init__(self) -> None:
        """Reject empty identities before background refresh starts."""

        if not str(self.follower_account).strip() or not str(self.wallet).strip():
            raise ValueError("portfolio account binding identities are required")


@dataclass(frozen=True, slots=True)
class PortfolioRuntimeBundle:
    """Own the local portfolio cache and its isolated refresh lifecycle."""

    cache: PortfolioSnapshotCache
    refresh: PortfolioRefreshService


class PortfolioRuntimeFactory:
    """Compose cached portfolio services from explicit account bindings."""

    def __init__(self, *, clock: Clock, balance: BalanceReader) -> None:
        """Store shared deterministic time and collateral reader boundaries.

        Args:
            clock: Aware wall-clock source for snapshot age.
            balance: Existing follower collateral read adapter.
        """

        self._clock = clock
        self._balance = balance

    def build(
        self,
        *,
        bindings: tuple[PortfolioAccountBinding, ...],
        refresh_interval_seconds: float,
        stale_after_seconds: float,
    ) -> PortfolioRuntimeBundle | None:
        """Return one portfolio runtime or ``None`` for no bound accounts.

        Args:
            bindings: Fully bound canonical account repository inputs.
            refresh_interval_seconds: Positive background refresh cadence.
            stale_after_seconds: Positive maximum usable snapshot age.

        Returns:
            An unstarted runtime bundle, or ``None`` when no account has all
            required authorities.
        """

        if not isinstance(bindings, tuple):
            raise TypeError("portfolio account bindings must be a tuple")
        if not bindings:
            return None
        cache = PortfolioSnapshotCache(
            clock=self._clock,
            stale_after=timedelta(seconds=stale_after_seconds),
        )
        sources = tuple(
            TradingContextPortfolioSource(
                follower_account=binding.follower_account,
                wallet=binding.wallet,
                positions=binding.positions,
                reservations=binding.reservations,
                balance=self._balance,
                quotes=binding.quotes,
                quote_max_age_seconds=binding.quote_max_age_seconds,
            )
            for binding in bindings
        )
        refresh = PortfolioRefreshService(
            sources=sources,
            cache=cache,
            refresh_interval_seconds=refresh_interval_seconds,
        )
        return PortfolioRuntimeBundle(cache=cache, refresh=refresh)


__all__ = [
    "BalanceReader",
    "PortfolioAccountBinding",
    "PortfolioRuntimeBundle",
    "PortfolioRuntimeFactory",
    "PositionReader",
    "PositionRecord",
    "ReservationReader",
    "TradingContextPortfolioSource",
]
