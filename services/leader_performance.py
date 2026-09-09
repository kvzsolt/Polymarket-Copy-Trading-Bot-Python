"""Scoped performance from confirmed flat-to-flat follower position episodes.

Partial exits contribute to the open episode, never to independent win counts.
Inventory with incomplete initial cost evidence is excluded explicitly.
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from domain.detection import TradeSide
from domain.performance import PerformanceUnits


@dataclass(frozen=True, slots=True)
class FollowerLeaderKey:
    """Exact follower account and normalized leader scope, optionally one task."""

    follower_account: str
    leader_wallet: str
    task_id: str | None = None

    def __post_init__(self) -> None:
        if not self.follower_account.strip():
            raise ValueError("Follower account is required")
        leader = self.leader_wallet.strip().lower()
        if (
            len(leader) != 42
            or not leader.startswith("0x")
            or any(char not in "0123456789abcdef" for char in leader[2:])
        ):
            raise ValueError("Leader must be a hexadecimal wallet address")
        if self.task_id is not None and not self.task_id.strip():
            raise ValueError("Task identity cannot be empty")
        object.__setattr__(self, "leader_wallet", leader)


@dataclass(frozen=True, slots=True)
class PerformanceFill:
    """One durable, deduplicated fill; fees are collateral units."""

    fill_id: str
    token_id: str
    task_id: str | None
    side: TradeSide
    shares: Decimal
    price: Decimal
    fee: Decimal | None

    def __post_init__(self) -> None:
        if not self.fill_id or not self.token_id or not isinstance(self.side, TradeSide):
            raise ValueError("Fill identity, token identity, and side are required")
        for name in ("shares", "price", "fee"):
            if name == "fee" and self.fee is None:
                continue
            value = PerformanceUnits.finite(getattr(self, name))
            if value is None or value < 0 or (name == "shares" and value == 0):
                raise ValueError("Fill money must be non-negative with positive shares")
            object.__setattr__(self, name, value)
        if self.price > 1:
            raise ValueError("Fill price must not exceed one")


@dataclass(frozen=True, slots=True)
class ClosedEpisodeStats:
    """Observed closed episodes, excluding scopes with incomplete inventory."""

    closed_episodes: int = 0
    winning_episodes: int = 0
    realized_pnl: Decimal = Decimal(0)
    excluded_scopes: int = 0

    def __post_init__(self) -> None:
        if not 0 <= self.winning_episodes <= self.closed_episodes or self.excluded_scopes < 0:
            raise ValueError("Episode counts must be non-negative and consistent")
        if not self.realized_pnl.is_finite():
            raise ValueError("Episode PnL must be finite")

    @property
    def win_rate_fraction(self) -> Decimal | None:
        """Return the observed win fraction, or missing for a cold start."""
        if self.closed_episodes == 0:
            return None
        return Decimal(self.winning_episodes) / Decimal(self.closed_episodes)


class LeaderPerformanceReader(Protocol):
    """Read financial evidence only for the requested attribution scope."""

    async def get_closed_episode_stats(self, key: FollowerLeaderKey) -> ClosedEpisodeStats:
        """Return observed closed episodes from confirmed durable fills."""
        ...


@dataclass(frozen=True, slots=True)
class _Episode:
    shares: Decimal = Decimal(0)
    cash_flow: Decimal = Decimal(0)


class ClosedEpisodeAccumulator:
    """Reduce ordered, deduplicated fills independently by task and token."""

    def __init__(self) -> None:
        self._open: dict[tuple[str | None, str], _Episode] = {}
        self._excluded: set[tuple[str | None, str]] = set()
        self._closed = 0
        self._wins = 0
        self._pnl = Decimal(0)

    def apply(self, fill: PerformanceFill) -> None:
        """Apply confirmed evidence; an unexplained sell invalidates its scope."""
        scope = (fill.task_id, fill.token_id)
        if fill.fee is None:
            self._excluded.add(scope)
            self._open.pop(scope, None)
            return
        if scope in self._excluded:
            return
        episode = self._open.get(scope, _Episode())
        if fill.side is TradeSide.BUY:
            if fill.price == 0:
                self._excluded.add(scope)
                self._open.pop(scope, None)
                return
            self._open[scope] = _Episode(
                episode.shares + fill.shares,
                episode.cash_flow - fill.shares * fill.price - fill.fee,
            )
            return
        if fill.shares > episode.shares:
            self._excluded.add(scope)
            self._open.pop(scope, None)
            return
        shares = episode.shares - fill.shares
        cash_flow = episode.cash_flow + fill.shares * fill.price - fill.fee
        if shares:
            self._open[scope] = _Episode(shares, cash_flow)
            return
        self._open.pop(scope, None)
        self._closed += 1
        self._wins += int(cash_flow > 0)
        self._pnl += cash_flow

    @property
    def stats(self) -> ClosedEpisodeStats:
        """Return immutable closed-episode totals; open inventory is excluded."""
        return ClosedEpisodeStats(self._closed, self._wins, self._pnl, len(self._excluded))
