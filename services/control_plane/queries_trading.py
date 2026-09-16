"""Read-only trading queries for fills, session PnL, and risk state.

Valuation honesty is the rule these services exist to enforce. Realized PnL is
whatever the in-memory session ledger recorded; unrealized PnL exists only
where a fresh two-sided cached WSS quote exists; BUY headroom exists only when
a cached portfolio snapshot exists. Nothing here calls a venue, and the only
database access happens behind one injected control-plane query port.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

from config.app_config import AppConfig
from domain.cost_basis import PositionCostBasis
from services.control_plane.valuation import CachedMarkPolicy, MarketQuoteSource, QuoteLike
from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    Page,
    PageRequest,
    PortfolioView,
)
from services.control_plane.contracts_reads import (
    FillView,
    PnlPositionView,
    PnlSummaryView,
    PnlView,
    RiskBlockView,
    RiskHeadroomView,
    RiskPolicyView,
    RiskStateView,
    RiskTriggerView,
)
from services.control_plane.queries_monitoring import AccountScope
from services.risk.risk_engine import headroom_for_buy


_ZERO = Decimal("0")

# Hard ceilings so one operator read can never scan an unbounded book.
_MAX_PNL_POSITIONS = 200
_MAX_RISK_TRIGGERS = 25


class Clock(Protocol):
    """Provide aware wall-clock time for response timestamps."""

    def now(self) -> datetime:
        """Return the current aware instant."""

        ...


class FillQueryPort(Protocol):
    """Read follower-scoped durable fills."""

    async def list_fills(self, follower_account: str, page: PageRequest) -> Page[FillView]:
        """Return one bounded newest-first fill page."""

        ...


class PnlTrackerPort(Protocol):
    """Read the session-scoped realized ledger for one follower."""

    def metrics(self) -> Mapping[str, object]:
        """Return running realized ledger metrics."""

        ...


class OpenPositionLike(Protocol):
    """Read one durable open-position record."""

    @property
    def condition_id(self) -> str:
        """Return the market identity."""

        ...

    @property
    def token_id(self) -> str:
        """Return the outcome token identity."""

        ...

    @property
    def leader_id(self) -> str | None:
        """Return the attributed leader, when known."""

        ...

    @property
    def size_tokens(self) -> Decimal:
        """Return the exact held share count."""

        ...

    @property
    def avg_entry_price(self) -> Decimal:
        """Return the exact average entry price."""

        ...


class OpenPositionsPort(Protocol):
    """Read durable open positions for one follower account."""

    async def list_open_positions(self, user_id: str) -> Sequence[OpenPositionLike]:
        """Return current non-dust positions."""

        ...


class CachedPortfolioPort(Protocol):
    """Read one locally cached portfolio snapshot without I/O."""

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return the cached snapshot, or ``None`` when never refreshed."""

        ...


class RiskQueryPort(Protocol):
    """Read durable risk blocks and protective-exit triggers."""

    async def list_active_risk_blocks(self, follower_account: str) -> tuple[RiskBlockView, ...]:
        """Return every currently active block for the follower."""

        ...

    async def list_recent_risk_triggers(
        self,
        follower_account: str,
        limit: int,
    ) -> tuple[RiskTriggerView, ...]:
        """Return the newest triggers for the follower."""

        ...


class FillQueryService:
    """Serve durable fills through one narrow keyset-paginated port."""

    def __init__(self, port: FillQueryPort, *, scope: AccountScope | None = None) -> None:
        """Store the durable fill reader and the composed account scope.

        Args:
            port: Control-plane query repository exposing ``list_fills``.
            scope: Optional composed-account resolver. When supplied, an
                account this process did not compose is a stable not-found
                instead of an empty page that looks like "no fills".
        """

        self._port = port
        self._scope = scope

    async def list(self, follower_account: str, page: PageRequest) -> Page[FillView]:
        """Return one bounded follower-scoped fill page.

        Raises:
            ControlPlaneError: If the account is outside the composed scope.
        """

        _require_known_account(self._scope, follower_account)
        return await self._port.list_fills(follower_account, page)


class PnlQueryService:
    """Pair the session realized ledger with cache-only position valuation."""

    def __init__(
        self,
        *,
        pnl_trackers: Mapping[str, PnlTrackerPort],
        positions: OpenPositionsPort,
        market_stores: Mapping[str, MarketQuoteSource],
        quote_max_age_seconds: float,
        clock: Clock,
        max_positions: int = _MAX_PNL_POSITIONS,
    ) -> None:
        """Store per-follower ledgers and the local valuation inputs.

        Args:
            pnl_trackers: Session ledger per follower account.
            positions: Durable open-position reader.
            market_stores: Cached quote source per follower account.
            quote_max_age_seconds: Executable quote age window in seconds.
            clock: Aware UTC clock; also fixes ``session_started_at``.
            max_positions: Hard ceiling on rendered positions.

        Raises:
            ValueError: If the age window or position ceiling is not positive.
        """

        if quote_max_age_seconds <= 0:
            raise ValueError("quote_max_age_seconds must be positive")
        if max_positions <= 0:
            raise ValueError("max_positions must be positive")
        self._pnl_trackers = dict(pnl_trackers)
        self._positions = positions
        self._market_stores = dict(market_stores)
        self._marks = CachedMarkPolicy(quote_max_age_seconds)
        self._clock = clock
        self._max_positions = int(max_positions)
        self._composed_at = clock.now()

    async def get(self, follower_account: str) -> PnlView:
        """Return realized session PnL and honestly marked open positions.

        Args:
            follower_account: Composed follower account scope.

        Returns:
            One immutable PnL projection.

        Raises:
            ControlPlaneError: If the account has no composed session ledger.
        """

        tracker = self._pnl_trackers.get(follower_account)
        if tracker is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        store = self._market_stores.get(follower_account)
        records = tuple(await self._positions.list_open_positions(follower_account))
        positions: list[PnlPositionView] = []
        cost_basis = _ZERO
        cost_complete = True
        unrealized = _ZERO
        marked = 0
        # Aggregates cover the complete book; only the rendered list is capped,
        # so a truncated response still reports an exact cost basis and an
        # exact marked count instead of a partial sum presented as a total.
        for index, record in enumerate(records):
            size = _decimal(record.size_tokens)
            entry = PositionCostBasis.from_value(record.avg_entry_price).entry_price
            position_cost = None if entry is None else size * entry
            if position_cost is None:
                cost_complete = False
            else:
                cost_basis += position_cost
            mark = self._mark(store, record.token_id)
            value = None if mark is None else size * mark
            position_pnl = None if value is None or position_cost is None else value - position_cost
            if position_pnl is not None:
                marked += 1
                unrealized += position_pnl
            if index >= self._max_positions:
                continue
            positions.append(
                PnlPositionView(
                    condition_id=str(record.condition_id),
                    token_id=str(record.token_id),
                    leader_wallet=(None if not record.leader_id else str(record.leader_id)),
                    size=_text(size),
                    avg_entry_price=None if entry is None else _text(entry),
                    cost_basis_usdc=None if position_cost is None else _text(position_cost),
                    mark_price=None if mark is None else _text(mark),
                    market_value_usdc=None if value is None else _text(value),
                    unrealized_pnl_usdc=(None if position_pnl is None else _text(position_pnl)),
                )
            )
        total = len(records)
        truncated = total > len(positions)
        complete = bool(records) and marked == total and not truncated
        return PnlView(
            follower_account=follower_account,
            realized=self._summary(tracker),
            positions=tuple(positions),
            position_count=total,
            truncated=truncated,
            cost_basis_usdc=_text(cost_basis) if cost_complete else None,
            unrealized_pnl_usdc=_text(unrealized) if complete else None,
            unrealized_basis=(
                _valuation_basis(marked=marked, total=total, truncated=truncated)
                if cost_complete
                else "unknown_cost_basis"
            ),
            marked_position_count=marked,
            observed_at=self._clock.now(),
        )

    def _mark(self, store: MarketQuoteSource | None, token_id: str) -> Decimal | None:
        """Return a fresh two-sided cached mid, or ``None``."""

        return self._marks.midpoint(store, token_id)

    def _summary(self, tracker: PnlTrackerPort) -> PnlSummaryView:
        """Project the session ledger into exact decimal strings."""

        metrics = tracker.metrics()
        return PnlSummaryView(
            realized_pnl_usdc=(
                None
                if metrics.get("gross_realized_pnl") is None
                else _text(_decimal(metrics["gross_realized_pnl"]))
            ),
            total_invested_usdc=_text(_decimal(metrics.get("total_invested"))),
            total_returned_usdc=_text(_decimal(metrics.get("total_returned"))),
            max_drawdown_usdc=(
                None
                if metrics.get("max_drawdown") is None
                else _text(_decimal(metrics["max_drawdown"]))
            ),
            trade_count=_count(metrics.get("total_trades")),
            buy_count=_count(metrics.get("total_buys")),
            sell_count=_count(metrics.get("total_sells")),
            win_count=_count(metrics.get("wins")),
            loss_count=_count(metrics.get("losses")),
            session_started_at=self._session_start(tracker),
        )

    def _session_start(self, tracker: PnlTrackerPort) -> datetime:
        """Return when this ledger began recording.

        A ledger that records its own creation instant is authoritative. A
        collaborator without one falls back to the instant this service was
        composed, which is never earlier than the real session start.
        """

        started_at = getattr(tracker, "started_at", None)
        if isinstance(started_at, datetime) and started_at.tzinfo is not None:
            return started_at
        return self._composed_at


class RiskQueryService:
    """Project configured risk limits, durable risk state, and headroom."""

    def __init__(
        self,
        *,
        port: RiskQueryPort,
        portfolio: CachedPortfolioPort | None,
        config: AppConfig,
        clock: Clock,
        day_start_equity: Callable[[str], Decimal | None] | None = None,
        scope: AccountScope | None = None,
        trigger_limit: int = _MAX_RISK_TRIGGERS,
    ) -> None:
        """Store the durable risk port and the local-only headroom inputs.

        Args:
            port: Durable risk block and trigger reader.
            portfolio: Optional cached portfolio snapshot reader.
            config: Effective immutable application configuration.
            clock: Aware UTC observation clock.
            day_start_equity: Optional account-scoped read-only baseline probe.
                The synchronous reader runs on a worker thread so local I/O
                cannot block the trading event loop.
            scope: Optional composed-account resolver.
            trigger_limit: Hard ceiling on returned recent triggers.

        Raises:
            ValueError: If the trigger ceiling is not positive.
        """

        if trigger_limit <= 0:
            raise ValueError("trigger_limit must be positive")
        self._port = port
        self._portfolio = portfolio
        self._config = config
        self._clock = clock
        self._day_start_equity = day_start_equity
        self._scope = scope
        self._trigger_limit = int(trigger_limit)

    async def get(self, follower_account: str) -> RiskStateView:
        """Return one follower-scoped risk projection.

        Raises:
            ControlPlaneError: If the account is outside the composed scope.
        """

        _require_known_account(self._scope, follower_account)
        blocks = await self._port.list_active_risk_blocks(follower_account)
        triggers = await self._port.list_recent_risk_triggers(
            follower_account,
            self._trigger_limit,
        )
        snapshot = await self._snapshot(follower_account)
        day_start = await self._day_start(follower_account)
        return RiskStateView(
            follower_account=follower_account,
            policy=self._policy(),
            active_blocks=tuple(blocks),
            recent_triggers=tuple(triggers),
            headroom=self._headroom(snapshot, day_start),
            day_start_equity_usdc=None if day_start is None else _text(day_start),
            observed_at=self._clock.now(),
        )

    async def _snapshot(self, follower_account: str) -> PortfolioView | None:
        """Return one usable cached snapshot, never a stale or absent one."""

        if self._portfolio is None:
            return None
        snapshot = await self._portfolio.get_cached_portfolio(follower_account)
        if snapshot is None or snapshot.stale:
            return None
        return snapshot

    async def _day_start(self, follower_account: str) -> Decimal | None:
        """Read the explicit configured baseline or this account's observation."""

        configured = _decimal(self._config.risk.day_start_equity_usdc)
        if configured > _ZERO:
            return configured
        probe = self._day_start_equity
        if probe is None:
            return None
        try:
            recorded = await asyncio.to_thread(probe, follower_account)
        except Exception:
            return None
        if recorded is None:
            return None
        parsed = _decimal(recorded)
        return parsed if parsed > _ZERO else None

    def _headroom(
        self,
        snapshot: PortfolioView | None,
        day_start: Decimal | None,
    ) -> RiskHeadroomView | None:
        """Recompute account-level BUY headroom from the cached snapshot."""

        if snapshot is None or snapshot.equity_usdc is None:
            return None
        if self._config.risk.enable_balance_tiered_policy and day_start is None:
            return None
        equity = _decimal(snapshot.equity_usdc)
        free_cash = _decimal(snapshot.cash_usdc)
        daily_loss = _ZERO if day_start is None else max(_ZERO, day_start - equity)
        headroom = headroom_for_buy(
            equity_usdc=equity,
            free_cash_usdc=free_cash,
            open_positions_count=snapshot.position_count,
            daily_loss_usdc=daily_loss,
            day_start_equity_usdc=day_start,
            config=self._config,
        )
        return RiskHeadroomView(
            allowed=headroom.allow,
            reason=headroom.reason,
            policy_name=headroom.policy_name,
            cap_notional_usdc=_text(headroom.cap_notional_usdc),
            equity_usdc=snapshot.equity_usdc,
            free_cash_usdc=snapshot.cash_usdc,
            daily_loss_usdc=None if day_start is None else _text(daily_loss),
            open_position_count=snapshot.position_count,
        )

    def _policy(self) -> RiskPolicyView:
        """Project the effective configured account-risk limits."""

        risk = self._config.risk
        tiered = bool(risk.enable_balance_tiered_policy)
        max_open = risk.balance_policy_max_open_positions if tiered else risk.max_open_positions
        return RiskPolicyView(
            name="account_cap",
            risk_management_enabled=bool(risk.enable_risk_mgmt),
            tiered_policy_enabled=tiered,
            kill_switch=bool(risk.kill_switch),
            max_open_positions=int(max_open),
            max_market_exposure_usdc=_config_text(risk.max_market_exposure_usdc),
            max_category_exposure_usdc=_config_text(risk.max_category_exposure_usdc),
            max_leader_exposure_usdc=_config_text(risk.max_leader_exposure_usdc),
            max_daily_drawdown_pct=_config_text(risk.max_daily_drawdown_pct),
            reserve_pct=_config_text(risk.balance_policy_reserve_pct),
            reserve_floor_usdc=_config_text(risk.balance_policy_reserve_floor),
            hard_cap_pct=_config_text(risk.balance_policy_hard_cap_pct),
            hard_cap_ceiling_usdc=_config_text(risk.balance_policy_hard_cap_ceiling),
            allow_partial_exits=bool(risk.balance_policy_allow_partial_exits),
            allow_pyramiding=bool(risk.balance_policy_allow_pyramiding),
        )


def _require_known_account(scope: AccountScope | None, follower_account: str) -> None:
    """Reject a follower account this process never composed."""

    if scope is not None and not scope.contains(follower_account):
        raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)


def _valuation_basis(*, marked: int, total: int, truncated: bool) -> str:
    """Return the honest valuation basis for a possibly partial book."""

    if total == 0 or marked == 0:
        return "unavailable"
    if truncated:
        return "truncated"
    return "wss_mid" if marked == total else "partial_wss_mid"


def _decimal(value: object) -> Decimal:
    """Return an exact finite Decimal, treating unusable input as zero."""

    if isinstance(value, Decimal):
        return value if value.is_finite() else _ZERO
    if isinstance(value, bool) or value is None:
        return _ZERO
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return _ZERO
    return parsed if parsed.is_finite() else _ZERO


def _config_text(value: object) -> str:
    """Return one configured numeric limit as an exact decimal string."""

    return _text(_decimal(value))


def _text(value: Decimal) -> str:
    """Return the exact plain-notation text of a finite Decimal."""

    return format(value, "f")


def _count(value: object) -> int:
    """Return one non-negative integer count from ledger metrics."""

    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return 0
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return 0


__all__ = [
    "CachedPortfolioPort",
    "Clock",
    "FillQueryPort",
    "FillQueryService",
    "MarketQuoteSource",
    "QuoteLike",
    "OpenPositionLike",
    "OpenPositionsPort",
    "PnlQueryService",
    "PnlTrackerPort",
    "RiskQueryPort",
    "RiskQueryService",
]
