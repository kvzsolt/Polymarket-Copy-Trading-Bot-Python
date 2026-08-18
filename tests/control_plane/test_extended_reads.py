"""Read-only query services for detection, fills, PnL, risk, leaders, metrics.

Every service here must answer from local process state or one injected
database port. No test in this module may allow a venue, network, or clock
dependency to leak into a read path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from config.app_config import AppConfig, RiskConfig
from services.control_plane.contracts import (
    AccountView,
    ControlPlaneError,
    ControlPlaneReason,
    FillView,
    Page,
    PageRequest,
    PendingConfirmationStatsView,
    PortfolioView,
    RiskBlockView,
    RiskTriggerView,
)
from services.control_plane.queries_monitoring import (
    AccountQueryService,
    DetectionQueryService,
    LeaderQueryService,
    MetricsQueryService,
)
from services.control_plane.queries_trading import (
    FillQueryService,
    PnlQueryService,
    RiskQueryService,
)
from services.healthServer import ReadinessTracker
from services.metrics import METRIC_WSS_RECONNECT, MetricsRegistry
from services.telemetry import TelemetryHub, TelemetryQueryService, TelemetryService
from services.telemetry.emitters import DetectionTelemetryEmitter


NOW = datetime(2026, 8, 18, 12, 0, tzinfo=UTC)
SIGNAL_EPOCH = 1_766_000_000.0
LEADER = "0xC4Ac0000000000000000000000000000000A9722"
OTHER_LEADER = "0x3186000000000000000000000000000000E09CA1"


class FixedClock:
    """Return one deterministic aware instant."""

    def now(self) -> datetime:
        """Return the fixed observation instant."""

        return NOW


@dataclass(frozen=True, slots=True)
class FakePosition:
    """Structural stand-in for one durable open position record."""

    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal


class FakePositions:
    """Return scripted open positions without touching PostgreSQL."""

    def __init__(self, positions: Mapping[str, Sequence[FakePosition]]) -> None:
        self._positions = positions
        self.calls: list[str] = []

    async def list_open_positions(self, user_id: str) -> Sequence[FakePosition]:
        """Record the follower scope and return its positions."""

        self.calls.append(user_id)
        return tuple(self._positions.get(user_id, ()))


@dataclass(frozen=True, slots=True)
class FakeQuote:
    """Structural stand-in for one cached WSS quote snapshot."""

    best_bid: Decimal | None
    best_ask: Decimal | None
    fresh: bool

    def is_fresh(self, max_age_seconds: float) -> bool:
        """Return the scripted freshness verdict."""

        del max_age_seconds
        return self.fresh


class FakeMarketStore:
    """Serve cached quotes for one follower runtime."""

    def __init__(self, quotes: Mapping[str, FakeQuote]) -> None:
        self._quotes = quotes

    def get_quote(self, token_id: str) -> FakeQuote | None:
        """Return one cached quote or ``None``."""

        return self._quotes.get(token_id)


class FakePnlTracker:
    """Return one deterministic session-scoped metrics mapping."""

    def __init__(self, **overrides: object) -> None:
        self._metrics: dict[str, object] = {
            "total_trades": 4,
            "total_buys": 3,
            "total_sells": 1,
            "wins": 1,
            "losses": 0,
            "win_rate": 100.0,
            "total_invested": Decimal("20.00"),
            "total_returned": Decimal("12.50"),
            "net_pnl": Decimal("2.50"),
            "max_drawdown": Decimal("0.00"),
        }
        self._metrics.update(overrides)

    def metrics(self) -> Mapping[str, object]:
        """Return the scripted running ledger metrics."""

        return dict(self._metrics)


class FakeRiskPort:
    """Return scripted risk projections from one injected database port."""

    def __init__(
        self,
        *,
        blocks: tuple[RiskBlockView, ...] = (),
        triggers: tuple[RiskTriggerView, ...] = (),
    ) -> None:
        self.blocks = blocks
        self.triggers = triggers
        self.calls: list[tuple[str, str, int]] = []

    async def list_active_risk_blocks(self, follower_account: str) -> tuple[RiskBlockView, ...]:
        """Return the scripted active blocks."""

        self.calls.append(("blocks", follower_account, 0))
        return self.blocks

    async def list_recent_risk_triggers(
        self,
        follower_account: str,
        limit: int,
    ) -> tuple[RiskTriggerView, ...]:
        """Return the scripted recent triggers."""

        self.calls.append(("triggers", follower_account, limit))
        return self.triggers


class FakePortfolioCache:
    """Return one cached portfolio snapshot without any I/O."""

    def __init__(self, snapshot: PortfolioView | None) -> None:
        self._snapshot = snapshot
        self.calls: list[str] = []

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return the scripted snapshot."""

        self.calls.append(follower_account)
        return self._snapshot


class FakeRoster:
    """Return one scripted discovery roster."""

    def __init__(self, records: Sequence[object]) -> None:
        self._records = tuple(records)

    async def list_roster(self) -> Sequence[object]:
        """Return the scripted roster records."""

        return self._records


@dataclass(frozen=True, slots=True)
class FakeRosterRecord:
    """Structural stand-in for one persisted roster row."""

    wallet: str
    tier: str
    score: float


class FakeLeaderCounts:
    """Return scripted per-leader aggregate counts."""

    def __init__(
        self,
        *,
        trades: Mapping[str, int],
        positions: Mapping[str, int],
    ) -> None:
        self._trades = trades
        self._positions = positions

    async def leader_trade_counts(self) -> Mapping[str, int]:
        """Return the scripted per-leader fill counts."""

        return dict(self._trades)

    async def leader_open_position_counts(self) -> Mapping[str, int]:
        """Return the scripted per-leader open-position counts."""

        return dict(self._positions)


class FakeFillPort:
    """Return one scripted fill page."""

    def __init__(self, page: Page[FillView]) -> None:
        self._page = page
        self.calls: list[tuple[str, PageRequest]] = []

    async def list_fills(self, follower_account: str, page: PageRequest) -> Page[FillView]:
        """Record the query scope and return the scripted page."""

        self.calls.append((follower_account, page))
        return self._page


def build_config(**risk_overrides: object) -> AppConfig:
    """Return a minimal valid configuration for risk projections."""

    return AppConfig(risk=RiskConfig(**risk_overrides))  # type: ignore[arg-type]


def detection_counters() -> DetectionTelemetryEmitter:
    """Return a null-service emitter carrying deterministic counters."""

    emitter = DetectionTelemetryEmitter(None, clock=lambda: SIGNAL_EPOCH)
    emitter.signal_received("on_chain_pending", LEADER, "0xabc")
    emitter.signal_received("on_chain_pending", LEADER, "0xdef")
    emitter.signal_deduplicated("on_chain_pending", LEADER)
    emitter.signal_received("polling", OTHER_LEADER, "0x123")
    emitter.roster_updated(2)
    return emitter


def fill_view(fill_id: str = "41") -> FillView:
    """Return one exact-decimal fill projection."""

    return FillView(
        fill_id=fill_id,
        follower_account="account-a",
        leader_wallet=LEADER,
        order_id="order-a",
        condition_id="condition-a",
        token_id="token-a",
        side="BUY",
        size="12.5",
        price="0.42",
        fee="0",
        filled_at=NOW,
        created_at=NOW,
    )


def portfolio(**overrides: object) -> PortfolioView:
    """Return one cached portfolio snapshot."""

    values: dict[str, object] = {
        "follower_account": "account-a",
        "cash_usdc": "352.02",
        "reserved_usdc": "0",
        "positions_value_usdc": "40",
        "equity_usdc": "392.02",
        "observed_at": NOW,
        "stale": False,
        "copyable_balance_usdc": "352.02",
        "position_count": 2,
    }
    values.update(overrides)
    return PortfolioView(**values)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------


async def test_account_query_service_returns_composed_accounts_only() -> None:
    """Accounts are explicit composition data, never a database scan."""

    accounts = (
        AccountView(
            follower_account="account-a",
            wallet_address="0x68E60000000000000000000000000000000A83F1",
            leader_count=2,
        ),
    )

    result = await AccountQueryService(accounts).list()

    assert result == accounts
    assert result[0].leader_count == 2


def test_account_view_rejects_a_negative_leader_count() -> None:
    """A defective composition cannot publish an impossible account."""

    with pytest.raises(ControlPlaneError):
        AccountView(
            follower_account="account-a",
            wallet_address="0x68E6",
            leader_count=-1,
        )


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


async def test_detection_status_projects_counters_readiness_and_reconnects() -> None:
    """Detection health is assembled from local counters and readiness only."""

    emitter = detection_counters()
    emitter.attach_connection_probe(lambda: True)
    readiness = ReadinessTracker()
    readiness.update(wss_market_ok=True, wss_user_ok=False, monitored_leader_count=2)
    metrics = MetricsRegistry()
    metrics.inc(METRIC_WSS_RECONNECT, 3.0, {"stream": "market"})
    metrics.inc(METRIC_WSS_RECONNECT, 1.0, {"stream": "user"})

    service = DetectionQueryService(
        counters=emitter,
        readiness=readiness,
        metrics=metrics,
        detection_mode="on_chain_pending",
        pending_stats=lambda: PendingConfirmationStatsView(
            tracked=2,
            running=True,
            deadline_seconds=60.0,
            max_tracked=2048,
        ),
        clock=FixedClock(),
    )

    view = await service.get()

    assert view.mode == "on_chain_pending"
    assert view.observed_at == NOW
    assert view.leader_ws_connected is True
    assert view.market_ws_ok is True
    assert view.user_ws_ok is False
    assert view.market_ws_reconnects == 3
    assert view.user_ws_reconnects == 1
    assert view.watched_leader_count == 2
    assert view.pending_confirmations is not None
    assert view.pending_confirmations.tracked == 2
    sources = {source.source: source for source in view.sources}
    assert set(sources) == {"on_chain_pending", "polling"}
    assert sources["on_chain_pending"].signals_total == 2
    assert sources["on_chain_pending"].deduplicated_total == 1
    assert sources["on_chain_pending"].last_signal_at == datetime.fromtimestamp(
        SIGNAL_EPOCH, tz=UTC
    )
    assert sources["polling"].deduplicated_total == 0


async def test_detection_status_reports_unknown_probe_and_absent_tracker() -> None:
    """An unattached probe and a missing tracker stay honest nulls."""

    service = DetectionQueryService(
        counters=DetectionTelemetryEmitter(None, clock=lambda: SIGNAL_EPOCH),
        readiness=ReadinessTracker(),
        metrics=MetricsRegistry(),
        detection_mode="polling",
        pending_stats=None,
        clock=FixedClock(),
    )

    view = await service.get()

    assert view.leader_ws_connected is None
    assert view.pending_confirmations is None
    assert view.sources == ()
    assert view.market_ws_reconnects == 0
    assert view.watched_leader_count == 0


async def test_detection_status_survives_a_failing_pending_stats_probe() -> None:
    """A defective local probe degrades to ``null``, never to a 500."""

    def explode() -> PendingConfirmationStatsView | None:
        raise RuntimeError("tracker exploded")

    service = DetectionQueryService(
        counters=DetectionTelemetryEmitter(None),
        readiness=ReadinessTracker(),
        metrics=MetricsRegistry(),
        detection_mode="on_chain_pending",
        pending_stats=explode,
        clock=FixedClock(),
    )

    view = await service.get()

    assert view.pending_confirmations is None


# ---------------------------------------------------------------------------
# Fills
# ---------------------------------------------------------------------------


async def test_fill_query_service_delegates_one_bounded_page() -> None:
    """The fill service adds no logic beyond its narrow injected port."""

    port = FakeFillPort(Page(items=(fill_view(),), next_cursor="cursor-a"))
    request = PageRequest(limit=25)

    page = await FillQueryService(port).list("account-a", request)

    assert page.items[0].size == "12.5"
    assert page.next_cursor == "cursor-a"
    assert port.calls == [("account-a", request)]


def test_fill_view_requires_decimal_strings_for_size_price_and_fee() -> None:
    """A non-decimal money field is rejected at the contract boundary."""

    with pytest.raises(ControlPlaneError) as captured:
        FillView(
            fill_id="1",
            follower_account="account-a",
            leader_wallet=None,
            order_id=None,
            condition_id=None,
            token_id=None,
            side=None,
            size="12,5",
            price="0.42",
            fee="0",
            filled_at=NOW,
            created_at=NOW,
        )

    assert captured.value.reason is ControlPlaneReason.INVALID_DECIMAL_STRING


# ---------------------------------------------------------------------------
# PnL
# ---------------------------------------------------------------------------


def pnl_service(
    *,
    quotes: Mapping[str, FakeQuote],
    positions: Sequence[FakePosition],
) -> PnlQueryService:
    """Build one PnL service over fake local state."""

    return PnlQueryService(
        pnl_trackers={"account-a": FakePnlTracker()},
        positions=FakePositions({"account-a": positions}),
        market_stores={"account-a": FakeMarketStore(quotes)},
        quote_max_age_seconds=2.0,
        clock=FixedClock(),
    )


async def test_pnl_marks_positions_only_from_fresh_cached_mid_quotes() -> None:
    """Unrealized PnL exists only where a fresh two-sided WSS mid exists."""

    service = pnl_service(
        quotes={
            "token-a": FakeQuote(best_bid=Decimal("0.50"), best_ask=Decimal("0.60"), fresh=True),
        },
        positions=(
            FakePosition(
                condition_id="condition-a",
                token_id="token-a",
                leader_id=LEADER,
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.40"),
            ),
        ),
    )

    view = await service.get("account-a")

    assert view.follower_account == "account-a"
    assert view.realized.realized_pnl_usdc == "2.50"
    assert view.realized.session_started_at == NOW
    assert view.positions[0].mark_price == "0.55"
    assert view.positions[0].cost_basis_usdc == "4.00"
    assert view.positions[0].market_value_usdc == "5.50"
    assert view.positions[0].unrealized_pnl_usdc == "1.50"
    assert view.unrealized_pnl_usdc == "1.50"
    assert view.unrealized_basis == "wss_mid"
    assert view.marked_position_count == 1
    assert view.position_count == 1


async def test_pnl_reports_null_marks_when_no_fresh_quote_exists() -> None:
    """A stale, one-sided, or absent quote never becomes an invented mark."""

    service = pnl_service(
        quotes={
            "token-a": FakeQuote(best_bid=Decimal("0.50"), best_ask=Decimal("0.60"), fresh=False),
            "token-b": FakeQuote(best_bid=Decimal("0.50"), best_ask=None, fresh=True),
        },
        positions=(
            FakePosition(
                condition_id="condition-a",
                token_id="token-a",
                leader_id=None,
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.40"),
            ),
            FakePosition(
                condition_id="condition-b",
                token_id="token-b",
                leader_id=None,
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.20"),
            ),
            FakePosition(
                condition_id="condition-c",
                token_id="token-c",
                leader_id=None,
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.20"),
            ),
        ),
    )

    view = await service.get("account-a")

    assert [position.mark_price for position in view.positions] == [None, None, None]
    assert [position.unrealized_pnl_usdc for position in view.positions] == [None, None, None]
    assert view.unrealized_pnl_usdc is None
    assert view.unrealized_basis == "unavailable"
    assert view.marked_position_count == 0
    assert view.cost_basis_usdc == "6.00"


async def test_pnl_partial_marks_never_publish_a_whole_account_total() -> None:
    """A partially marked book reports per-position marks and a null total."""

    service = pnl_service(
        quotes={
            "token-a": FakeQuote(best_bid=Decimal("0.50"), best_ask=Decimal("0.60"), fresh=True),
        },
        positions=(
            FakePosition(
                condition_id="condition-a",
                token_id="token-a",
                leader_id=None,
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.40"),
            ),
            FakePosition(
                condition_id="condition-b",
                token_id="token-b",
                leader_id=None,
                size_tokens=Decimal("5"),
                avg_entry_price=Decimal("0.20"),
            ),
        ),
    )

    view = await service.get("account-a")

    assert view.positions[0].unrealized_pnl_usdc == "1.50"
    assert view.positions[1].unrealized_pnl_usdc is None
    assert view.unrealized_pnl_usdc is None
    assert view.unrealized_basis == "partial_wss_mid"
    assert view.marked_position_count == 1


async def test_pnl_rejects_an_unknown_follower_account() -> None:
    """An account without a composed session ledger is a stable not-found."""

    service = pnl_service(quotes={}, positions=())

    with pytest.raises(ControlPlaneError) as captured:
        await service.get("account-unknown")

    assert captured.value.reason is ControlPlaneReason.NOT_FOUND


# ---------------------------------------------------------------------------
# Risk
# ---------------------------------------------------------------------------


def risk_block() -> RiskBlockView:
    """Return one active risk block projection."""

    return RiskBlockView(
        follower_account="account-a",
        condition_id="condition-a",
        token_id="token-a",
        reason="stop_loss",
        source="risk_monitor",
        leader_wallet=None,
        triggered_at=NOW,
        expires_at=None,
    )


def risk_trigger() -> RiskTriggerView:
    """Return one recent risk trigger projection."""

    return RiskTriggerView(
        trigger_id="7",
        follower_account="account-a",
        condition_id="condition-a",
        token_id="token-a",
        reason="stop_loss",
        source="risk_monitor",
        fill_status="filled",
        fill_order_id="order-a",
        triggered_at=NOW,
    )


async def test_risk_state_omits_headroom_without_a_cached_portfolio() -> None:
    """Headroom is never computed from a venue call or an invented balance."""

    port = FakeRiskPort(blocks=(risk_block(),), triggers=(risk_trigger(),))
    service = RiskQueryService(
        port=port,
        portfolio=None,
        config=build_config(),
        clock=FixedClock(),
        day_start_equity=lambda: None,
    )

    view = await service.get("account-a")

    assert view.headroom is None
    assert view.day_start_equity_usdc is None
    assert view.active_blocks == (risk_block(),)
    assert view.recent_triggers == (risk_trigger(),)
    assert view.policy.max_open_positions == 40
    assert view.policy.max_market_exposure_usdc == "300.0"
    assert view.observed_at == NOW
    assert port.calls[0][1] == "account-a"


async def test_risk_state_computes_headroom_from_the_cached_snapshot_only() -> None:
    """Headroom mirrors the pure engine over cached cash, equity, and day start."""

    service = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=FakePortfolioCache(portfolio()),
        config=build_config(enable_balance_tiered_policy=True),
        clock=FixedClock(),
        day_start_equity=lambda: Decimal("400"),
    )

    view = await service.get("account-a")

    assert view.day_start_equity_usdc == "400"
    assert view.headroom is not None
    assert view.headroom.allowed is True
    assert view.headroom.policy_name == "account_cap"
    assert view.headroom.cap_notional_usdc == "8.0"
    assert view.headroom.equity_usdc == "392.02"
    assert view.headroom.free_cash_usdc == "352.02"
    assert view.headroom.daily_loss_usdc == "7.98"
    assert view.headroom.open_position_count == 2


async def test_risk_state_omits_headroom_for_a_stale_cached_snapshot() -> None:
    """A stale cache is not a balance; the read reports no headroom."""

    service = RiskQueryService(
        port=FakeRiskPort(),
        portfolio=FakePortfolioCache(portfolio(stale=True)),
        config=build_config(),
        clock=FixedClock(),
        day_start_equity=lambda: None,
    )

    view = await service.get("account-a")

    assert view.headroom is None


# ---------------------------------------------------------------------------
# Leaders
# ---------------------------------------------------------------------------


async def test_leader_list_merges_configuration_roster_monitoring_and_counters() -> None:
    """One leader identity carries configuration, roster, and local counters."""

    service = LeaderQueryService(
        roster=FakeRoster(
            (
                FakeRosterRecord(wallet=OTHER_LEADER.lower(), tier="active", score=0.75),
                FakeRosterRecord(wallet="0x9999", tier="watchlist", score=0.10),
            )
        ),
        counts=FakeLeaderCounts(
            trades={LEADER.lower(): 4},
            positions={LEADER.lower(): 1},
        ),
        configured=(LEADER,),
        monitored=lambda: (LEADER, OTHER_LEADER),
        counters=detection_counters(),
        clock=FixedClock(),
    )

    leaders = {view.wallet: view for view in await service.list()}

    assert set(leaders) == {LEADER.lower(), OTHER_LEADER.lower(), "0x9999"}
    primary = leaders[LEADER.lower()]
    assert primary.configured is True
    assert primary.monitored is True
    assert primary.tier is None
    assert primary.score is None
    assert primary.signals_total == 2
    assert primary.deduplicated_total == 1
    assert primary.last_signal_at == datetime.fromtimestamp(SIGNAL_EPOCH, tz=UTC)
    assert primary.trade_count == 4
    assert primary.open_position_count == 1
    discovered = leaders[OTHER_LEADER.lower()]
    assert discovered.configured is False
    assert discovered.monitored is True
    assert discovered.tier == "active"
    assert discovered.score == "0.75"
    assert discovered.trade_count == 0
    inactive = leaders["0x9999"]
    assert inactive.monitored is False
    assert inactive.signals_total == 0


async def test_leader_list_is_bounded_and_survives_absent_sources() -> None:
    """The merge caps its output and stays available without a database."""

    service = LeaderQueryService(
        roster=None,
        counts=None,
        configured=tuple(f"0x{index:040x}" for index in range(600)),
        monitored=lambda: (),
        counters=DetectionTelemetryEmitter(None),
        clock=FixedClock(),
    )

    leaders = await service.list()

    assert len(leaders) == 500
    assert all(view.trade_count == 0 for view in leaders)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def test_metrics_snapshot_exposes_counters_gauges_and_histograms() -> None:
    """The metrics read reuses the telemetry projection without event history."""

    metrics = MetricsRegistry()
    hub = TelemetryHub(metrics=metrics)
    service = TelemetryService(hub=hub, metrics=metrics, clock=FixedClock())
    query = TelemetryQueryService(service=service, metrics=metrics, clock=FixedClock())
    metrics.inc(METRIC_WSS_RECONNECT, 2.0, {"stream": "market"})
    metrics.set_gauge("telemetry_subscriptions", 1.0)
    metrics.observe("copy_latency_seconds", 0.25, buckets=(0.1, 0.5))

    snapshot = MetricsQueryService(query).snapshot()

    assert snapshot["generated_at"] == NOW.isoformat()
    assert "events" not in snapshot
    counters = snapshot["counters"]
    assert isinstance(counters, dict)
    assert counters['wss_reconnect_total{stream="market"}'] == 2.0
    gauges = snapshot["gauges"]
    assert isinstance(gauges, dict)
    assert gauges["telemetry_subscriptions"] == 1.0
    histograms = snapshot["histograms"]
    assert isinstance(histograms, list)
    assert histograms[0]["name"] == "copy_latency_seconds"
    assert histograms[0]["count"] == 1
