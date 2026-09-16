"""Immutable read projections for the extended operator control plane.

These views back the detection, fill, PnL, risk, leader, and account reads.
They live beside :mod:`services.control_plane.contracts` rather than inside it
because that module already carries the task, order, settings, and runtime
contracts; the validation primitives are shared, the vocabularies are not.

Money, prices, and share counts cross this boundary as validated decimal
strings, timestamps are timezone-aware, and any value the process cannot
observe is ``None`` rather than a substituted default.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime

from services.control_plane.contracts import (
    ContractValidation,
    ControlPlaneError,
    ControlPlaneReason,
)


# Accepted ``PnlView.unrealized_basis`` values. ``wss_mid`` means every open
# position carried a fresh two-sided cached quote; ``partial_wss_mid`` means
# only some did; ``truncated`` means the book exceeded the rendered ceiling;
# ``unavailable`` means nothing could be marked. Only ``wss_mid`` permits an
# account-level total, and no basis permits inventing a mark.
PNL_VALUATION_BASES = (
    "wss_mid",
    "partial_wss_mid",
    "truncated",
    "unavailable",
    "unknown_cost_basis",
)


@dataclass(frozen=True, slots=True)
class AccountView:
    """One composed follower account and the leaders bound to it."""

    follower_account: str
    wallet_address: str
    leader_count: int

    def __post_init__(self) -> None:
        """Validate account identity and a non-negative leader count."""

        ContractValidation.require_text(self.follower_account)
        ContractValidation.require_text(self.wallet_address)
        ContractValidation.require_count(self.leader_count)


@dataclass(frozen=True, slots=True)
class DetectionSourceStatusView:
    """Local signal counters for exactly one detection source."""

    source: str
    signals_total: int
    deduplicated_total: int
    last_signal_at: datetime | None

    def __post_init__(self) -> None:
        """Validate the source code, counters, and optional timestamp."""

        ContractValidation.require_text(self.source)
        ContractValidation.require_count(self.signals_total)
        ContractValidation.require_count(self.deduplicated_total)
        if self.last_signal_at is not None:
            ContractValidation.require_aware_datetime(self.last_signal_at)


@dataclass(frozen=True, slots=True)
class PendingConfirmationStatsView:
    """Bounded state of the provisional-copy confirmation tracker."""

    tracked: int
    running: bool
    deadline_seconds: float
    max_tracked: int

    def __post_init__(self) -> None:
        """Validate tracker counts and a finite positive deadline."""

        ContractValidation.require_count(self.tracked)
        ContractValidation.require_count(self.max_tracked)
        if not math.isfinite(self.deadline_seconds) or self.deadline_seconds <= 0:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)


@dataclass(frozen=True, slots=True)
class DetectionStatusView:
    """Detection-source health assembled from local counters and readiness.

    ``leader_ws_connected`` is ``None`` when no detection transport probe is
    attached, which is different from a probed disconnected transport.
    """

    mode: str
    sources: tuple[DetectionSourceStatusView, ...]
    leader_ws_connected: bool | None
    market_ws_ok: bool
    user_ws_ok: bool
    market_ws_reconnects: int
    user_ws_reconnects: int
    pending_confirmations: PendingConfirmationStatsView | None
    watched_leader_count: int
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate the mode, bounded source list, and counters."""

        ContractValidation.require_text(self.mode)
        if not isinstance(self.sources, tuple) or len(self.sources) > 32:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_count(self.market_ws_reconnects)
        ContractValidation.require_count(self.user_ws_reconnects)
        ContractValidation.require_count(self.watched_leader_count)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class FillView:
    """One durable fill projection using exact decimal strings.

    ``filled_at`` is nullable because the durable fill timestamp is nullable:
    a row without one reports ``None`` instead of failing the whole page.
    Money fields stay required — a fill with no recorded size or price is
    corrupt, and the read fails closed rather than substituting a number.
    """

    fill_id: str
    follower_account: str
    leader_wallet: str | None
    order_id: str | None
    condition_id: str | None
    token_id: str | None
    side: str | None
    size: str
    price: str
    fee: str | None
    filled_at: datetime | None
    created_at: datetime

    def __post_init__(self) -> None:
        """Validate identity, optional attribution, and exact money text."""

        ContractValidation.require_text(self.fill_id)
        ContractValidation.require_text(self.follower_account)
        for optional_value in (
            self.leader_wallet,
            self.order_id,
            self.condition_id,
            self.token_id,
            self.side,
        ):
            if optional_value is not None:
                ContractValidation.require_text(optional_value)
        for value in (self.size, self.price):
            ContractValidation.require_decimal_string(value)
        ContractValidation.optional_decimal_string(self.fee)
        if self.filled_at is not None:
            ContractValidation.require_aware_datetime(self.filled_at)
        ContractValidation.require_aware_datetime(self.created_at)


@dataclass(frozen=True, slots=True)
class PnlSummaryView:
    """Session-scoped realized ledger metrics for one follower account.

    Every value comes from the in-memory :class:`~services.pnlTracker.PnLTracker`
    ledger, so it covers this process session only and is reset by a restart.
    """

    realized_pnl_usdc: str | None
    total_invested_usdc: str
    total_returned_usdc: str
    max_drawdown_usdc: str | None
    trade_count: int
    buy_count: int
    sell_count: int
    win_count: int
    loss_count: int
    session_started_at: datetime
    realized_basis: str = "gross_before_fees"
    net_realized_pnl_usdc: str | None = None

    def __post_init__(self) -> None:
        """Validate exact ledger money text and non-negative counts."""

        for value in (
            self.total_invested_usdc,
            self.total_returned_usdc,
        ):
            ContractValidation.require_decimal_string(value)
        ContractValidation.optional_decimal_string(self.max_drawdown_usdc)
        ContractValidation.optional_decimal_string(self.realized_pnl_usdc)
        ContractValidation.optional_decimal_string(self.net_realized_pnl_usdc)
        if self.realized_basis != "gross_before_fees":
            raise ValueError("Session realized PnL is gross before fees")
        for count in (
            self.trade_count,
            self.buy_count,
            self.sell_count,
            self.win_count,
            self.loss_count,
        ):
            ContractValidation.require_count(count)
        ContractValidation.require_aware_datetime(self.session_started_at)


@dataclass(frozen=True, slots=True)
class PnlPositionView:
    """One open position valued only from a fresh cached WSS mid quote.

    ``mark_price``, ``market_value_usdc``, and ``unrealized_pnl_usdc`` stay
    ``None`` whenever no fresh two-sided quote exists. The control plane never
    substitutes a cost basis, a stale book, or a venue call for a mark.
    """

    condition_id: str
    token_id: str
    leader_wallet: str | None
    size: str
    avg_entry_price: str | None
    cost_basis_usdc: str | None
    mark_price: str | None
    market_value_usdc: str | None
    unrealized_pnl_usdc: str | None

    def __post_init__(self) -> None:
        """Validate identity plus exact required and optional money text."""

        ContractValidation.require_text(self.condition_id)
        ContractValidation.require_text(self.token_id)
        if self.leader_wallet is not None:
            ContractValidation.require_text(self.leader_wallet)
        ContractValidation.require_decimal_string(self.size)
        ContractValidation.optional_decimal_string(self.avg_entry_price)
        ContractValidation.optional_decimal_string(self.cost_basis_usdc)
        for optional_value in (
            self.mark_price,
            self.market_value_usdc,
            self.unrealized_pnl_usdc,
        ):
            ContractValidation.optional_decimal_string(optional_value)


@dataclass(frozen=True, slots=True)
class PnlView:
    """Realized session PnL paired with honestly marked open positions.

    ``position_count``, ``cost_basis_usdc``, and ``marked_position_count``
    always describe the complete open book. ``positions`` may be truncated to
    a rendering ceiling, which ``truncated`` reports; an account-level
    ``unrealized_pnl_usdc`` is published only when the client can reconcile it
    against the rendered rows, so it stays ``None`` unless every open position
    is both rendered and marked.
    """

    follower_account: str
    realized: PnlSummaryView
    positions: tuple[PnlPositionView, ...]
    position_count: int
    truncated: bool
    cost_basis_usdc: str | None
    unrealized_pnl_usdc: str | None
    unrealized_basis: str
    marked_position_count: int
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate the valuation basis, bounded positions, and money text."""

        ContractValidation.require_text(self.follower_account)
        if not isinstance(self.positions, tuple) or len(self.positions) > 500:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_count(self.position_count)
        ContractValidation.require_count(self.marked_position_count)
        if self.truncated != (len(self.positions) < self.position_count):
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        if self.truncated and self.unrealized_pnl_usdc is not None:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.optional_decimal_string(self.cost_basis_usdc)
        ContractValidation.optional_decimal_string(self.unrealized_pnl_usdc)
        if self.unrealized_basis not in PNL_VALUATION_BASES:
            raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class RiskPolicyView:
    """Effective account-level risk limits as configured for this process."""

    name: str
    risk_management_enabled: bool
    tiered_policy_enabled: bool
    kill_switch: bool
    max_open_positions: int
    max_market_exposure_usdc: str
    max_category_exposure_usdc: str
    max_leader_exposure_usdc: str
    max_daily_drawdown_pct: str
    reserve_pct: str
    reserve_floor_usdc: str
    hard_cap_pct: str
    hard_cap_ceiling_usdc: str
    allow_partial_exits: bool
    allow_pyramiding: bool

    def __post_init__(self) -> None:
        """Validate the policy name, the position cap, and exact limits."""

        ContractValidation.require_text(self.name)
        ContractValidation.require_count(self.max_open_positions)
        for value in (
            self.max_market_exposure_usdc,
            self.max_category_exposure_usdc,
            self.max_leader_exposure_usdc,
            self.max_daily_drawdown_pct,
            self.reserve_pct,
            self.reserve_floor_usdc,
            self.hard_cap_pct,
            self.hard_cap_ceiling_usdc,
        ):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class RiskBlockView:
    """One active durable risk block on a follower position."""

    follower_account: str
    condition_id: str
    token_id: str
    reason: str
    source: str
    leader_wallet: str | None
    triggered_at: datetime
    expires_at: datetime | None

    def __post_init__(self) -> None:
        """Validate block identity, reason vocabulary, and timestamps."""

        for value in (
            self.follower_account,
            self.condition_id,
            self.token_id,
            self.reason,
            self.source,
        ):
            ContractValidation.require_text(value)
        if self.leader_wallet is not None:
            ContractValidation.require_text(self.leader_wallet)
        ContractValidation.require_aware_datetime(self.triggered_at)
        if self.expires_at is not None:
            ContractValidation.require_aware_datetime(self.expires_at)


@dataclass(frozen=True, slots=True)
class RiskTriggerView:
    """One recorded protective-exit trigger and its fill status."""

    trigger_id: str
    follower_account: str
    condition_id: str
    token_id: str
    reason: str
    source: str
    fill_status: str
    fill_order_id: str | None
    triggered_at: datetime

    def __post_init__(self) -> None:
        """Validate trigger identity, status vocabulary, and timestamp."""

        for value in (
            self.trigger_id,
            self.follower_account,
            self.condition_id,
            self.token_id,
            self.reason,
            self.source,
            self.fill_status,
        ):
            ContractValidation.require_text(value)
        if self.fill_order_id is not None:
            ContractValidation.require_text(self.fill_order_id)
        ContractValidation.require_aware_datetime(self.triggered_at)


@dataclass(frozen=True, slots=True)
class RiskHeadroomView:
    """Account-level BUY headroom recomputed from one cached snapshot.

    The projection applies the account cap, reserve, and daily-drawdown gates
    only. Per-market, per-category, and per-leader exposure are unknown to a
    cached portfolio snapshot, so this is an upper bound on the cap the trade
    pipeline computes immediately before a real submission.
    """

    allowed: bool
    reason: str
    policy_name: str
    cap_notional_usdc: str
    equity_usdc: str
    free_cash_usdc: str
    daily_loss_usdc: str | None
    open_position_count: int

    def __post_init__(self) -> None:
        """Validate the decision vocabulary and exact money inputs."""

        ContractValidation.require_text(self.reason)
        ContractValidation.require_text(self.policy_name)
        for value in (
            self.cap_notional_usdc,
            self.equity_usdc,
            self.free_cash_usdc,
        ):
            ContractValidation.require_decimal_string(value)
        ContractValidation.optional_decimal_string(self.daily_loss_usdc)
        ContractValidation.require_count(self.open_position_count)


@dataclass(frozen=True, slots=True)
class RiskStateView:
    """Configured policy, active blocks, recent triggers, and headroom."""

    follower_account: str
    policy: RiskPolicyView
    active_blocks: tuple[RiskBlockView, ...]
    recent_triggers: tuple[RiskTriggerView, ...]
    headroom: RiskHeadroomView | None
    day_start_equity_usdc: str | None
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate bounded collections and the optional day-start equity."""

        for collection in (self.active_blocks, self.recent_triggers):
            if not isinstance(collection, tuple) or len(collection) > 200:
                raise ControlPlaneError(ControlPlaneReason.INTERNAL_ERROR)
        ContractValidation.require_text(self.follower_account)
        ContractValidation.optional_decimal_string(self.day_start_equity_usdc)
        ContractValidation.require_aware_datetime(self.observed_at)


@dataclass(frozen=True, slots=True)
class LeaderView:
    """One leader identity merged from configuration, roster, and counters."""

    wallet: str
    configured: bool
    monitored: bool
    tier: str | None
    score: str | None
    signals_total: int
    deduplicated_total: int
    last_signal_at: datetime | None
    trade_count: int
    open_position_count: int

    def __post_init__(self) -> None:
        """Validate wallet identity, optional roster fields, and counters."""

        ContractValidation.require_text(self.wallet)
        if self.tier is not None:
            ContractValidation.require_text(self.tier)
        ContractValidation.optional_decimal_string(self.score)
        for count in (
            self.signals_total,
            self.deduplicated_total,
            self.trade_count,
            self.open_position_count,
        ):
            ContractValidation.require_count(count)
        if self.last_signal_at is not None:
            ContractValidation.require_aware_datetime(self.last_signal_at)


__all__ = [
    "AccountView",
    "DetectionSourceStatusView",
    "DetectionStatusView",
    "FillView",
    "LeaderView",
    "PNL_VALUATION_BASES",
    "PendingConfirmationStatsView",
    "PnlPositionView",
    "PnlSummaryView",
    "PnlView",
    "RiskBlockView",
    "RiskHeadroomView",
    "RiskPolicyView",
    "RiskStateView",
    "RiskTriggerView",
]


@dataclass(frozen=True, slots=True)
class CopyPerformanceView:
    """Persisted source-specific evaluation with exact money and evidence age."""

    evaluation_id: str
    observed_at: datetime
    started_at: datetime
    closed_episodes: int
    realized_pnl: str
    recent_pnl: str
    drawdown: str | None
    complete: bool

    def __post_init__(self) -> None:
        """Validate exact transport money and temporal evidence."""
        ContractValidation.require_text(self.evaluation_id)
        ContractValidation.require_aware_datetime(self.observed_at)
        ContractValidation.require_aware_datetime(self.started_at)
        ContractValidation.require_count(self.closed_episodes)
        ContractValidation.optional_decimal_string(self.drawdown)
        for value in (self.realized_pnl, self.recent_pnl):
            ContractValidation.require_decimal_string(value)


@dataclass(frozen=True, slots=True)
class AutoTraderEvaluationView:
    """One saved membership revision with separate paper and live evidence."""

    revision: int
    status: str
    falcon_score: str
    falcon_observed_at: datetime | None
    degradation_reason: str
    paper: CopyPerformanceView | None
    live: CopyPerformanceView | None

    def __post_init__(self) -> None:
        """Validate exact transport score and revision."""
        ContractValidation.require_count(self.revision)
        ContractValidation.require_decimal_string(self.falcon_score)
        if self.falcon_observed_at is not None:
            ContractValidation.require_aware_datetime(self.falcon_observed_at)


@dataclass(frozen=True, slots=True)
class AutoTraderPerformanceView:
    """Durable membership and the evaluation required for operator promotion."""

    follower_account: str
    trading_mode: str
    leader_wallet: str
    status: str
    revision: int
    target_count: int
    active_count: int
    activation_source: str
    entry_block_reason: str
    falcon_score: str
    falcon_observed_at: datetime | None
    degradation_reason: str
    qualification_reasons: tuple[str, ...]
    paper: CopyPerformanceView | None
    live: CopyPerformanceView | None

    history: tuple[AutoTraderEvaluationView, ...] = ()

    def __post_init__(self) -> None:
        """Validate the public source and membership identities."""
        ContractValidation.require_text(self.follower_account)
        ContractValidation.require_text(self.leader_wallet)
        ContractValidation.require_count(self.revision)
        ContractValidation.require_count(self.target_count)
        ContractValidation.require_count(self.active_count)
        ContractValidation.require_decimal_string(self.falcon_score)
        if self.falcon_observed_at is not None:
            ContractValidation.require_aware_datetime(self.falcon_observed_at)
