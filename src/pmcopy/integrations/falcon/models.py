"""Exact, tolerant parsers for Falcon prediction-intelligence rows."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from pmcopy.domain.performance import PerformanceUnits
from pmcopy.integrations.falcon.errors import FalconProtocolError


@dataclass(frozen=True, slots=True)
class PnlOutcomeCounts:
    """Agent 569 counts have explicit integer units; its unversioned rate does not."""

    wins: int | None
    losses: int | None

    @classmethod
    def from_mapping(cls, row: Mapping[str, object]) -> "PnlOutcomeCounts":
        """Validate optional counts without magnitude-based win-rate interpretation."""
        return cls(cls._count(row.get("wins")), cls._count(row.get("losses")))

    @staticmethod
    def _count(value: object) -> int | None:
        try:
            number = PerformanceUnits.finite(value)
            if number is None:
                return None
            if number < 0 or number != number.to_integral_value():
                raise ValueError("Invalid count")
            return int(number)
        except ValueError:
            raise FalconProtocolError("Falcon PnL outcome counts are invalid") from None


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _integer(value: Any) -> int | None:
    number = _decimal(value)
    if number is None:
        return None
    try:
        return int(number)
    except (ValueError, OverflowError):
        return None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    return None


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None or value == "":
        return None
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(number, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fraction_from_percent(value: Any) -> Decimal | None:
    """Convert fields documented as percentage points without guessing units."""
    return PerformanceUnits.win_rate_percent_to_fraction(value)


def _decode_categories(value: Any) -> tuple[dict[str, Any], ...]:
    if value in (None, ""):
        return ()
    decoded: Any = value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            try:
                decoded = json.loads(base64.b64decode(value).decode("utf-8"))
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
                return ()
    if not isinstance(decoded, list):
        return ()
    return tuple(dict(item) for item in decoded if isinstance(item, Mapping))


def _wallet(value: Any) -> str:
    return str(value or "").strip().lower()


MARKET_TOXICITY_FLAGS: frozenset[str] = frozenset(
    {
        "liquidity_risk",
        "volume_collapse",
        "whale_control",
        "trade_concentration",
        "squeeze",
        "not_active",
        "closed",
    }
)
"""Stable names of every market-toxicity signal exposed by Market360."""


@dataclass(frozen=True, slots=True)
class LeaderboardEntry:
    """Represent one normalized Falcon leaderboard row."""

    wallet: str
    h_score: Decimal
    roi_15d: Decimal | None
    pnl_15d: Decimal | None
    win_rate_15d: Decimal | None
    trades_15d: int | None
    sharpe_15d: Decimal | None
    trajectory: str | None
    sybil_flag: bool
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "LeaderboardEntry":
        """Build an entry from the current tolerant Falcon wire schema.

        Args:
            row: Provider result row from ``data.results``.

        Returns:
            A normalized immutable leaderboard entry.
        """
        h_score = PerformanceUnits.finite(_first(row, "h_score", "score"))
        if h_score is None or not Decimal(0) <= h_score <= Decimal(100):
            raise ValueError("Falcon leaderboard requires an h_score between zero and 100")
        return cls(
            wallet=_wallet(_first(row, "proxy_wallet", "wallet", "wallet_address", "address")),
            h_score=h_score,
            roi_15d=_decimal(_first(row, "roi_15d", "roi", "return_15d")),
            pnl_15d=_decimal(_first(row, "pnl_15d", "pnl", "realized_pnl_15d")),
            win_rate_15d=PerformanceUnits.fraction(_first(row, "win_rate_15d", "win_rate")),
            trades_15d=_integer(_first(row, "total_trades_15d", "trades_15d", "trades")),
            sharpe_15d=_decimal(_first(row, "sharpe_15d", "sharpe_ratio", "sharpe")),
            trajectory=str(_first(row, "trajectory", "performance_trend", "flow") or "")
            .strip()
            .lower()
            or None,
            sybil_flag=bool(_bool(_first(row, "sybil_flag", "sybil_risk_flag")) or False),
            raw=dict(row),
        )


@dataclass(frozen=True, slots=True)
class Wallet360Snapshot:
    """Represent normalized multi-window wallet analytics evidence.

    ``max_position_pct`` retains its provider field name but stores a fraction
    in [0, 1], converted from percentage points in [0, 100]. ``max_drawdown``
    preserves the non-negative provider metric, which can exceed one; it is
    never rescaled into a smaller risk observation.
    """

    wallet: str
    window_days: int
    snapshot_at: datetime | None
    last_active: datetime | None
    avg_position_size: Decimal | None
    market_concentration_ratio: Decimal | None
    max_drawdown: Decimal | None
    max_position_pct: Decimal | None
    profit_factor: Decimal | None
    roi: Decimal | None
    sharpe_ratio: Decimal | None
    statistical_confidence: Decimal | None
    performance_trend: str | None
    sybil_risk_flag: bool
    sybil_risk_score: Decimal | None
    timing_anomaly_flag: bool
    suspicious_win_rate_flag: bool
    position_size_volatility_flag: bool
    perfect_timing_flag: bool
    similar_wallets_count: int | None
    days_active: int | None
    markets_traded: int | None
    avg_trade_size: Decimal | None
    total_trades: int | None
    win_rate: Decimal | None
    win_rate_last_30d: Decimal | None
    total_pnl: Decimal | None
    risk_level: str | None
    combined_risk_score: Decimal | None
    performance_by_category: tuple[dict[str, Any], ...]
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "Wallet360Snapshot":
        """Build a Wallet360 snapshot using explicit provider field units.

        Args:
            row: Provider result row; ``max_position_pct`` is percentage points,
                including values below one. Drawdown retains its raw magnitude;
                win rate, concentration, and confidence remain bounded fractions.

        Returns:
            Normalized analytics with the original provider row preserved.

        Raises:
            ValueError: If a performance observation is malformed or out of range.
        """
        raw = dict(row)
        drawdown = PerformanceUnits.finite(row.get("max_drawdown"))
        if drawdown is not None and drawdown < 0:
            raise ValueError("Falcon Wallet360 drawdown must be non-negative")
        return cls(
            wallet=_wallet(_first(row, "proxy_wallet", "wallet", "wallet_address")),
            window_days=_integer(_first(row, "calculation_window_days", "window_days")) or 0,
            snapshot_at=_datetime(
                _first(row, "date_range_end", "date", "snapshot_time", "updated_at")
            ),
            last_active=_datetime(_first(row, "last_active", "last_trade_at")),
            avg_position_size=_decimal(row.get("avg_position_size")),
            market_concentration_ratio=PerformanceUnits.fraction(
                row.get("market_concentration_ratio")
            ),
            max_drawdown=drawdown,
            max_position_pct=PerformanceUnits.fraction(
                PerformanceUnits.percentage_points_to_fraction(row.get("max_position_pct"))
            ),
            profit_factor=_decimal(row.get("profit_factor")),
            # Wallet360 documents ROI as percentage points (e.g. 3.8743 == 3.8743%).
            roi=PerformanceUnits.percentage_points_to_fraction(row.get("roi")),
            sharpe_ratio=_decimal(row.get("sharpe_ratio")),
            statistical_confidence=PerformanceUnits.fraction(row.get("statistical_confidence")),
            performance_trend=str(row.get("performance_trend") or "").strip().lower() or None,
            sybil_risk_flag=bool(_bool(row.get("sybil_risk_flag")) or False),
            sybil_risk_score=_decimal(row.get("sybil_risk_score")),
            timing_anomaly_flag=bool(_bool(row.get("timing_anomaly_flag")) or False),
            suspicious_win_rate_flag=bool(_bool(row.get("suspicious_win_rate_flag")) or False),
            position_size_volatility_flag=bool(
                _bool(row.get("position_size_volatility_flag")) or False
            ),
            perfect_timing_flag=bool(_bool(row.get("perfect_timing_flag")) or False),
            similar_wallets_count=_integer(row.get("similar_wallets_count")),
            days_active=_integer(row.get("days_active")),
            markets_traded=_integer(_first(row, "markets_traded", "num_markets_traded")),
            avg_trade_size=_decimal(_first(row, "avg_trade_size", "avg_position_size")),
            total_trades=_integer(row.get("total_trades")),
            win_rate=PerformanceUnits.fraction(row.get("win_rate")),
            win_rate_last_30d=PerformanceUnits.fraction(
                _first(row, "win_rate_last_30d", "win_rate_last_30day")
            ),
            total_pnl=_decimal(row.get("total_pnl")),
            risk_level=str(row.get("risk_level") or "").strip().upper() or None,
            combined_risk_score=_decimal(row.get("combined_risk_score")),
            performance_by_category=_decode_categories(row.get("performance_by_category")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class WalletAllTimeSnapshot:
    """Represent normalized all-time wallet performance evidence."""

    wallet: str
    total_trades: int | None
    avg_trade_size: Decimal | None
    avg_pnl_per_trade: Decimal | None
    total_invested: Decimal | None
    total_pnl: Decimal | None
    roi_fraction: Decimal | None
    last_updated: datetime | None
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "WalletAllTimeSnapshot":
        """Build an all-time wallet snapshot from a provider result row."""
        roi_pct = _decimal(row.get("roi_pct"))
        return cls(
            wallet=_wallet(_first(row, "proxy_wallet", "wallet", "wallet_address")),
            total_trades=_integer(row.get("total_trades")),
            avg_trade_size=_decimal(row.get("avg_trade_size")),
            avg_pnl_per_trade=_decimal(row.get("avg_pnl_per_trade")),
            total_invested=_decimal(row.get("total_invested")),
            total_pnl=_decimal(row.get("total_pnl")),
            roi_fraction=(roi_pct / Decimal("100")) if roi_pct is not None else None,
            last_updated=_datetime(row.get("last_updated")),
            raw=dict(row),
        )


@dataclass(frozen=True, slots=True)
class TradeSnapshot:
    """Represent one immutable provider trade used for discovery evidence."""

    trade_id: str
    wallet: str
    condition_id: str
    market_slug: str | None
    side: str
    price: Decimal
    size: Decimal
    timestamp: datetime
    outcome: str | None
    transaction_hash: str | None
    raw: dict[str, Any]

    @property
    def notional(self) -> Decimal:
        """Return the exact trade notional as price times shares."""
        return self.price * self.size

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "TradeSnapshot":
        """Build a trade snapshot and reject missing required money fields."""
        timestamp = _datetime(_first(row, "timestamp", "created_at", "match_time"))
        price = _decimal(row.get("price"))
        size = _decimal(_first(row, "size", "amount"))
        if timestamp is None or price is None or size is None:
            raise ValueError("trade row omitted timestamp, price or size")
        return cls(
            trade_id=str(_first(row, "id", "trade_id", "transaction_hash") or ""),
            wallet=_wallet(_first(row, "proxy_wallet", "wallet", "wallet_address")),
            condition_id=str(_first(row, "condition_id", "conditionId", "market") or ""),
            market_slug=str(_first(row, "slug", "market_slug") or "") or None,
            side=str(row.get("side") or "").strip().upper(),
            price=price,
            size=size,
            timestamp=timestamp,
            outcome=str(row.get("outcome") or "") or None,
            transaction_hash=str(_first(row, "transaction_hash", "transactionHash") or "") or None,
            raw=dict(row),
        )


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Represent normalized market identity and lifecycle metadata."""

    condition_id: str
    slug: str | None
    event_slug: str | None
    question: str | None
    closed: bool
    start_date: datetime | None
    end_date: datetime | None
    volume_total: Decimal | None
    yes_token_id: str | None
    no_token_id: str | None
    side_a_outcome: str | None
    side_b_outcome: str | None
    winning_outcome: str | None
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "MarketSnapshot":
        """Build a market snapshot from a provider result row."""
        return cls(
            condition_id=str(_first(row, "condition_id", "conditionId", "id") or ""),
            slug=str(row.get("slug") or "") or None,
            event_slug=str(row.get("event_slug") or "") or None,
            question=str(_first(row, "question", "title") or "") or None,
            closed=bool(_bool(row.get("closed")) or False),
            start_date=_datetime(_first(row, "start_date", "startDate", "created_at")),
            end_date=_datetime(_first(row, "end_date", "endDate", "close_date")),
            volume_total=_decimal(_first(row, "volume_total", "volume")),
            yes_token_id=str(_first(row, "side_a_token_id", "yes_token_id", "yesTokenId") or "")
            or None,
            no_token_id=str(_first(row, "side_b_token_id", "no_token_id", "noTokenId") or "")
            or None,
            side_a_outcome=str(_first(row, "side_a_outcome", "yes_outcome") or "") or None,
            side_b_outcome=str(_first(row, "side_b_outcome", "no_outcome") or "") or None,
            winning_outcome=str(row.get("winning_outcome") or "") or None,
            raw=dict(row),
        )

    def is_short_horizon(self, at: datetime, max_days: int) -> bool:
        """Return whether the market resolves within ``max_days`` after ``at``.

        Resolution horizon is derived from ``end_date`` alone.  A market that
        has already been settled early still describes a short-duration trade
        when it was still open at trade time, so the ``closed`` lifecycle flag
        must not suppress the classification.

        Args:
            at: Reference instant, normally the trade timestamp.
            max_days: Inclusive upper bound of the resolution horizon in days.

        Returns:
            True when ``end_date`` is known and lies between ``at`` and
            ``at + max_days``.
        """
        if self.end_date is None:
            return False
        horizon = self.end_date - at
        return horizon.total_seconds() >= 0 and horizon <= timedelta(days=max_days)

    def token_for_outcome(self, outcome: str | None) -> str | None:
        """Return the raw outcome token matching a normalized outcome label."""
        normalized = str(outcome or "").strip().lower()
        if self.side_a_outcome and normalized == self.side_a_outcome.strip().lower():
            return self.yes_token_id
        if self.side_b_outcome and normalized == self.side_b_outcome.strip().lower():
            return self.no_token_id
        if normalized in {"yes", "y", "up", "1", "true"}:
            return self.yes_token_id
        if normalized in {"no", "n", "down", "0", "false"}:
            return self.no_token_id
        return None


@dataclass(frozen=True, slots=True)
class Market360Snapshot:
    """Represent normalized market liquidity and concentration evidence."""

    condition_id: str
    snapshot_at: datetime | None
    liquidity_percentile: Decimal | None
    current_volume_24h: Decimal | None
    current_volume_7d: Decimal | None
    top1_wallet_fraction: Decimal | None
    top3_wallet_fraction: Decimal | None
    top10_wallet_fraction: Decimal | None
    unique_traders_7d: int | None
    trades_per_hour_avg: Decimal | None
    volume_trend: str | None
    market_active: bool
    market_closed: bool
    liquidity_risk_flag: bool
    volume_collapse_risk_flag: bool
    whale_control_flag: bool
    trade_concentration_flag: bool
    squeeze_risk_flag: bool
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "Market360Snapshot":
        """Build a Market360 snapshot from a provider result row."""
        return cls(
            condition_id=str(_first(row, "condition_id", "conditionId") or ""),
            snapshot_at=_datetime(_first(row, "snapshot_time", "updated_at", "created_at")),
            liquidity_percentile=_decimal(row.get("liquidity_percentile")),
            current_volume_24h=_decimal(row.get("current_volume_24h")),
            current_volume_7d=_decimal(row.get("current_volume_7d")),
            top1_wallet_fraction=_fraction_from_percent(row.get("top1_wallet_pct")),
            top3_wallet_fraction=_fraction_from_percent(row.get("top3_wallet_pct")),
            top10_wallet_fraction=_fraction_from_percent(row.get("top10_wallet_pct")),
            unique_traders_7d=_integer(row.get("unique_traders_7d")),
            trades_per_hour_avg=_decimal(row.get("trades_per_hour_avg")),
            volume_trend=str(row.get("volume_trend") or "").strip() or None,
            market_active=bool(_bool(row.get("market_active")) or False),
            market_closed=bool(_bool(row.get("market_closed")) or False),
            liquidity_risk_flag=bool(_bool(row.get("liquidity_risk_flag")) or False),
            volume_collapse_risk_flag=bool(_bool(row.get("volume_collapse_risk_flag")) or False),
            whale_control_flag=bool(_bool(row.get("whale_control_flag")) or False),
            trade_concentration_flag=bool(_bool(row.get("trade_concentration_flag")) or False),
            squeeze_risk_flag=bool(_bool(row.get("squeeze_risk_flag")) or False),
            raw=dict(row),
        )

    def toxic_flags(self) -> frozenset[str]:
        """Return the stable names of every raised market-toxicity signal.

        Returns:
            The subset of ``liquidity_risk``, ``volume_collapse``,
            ``whale_control``, ``trade_concentration``, ``squeeze``,
            ``not_active`` and ``closed`` that currently applies.
        """
        raised = {
            "liquidity_risk": self.liquidity_risk_flag,
            "volume_collapse": self.volume_collapse_risk_flag,
            "whale_control": self.whale_control_flag,
            "trade_concentration": self.trade_concentration_flag,
            "squeeze": self.squeeze_risk_flag,
            "not_active": not self.market_active,
            "closed": self.market_closed,
        }
        return frozenset(name for name, active in raised.items() if active)


def select_latest_wallet360(
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_window_days: int | None = None,
) -> Wallet360Snapshot | None:
    """Return the newest Wallet360 snapshot for the requested window."""
    snapshots = [Wallet360Snapshot.from_mapping(row) for row in rows]
    if expected_window_days is not None:
        snapshots = [item for item in snapshots if item.window_days == expected_window_days]
    if not snapshots:
        return None
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    return max(snapshots, key=lambda item: item.snapshot_at or epoch)


def select_latest_market360(rows: Iterable[Mapping[str, Any]]) -> Market360Snapshot | None:
    """Return the newest Market360 snapshot from provider rows."""
    snapshots = [Market360Snapshot.from_mapping(row) for row in rows]
    if not snapshots:
        return None
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    return max(snapshots, key=lambda item: item.snapshot_at or epoch)


__all__ = [
    "MARKET_TOXICITY_FLAGS",
    "LeaderboardEntry",
    "Market360Snapshot",
    "MarketSnapshot",
    "TradeSnapshot",
    "Wallet360Snapshot",
    "WalletAllTimeSnapshot",
    "select_latest_market360",
    "select_latest_wallet360",
]
