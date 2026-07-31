"""Bankroll-aware auto-trader discovery using Heisenberg intelligence.

The H-Score is used as a shortlist signal, not as a trading decision.  Every
wallet is re-evaluated with fresh trades, Wallet360 windows, market duration,
Market360 quality and a follower-specific bankroll-fit calculation.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import median
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence

from integrations.heisenberg.models import (
    LeaderboardEntry,
    Market360Snapshot,
    MarketSnapshot,
    TradeSnapshot,
    Wallet360Snapshot,
    WalletAllTimeSnapshot,
    parse_leaderboard_entry,
    select_latest_market360,
    select_latest_wallet360,
)
from services.discovery.bankroll_compatibility import (
    BankrollCompatibilityPolicy,
    evaluate_bankroll_compatibility,
)
from services.discovery.discovery_models import (
    DiscoveryEvidence,
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)

_ZERO = Decimal("0")
_ONE = Decimal("1")


class CapacityProbe(Protocol):
    async def visible_capacity_usdc(
        self,
        trade: TradeSnapshot,
        market: MarketSnapshot,
    ) -> Decimal | None: ...


def _clamp(value: Decimal, low: Decimal = _ZERO, high: Decimal = _ONE) -> Decimal:
    return max(low, min(high, value))


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(median(values)))


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _is_evm_address(value: str) -> bool:
    return bool(_EVM_ADDRESS_RE.fullmatch(str(value or "").strip()))


@dataclass(frozen=True, slots=True)
class HeisenbergDiscoveryPolicy:
    candidate_limit: int = 50
    candidate_concurrency: int = 6
    wallet360_windows: tuple[int, ...] = (1, 3, 7, 15)
    required_wallet360_windows: int = 3
    max_wallet360_snapshot_age_hours: int = 72
    recent_trade_lookback_hours: int = 48
    max_last_trade_age_hours: int = 12
    min_today_trades: int = 1
    min_recent_trades: int = 10
    max_recent_trades: int = 500
    max_recent_markets_per_wallet: int = 30
    max_depth_probes_per_wallet: int = 8
    max_price_jump_probes_per_wallet: int = 3
    price_jump_lookback_hours: int = 24
    price_jump_min_change_pct: Decimal = Decimal("10")
    max_recent_price_jumps: int = 8
    enable_price_jump_risk: bool = True
    short_market_max_days: int = 30
    min_short_trade_count: int = 10
    min_short_trade_share: Decimal = Decimal("0.50")
    min_h_score: Decimal = Decimal("65")
    min_liquidity_percentile: Decimal = Decimal("50")
    max_toxic_market_share: Decimal = Decimal("0.25")
    max_market_concentration: Decimal = Decimal("0.65")
    max_missing_field_ratio: Decimal = Decimal("0.25")
    max_drawdown: Decimal = Decimal("0.35")
    max_sybil_score: Decimal = Decimal("20")
    leaderboard_min_win_rate_15d: Decimal = Decimal("0.45")
    leaderboard_max_win_rate_15d: Decimal = Decimal("0.95")
    leaderboard_min_roi_15d: Decimal = Decimal("0")
    leaderboard_min_trades_15d: int = 25
    leaderboard_max_trades_15d: int = 100_000
    leaderboard_min_pnl_15d: Decimal = Decimal("0")
    bankroll: BankrollCompatibilityPolicy = field(default_factory=BankrollCompatibilityPolicy)

    def __post_init__(self) -> None:
        allowed_windows = {1, 3, 7, 15}
        try:
            windows = tuple(int(window) for window in self.wallet360_windows)
        except (TypeError, ValueError) as exc:
            raise ValueError("wallet360_windows must contain integers") from exc
        if not windows or any(window not in allowed_windows for window in windows):
            raise ValueError("wallet360_windows must be selected from 1, 3, 7 and 15")
        if len(set(windows)) != len(windows):
            raise ValueError("wallet360_windows must not contain duplicates")
        object.__setattr__(self, "wallet360_windows", windows)

        positive_ints = (
            "candidate_limit",
            "candidate_concurrency",
            "required_wallet360_windows",
            "max_wallet360_snapshot_age_hours",
            "recent_trade_lookback_hours",
            "max_last_trade_age_hours",
            "max_recent_trades",
            "max_recent_markets_per_wallet",
            "price_jump_lookback_hours",
            "short_market_max_days",
            "leaderboard_max_trades_15d",
        )
        for name in positive_ints:
            value = getattr(self, name)
            if isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be positive")

        non_negative_ints = (
            "min_today_trades",
            "min_recent_trades",
            "max_depth_probes_per_wallet",
            "max_price_jump_probes_per_wallet",
            "max_recent_price_jumps",
            "min_short_trade_count",
            "leaderboard_min_trades_15d",
        )
        for name in non_negative_ints:
            value = getattr(self, name)
            if isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must not be negative")

        if self.required_wallet360_windows > len(windows):
            raise ValueError("required_wallet360_windows must not exceed configured wallet360_windows")
        if self.min_recent_trades > self.max_recent_trades:
            raise ValueError("min_recent_trades must not exceed max_recent_trades")
        if self.min_short_trade_count > self.max_recent_trades:
            raise ValueError("min_short_trade_count must not exceed max_recent_trades")
        if self.leaderboard_min_trades_15d > self.leaderboard_max_trades_15d:
            raise ValueError("leaderboard_min_trades_15d must not exceed leaderboard_max_trades_15d")

        decimal_fields = (
            "price_jump_min_change_pct",
            "min_short_trade_share",
            "min_h_score",
            "min_liquidity_percentile",
            "max_toxic_market_share",
            "max_market_concentration",
            "max_missing_field_ratio",
            "max_drawdown",
            "max_sybil_score",
            "leaderboard_min_win_rate_15d",
            "leaderboard_max_win_rate_15d",
            "leaderboard_min_roi_15d",
            "leaderboard_min_pnl_15d",
        )
        for name in decimal_fields:
            try:
                value = Decimal(str(getattr(self, name)))
            except Exception as exc:
                raise ValueError(f"{name} must be a decimal") from exc
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
            object.__setattr__(self, name, value)

        zero_to_one = (
            "min_short_trade_share",
            "max_toxic_market_share",
            "max_market_concentration",
            "max_missing_field_ratio",
            "max_drawdown",
            "leaderboard_min_win_rate_15d",
            "leaderboard_max_win_rate_15d",
        )
        for name in zero_to_one:
            value = getattr(self, name)
            if not _ZERO <= value <= _ONE:
                raise ValueError(f"{name} must be between zero and one")
        if not _ZERO <= self.min_h_score <= Decimal("100"):
            raise ValueError("min_h_score must be between zero and 100")
        if not _ZERO <= self.min_liquidity_percentile <= Decimal("100"):
            raise ValueError("min_liquidity_percentile must be between zero and 100")
        if self.max_sybil_score < _ZERO:
            raise ValueError("max_sybil_score must not be negative")
        if self.price_jump_min_change_pct < _ZERO:
            raise ValueError("price_jump_min_change_pct must not be negative")
        if self.leaderboard_min_win_rate_15d > self.leaderboard_max_win_rate_15d:
            raise ValueError("leaderboard_min_win_rate_15d must not exceed leaderboard_max_win_rate_15d")
        if not isinstance(self.bankroll, BankrollCompatibilityPolicy):
            raise ValueError("bankroll must be a BankrollCompatibilityPolicy")



@dataclass(slots=True)
class _CandidateContext:
    leaderboard: LeaderboardEntry
    wallet360: dict[int, Wallet360Snapshot]
    trades: list[TradeSnapshot]
    markets: dict[str, MarketSnapshot]
    market_quality: dict[str, Market360Snapshot]
    all_time: WalletAllTimeSnapshot | None
    pnl_rows: list[dict[str, Any]]
    source_errors: list[str]
    visible_capacity: dict[str, Decimal]
    price_jump_count: int
    official_leaderboard_rank: int | None


class HeisenbergCandidateService:
    """Discover and rank wallets for a specific follower bankroll."""

    SOURCE = "heisenberg"

    def __init__(
        self,
        client: Any,
        *,
        policy: HeisenbergDiscoveryPolicy | None = None,
        capacity_probe: CapacityProbe | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._policy = policy or HeisenbergDiscoveryPolicy()
        self._capacity_probe = capacity_probe
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._semaphore = asyncio.Semaphore(self._policy.candidate_concurrency)
        self._market_tasks: dict[
            str, asyncio.Task[tuple[MarketSnapshot | None, str | None]]
        ] = {}
        self._quality_tasks: dict[
            str, asyncio.Task[tuple[Market360Snapshot | None, str | None]]
        ] = {}

    async def discover(self, funds: Decimal) -> DiscoveryResult:
        generated_at = self._normalize_now(self._now())
        self._market_tasks = {}
        self._quality_tasks = {}
        official_ranks = await self._load_official_leaderboard()
        raw = await self._client.leaderboard(
            min_win_rate_15d=self._policy.leaderboard_min_win_rate_15d,
            max_win_rate_15d=self._policy.leaderboard_max_win_rate_15d,
            min_roi_15d=self._policy.leaderboard_min_roi_15d,
            min_total_trades_15d=self._policy.leaderboard_min_trades_15d,
            max_total_trades_15d=self._policy.leaderboard_max_trades_15d,
            min_pnl_15d=self._policy.leaderboard_min_pnl_15d,
            sort_by="h_score",
            limit=self._policy.candidate_limit,
        )
        entries: list[LeaderboardEntry] = []
        rejected: list[RejectedTrader] = []
        seen: set[str] = set()
        for row in raw:
            entry = parse_leaderboard_entry(row)
            if not entry.wallet or entry.wallet in seen:
                continue
            seen.add(entry.wallet)
            if not _is_evm_address(entry.wallet):
                rejected.append(
                    RejectedTrader(
                        wallet=entry.wallet,
                        reasons=("invalid_wallet_address",),
                        source=self.SOURCE,
                        h_score=entry.h_score,
                    )
                )
                continue
            if entry.h_score < self._policy.min_h_score:
                rejected.append(
                    RejectedTrader(
                        wallet=entry.wallet,
                        reasons=("h_score_below_threshold",),
                        source=self.SOURCE,
                        h_score=entry.h_score,
                    )
                )
                continue
            entries.append(entry)
            if len(entries) >= self._policy.candidate_limit:
                break

        analyses = await asyncio.gather(
            *(
                self._analyze(
                    entry,
                    Decimal(str(funds)),
                    generated_at,
                    official_ranks.get(entry.wallet),
                )
                for entry in entries
            ),
            return_exceptions=True,
        )
        ranked: list[RankedTrader] = []
        warnings: list[str] = []
        for entry, analysis in zip(entries, analyses, strict=False):
            if isinstance(analysis, BaseException):
                if isinstance(analysis, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise analysis
                rejected.append(
                    RejectedTrader(
                        wallet=entry.wallet,
                        reasons=("candidate_analysis_unavailable",),
                        source=self.SOURCE,
                        h_score=entry.h_score,
                        metrics={"error_type": type(analysis).__name__},
                    )
                )
                warnings.append(f"{entry.wallet}:candidate_analysis_unavailable")
                continue
            candidate, rejection = analysis
            if candidate is not None:
                ranked.append(candidate)
            elif rejection is not None:
                rejected.append(rejection)

        ranked.sort(key=lambda item: (-item.score, item.wallet))
        rejected.sort(key=lambda item: item.wallet)
        return DiscoveryResult(
            ranked=tuple(ranked),
            rejected=tuple(rejected),
            source=self.SOURCE,
            generated_at=generated_at,
            warnings=tuple(warnings),
        )

    @staticmethod
    def _normalize_now(value: datetime) -> datetime:
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    async def _analyze(
        self,
        entry: LeaderboardEntry,
        funds: Decimal,
        now: datetime,
        official_rank: int | None,
    ) -> tuple[RankedTrader | None, RejectedTrader | None]:
        async with self._semaphore:
            context = await self._load_context(entry, now, official_rank)
        reasons, evidence, metrics = self._hard_gate_inputs(context, now)
        avg_position = self._avg_position_size(context.wallet360)
        bankroll_fit = evaluate_bankroll_compatibility(
            funds=funds,
            recent_trades=context.trades,
            market_quality=context.market_quality,
            avg_position_size=avg_position,
            visible_capacity_usdc=context.visible_capacity,
            policy=self._policy.bankroll,
        )
        if not bankroll_fit.viable:
            reasons.append("bankroll_incompatible")
            metrics["bankroll_reasons"] = bankroll_fit.reasons
        if reasons:
            return None, RejectedTrader(
                wallet=entry.wallet,
                reasons=tuple(dict.fromkeys(reasons)),
                source=self.SOURCE,
                h_score=entry.h_score,
                metrics=metrics,
            )

        score, components = self._score(context, evidence, bankroll_fit)
        pnl_total, pnl_invested, _pnl_win_rate = self._pnl_summary(context.pnl_rows)
        metrics.update(
            {
                "recommended_copy_ratio": str(bankroll_fit.recommended_copy_ratio),
                "recommended_stake_usdc": str(bankroll_fit.recommended_stake_usdc),
                "copyable_trade_share": str(bankroll_fit.copyable_trade_share),
                "trade_scale_ratio": str(bankroll_fit.trade_scale_ratio),
                "position_scale_ratio": str(bankroll_fit.position_scale_ratio),
                "recent_realized_pnl": str(pnl_total),
                "recent_invested": str(pnl_invested),
                "all_time_roi_fraction": (
                    str(context.all_time.roi_fraction)
                    if context.all_time and context.all_time.roi_fraction is not None
                    else None
                ),
            }
        )
        return RankedTrader(
            wallet=entry.wallet,
            score=score,
            source=self.SOURCE,
            h_score=entry.h_score,
            bankroll_fit=bankroll_fit,
            evidence=evidence,
            components=components,
            metrics=metrics,
        ), None

    async def _load_context(
        self,
        entry: LeaderboardEntry,
        now: datetime,
        official_rank: int | None,
    ) -> _CandidateContext:
        source_errors: list[str] = []

        async def wallet_window(window: int) -> tuple[int, Wallet360Snapshot | None]:
            try:
                rows = await self._client.wallet_360(entry.wallet, window)
                return window, select_latest_wallet360(rows, expected_window_days=window)
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                raise
            except BaseException as exc:
                source_errors.append(f"wallet360_{window}:{type(exc).__name__}")
                return window, None

        wallet_rows, trades_result, all_time_result, pnl_result = await asyncio.gather(
            asyncio.gather(*(wallet_window(window) for window in self._policy.wallet360_windows)),
            self._safe_trades(entry.wallet, now, source_errors),
            self._safe_all_time(entry.wallet, source_errors),
            self._safe_pnl(entry.wallet, now, source_errors),
        )
        wallet360 = {window: item for window, item in wallet_rows if item is not None}
        trades = trades_result
        condition_ids: list[str] = []
        for trade in sorted(trades, key=lambda item: item.timestamp, reverse=True):
            if trade.condition_id and trade.condition_id not in condition_ids:
                condition_ids.append(trade.condition_id)
            if len(condition_ids) >= self._policy.max_recent_markets_per_wallet:
                break
        market_pairs = await asyncio.gather(
            *(self._market_and_quality(condition_id, source_errors) for condition_id in condition_ids)
        )
        markets = {condition: market for condition, market, _quality in market_pairs if market is not None}
        market_quality = {
            condition: quality
            for condition, _market, quality in market_pairs
            if quality is not None
        }
        visible_capacity: dict[str, Decimal] = {}
        if self._capacity_probe is not None:
            latest_per_condition: dict[str, TradeSnapshot] = {}
            for trade in sorted(trades, key=lambda item: item.timestamp, reverse=True):
                if trade.condition_id in markets and trade.condition_id not in latest_per_condition:
                    latest_per_condition[trade.condition_id] = trade
                if len(latest_per_condition) >= self._policy.max_depth_probes_per_wallet:
                    break
            capacities = await asyncio.gather(
                *(
                    self._safe_capacity(trade, markets[condition], source_errors)
                    for condition, trade in latest_per_condition.items()
                )
            )
            for condition, capacity in zip(latest_per_condition, capacities, strict=False):
                if capacity is not None:
                    visible_capacity[condition] = capacity
        price_jump_count = await self._load_price_jump_count(
            trades,
            markets,
            source_errors,
        )
        return _CandidateContext(
            leaderboard=entry,
            wallet360=wallet360,
            trades=trades,
            markets=markets,
            market_quality=market_quality,
            all_time=all_time_result,
            pnl_rows=pnl_result,
            source_errors=source_errors,
            visible_capacity=visible_capacity,
            price_jump_count=price_jump_count,
            official_leaderboard_rank=official_rank,
        )

    async def _load_official_leaderboard(self) -> dict[str, int]:
        method = getattr(self._client, "pnl_leaderboard", None)
        if not callable(method):
            return {}
        try:
            rows = await method(
                wallet_address="ALL",
                leaderboard_period="7d",
                limit=self._policy.candidate_limit,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException:
            return {}
        ranks: dict[str, int] = {}
        for row in rows:
            wallet = str(row.get("address") or row.get("wallet") or "").strip().lower()
            try:
                rank = int(row.get("rank"))
            except (TypeError, ValueError):
                continue
            if wallet and rank > 0:
                ranks[wallet] = rank
        return ranks

    async def _load_price_jump_count(
        self,
        trades: Sequence[TradeSnapshot],
        markets: Mapping[str, MarketSnapshot],
        errors: list[str],
    ) -> int:
        if not self._policy.enable_price_jump_risk:
            return 0
        method = getattr(self._client, "price_jumps", None)
        if not callable(method):
            return 0
        token_ids: list[str] = []
        for trade in sorted(trades, key=lambda item: item.timestamp, reverse=True):
            market = markets.get(trade.condition_id)
            token_id = market.token_for_outcome(trade.outcome) if market else None
            if token_id and token_id not in token_ids:
                token_ids.append(token_id)
            if len(token_ids) >= self._policy.max_price_jump_probes_per_wallet:
                break
        if not token_ids:
            return 0

        async def load(token_id: str) -> int:
            try:
                rows = await method(
                    token_id=token_id,
                    resolution="15m",
                    min_change_pct=self._policy.price_jump_min_change_pct,
                    lookback_hours=self._policy.price_jump_lookback_hours,
                    limit=100,
                )
                return len(rows)
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                raise
            except BaseException as exc:
                errors.append(f"price_jumps:{type(exc).__name__}")
                return 0

        counts = await asyncio.gather(*(load(token_id) for token_id in token_ids))
        return sum(counts)

    async def _safe_trades(
        self,
        wallet: str,
        now: datetime,
        errors: list[str],
    ) -> list[TradeSnapshot]:
        try:
            rows = await self._client.trades(
                proxy_wallet=wallet,
                start_time=int((now - timedelta(hours=self._policy.recent_trade_lookback_hours)).timestamp()),
                end_time=int(now.timestamp()),
                limit=self._policy.max_recent_trades,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            errors.append(f"trades:{type(exc).__name__}")
            return []
        parsed: list[TradeSnapshot] = []
        seen: set[tuple[str, str | None, str, str, str]] = set()
        for row in rows:
            try:
                item = TradeSnapshot.from_mapping(row)
            except (TypeError, ValueError):
                continue
            key = (
                item.trade_id,
                item.transaction_hash,
                item.condition_id,
                str(item.price),
                str(item.size),
            )
            if key in seen:
                continue
            seen.add(key)
            parsed.append(item)
        parsed.sort(key=lambda item: item.timestamp, reverse=True)
        return parsed[: self._policy.max_recent_trades]

    async def _safe_all_time(
        self,
        wallet: str,
        errors: list[str],
    ) -> WalletAllTimeSnapshot | None:
        try:
            rows = await self._client.wallet_stats_all_time(wallet)
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            errors.append(f"all_time:{type(exc).__name__}")
            return None
        snapshots = [WalletAllTimeSnapshot.from_mapping(row) for row in rows]
        snapshots = [item for item in snapshots if item.wallet in {"", wallet}]
        if not snapshots:
            return None
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        return max(snapshots, key=lambda item: item.last_updated or epoch)

    async def _safe_pnl(
        self,
        wallet: str,
        now: datetime,
        errors: list[str],
    ) -> list[dict[str, Any]]:
        try:
            return await self._client.pnl(
                wallet=wallet,
                granularity="1d",
                start_time=int((now - timedelta(days=15)).timestamp()),
                end_time=int(now.timestamp()),
                limit=30,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            errors.append(f"pnl:{type(exc).__name__}")
            return []

    async def _market_and_quality(
        self,
        condition_id: str,
        errors: list[str],
    ) -> tuple[str, MarketSnapshot | None, Market360Snapshot | None]:
        # The request task is shared across candidates, but error attribution is
        # candidate-local.  Cache the result and an error label, then copy that
        # label into every affected candidate's evidence instead of mutating the
        # first caller's list from inside the shared task.
        if condition_id not in self._market_tasks:
            self._market_tasks[condition_id] = asyncio.create_task(
                self._load_market(condition_id)
            )
        if condition_id not in self._quality_tasks:
            self._quality_tasks[condition_id] = asyncio.create_task(
                self._load_quality(condition_id)
            )
        market_result, quality_result = await asyncio.gather(
            self._market_tasks[condition_id],
            self._quality_tasks[condition_id],
        )
        market, market_error = market_result
        quality, quality_error = quality_result
        for error in (market_error, quality_error):
            if error is not None and error not in errors:
                errors.append(error)
        return condition_id, market, quality

    async def _load_market(
        self,
        condition_id: str,
    ) -> tuple[MarketSnapshot | None, str | None]:
        try:
            rows = await self._client.markets(condition_id=condition_id, limit=10)
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            return None, f"market:{type(exc).__name__}"
        try:
            candidates = [MarketSnapshot.from_mapping(row) for row in rows]
        except (TypeError, ValueError) as exc:
            return None, f"market:{type(exc).__name__}"
        for item in candidates:
            if item.condition_id == condition_id:
                return item, None
        return (candidates[0], None) if candidates else (None, "market:not_found")

    async def _load_quality(
        self,
        condition_id: str,
    ) -> tuple[Market360Snapshot | None, str | None]:
        try:
            rows = await self._client.market_360(condition_id=condition_id, limit=10)
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            return None, f"market360:{type(exc).__name__}"
        try:
            quality = select_latest_market360(rows)
        except (TypeError, ValueError) as exc:
            return None, f"market360:{type(exc).__name__}"
        return (quality, None) if quality is not None else (None, "market360:not_found")

    async def _safe_capacity(
        self,
        trade: TradeSnapshot,
        market: MarketSnapshot,
        errors: list[str],
    ) -> Decimal | None:
        try:
            return await self._capacity_probe.visible_capacity_usdc(trade, market)  # type: ignore[union-attr]
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except BaseException as exc:
            errors.append(f"depth:{type(exc).__name__}")
            return None

    def _hard_gate_inputs(
        self,
        context: _CandidateContext,
        now: datetime,
    ) -> tuple[list[str], DiscoveryEvidence, dict[str, Any]]:
        reasons: list[str] = []
        if len(context.wallet360) < self._policy.required_wallet360_windows:
            reasons.append("insufficient_wallet360_evidence")
        snapshots = list(context.wallet360.values())
        freshness_limit = timedelta(hours=self._policy.max_wallet360_snapshot_age_hours)
        future_skew = timedelta(hours=1)
        fresh_snapshots = [
            item
            for item in snapshots
            if item.snapshot_at is not None
            and -future_skew <= now - item.snapshot_at <= freshness_limit
        ]
        if (
            len(context.wallet360) >= self._policy.required_wallet360_windows
            and len(fresh_snapshots) < self._policy.required_wallet360_windows
        ):
            reasons.append("stale_wallet360_evidence")
        if context.leaderboard.sybil_flag or any(item.sybil_risk_flag for item in snapshots):
            reasons.append("sybil_risk")
        if any(
            item.sybil_risk_score is not None
            and item.sybil_risk_score > self._policy.max_sybil_score
            for item in snapshots
        ):
            reasons.append("sybil_risk")
        if any(item.timing_anomaly_flag for item in snapshots):
            reasons.append("timing_anomaly")

        trades = context.trades
        last_trade = trades[0].timestamp if trades else None
        today_count = sum(1 for item in trades if item.timestamp.date() == now.date())
        stale = (
            last_trade is None
            or now - last_trade > timedelta(hours=self._policy.max_last_trade_age_hours)
            or today_count < self._policy.min_today_trades
        )
        if stale:
            reasons.append("inactive_or_stale")
        if len(trades) < self._policy.min_recent_trades:
            reasons.append("insufficient_recent_trades")

        short_count = 0
        for trade in trades:
            market = context.markets.get(trade.condition_id)
            if market is not None and market.is_short_horizon(
                trade.timestamp,
                self._policy.short_market_max_days,
            ):
                short_count += 1
        short_share = Decimal(short_count) / Decimal(len(trades)) if trades else _ZERO
        if short_count < self._policy.min_short_trade_count:
            reasons.append("insufficient_short_duration_sample")
        if short_share < self._policy.min_short_trade_share:
            reasons.append("short_duration_share_below_threshold")

        liquidities = [
            item.liquidity_percentile
            for item in context.market_quality.values()
            if item.liquidity_percentile is not None
        ]
        median_liquidity = _median(liquidities)
        if median_liquidity is None or median_liquidity < self._policy.min_liquidity_percentile:
            reasons.append("market_liquidity_below_threshold")
        if context.price_jump_count > self._policy.max_recent_price_jumps:
            reasons.append("excessive_recent_price_jumps")
        toxic_market_count = sum(
            1
            for item in context.market_quality.values()
            if (
                item.liquidity_risk_flag
                or item.volume_collapse_risk_flag
                or item.whale_control_flag
                or item.trade_concentration_flag
                or item.squeeze_risk_flag
                or not item.market_active
                or item.market_closed
            )
        )
        toxic_market_share = (
            Decimal(toxic_market_count) / Decimal(len(context.market_quality))
            if context.market_quality
            else _ZERO
        )
        if toxic_market_share > self._policy.max_toxic_market_share:
            reasons.append("toxic_or_inactive_market_mix")

        long_snapshot = context.wallet360.get(15) or context.wallet360.get(7)
        concentration = long_snapshot.market_concentration_ratio if long_snapshot else None
        max_drawdown = long_snapshot.max_drawdown if long_snapshot else None
        if concentration is not None and concentration > self._policy.max_market_concentration:
            reasons.append("wallet_market_concentration")
        if max_drawdown is not None and max_drawdown > self._policy.max_drawdown:
            reasons.append("drawdown_above_threshold")

        required_values = [
            self._avg_position_size(context.wallet360),
            concentration,
            max_drawdown,
            long_snapshot.profit_factor if long_snapshot else None,
            long_snapshot.win_rate if long_snapshot else None,
            long_snapshot.total_trades if long_snapshot else None,
            median_liquidity,
            last_trade,
        ]
        missing = sum(value is None for value in required_values)
        missing_ratio = Decimal(missing) / Decimal(len(required_values))
        if missing_ratio > self._policy.max_missing_field_ratio:
            reasons.append("missing_evidence_above_threshold")

        latest_snapshot_at = max(
            (item.snapshot_at for item in snapshots if item.snapshot_at is not None),
            default=None,
        )
        evidence = DiscoveryEvidence(
            wallet360_windows=tuple(sorted(context.wallet360)),
            fresh_wallet360_windows=tuple(
                sorted(item.window_days for item in fresh_snapshots)
            ),
            latest_wallet360_at=latest_snapshot_at,
            last_trade_at=last_trade,
            recent_trade_count=len(trades),
            today_trade_count=today_count,
            short_trade_count=short_count,
            short_trade_share=short_share,
            median_liquidity_percentile=median_liquidity,
            toxic_market_share=toxic_market_share,
            missing_field_ratio=missing_ratio,
            all_time_trades=context.all_time.total_trades if context.all_time else None,
            realized_pnl_rows=len(context.pnl_rows),
            condition_count=len(context.markets),
            recent_price_jump_count=context.price_jump_count,
            official_leaderboard_rank=context.official_leaderboard_rank,
            source_errors=tuple(context.source_errors),
        )
        metrics: dict[str, Any] = {
            "fresh_wallet360_windows": tuple(
                sorted(item.window_days for item in fresh_snapshots)
            ),
            "short_trade_share": str(short_share),
            "median_liquidity_percentile": (
                str(median_liquidity) if median_liquidity is not None else None
            ),
            "market_concentration_ratio": (
                str(concentration) if concentration is not None else None
            ),
            "toxic_market_share": str(toxic_market_share),
            "max_drawdown": str(max_drawdown) if max_drawdown is not None else None,
            "recent_price_jump_count": context.price_jump_count,
            "official_leaderboard_rank": context.official_leaderboard_rank,
            "source_errors": tuple(context.source_errors),
        }
        return list(dict.fromkeys(reasons)), evidence, metrics

    @staticmethod
    def _avg_position_size(windows: Mapping[int, Wallet360Snapshot]) -> Decimal | None:
        for window in (3, 7, 1, 15):
            snapshot = windows.get(window)
            if snapshot and snapshot.avg_position_size is not None:
                return snapshot.avg_position_size
        return None

    @staticmethod
    def _pnl_summary(rows: Sequence[Mapping[str, Any]]) -> tuple[Decimal, Decimal, Decimal | None]:
        total_pnl = _ZERO
        total_invested = _ZERO
        wins = 0
        losses = 0
        weighted_win_numerator = _ZERO
        weighted_win_denominator = _ZERO
        for row in rows:
            pnl = _safe_decimal(row.get("pnl"))
            invested = _safe_decimal(row.get("invested"))
            if pnl is not None:
                total_pnl += pnl
            if invested is not None and invested > 0:
                total_invested += invested
            row_wins = int(_safe_decimal(row.get("wins")) or 0)
            row_losses = int(_safe_decimal(row.get("losses")) or 0)
            wins += max(0, row_wins)
            losses += max(0, row_losses)
            row_trades = _safe_decimal(row.get("trades")) or _ZERO
            row_win_rate = _safe_decimal(row.get("win_rate"))
            if row_win_rate is not None and row_trades > 0:
                row_win_rate = row_win_rate / Decimal("100") if abs(row_win_rate) > _ONE else row_win_rate
                weighted_win_numerator += _clamp(row_win_rate) * row_trades
                weighted_win_denominator += row_trades
        if wins + losses > 0:
            win_rate = Decimal(wins) / Decimal(wins + losses)
        elif weighted_win_denominator > 0:
            win_rate = weighted_win_numerator / weighted_win_denominator
        else:
            win_rate = None
        return total_pnl, total_invested, win_rate

    def _official_rank_corroboration(self, rank: int | None) -> Decimal:
        if rank is None or rank <= 0:
            return _ZERO
        denominator = max(1, self._policy.candidate_limit - 1)
        return _clamp(_ONE - Decimal(rank - 1) / Decimal(denominator))

    @classmethod
    def _realized_pnl_corroboration(cls, rows: Sequence[Mapping[str, Any]]) -> Decimal:
        if not rows:
            return Decimal("0.5")
        total_pnl, total_invested, win_rate = cls._pnl_summary(rows)
        if total_invested > 0:
            realized_roi = total_pnl / total_invested
            roi_score = _clamp((realized_roi + Decimal("0.10")) / Decimal("0.40"))
        else:
            roi_score = Decimal("0.5") if total_pnl == 0 else (_ONE if total_pnl > 0 else _ZERO)
        win_score = (
            _clamp(((win_rate or Decimal("0.5")) - Decimal("0.40")) / Decimal("0.40"))
            if win_rate is not None
            else Decimal("0.5")
        )
        return _clamp(roi_score * Decimal("0.60") + win_score * Decimal("0.40"))

    @staticmethod
    def _all_time_confidence(snapshot: WalletAllTimeSnapshot | None) -> Decimal:
        if snapshot is None:
            return _ZERO
        trade_confidence = _clamp(Decimal(snapshot.total_trades or 0) / Decimal("1000"))
        roi = snapshot.roi_fraction
        roi_sanity = (
            _clamp((roi + Decimal("0.10")) / Decimal("0.30"))
            if roi is not None
            else Decimal("0.5")
        )
        return _clamp(trade_confidence * Decimal("0.75") + roi_sanity * Decimal("0.25"))

    def _score(
        self,
        context: _CandidateContext,
        evidence: DiscoveryEvidence,
        bankroll_fit: Any,
    ) -> tuple[Decimal, dict[str, Decimal]]:
        entry = context.leaderboard
        h_quality = _clamp((entry.h_score - Decimal("60")) / Decimal("40"))
        short_quality = _clamp(evidence.short_trade_share)
        activity = _clamp(Decimal(evidence.today_trade_count) / Decimal("5"))
        bankroll = _clamp(bankroll_fit.fit_score)

        short_window = context.wallet360.get(3) or context.wallet360.get(7)
        long_window = context.wallet360.get(15) or context.wallet360.get(7)
        roi = (
            short_window.roi
            if short_window and short_window.roi is not None
            else entry.roi_15d
        )
        win_rate = (
            short_window.win_rate
            if short_window and short_window.win_rate is not None
            else entry.win_rate_15d
        )
        profit_factor = short_window.profit_factor if short_window else None
        roi_score = _clamp(((roi or _ZERO) + Decimal("0.10")) / Decimal("0.50"))
        win_score = _clamp(((win_rate or Decimal("0.5")) - Decimal("0.45")) / Decimal("0.30"))
        pf_score = _clamp(((profit_factor or Decimal("1")) - Decimal("0.8")) / Decimal("1.2"))
        base_performance = (
            roi_score * Decimal("0.4")
            + win_score * Decimal("0.3")
            + pf_score * Decimal("0.3")
        )
        realized_pnl_corroboration = self._realized_pnl_corroboration(context.pnl_rows)
        performance = (
            base_performance * Decimal("0.80")
            + realized_pnl_corroboration * Decimal("0.20")
        )

        liquidity = _clamp(
            (evidence.median_liquidity_percentile or Decimal("0")) / Decimal("100")
        )
        trajectory_map = {
            "rising": Decimal("1"),
            "improving": Decimal("1"),
            "stable": Decimal("0.65"),
            "flat": Decimal("0.5"),
            "declining": Decimal("0"),
        }
        trajectory_key = entry.trajectory or (
            short_window.performance_trend if short_window else None
        )
        trajectory = trajectory_map.get(str(trajectory_key or "").lower(), Decimal("0.4"))
        sample = _clamp(Decimal(evidence.recent_trade_count) / Decimal("50"))
        if short_window and short_window.statistical_confidence is not None:
            sample = (sample + _clamp(short_window.statistical_confidence)) / Decimal("2")
        official_rank_corroboration = self._official_rank_corroboration(
            context.official_leaderboard_rank
        )
        if context.official_leaderboard_rank is not None:
            sample = sample * Decimal("0.85") + official_rank_corroboration * Decimal("0.15")
        all_time_confidence = self._all_time_confidence(context.all_time)
        if context.all_time is not None:
            sample = sample * Decimal("0.90") + all_time_confidence * Decimal("0.10")

        components = {
            "h_score": h_quality,
            "short_duration": short_quality,
            "today_activity": activity,
            "bankroll_fit": bankroll,
            "performance": performance,
            "liquidity": liquidity,
            "trajectory": trajectory,
            "sample_confidence": sample,
            "official_rank_corroboration": official_rank_corroboration,
            "realized_pnl_corroboration": realized_pnl_corroboration,
            "all_time_confidence": all_time_confidence,
        }
        score = (
            h_quality * Decimal("0.28")
            + short_quality * Decimal("0.22")
            + activity * Decimal("0.16")
            + bankroll * Decimal("0.14")
            + performance * Decimal("0.08")
            + liquidity * Decimal("0.05")
            + trajectory * Decimal("0.04")
            + sample * Decimal("0.03")
        )

        penalty = _ZERO
        if long_window:
            if long_window.max_drawdown is not None:
                penalty += _clamp(long_window.max_drawdown) * Decimal("0.08")
            if long_window.market_concentration_ratio is not None:
                penalty += _clamp(
                    (long_window.market_concentration_ratio - Decimal("0.30"))
                    / Decimal("0.70")
                ) * Decimal("0.06")
            if long_window.position_size_volatility_flag:
                penalty += Decimal("0.04")
            if long_window.suspicious_win_rate_flag:
                penalty += Decimal("0.10")
        if evidence.toxic_market_share:
            penalty += _clamp(evidence.toxic_market_share) * Decimal("0.08")
        if (
            context.all_time is not None
            and context.all_time.roi_fraction is not None
            and context.all_time.roi_fraction < 0
            and realized_pnl_corroboration >= Decimal("0.5")
        ):
            penalty += min(
                Decimal("0.03"),
                abs(context.all_time.roi_fraction) * Decimal("0.10"),
            )
        if context.price_jump_count:
            penalty += min(Decimal("0.05"), Decimal(context.price_jump_count) * Decimal("0.005"))
        if context.source_errors:
            penalty += min(Decimal("0.10"), Decimal(len(context.source_errors)) * Decimal("0.02"))
        components["penalty"] = penalty
        return _clamp(score - penalty).quantize(Decimal("0.0001")), components


__all__ = [
    "CapacityProbe",
    "HeisenbergCandidateService",
    "HeisenbergDiscoveryPolicy",
]
