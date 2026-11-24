"""Bankroll-aware auto-trader discovery using Falcon intelligence.

The H-Score is used as a shortlist signal, not as a trading decision.  Every
wallet is re-evaluated with fresh trades, Wallet360 windows, market duration,
Market360 quality and a follower-specific bankroll-fit calculation.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from pmcopy.integrations.falcon.errors import (
    FalconError,
    FalconPermissionError,
)
from pmcopy.integrations.falcon.models import (
    LeaderboardEntry,
    Market360Snapshot,
    MarketSnapshot,
    TradeSnapshot,
    Wallet360Snapshot,
    WalletAllTimeSnapshot,
    select_latest_market360,
    select_latest_wallet360,
)
from pmcopy.services.discovery.bankroll_compatibility import (
    evaluate_bankroll_compatibility,
)
from pmcopy.services.discovery.bot_signals import WalletActivitySignals
from pmcopy.services.discovery.discovery_models import (
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)


from pmcopy.services.discovery.falcon_policy import (
    PROVIDER_LEADERBOARD_ROW_CAP,
    CapacityProbe,
    FalconDiscoveryPolicy,
    _CandidateContext,
    _is_evm_address,
)
from pmcopy.services.discovery.falcon_scoring import _FalconScoringMixin



class FalconCandidateService(_FalconScoringMixin):
    """Discover and rank wallets for a specific follower bankroll."""

    SOURCE = "falcon"

    def __init__(
        self,
        client: Any,
        *,
        policy: FalconDiscoveryPolicy | None = None,
        capacity_probe: CapacityProbe | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._policy = policy or FalconDiscoveryPolicy()
        self._capacity_probe = capacity_probe
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._semaphore = asyncio.Semaphore(self._policy.candidate_concurrency)
        self._market_tasks: dict[str, asyncio.Task[tuple[MarketSnapshot | None, str | None]]] = {}
        self._quality_tasks: dict[
            str, asyncio.Task[tuple[Market360Snapshot | None, str | None]]
        ] = {}

    async def discover(
        self,
        funds: Decimal,
        *,
        wallets: Sequence[Mapping[str, Any]] | None = None,
    ) -> DiscoveryResult:
        """Return safe discovery candidates from persisted provider evidence.

        Args:
            funds: Follower bankroll the candidates are scored against.
            wallets: Optional targeted rows in the tolerant leaderboard wire
                shape (at minimum a wallet and its stored ``h_score``). When
                supplied, the provider leaderboard shortlist is skipped and
                exactly these wallets are analysed. ``None`` runs the normal
                full sweep.

        Returns:
            The ranked and rejected wallets for this cycle.
        """
        generated_at = self._normalize_now(self._now())
        self._market_tasks = {}
        self._quality_tasks = {}
        official_ranks = await self._load_official_leaderboard()
        if wallets is None:
            raw, shortlist_warnings = await self._load_shortlist_rows()
            entries, rejected = self._shortlist_entries(raw)
        else:
            shortlist_warnings = []
            entries, rejected = self._targeted_entries(wallets)

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
        warnings: list[str] = list(shortlist_warnings)
        for entry, analysis in zip(entries, analyses, strict=False):
            if isinstance(analysis, BaseException):
                if isinstance(analysis, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise analysis
                if isinstance(analysis, FalconError) and not analysis.retryable:
                    raise analysis.with_context(
                        "Falcon discovery evidence is unavailable"
                    ) from None
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

    def _shortlist_entries(
        self,
        rows: Sequence[Any],
    ) -> tuple[list[LeaderboardEntry], list[RejectedTrader]]:
        """Normalize provider leaderboard rows into the analysis shortlist.

        Args:
            rows: Concatenated provider leaderboard rows, in sort-order order.

        Returns:
            The deduplicated shortlist truncated to ``candidate_limit``, and the
            rows rejected before any per-wallet evidence was fetched.
        """
        entries: list[LeaderboardEntry] = []
        rejected: list[RejectedTrader] = []
        seen: set[str] = set()
        for row in rows:
            entry = LeaderboardEntry.from_mapping(row)
            if not entry.wallet or entry.wallet in seen:
                continue
            seen.add(entry.wallet)
            if not _is_evm_address(entry.wallet):
                rejected.append(self._pre_analysis_rejection(entry, "invalid_wallet_address"))
                continue
            if entry.h_score < self._policy.min_h_score:
                rejected.append(self._pre_analysis_rejection(entry, "h_score_below_threshold"))
                continue
            entries.append(entry)
            if len(entries) >= self._policy.candidate_limit:
                break
        return entries, rejected

    def _targeted_entries(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[list[LeaderboardEntry], list[RejectedTrader]]:
        """Normalize an explicit wallet list into the analysis shortlist.

        Targeted rows come from the persisted accepted roster, which already
        passed ``min_h_score`` when it was scored, so that shortlist gate is not
        re-applied and ``candidate_limit`` does not truncate the caller's list.
        Every evidence gate in ``_analyze`` still runs against fresh data.

        Args:
            rows: Targeted rows in the tolerant leaderboard wire shape.

        Returns:
            The deduplicated targeted entries, and the rows rejected before any
            per-wallet evidence was fetched.
        """
        entries: list[LeaderboardEntry] = []
        rejected: list[RejectedTrader] = []
        seen: set[str] = set()
        for row in rows:
            entry = LeaderboardEntry.from_mapping(row)
            if not entry.wallet or entry.wallet in seen:
                continue
            seen.add(entry.wallet)
            if not _is_evm_address(entry.wallet):
                rejected.append(self._pre_analysis_rejection(entry, "invalid_wallet_address"))
                continue
            entries.append(entry)
        return entries, rejected

    def _pre_analysis_rejection(self, entry: LeaderboardEntry, reason: str) -> RejectedTrader:
        """Return a rejection recorded before any evidence was fetched.

        Args:
            entry: The normalized leaderboard row that was rejected.
            reason: Stable machine-readable rejection reason.

        Returns:
            The sanitized rejection record for this cycle.
        """
        return RejectedTrader(
            wallet=entry.wallet,
            reasons=(reason,),
            source=self.SOURCE,
            h_score=entry.h_score,
        )

    async def _load_shortlist_rows(self) -> tuple[list[Any], list[str]]:
        """Union the configured leaderboard sort orders into one shortlist.

        The provider returns at most ``PROVIDER_LEADERBOARD_ROW_CAP`` rows per
        sort order, so a larger shortlist is assembled by querying several
        orders concurrently and concatenating them in configuration order.
        Row-level de-duplication stays in the caller, which already tracks
        seen wallets.

        Returns:
            The concatenated provider rows and one warning per sort order that
            failed.

        Raises:
            FalconError: If every configured sort order failed, so the cycle
                has no shortlist evidence at all.
        """
        modes = self._policy.leaderboard_sort_modes
        per_call = min(self._policy.candidate_limit, PROVIDER_LEADERBOARD_ROW_CAP)
        results = await asyncio.gather(
            *(self._leaderboard_page(mode, per_call) for mode in modes),
            return_exceptions=True,
        )
        rows: list[Any] = []
        warnings: list[str] = []
        first_error: FalconError | None = None
        succeeded = 0
        for mode, result in zip(modes, results, strict=False):
            if isinstance(result, BaseException):
                if not isinstance(result, FalconError):
                    raise result
                if not result.retryable:
                    raise result.with_context("Falcon discovery shortlist is unavailable") from None
                first_error = first_error or result
                warnings.append(f"leaderboard:{mode}:{type(result).__name__}")
                continue
            succeeded += 1
            rows.extend(result)
        if succeeded == 0 and first_error is not None:
            raise first_error
        return rows, warnings

    async def _leaderboard_page(self, sort_by: str, limit: int) -> list[Any]:
        """Return one provider leaderboard page for a single sort order.

        Args:
            sort_by: Provider sort order name.
            limit: Row cap for this call, never above the provider row cap.

        Returns:
            The decoded provider rows for that sort order.
        """
        return list(
            await self._client.leaderboard(
                min_win_rate_15d=self._policy.leaderboard_min_win_rate_15d,
                max_win_rate_15d=self._policy.leaderboard_max_win_rate_15d,
                min_roi_15d=self._policy.leaderboard_min_roi_15d,
                min_total_trades_15d=self._policy.leaderboard_min_trades_15d,
                max_total_trades_15d=self._policy.leaderboard_max_trades_15d,
                min_pnl_15d=self._policy.leaderboard_min_pnl_15d,
                sort_by=sort_by,
                limit=limit,
            )
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
        self._require_usable_provider_evidence(context.source_errors)
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
            """Fetch one Wallet360 window without failing sibling evidence."""
            try:
                rows = await self._client.wallet_360(entry.wallet, window)
                return window, select_latest_wallet360(rows, expected_window_days=window)
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                raise
            except Exception as exc:
                error_type = (
                    "FalconProtocolError"
                    if isinstance(exc, (TypeError, ValueError))
                    else type(exc).__name__
                )
                source_errors.append(f"wallet360_{window}:{error_type}")
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
            *(
                self._market_and_quality(condition_id, source_errors)
                for condition_id in condition_ids
            )
        )
        markets = {
            condition: market for condition, market, _quality in market_pairs if market is not None
        }
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
            signals=WalletActivitySignals.from_windows(wallet360),
        )

    async def _load_official_leaderboard(self) -> dict[str, int]:
        method = getattr(self._client, "pnl_leaderboard", None)
        if not callable(method):
            return {}
        try:
            result = method(
                wallet_address="ALL",
                leaderboard_period="7d",
                limit=self._policy.candidate_limit,
            )
            rows = await result if inspect.isawaitable(result) else result
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except FalconError as exc:
            if not exc.retryable:
                raise exc.with_context("Falcon corroborating leaderboard is unavailable") from None
            return {}
        except Exception:
            return {}
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            return {}
        ranks: dict[str, int] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            wallet = str(row.get("address") or row.get("wallet") or "").strip().lower()
            rank_value = row.get("rank")
            if isinstance(rank_value, bool) or not isinstance(rank_value, (str, int)):
                continue
            try:
                rank = int(rank_value)
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
        permission_errors: list[FalconPermissionError] = []

        async def load(token_id: str) -> int:
            """Return the bounded price-jump count for one outcome token."""
            try:
                result = method(
                    token_id=token_id,
                    resolution="15m",
                    min_change_pct=self._policy.price_jump_min_change_pct,
                    lookback_hours=self._policy.price_jump_lookback_hours,
                    limit=100,
                )
                rows = await result if inspect.isawaitable(result) else result
                return (
                    len(rows)
                    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes))
                    else 0
                )
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
                raise
            except FalconPermissionError as exc:
                permission_errors.append(
                    exc.with_context("Falcon price-jump evidence is unavailable")
                )
                return 0
            except Exception as exc:
                errors.append(f"price_jumps:{type(exc).__name__}")
                return 0

        counts = await asyncio.gather(*(load(token_id) for token_id in token_ids))
        if permission_errors:
            raise permission_errors[0] from None
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
                start_time=int(
                    (now - timedelta(hours=self._policy.recent_trade_lookback_hours)).timestamp()
                ),
                end_time=int(now.timestamp()),
                limit=self._policy.max_recent_trades,
            )
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except Exception as exc:
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
        except Exception as exc:
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
        except Exception as exc:
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
            self._market_tasks[condition_id] = asyncio.create_task(self._load_market(condition_id))
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
        except Exception as exc:
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
        except Exception as exc:
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
        except Exception as exc:
            errors.append(f"depth:{type(exc).__name__}")
            return None



__all__ = [
    "CapacityProbe",
    "FalconCandidateService",
    "FalconDiscoveryPolicy",
]
