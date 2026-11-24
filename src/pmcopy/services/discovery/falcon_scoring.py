"""Hard-gate evaluation, corroboration, and bankroll-aware candidate scoring mixin."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence

from pmcopy.integrations.falcon.errors import (
    FalconAuthenticationError,
    FalconBadRequestError,
    FalconConfigurationError,
    FalconPermissionError,
    FalconProtocolError,
)
from pmcopy.integrations.falcon.models import (
    PnlOutcomeCounts,
    Wallet360Snapshot,
    WalletAllTimeSnapshot,
)
from pmcopy.services.discovery.bot_signals import WalletActivitySignals
from pmcopy.services.discovery.discovery_models import (
    DiscoveryEvidence,
)
from pmcopy.services.discovery.falcon_policy import (
    _ONE,
    _ZERO,
    FalconDiscoveryPolicy,
    _CandidateContext,
    _clamp,
    _median,
    _safe_decimal,
)


class _FalconScoringMixin:
    """Pure quantitative scoring and hard-gate filtering for Falcon candidates."""

    _policy: FalconDiscoveryPolicy


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
        activity_window = timedelta(hours=self._policy.activity_window_hours)
        activity_count = sum(1 for item in trades if now - item.timestamp <= activity_window)
        stale = (
            last_trade is None
            or now - last_trade > timedelta(hours=self._policy.max_last_trade_age_hours)
            or activity_count < self._policy.min_activity_window_trades
        )
        if stale:
            reasons.append("inactive_or_stale")
        if len(trades) < self._policy.min_recent_trades:
            reasons.append("insufficient_recent_trades")

        short_count = 0
        classified_count = 0
        for trade in trades:
            market = context.markets.get(trade.condition_id)
            if market is None:
                continue
            classified_count += 1
            if market.is_short_horizon(trade.timestamp, self._policy.short_market_max_days):
                short_count += 1
        short_share = (
            Decimal(short_count) / Decimal(classified_count) if classified_count else _ZERO
        )
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
        selected_flags = frozenset(self._policy.toxic_market_flags)
        toxic_market_count = sum(
            1 for item in context.market_quality.values() if item.toxic_flags() & selected_flags
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
        if (
            self._policy.drawdown_gate_enabled
            and max_drawdown is not None
            and max_drawdown > self._policy.max_drawdown
        ):
            reasons.append("drawdown_above_threshold")

        signals = context.signals
        excluded_share = (
            None
            if signals.category_profile.is_empty
            else signals.category_profile.share_of(self._policy.excluded_categories)
        )
        reasons.extend(self._bot_signal_reasons(signals, excluded_share))

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
            fresh_wallet360_windows=tuple(sorted(item.window_days for item in fresh_snapshots)),
            latest_wallet360_at=latest_snapshot_at,
            last_trade_at=last_trade,
            recent_trade_count=len(trades),
            activity_window_trade_count=activity_count,
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
            "fresh_wallet360_windows": tuple(sorted(item.window_days for item in fresh_snapshots)),
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
            "trades_per_day": (
                str(signals.trades_per_day) if signals.trades_per_day is not None else None
            ),
            "avg_trade_size": (
                str(signals.avg_trade_size) if signals.avg_trade_size is not None else None
            ),
            "similar_wallets_count": signals.similar_wallets_count,
            "excluded_category_share": (
                str(excluded_share) if excluded_share is not None else None
            ),
            "specialist_dominant_category": signals.category_profile.dominant_specialty(),
            "specialist_dominant_share": str(signals.category_profile.dominant_specialty_share()),
            "specialist_win_rate": (
                str(signals.recent_win_rate) if signals.recent_win_rate is not None else None
            ),
            "source_errors": tuple(context.source_errors),
        }
        return list(dict.fromkeys(reasons)), evidence, metrics

    def _bot_signal_reasons(
        self,
        signals: WalletActivitySignals,
        excluded_share: Decimal | None,
    ) -> list[str]:
        """Return every bot or uncopyability rejection reason for one wallet.

        Each heuristic depends on an optional provider field.  A missing value
        skips its own gate instead of guessing, which is documented fail-open
        behaviour; the core evidence-completeness gate stays fail-closed.

        Args:
            signals: Optional activity evidence for the wallet.
            excluded_share: Share of trades in excluded top-level categories,
                or ``None`` when no category evidence exists.

        Returns:
            Stable machine-readable rejection reasons, possibly empty.
        """
        reasons: list[str] = []
        if self._policy.reject_perfect_timing_flag and signals.perfect_timing_flag:
            reasons.append("perfect_timing")
        if self._policy.reject_suspicious_win_rate_flag and signals.suspicious_win_rate_flag:
            reasons.append("suspicious_win_rate")
        if (
            signals.similar_wallets_count is not None
            and signals.similar_wallets_count > self._policy.max_similar_wallets
        ):
            reasons.append("sybil_risk")
        if (
            signals.trades_per_day is not None
            and signals.trades_per_day > self._policy.max_trades_per_day
        ):
            reasons.append("hyperactive_trader")
        if (
            signals.avg_trade_size is not None
            and signals.avg_trade_size < self._policy.min_avg_trade_size_usdc
        ):
            reasons.append("micro_trade_bot")
        if (
            excluded_share is not None
            and excluded_share > self._policy.max_excluded_category_trade_share
        ):
            reasons.append("excluded_category_dominant")
        return reasons

    @staticmethod
    def _avg_position_size(windows: Mapping[int, Wallet360Snapshot]) -> Decimal | None:
        for window in (3, 7, 1, 15):
            snapshot = windows.get(window)
            if snapshot and snapshot.avg_position_size is not None:
                return snapshot.avg_position_size
        return None

    @staticmethod
    def _require_usable_provider_evidence(errors: Sequence[str]) -> None:
        """Fail the cycle on authentication, configuration or malformed provider evidence."""
        for error_type in (
            FalconAuthenticationError,
            FalconPermissionError,
            FalconConfigurationError,
            FalconBadRequestError,
            FalconProtocolError,
        ):
            if any(error.rsplit(":", 1)[-1] == error_type.__name__ for error in errors):
                raise error_type("Falcon discovery evidence is unavailable")

    @staticmethod
    def _pnl_summary(rows: Sequence[Mapping[str, Any]]) -> tuple[Decimal, Decimal, Decimal | None]:
        total_pnl = _ZERO
        total_invested = _ZERO
        wins = 0
        losses = 0
        for row in rows:
            pnl = _safe_decimal(row.get("pnl"))
            invested = _safe_decimal(row.get("invested"))
            if pnl is not None:
                total_pnl += pnl
            if invested is not None and invested > 0:
                total_invested += invested
            counts = PnlOutcomeCounts.from_mapping(row)
            if counts.wins is not None and counts.losses is not None:
                wins += counts.wins
                losses += counts.losses
        if wins + losses > 0:
            win_rate = Decimal(wins) / Decimal(wins + losses)
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
            _clamp((win_rate - Decimal("0.40")) / Decimal("0.40"))
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
            _clamp((roi + Decimal("0.10")) / Decimal("0.30")) if roi is not None else Decimal("0.5")
        )
        return _clamp(trade_confidence * Decimal("0.75") + roi_sanity * Decimal("0.25"))

    def _specialist_bonus(self, context: _CandidateContext) -> Decimal:
        """Return one when the wallet is a winning single-category specialist.

        Specialization is measured as the share of the dominant specialty
        bucket (sub-category such as ``"Esports / MOBA"``, or a childless
        top-level category) rather than as a category count or top-level share:
        almost every wallet carries a small residual bucket, and the provider
        files every esports market under the ``"Sports"`` umbrella too, so
        coarser measures fire for generalists.  The bonus is a soft preference,
        never a gate: wallets without category or win-rate evidence simply
        score zero here.

        Args:
            context: Loaded provider evidence for one wallet.

        Returns:
            ``Decimal("1")`` when the dominant specialty bucket holds at least
            ``specialist_min_dominant_share`` of the wallet's trades and its
            recent win rate reaches ``specialist_min_win_rate``, otherwise
            ``Decimal("0")``.
        """
        if not self._policy.specialist_bonus_enabled:
            return _ZERO
        signals = context.signals
        profile = signals.category_profile
        if profile.is_empty:
            return _ZERO
        if profile.dominant_specialty_share() < self._policy.specialist_min_dominant_share:
            return _ZERO
        win_rate = signals.recent_win_rate
        if win_rate is None or win_rate < self._policy.specialist_min_win_rate:
            return _ZERO
        return _ONE

    def _score(
        self,
        context: _CandidateContext,
        evidence: DiscoveryEvidence,
        bankroll_fit: Any,
    ) -> tuple[Decimal, dict[str, Decimal]]:
        entry = context.leaderboard
        h_quality = _clamp((entry.h_score - Decimal("60")) / Decimal("40"))
        short_quality = _clamp(evidence.short_trade_share)
        activity = _clamp(Decimal(evidence.activity_window_trade_count) / Decimal("5"))
        bankroll = _clamp(bankroll_fit.fit_score)

        short_window = context.wallet360.get(3) or context.wallet360.get(7)
        long_window = context.wallet360.get(15) or context.wallet360.get(7)
        roi = short_window.roi if short_window and short_window.roi is not None else entry.roi_15d
        win_rate = (
            short_window.win_rate
            if short_window and short_window.win_rate is not None
            else entry.win_rate_15d
        )
        profit_factor = short_window.profit_factor if short_window else None
        roi_score = _clamp(((roi or _ZERO) + Decimal("0.10")) / Decimal("0.50"))
        win_score = _clamp(
            ((Decimal("0.5") if win_rate is None else win_rate) - Decimal("0.45")) / Decimal("0.30")
        )
        pf_score = _clamp(((profit_factor or Decimal("1")) - Decimal("0.8")) / Decimal("1.2"))
        base_performance = (
            roi_score * Decimal("0.4") + win_score * Decimal("0.3") + pf_score * Decimal("0.3")
        )
        realized_pnl_corroboration = self._realized_pnl_corroboration(context.pnl_rows)
        performance = base_performance * Decimal("0.80") + realized_pnl_corroboration * Decimal(
            "0.20"
        )

        liquidity = _clamp((evidence.median_liquidity_percentile or Decimal("0")) / Decimal("100"))
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

        specialist = self._specialist_bonus(context)
        components = {
            "h_score": h_quality,
            "short_duration": short_quality,
            "recent_activity": activity,
            "specialist": specialist,
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
            + specialist * self._policy.specialist_bonus_weight
        )

        penalty = _ZERO
        if long_window:
            if long_window.max_drawdown is not None:
                penalty += _clamp(long_window.max_drawdown) * Decimal("0.08")
            if long_window.market_concentration_ratio is not None:
                penalty += _clamp(
                    (long_window.market_concentration_ratio - Decimal("0.30")) / Decimal("0.70")
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
