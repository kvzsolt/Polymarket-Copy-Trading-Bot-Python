"""Immutable automatic-trader lifecycle and exact performance policy.

Provider calls, persistence, and order execution belong to injected adapters.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum


class TraderStatus(StrEnum):
    """Persisted membership; draining and degraded traders retain exit handling."""

    ACTIVE = "active"
    RESERVE = "reserve"
    PAPER = "paper"
    DRAINING = "draining"
    DEGRADED = "degraded"


class AutoTraderValidation:
    """Validate identities and evidence before lifecycle decisions."""

    @staticmethod
    def wallet(value: str) -> str:
        """Return a normalized hexadecimal leader identity."""
        value = value.strip().lower()
        if (
            len(value) != 42
            or not value.startswith("0x")
            or any(character not in "0123456789abcdef" for character in value[2:])
        ):
            raise ValueError("invalid trader wallet")
        return value

    @staticmethod
    def instant(value: datetime | None) -> None:
        """Reject naive timestamps rather than guessing their timezone."""
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("trader timestamps must be timezone aware")


@dataclass(frozen=True, slots=True)
class AutoTraderScope:
    """Separate follower and paper/live membership without credential inference."""

    follower_account: str
    trading_mode: str

    def __post_init__(self) -> None:
        """Validate the durable scope."""
        if not self.follower_account.strip() or self.trading_mode not in {"paper", "live"}:
            raise ValueError("invalid automatic trader scope")


@dataclass(frozen=True, slots=True)
class CopyPerformance:
    """Source-specific, fully attributed financial evidence."""

    evaluation_id: str
    observed_at: datetime
    started_at: datetime
    closed_episodes: int
    realized_pnl: Decimal
    recent_pnl: Decimal
    drawdown: Decimal | None
    watermark: str
    complete: bool = True

    def __post_init__(self) -> None:
        """Reject malformed financial observations."""
        AutoTraderValidation.instant(self.observed_at)
        AutoTraderValidation.instant(self.started_at)
        if not self.evaluation_id or not self.watermark or self.closed_episodes < 0:
            raise ValueError("invalid copy performance identity or sample")
        if any(not value.is_finite() for value in (self.realized_pnl, self.recent_pnl)) or (
            self.drawdown is not None and (not self.drawdown.is_finite() or self.drawdown < 0)
        ):
            raise ValueError("invalid copy performance amounts")


@dataclass(frozen=True, slots=True)
class FalconObservation:
    """One successful targeted provider observation, never an outage placeholder."""

    wallet: str
    score: Decimal
    observed_at: datetime
    observation_id: str
    accepted: bool = True

    def __post_init__(self) -> None:
        """Validate provider-neutral evidence."""
        object.__setattr__(self, "wallet", AutoTraderValidation.wallet(self.wallet))
        AutoTraderValidation.instant(self.observed_at)
        if not self.observation_id or not self.score.is_finite() or not 0 <= self.score <= 1:
            raise ValueError("invalid Falcon observation")


@dataclass(frozen=True, slots=True)
class Trader:
    """One durable trader membership and its latest independently scoped evidence."""

    wallet: str
    status: TraderStatus = TraderStatus.RESERVE
    score: Decimal = Decimal(0)
    observed_at: datetime | None = None
    selected_at: datetime | None = None
    qualification_streak: int = 0
    falcon_strikes: int = 0
    result_strikes: int = 0
    observation_id: str = ""
    result_watermark: str = ""
    result_episode_count: int = 0
    result_source: str = ""
    activation_source: str = ""
    degradation_reason: str = ""
    paper: CopyPerformance | None = None
    live: CopyPerformance | None = None
    revision: int = 1

    def __post_init__(self) -> None:
        """Normalize identity and reject corrupt membership state."""
        object.__setattr__(self, "wallet", AutoTraderValidation.wallet(self.wallet))
        AutoTraderValidation.instant(self.observed_at)
        AutoTraderValidation.instant(self.selected_at)
        if not isinstance(self.status, TraderStatus) or not self.score.is_finite():
            raise ValueError("invalid trader state")
        if min(self.qualification_streak, self.falcon_strikes, self.result_strikes) < 0:
            raise ValueError("negative trader evidence streak")
        if self.revision < 1:
            raise ValueError("invalid trader revision")


@dataclass(frozen=True, slots=True)
class AutoRoster:
    """Atomic roster revision, including initialization and consumed rotation events."""

    revision: int = 0
    initialized: bool = False
    traders: tuple[Trader, ...] = ()
    rotation_events: tuple[str, ...] = ()
    entry_block_reason: str = ""
    last_command_fingerprint: str = ""

    def __post_init__(self) -> None:
        """Reject duplicate memberships."""
        if self.revision < 0 or len({row.wallet for row in self.traders}) != len(self.traders):
            raise ValueError("invalid automatic roster")


@dataclass(frozen=True, slots=True)
class AutoTraderPolicy:
    """Select candidates and evaluate fresh evidence without I/O or clock reads."""

    target_count: int
    promote_score: Decimal = Decimal("0.55")
    demote_score: Decimal = Decimal("0.5")
    required_cycles: int = 3
    minimum_tenure_seconds: int = 10800
    evidence_max_age_seconds: int = 3600
    paper_days: int = 7
    minimum_episodes: int = 20
    max_paper_drawdown: Decimal = Decimal("0.10")

    def __post_init__(self) -> None:
        """Validate one source of roster capacity and qualification thresholds."""
        if isinstance(self.target_count, bool) or self.target_count < 1:
            raise ValueError("target_count must be a positive integer")
        if not 0 <= self.demote_score < self.promote_score <= 1:
            raise ValueError("invalid automatic trader score thresholds")
        if (
            min(
                self.required_cycles,
                self.evidence_max_age_seconds,
                self.paper_days,
                self.minimum_episodes,
            )
            < 1
        ):
            raise ValueError("invalid automatic trader evidence bounds")
        if self.minimum_tenure_seconds < 0 or not 0 < self.max_paper_drawdown <= 1:
            raise ValueError("invalid automatic trader risk bounds")

    def observe_falcon(self, trader: Trader, observation: FalconObservation) -> Trader:
        """Advance evidence once; rejected evaluations are distinct from failures."""
        if observation.wallet != trader.wallet:
            raise ValueError("Falcon observation belongs to another wallet")
        if observation.observation_id == trader.observation_id:
            return trader
        if trader.observed_at is not None and observation.observed_at < trader.observed_at:
            return trader
        qualifying = observation.accepted and observation.score >= self.promote_score
        bad = not observation.accepted or observation.score < self.demote_score
        updated = replace(
            trader,
            score=observation.score,
            observed_at=observation.observed_at,
            observation_id=observation.observation_id,
            qualification_streak=trader.qualification_streak + 1 if qualifying else 0,
            falcon_strikes=trader.falcon_strikes + 1 if bad else 0,
            revision=trader.revision + 1,
        )
        if updated.falcon_strikes >= self.required_cycles and self._tenure_met(
            updated, observation.observed_at
        ):
            return replace(
                updated, status=TraderStatus.DEGRADED, degradation_reason="falcon_quality"
            )
        return updated

    def observe_results(self, trader: Trader, results: CopyPerformance, now: datetime) -> Trader:
        """Count loss streaks only when complete closed-episode evidence changes."""
        if not results.complete or results.closed_episodes < self.minimum_episodes:
            return trader
        if (
            results.watermark == trader.result_watermark
            or results.closed_episodes <= trader.result_episode_count
        ):
            return trader
        updated = replace(
            trader,
            result_watermark=results.watermark,
            result_episode_count=results.closed_episodes,
            result_strikes=trader.result_strikes + 1 if results.recent_pnl < 0 else 0,
            revision=trader.revision + 1,
        )
        if updated.result_strikes >= self.required_cycles and self._tenure_met(updated, now):
            return replace(updated, status=TraderStatus.DEGRADED, degradation_reason="copy_losses")
        return updated

    def eligible(self, trader: Trader, now: datetime) -> bool:
        """Require current Falcon qualification and no demonstrated copy loss streak."""
        return bool(
            trader.observed_at is not None
            and timedelta(0)
            <= now - trader.observed_at
            <= timedelta(seconds=self.evidence_max_age_seconds)
            and trader.score >= self.promote_score
            and trader.qualification_streak >= self.required_cycles
            and trader.falcon_strikes == 0
            and trader.result_strikes < self.required_cycles
            and trader.status is not TraderStatus.DEGRADED
        )

    def select(self, traders: tuple[Trader, ...], now: datetime) -> tuple[Trader, ...]:
        """Keep incumbents, drain excess, then fill vacancies from qualified reserves."""
        ranked = sorted(traders, key=self.rank)
        active = [row for row in ranked if row.status is TraderStatus.ACTIVE]
        selected = {row.wallet for row in active[: self.target_count]}
        for row in ranked:
            if len(selected) >= self.target_count:
                break
            if row.wallet not in selected and self.eligible(row, now):
                selected.add(row.wallet)
        result = []
        for row in traders:
            if row.wallet in selected and row.status is not TraderStatus.ACTIVE:
                row = replace(
                    row,
                    status=TraderStatus.ACTIVE,
                    selected_at=now,
                    activation_source=row.activation_source or "falcon",
                    degradation_reason="",
                    revision=row.revision + 1,
                )
            elif row.status is TraderStatus.ACTIVE and row.wallet not in selected:
                row = replace(
                    row,
                    status=TraderStatus.DRAINING,
                    degradation_reason="target_reduced",
                    revision=row.revision + 1,
                )
            result.append(row)
        return tuple(result)

    def paper_qualification(
        self, trader: Trader, results: CopyPerformance, now: datetime
    ) -> tuple[str, ...]:
        """Return every operator-visible reason paper promotion is unavailable."""
        reasons = []
        if not results.complete:
            reasons.append("incomplete_evidence")
        if (
            now - results.observed_at > timedelta(seconds=self.evidence_max_age_seconds)
            or results.observed_at > now
        ):
            reasons.append("stale_evidence")
        if now - results.started_at < timedelta(days=self.paper_days):
            reasons.append("observation_too_short")
        if results.closed_episodes < self.minimum_episodes:
            reasons.append("insufficient_episodes")
        if results.realized_pnl <= 0:
            reasons.append("not_profitable")
        if results.drawdown is None or results.drawdown >= self.max_paper_drawdown:
            reasons.append("drawdown")
        if not self.eligible(trader, now):
            reasons.append("falcon_quality")
        return tuple(reasons)

    @staticmethod
    def rank(trader: Trader) -> tuple[Decimal, Decimal, str]:
        """Order by bankroll-aware quality, then complete paper profits and identity."""
        paper_pnl = (
            trader.paper.realized_pnl if trader.paper and trader.paper.complete else Decimal(0)
        )
        return (-trader.score, -paper_pnl, trader.wallet)

    def _tenure_met(self, trader: Trader, now: datetime) -> bool:
        return trader.selected_at is None or now - trader.selected_at >= timedelta(
            seconds=self.minimum_tenure_seconds
        )
