"""Strategy-profile model for the strategy profiler / env tuner (PR3)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class StrategyArchetype(StrEnum):
    """Trader archetypes the profiler can classify a leader into."""

    FAST_CRYPTO_SCALPER = "fast_crypto_scalper"
    LOW_PRICE_LOTTERY = "low_price_lottery"
    NEAR_CERTAINTY_SWEEPER = "near_certainty_sweeper"
    EVENT_SPECIALIST = "event_specialist"
    BALANCED_SWING = "balanced_swing"
    NOISY_UNCOPYABLE = "noisy_uncopyable"


# Feature keys produced by ``strategy_profiler.compute_features``. Kept here
# so the loader/profiler/classifier agree on the canonical vocabulary.
FEATURE_KEYS: tuple[str, ...] = (
    "trade_count_7d",
    "trade_count_30d",
    "median_notional_usdc",
    "avg_entry_price",
    "low_price_entry_ratio",
    "near_certainty_entry_ratio",
    "crypto_fast_signal_ratio",
    "avg_hold_time_minutes",
    "grouped_signal_ratio",
    "maker_fill_ratio",
    "taker_fill_ratio",
    "win_rate_7d",
    "roi_7d",
    "pnl_30d_usdc",
    "max_drawdown_30d",
    "market_concentration_hhi",
    "near_resolution_ratio",
    "near_max_exit_ratio",
)


@dataclass(frozen=True)
class StrategyProfile:
    """Result of profiling a single leader wallet."""

    wallet: str
    lookback_days: int
    features: dict[str, float | None] = field(default_factory=dict)
    archetype: StrategyArchetype = StrategyArchetype.NOISY_UNCOPYABLE
    copyable: bool = False
    computed_at: int = 0
    notes: str = ""

    def feature(self, key: str, default: float | None = None) -> float | None:
        return self.features.get(key, default)
