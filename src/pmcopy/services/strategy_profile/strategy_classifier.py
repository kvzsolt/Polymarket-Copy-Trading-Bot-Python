"""Classify a feature set into one of six :class:`StrategyArchetype` values (PR3)."""

from __future__ import annotations

from pmcopy.domain.strategy_profile import StrategyArchetype

_CLFResult = tuple[StrategyArchetype, bool, str]


def _f(features: dict[str, float | None], key: str, default: float = 0.0) -> float:
    value = features.get(key)
    if value is None:
        return default
    return float(value)


def classify_profile(
    features: dict[str, float | None],
    lookback_days: int = 30,
) -> _CLFResult:
    """Classify a feature dict into an archetype.

    Returns ``(archetype, copyable, notes)``. Falls back conservatively to
    ``NOISY_UNCOPYABLE`` with ``copyable=False`` when the trader is too thin,
    too noisy, or too concentrated to copy safely.
    """
    trade_count_30d = _f(features, "trade_count_30d")
    trade_count_7d = _f(features, "trade_count_7d")
    hhi = _f(features, "market_concentration_hhi", default=1.0)
    avg_hold = _f(features, "avg_hold_time_minutes", default=-1.0)
    fast_ratio = _f(features, "crypto_fast_signal_ratio", default=-1.0)
    grouped_ratio = _f(features, "grouped_signal_ratio", default=-1.0)
    low_price_ratio = _f(features, "low_price_entry_ratio", default=-1.0)
    near_certainty_ratio = _f(features, "near_certainty_entry_ratio", default=-1.0)
    near_max_exit_ratio = _f(features, "near_max_exit_ratio", default=-1.0)
    win_rate_7d = _f(features, "win_rate_7d", default=-1.0)
    roi_7d = _f(features, "roi_7d", default=-999.0)

    # Too-thin guard: <5 trades in 30d is uncopyable (insufficient signal).
    if trade_count_30d < 5:
        return (
            StrategyArchetype.NOISY_UNCOPYABLE,
            False,
            "too few trades in lookback (<5) for reliable profiling",
        )

    # Noisy/uncopyable guard: extreme concentration on one market.
    if hhi >= 0.8 and trade_count_30d < 15:
        return (
            StrategyArchetype.NOISY_UNCOPYABLE,
            False,
            "extreme single-market concentration with low trade count",
        )

    # FAST_CRYPTO_SCALPER: short holds + tight clusters + many trades.
    if (
        0 <= avg_hold <= 30
        and fast_ratio >= 0.40
        and trade_count_7d >= 10
        and grouped_ratio >= 0.30
    ):
        return (
            StrategyArchetype.FAST_CRYPTO_SCALPER,
            True,
            "sub-30min holds + tight trade clusters + high 7d trade count",
        )

    # LOW_PRICE_LOTTERY: majority of entries below 0.10.
    if low_price_ratio >= 0.60:
        return (
            StrategyArchetype.LOW_PRICE_LOTTERY,
            True,
            "majority of entries at price < 0.10 (lottery-style)",
        )

    # NEAR_CERTAINTY_SWEEPER: majority of entries >= 0.75 + near-max exits.
    if near_certainty_ratio >= 0.55 and near_max_exit_ratio >= 0.30:
        return (
            StrategyArchetype.NEAR_CERTAINTY_SWEEPER,
            True,
            "entries clustered near certainty with near-max exits",
        )

    # BALANCED_SWING: reasonable trade count, mixed prices, decent recent
    # edge. Checked before EVENT_SPECIALIST so a profitable generalist is
    # not misclassified as an event specialist purely on concentration.
    if trade_count_30d >= 10 and 0.35 <= hhi <= 0.75 and win_rate_7d >= 0.45 and roi_7d >= -0.05:
        return (
            StrategyArchetype.BALANCED_SWING,
            True,
            "balanced activity with positive recent edge",
        )

    # EVENT_SPECIALIST: moderate concentration on a small set of markets,
    # multi-hour holds, without the positive-edge signature of BALANCED_SWING.
    if 0.25 <= hhi <= 0.70 and trade_count_30d >= 10 and avg_hold > 60:
        return (
            StrategyArchetype.EVENT_SPECIALIST,
            True,
            "moderate market concentration with multi-hour holds",
        )

    # Default: noisy/uncopyable. Recommend not auto-tuning.
    return (
        StrategyArchetype.NOISY_UNCOPYABLE,
        False,
        "no clear archetype signature; recommend manual review",
    )
