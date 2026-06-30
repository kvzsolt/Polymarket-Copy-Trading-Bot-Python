"""Compute the 17 strategy features from a loaded history (PR3).

Features (per the deep-research report):

  trade_count_7d / trade_count_30d
  median_notional_usdc
  avg_entry_price
  low_price_entry_ratio            — fraction of entries at price < 0.10
  near_certainty_entry_ratio       — fraction of entries at price >= 0.75
  crypto_fast_signal_ratio         — fraction of trades in <60s clusters
  avg_hold_time_minutes
  grouped_signal_ratio             — fraction of trades within 5s of another
  maker_fill_ratio / taker_fill_ratio
  win_rate_7d
  roi_7d
  pnl_30d_usdc
  max_drawdown_30d
  market_concentration_hhi         — Herfindahl index on conditionId shares
  near_resolution_ratio            — fraction of entries with price >= 0.90
  near_max_exit_ratio              — fraction of exits at price >= 0.95

Each feature degrades gracefully to ``None`` when the underlying data is
insufficient (e.g. no closed positions for win-rate). The classifier treats
``None`` as "unknown" and falls back conservatively.
"""
from __future__ import annotations

import statistics
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from typing import Any

from models.strategy_profile import StrategyArchetype, StrategyProfile
from services.strategy_profile.leader_history_loader import LoadedHistory

_DAY_MS = 86_400_000


def _to_decimal(value: Any, default: Decimal = Decimal(0)) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_timestamp(value: Any) -> int | None:
    if value is None:
        return None
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return None
    # Polymarket timestamps are ms epoch; anything < 1e12 is probably seconds.
    if ts < 1_000_000_000_000:
        ts *= 1000
    return ts


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def compute_features(history: LoadedHistory) -> dict[str, float | None]:
    """Compute all 17 features from ``history``."""
    now_ms = int(time.time() * 1000)
    cutoff_7d = now_ms - 7 * _DAY_MS
    cutoff_30d = now_ms - 30 * _DAY_MS

    activity = list(history.activity)
    trades = list(history.trades)
    closed = list(history.closed_positions)

    # Prefer ``activity`` (typed TRADE) for trade-count/entry analysis; fall
    # back to ``trades`` if activity is empty.
    trade_rows = activity or trades

    trade_count_7d = sum(
        1 for r in trade_rows if (_safe_timestamp(r.get("timestamp")) or 0) >= cutoff_7d
    )
    trade_count_30d = sum(
        1 for r in trade_rows if (_safe_timestamp(r.get("timestamp")) or 0) >= cutoff_30d
    )

    notionals: list[float] = []
    entry_prices: list[float] = []
    low_price_entries = 0
    near_certainty_entries = 0
    near_resolution_entries = 0
    for r in trade_rows:
        size = _to_decimal(r.get("size"))
        price = _to_decimal(r.get("price"))
        usdc = _to_decimal(r.get("usdcSize"), size * price)
        if usdc > 0:
            notionals.append(float(usdc))
        if price > 0:
            entry_prices.append(float(price))
            if price < Decimal("0.10"):
                low_price_entries += 1
            if price >= Decimal("0.75"):
                near_certainty_entries += 1
            if price >= Decimal("0.90"):
                near_resolution_entries += 1

    median_notional = _median(notionals)
    avg_entry_price = (
        float(sum(entry_prices) / len(entry_prices)) if entry_prices else None
    )
    low_price_entry_ratio = (
        float(low_price_entries / len(entry_prices)) if entry_prices else None
    )
    near_certainty_entry_ratio = (
        float(near_certainty_entries / len(entry_prices)) if entry_prices else None
    )
    near_resolution_ratio = (
        float(near_resolution_entries / len(entry_prices)) if entry_prices else None
    )

    # Hold time + grouped signals from closed positions.
    hold_times_minutes: list[float] = []
    for c in closed:
        entry_ts = _safe_timestamp(c.get("createdAt") or c.get("openedAt"))
        exit_ts = _safe_timestamp(c.get("closedAt") or c.get("timestamp"))
        if entry_ts and exit_ts and exit_ts > entry_ts:
            hold_times_minutes.append((exit_ts - entry_ts) / 60_000.0)
    avg_hold_time_minutes = (
        float(sum(hold_times_minutes) / len(hold_times_minutes))
        if hold_times_minutes
        else None
    )

    # Grouped signals: trades whose timestamp is within 5s of another trade.
    timestamps = sorted(
        t for t in (_safe_timestamp(r.get("timestamp")) for r in trade_rows) if t is not None
    )
    grouped = 0
    for i, ts in enumerate(timestamps):
        if i > 0 and ts - timestamps[i - 1] <= 5_000:
            grouped += 1
        elif i < len(timestamps) - 1 and timestamps[i + 1] - ts <= 5_000:
            grouped += 1
    grouped_signal_ratio = (
        float(grouped / len(timestamps)) if timestamps else None
    )

    # Crypto-fast signal ratio: trades within 60s of another (tight clusters).
    fast = 0
    for i, ts in enumerate(timestamps):
        if i > 0 and ts - timestamps[i - 1] <= 60_000:
            fast += 1
    crypto_fast_signal_ratio = (
        float(fast / len(timestamps)) if timestamps else None
    )

    # Maker/taker fill ratio from trade rows (``maker`` boolean or side).
    maker_count = sum(1 for r in trade_rows if _is_maker(r))
    taker_count = len(trade_rows) - maker_count
    if trade_rows:
        maker_fill_ratio = float(maker_count / len(trade_rows))
        taker_fill_ratio = float(taker_count / len(trade_rows))
    else:
        maker_fill_ratio = None
        taker_fill_ratio = None

    # Win-rate / ROI / PnL from closed positions.
    wins = 0
    pnl_values: list[float] = []
    for c in closed:
        pnl = _to_decimal(c.get("pnl") or c.get("realizedPnl"))
        if pnl != 0:
            pnl_values.append(float(pnl))
            if pnl > 0:
                wins += 1
    win_rate_7d = None
    closed_7d = [
        c for c in closed
        if (_safe_timestamp(c.get("closedAt") or c.get("timestamp")) or 0) >= cutoff_7d
    ]
    if closed_7d:
        wins_7d = sum(
            1 for c in closed_7d if _to_decimal(c.get("pnl") or c.get("realizedPnl")) > 0
        )
        win_rate_7d = float(wins_7d / len(closed_7d))
    roi_7d = None
    if closed_7d:
        total_pnl_7d = sum(
            float(_to_decimal(c.get("pnl") or c.get("realizedPnl"))) for c in closed_7d
        )
        total_cost_7d = sum(
            float(_to_decimal(c.get("entrySize")) * _to_decimal(c.get("avgPrice")))
            for c in closed_7d
        )
        if total_cost_7d > 0:
            roi_7d = total_pnl_7d / total_cost_7d
    pnl_30d = None
    closed_30d = [
        c for c in closed
        if (_safe_timestamp(c.get("closedAt") or c.get("timestamp")) or 0) >= cutoff_30d
    ]
    if closed_30d:
        pnl_30d = sum(
            float(_to_decimal(c.get("pnl") or c.get("realizedPnl"))) for c in closed_30d
        )

    # Max drawdown over 30d closed-position pnl series (cumulative).
    max_drawdown_30d = _max_drawdown(pnl_values)

    # Market concentration HHI on conditionId shares (sum of squared shares).
    conditions = [str(r.get("conditionId") or r.get("condition_id") or "") for r in trade_rows]
    conditions = [c for c in conditions if c]
    if conditions:
        counts = Counter(conditions)
        total = len(conditions)
        hhi = sum((count / total) ** 2 for count in counts.values())
        market_concentration_hhi = float(hhi)
    else:
        market_concentration_hhi = None

    # Near-max exit ratio: exits at price >= 0.95.
    near_max_exits = 0
    exit_prices: list[float] = []
    for c in closed:
        exit_price = _to_decimal(c.get("exitAvgPrice") or c.get("exitPrice") or c.get("curPrice"))
        if exit_price > 0:
            exit_prices.append(float(exit_price))
            if exit_price >= Decimal("0.95"):
                near_max_exits += 1
    near_max_exit_ratio = (
        float(near_max_exits / len(exit_prices)) if exit_prices else None
    )

    return {
        "trade_count_7d": float(trade_count_7d),
        "trade_count_30d": float(trade_count_30d),
        "median_notional_usdc": median_notional,
        "avg_entry_price": avg_entry_price,
        "low_price_entry_ratio": low_price_entry_ratio,
        "near_certainty_entry_ratio": near_certainty_entry_ratio,
        "crypto_fast_signal_ratio": crypto_fast_signal_ratio,
        "avg_hold_time_minutes": avg_hold_time_minutes,
        "grouped_signal_ratio": grouped_signal_ratio,
        "maker_fill_ratio": maker_fill_ratio,
        "taker_fill_ratio": taker_fill_ratio,
        "win_rate_7d": win_rate_7d,
        "roi_7d": roi_7d,
        "pnl_30d_usdc": pnl_30d,
        "max_drawdown_30d": max_drawdown_30d,
        "market_concentration_hhi": market_concentration_hhi,
        "near_resolution_ratio": near_resolution_ratio,
        "near_max_exit_ratio": near_max_exit_ratio,
    }


def _is_maker(row: dict[str, Any]) -> bool:
    """Best-effort maker detection from a trade/activity row."""
    maker = row.get("maker")
    if isinstance(maker, bool):
        return maker
    if isinstance(maker, str):
        return maker.lower() in ("true", "1", "yes")
    side = str(row.get("side") or "").upper()
    if side in ("MAKER_BUY", "MAKER_SELL", "MAKER"):
        return True
    if side in ("TAKER_BUY", "TAKER_SELL", "TAKER"):
        return False
    return False


def _max_drawdown(pnl_series: list[float]) -> float | None:
    if not pnl_series:
        return None
    peak = 0.0
    cumulative = 0.0
    max_dd = 0.0
    for pnl in pnl_series:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    return float(max_dd) if max_dd > 0 else 0.0


def profile_leader(
    history: LoadedHistory,
    *,
    classify: bool = True,
) -> StrategyProfile:
    """Compute features and (optionally) classify into an archetype."""
    from services.strategy_profile.strategy_classifier import classify_profile

    features = compute_features(history)
    archetype = StrategyArchetype.NOISY_UNCOPYABLE
    copyable = False
    notes = ""
    if classify:
        archetype, copyable, notes = classify_profile(features, history.lookback_days)
    return StrategyProfile(
        wallet=history.wallet,
        lookback_days=history.lookback_days,
        features=features,
        archetype=archetype,
        copyable=copyable,
        computed_at=int(time.time()),
        notes=notes,
    )