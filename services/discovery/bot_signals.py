"""Bot and uncopyable-trader heuristics derived from Wallet360 windows.

The provider exposes optional analytics that separate copyable human leaders
from high-frequency or micro-size automation: perfect-timing and suspicious
win-rate flags, similar-wallet counts, trade frequency, average trade size and
hierarchical category mix.  Every field is optional on the wire, so a missing
value yields ``None`` and the corresponding gate is skipped rather than
guessed.

Pure domain logic: no I/O, no provider access and no floating-point maths.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from integrations.falcon.models import Wallet360Snapshot
from services.discovery.category_profile import CategoryProfile

_LONG_WINDOW_PREFERENCE = (15, 7)
_ANY_WINDOW_PREFERENCE = (15, 7, 3, 1)


@dataclass(frozen=True, slots=True)
class WalletActivitySignals:
    """Summarize optional bot and copyability evidence for one wallet.

    Attributes:
        perfect_timing_flag: Whether any window raised the perfect-timing flag.
        suspicious_win_rate_flag: Whether any window raised the suspicious
            win-rate flag.
        similar_wallets_count: Highest reported count of near-identical
            wallets, or ``None`` when no window reported it.
        trades_per_day: Long-window trade frequency, or ``None`` when the
            trade total is unknown.
        avg_trade_size: Long-window average trade size in USDC, or ``None``.
        recent_win_rate: Thirty-day win rate when reported, otherwise the
            long-window win rate, or ``None``.
        category_profile: Long-window top-level category mix, empty when the
            provider reported no usable category rows.
    """

    perfect_timing_flag: bool
    suspicious_win_rate_flag: bool
    similar_wallets_count: int | None
    trades_per_day: Decimal | None
    avg_trade_size: Decimal | None
    recent_win_rate: Decimal | None
    category_profile: CategoryProfile

    @classmethod
    def from_windows(
        cls,
        windows: Mapping[int, Wallet360Snapshot],
    ) -> "WalletActivitySignals":
        """Derive activity signals from the loaded Wallet360 windows.

        Frequency, size, win rate and category mix come from the 15-day window
        and fall back to the 7-day window; flags and similar-wallet counts are
        aggregated across every loaded window.

        Args:
            windows: Loaded Wallet360 snapshots keyed by window length in days.

        Returns:
            The immutable signal set, with ``None`` wherever the provider gave
            no usable value.
        """
        snapshots = tuple(windows.values())
        long_window = cls._preferred(windows, _LONG_WINDOW_PREFERENCE)
        similar = [
            item.similar_wallets_count
            for item in snapshots
            if item.similar_wallets_count is not None
        ]
        return cls(
            perfect_timing_flag=any(item.perfect_timing_flag for item in snapshots),
            suspicious_win_rate_flag=any(item.suspicious_win_rate_flag for item in snapshots),
            similar_wallets_count=max(similar) if similar else None,
            trades_per_day=cls._trades_per_day(long_window),
            avg_trade_size=cls._avg_trade_size(windows),
            recent_win_rate=cls._recent_win_rate(long_window),
            category_profile=cls._category_profile(windows),
        )

    @staticmethod
    def _preferred(
        windows: Mapping[int, Wallet360Snapshot],
        preference: tuple[int, ...],
    ) -> Wallet360Snapshot | None:
        """Return the first available snapshot in ``preference`` order."""
        for window in preference:
            snapshot = windows.get(window)
            if snapshot is not None:
                return snapshot
        return None

    @staticmethod
    def _trades_per_day(snapshot: Wallet360Snapshot | None) -> Decimal | None:
        """Return exact trades per day for one window, or ``None``."""
        if snapshot is None or snapshot.total_trades is None:
            return None
        days = snapshot.days_active if snapshot.days_active else snapshot.window_days
        return Decimal(snapshot.total_trades) / Decimal(max(days or 0, 1))

    @classmethod
    def _avg_trade_size(cls, windows: Mapping[int, Wallet360Snapshot]) -> Decimal | None:
        """Return the first reported average trade size across windows."""
        for window in _ANY_WINDOW_PREFERENCE:
            snapshot = windows.get(window)
            if snapshot is not None and snapshot.avg_trade_size is not None:
                return snapshot.avg_trade_size
        return None

    @staticmethod
    def _recent_win_rate(snapshot: Wallet360Snapshot | None) -> Decimal | None:
        """Return the 30-day win rate, else the window win rate, else ``None``."""
        if snapshot is None:
            return None
        return (
            snapshot.win_rate_last_30d
            if snapshot.win_rate_last_30d is not None
            else (snapshot.win_rate)
        )

    @staticmethod
    def _category_profile(windows: Mapping[int, Wallet360Snapshot]) -> CategoryProfile:
        """Return the first non-empty long-window category profile."""
        for window in _LONG_WINDOW_PREFERENCE:
            profile = CategoryProfile.from_snapshot(windows.get(window))
            if not profile.is_empty:
                return profile
        return CategoryProfile.empty()


__all__ = ["WalletActivitySignals"]
