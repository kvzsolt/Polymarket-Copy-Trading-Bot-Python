"""Hierarchical Wallet360 trading-category shares.

Wallet360 reports ``performance_by_category`` as hierarchical rows such as
``"Crypto"``, ``"Crypto / Prices"`` and ``"Sports / Soccer"``.  A parent row
duplicates the numbers of its children, and sibling top-level rows can overlap,
so their trade counts may sum above the wallet's own trade total.  This module
turns those rows into exact, clamped top-level shares.

Pure domain logic: no I/O, no provider access and no floating-point maths.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from integrations.falcon.models import Wallet360Snapshot

_ZERO = Decimal("0")
_ONE = Decimal("1")
_CATEGORY_KEYS = ("category", "category_name", "name")
_TRADE_KEYS = ("trades", "trade_count", "num_trades", "total_trades")
_SEPARATOR = "/"


def _text(row: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _trade_count(row: Mapping[str, Any], keys: tuple[str, ...]) -> int | None:
    for key in keys:
        value = row.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            number = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if not number.is_finite() or number < 0:
            return None
        return int(number)
    return None


class _BucketAccumulator:
    """Accumulate hierarchical rows into named buckets with parent precedence.

    An exact row for a bucket wins over the sum of its deeper rows; deeper
    rows are summed only while no exact row has been seen.
    """

    def __init__(self) -> None:
        self.labels: dict[str, str] = {}
        self._exact: dict[str, int] = {}
        self._folded: dict[str, int] = {}

    def add(self, label: str, trades: int, *, exact: bool) -> None:
        """Record ``trades`` for ``label`` as an exact or folded contribution."""
        key = label.casefold()
        self.labels.setdefault(key, label)
        target = self._exact if exact else self._folded
        target[key] = target.get(key, 0) + trades

    def count(self, key: str) -> int:
        """Return the resolved trade count for a bucket key."""
        return self._exact.get(key, self._folded.get(key, 0))

    def ordered(self) -> tuple[tuple[str, int], ...]:
        """Return buckets ordered by descending trades, then by name."""
        return tuple(
            (self.labels[key], self.count(key))
            for key in sorted(self.labels, key=lambda item: (-self.count(item), self.labels[item]))
        )


@dataclass(frozen=True, slots=True)
class CategoryProfile:
    """Describe how a wallet's trades split across top-level categories.

    Two granularities are kept.  Top-level buckets (``"Sports"``,
    ``"Crypto"``) drive category exclusion.  Specialty buckets are the
    sub-category rows (``"Sports / Soccer"``, ``"Esports / MOBA"``), with
    deeper rows folded into their sub-category and a top-level category that
    has no sub-rows standing as its own bucket; they drive the specialist
    preference because the provider files every esports market under the
    ``"Sports"`` umbrella as well, which makes top-level dominance
    near-universal for sports traders.

    Attributes:
        trades_by_top_level: Top-level category name and trade count pairs,
            ordered by descending trade count then by name.
        trades_by_specialty: Specialty bucket name and trade count pairs,
            ordered by descending trade count then by name.
        total_trades: The wallet's own trade total used as share denominator.
    """

    trades_by_top_level: tuple[tuple[str, int], ...]
    trades_by_specialty: tuple[tuple[str, int], ...]
    total_trades: int

    @classmethod
    def from_snapshot(cls, snapshot: Wallet360Snapshot | None) -> "CategoryProfile":
        """Build a profile from one Wallet360 window snapshot.

        Args:
            snapshot: A parsed Wallet360 window, or ``None`` when the provider
                returned no usable window.

        Returns:
            The category profile, empty when evidence is missing.
        """
        if snapshot is None:
            return cls.empty()
        return cls.from_performance_by_category(
            snapshot.performance_by_category,
            total_trades=snapshot.total_trades,
        )

    @classmethod
    def from_performance_by_category(
        cls,
        rows: Iterable[Mapping[str, Any]],
        *,
        total_trades: int | None,
    ) -> "CategoryProfile":
        """Build a profile from raw hierarchical category rows.

        A parent row wins over its duplicated child rows; children are summed
        only when their parent row is absent.  Rows without a category name or
        without a parseable non-negative trade count are ignored.

        Args:
            rows: Decoded ``performance_by_category`` rows.
            total_trades: The wallet's own trade total for the same window.

        Returns:
            The category profile, empty when no row or no denominator remains.
        """
        top_level = _BucketAccumulator()
        specialty = _BucketAccumulator()
        top_levels_with_sub_rows: set[str] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            name = _text(row, _CATEGORY_KEYS)
            trades = _trade_count(row, _TRADE_KEYS)
            if not name or trades is None:
                continue
            parts = [part.strip() for part in name.split(_SEPARATOR) if part.strip()]
            if not parts:
                continue
            top_level.add(parts[0], trades, exact=len(parts) == 1)
            if len(parts) >= 2:
                sub_category = f"{parts[0]} {_SEPARATOR} {parts[1]}"
                top_levels_with_sub_rows.add(parts[0].casefold())
                specialty.add(sub_category, trades, exact=len(parts) == 2)
        for key, label in top_level.labels.items():
            if key not in top_levels_with_sub_rows:
                specialty.add(label, top_level.count(key), exact=True)
        denominator = total_trades if isinstance(total_trades, int) and total_trades > 0 else 0
        return cls(
            trades_by_top_level=top_level.ordered(),
            trades_by_specialty=specialty.ordered(),
            total_trades=denominator,
        )

    @classmethod
    def empty(cls) -> "CategoryProfile":
        """Return the profile used when no category evidence is available."""
        return cls(trades_by_top_level=(), trades_by_specialty=(), total_trades=0)

    @property
    def is_empty(self) -> bool:
        """Return whether no category share can be computed from this profile."""
        return not self.trades_by_top_level or self.total_trades <= 0

    def top_level_categories(self) -> tuple[str, ...]:
        """Return top-level category names ordered by descending trade count."""
        return tuple(name for name, _count in self.trades_by_top_level)

    def dominant_category(self) -> str | None:
        """Return the top-level category holding the most trades.

        Returns:
            The category name, or ``None`` when the profile is empty.
        """
        if self.is_empty:
            return None
        return self.trades_by_top_level[0][0]

    def dominant_share(self) -> Decimal:
        """Return the clamped exact share of the largest top-level category.

        Returns:
            A share between zero and one, zero when the profile is empty.
        """
        dominant = self.dominant_category()
        if dominant is None:
            return _ZERO
        return self.top_level_share(dominant)

    def dominant_specialty(self) -> str | None:
        """Return the specialty bucket holding the most trades.

        Returns:
            The sub-category (or childless top-level) name, or ``None`` when
            the profile is empty.
        """
        if self.is_empty or not self.trades_by_specialty:
            return None
        return self.trades_by_specialty[0][0]

    def dominant_specialty_share(self) -> Decimal:
        """Return the clamped exact share of the largest specialty bucket.

        Returns:
            A share between zero and one, zero when the profile is empty.
        """
        if self.is_empty or not self.trades_by_specialty:
            return _ZERO
        count = self.trades_by_specialty[0][1]
        if count <= 0:
            return _ZERO
        return min(_ONE, Decimal(count) / Decimal(self.total_trades))

    def top_level_share(self, name: str) -> Decimal:
        """Return the clamped exact share of trades in one top-level category.

        Args:
            name: Top-level category name; matched case-insensitively.

        Returns:
            A share between zero and one, zero when the profile is empty or the
            category is absent.
        """
        return self.share_of((name,))

    def share_of(self, names: Iterable[str]) -> Decimal:
        """Return the clamped exact share of trades in any of ``names``.

        Args:
            names: Top-level category names; matched case-insensitively and
                de-duplicated before summing.

        Returns:
            A share between zero and one, zero when the profile is empty or no
            name matches.
        """
        if self.is_empty:
            return _ZERO
        wanted = {str(name).strip().casefold() for name in names if str(name).strip()}
        if not wanted:
            return _ZERO
        matched = sum(
            count for label, count in self.trades_by_top_level if label.casefold() in wanted
        )
        if matched <= 0:
            return _ZERO
        return min(_ONE, Decimal(matched) / Decimal(self.total_trades))


__all__ = ["CategoryProfile"]
