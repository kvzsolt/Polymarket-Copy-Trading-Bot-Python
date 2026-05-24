"""Phase 4 — Context bucketing for episodes and trades.

Assigns categorical buckets along several dimensions so that
performance analysis can be sliced by context.
"""
from __future__ import annotations

import time
from decimal import Decimal

from domain.discovery_models import (
    DAYS_TO_RESOLUTION_BUCKETS,
    LIQUIDITY_BUCKETS,
    TRADE_SIZE_BUCKETS,
    ContextBucket,
    Episode,
    MarketMeta,
)


class ContextService:
    """Assigns context buckets to episodes (or individual trades)."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def bucket_episode(
        self,
        episode: Episode,
        market: MarketMeta | None = None,
        liquidity_value: Decimal | None = None,
    ) -> list[ContextBucket]:
        """Compute all applicable context buckets for an episode.

        Returns a list of ContextBucket and also mutates
        ``episode.context_buckets`` for convenience.
        """
        buckets: list[ContextBucket] = []

        # Category
        if market and market.category:
            b = ContextBucket(
                dimension="category",
                bucket=market.category.lower().strip(),
                raw_value=market.category,
            )
            buckets.append(b)

        # Days to resolution
        if market and market.end_date_ts is not None:
            b = self._days_to_resolution_bucket(market.end_date_ts, episode.opened_ts)
            if b:
                buckets.append(b)

        # Trade size (based on total entry USDC notional)
        entry_usdc = episode.entry_size * episode.entry_avg_price
        b = self._trade_size_bucket(entry_usdc)
        if b:
            buckets.append(b)

        # Liquidity (if value provided)
        if liquidity_value is not None:
            b = self._liquidity_bucket(liquidity_value)
            if b:
                buckets.append(b)

        # Write back onto episode
        for bucket in buckets:
            episode.context_buckets[bucket.dimension] = bucket.bucket

        return buckets

    # ------------------------------------------------------------------
    # Standalone bucket helpers (usable outside episode context)
    # ------------------------------------------------------------------

    def compute_trade_size_bucket(self, usdc_size: Decimal) -> ContextBucket | None:
        return self._trade_size_bucket(usdc_size)

    def compute_days_to_resolution_bucket(
        self, end_date_ts: int, reference_ts: int | None = None,
    ) -> ContextBucket | None:
        ref = reference_ts or int(time.time())
        return self._days_to_resolution_bucket(end_date_ts, ref)

    def compute_liquidity_bucket(self, value: Decimal) -> ContextBucket | None:
        return self._liquidity_bucket(value)

    def compute_category_bucket(self, category: str) -> ContextBucket | None:
        if not category:
            return None
        return ContextBucket(
            dimension="category",
            bucket=category.lower().strip(),
            raw_value=category,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _trade_size_bucket(self, usdc: Decimal) -> ContextBucket | None:
        for label, lo, hi in TRADE_SIZE_BUCKETS:
            if lo <= usdc < hi:
                return ContextBucket(
                    dimension="trade_size",
                    bucket=label,
                    raw_value=str(usdc),
                )
        return None

    def _days_to_resolution_bucket(self, end_ts: int, ref_ts: int) -> ContextBucket | None:
        days = max(0, (end_ts - ref_ts) // 86400)
        for label, lo, hi in DAYS_TO_RESOLUTION_BUCKETS:
            if lo <= days < hi:
                return ContextBucket(
                    dimension="days_to_resolution",
                    bucket=label,
                    raw_value=str(days),
                )
        return None

    def _liquidity_bucket(self, value: Decimal) -> ContextBucket | None:
        for label, lo, hi in LIQUIDITY_BUCKETS:
            if lo <= value < hi:
                return ContextBucket(
                    dimension="liquidity",
                    bucket=label,
                    raw_value=str(value),
                )
        return None
