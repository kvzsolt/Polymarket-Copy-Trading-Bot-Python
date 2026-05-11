from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from utils.createClobClient import MarketMeta
from utils.logger import get_logger

logger = get_logger("MarketMetaService")


@dataclass
class _CacheItem:
    value: MarketMeta
    ts: float


class MarketMetaService:
    def __init__(self, clob_client, ttl_seconds: int = 30):
        self._clob_client = clob_client
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, _CacheItem] = {}

    async def get(self, token_id: str, refresh: bool = False) -> MarketMeta:
        now = time.time()
        item = self._cache.get(token_id)
        if item and not refresh and (now - item.ts) < self._ttl_seconds:
            return item.value

        meta = await self._clob_client.get_market_meta(token_id, refresh=refresh)
        # Normalize defensive defaults
        tick = meta.tick_size if meta.tick_size > 0 else Decimal("0.01")
        min_order = meta.min_order_size if meta.min_order_size >= 0 else Decimal("0")
        normalized = MarketMeta(
            token_id=meta.token_id,
            tick_size=tick,
            min_order_size=min_order,
            neg_risk=bool(meta.neg_risk),
            best_bid=meta.best_bid,
            best_ask=meta.best_ask,
        )
        self._cache[token_id] = _CacheItem(value=normalized, ts=now)
        return normalized
