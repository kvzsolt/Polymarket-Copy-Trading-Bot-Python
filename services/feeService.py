from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from config.env import ENV
from utils.logger import get_logger

logger = get_logger("FeeService")


@dataclass
class _FeeCache:
    fee_bps: int
    ts: float


class FeeService:
    def __init__(self, clob_client, ttl_seconds: int = 60):
        self._clob_client = clob_client
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, _FeeCache] = {}
        self._default_fee_bps = int(float(ENV.get("TRANSACTION_FEE_BPS", 20)))

    async def get_fee_bps(self, token_id: str) -> int:
        now = time.time()
        cached = self._cache.get(token_id)
        if cached and (now - cached.ts) < self._ttl_seconds:
            return cached.fee_bps

        try:
            loop = asyncio.get_running_loop()
            fee_bps = await loop.run_in_executor(
                None,
                lambda: int(self._clob_client.client.get_fee_rate_bps(token_id)),
            )
            self._cache[token_id] = _FeeCache(fee_bps=fee_bps, ts=now)
            return fee_bps
        except Exception as exc:
            logger.warning(
                "Failed to fetch fee rate for token=%s, using default=%s bps: %s",
                token_id,
                self._default_fee_bps,
                exc,
            )
            return self._default_fee_bps
