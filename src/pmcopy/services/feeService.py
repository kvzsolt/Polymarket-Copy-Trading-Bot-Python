from __future__ import annotations

import time
from dataclasses import dataclass

from pmcopy.utils.logger import get_logger

logger = get_logger("FeeService")


@dataclass
class _FeeCache:
    fee_bps: int
    ts: float


class FeeService:
    def __init__(
        self,
        clob_client: object | None = None,
        ttl_seconds: int = 60,
        *,
        default_fee_bps: int = 20,
    ) -> None:
        del clob_client
        self._ttl_seconds = ttl_seconds
        self._cache: dict[str, _FeeCache] = {}
        self._default_fee_bps = default_fee_bps

    async def get_fee_bps(self, token_id: str) -> int:
        now = time.time()
        cached = self._cache.get(token_id)
        if cached and (now - cached.ts) < self._ttl_seconds:
            return cached.fee_bps

        # The unified SDK applies the current dynamic fee schedule while it
        # builds the order and deliberately does not expose the old standalone
        # ``get_fee_rate_bps`` endpoint.  This service therefore supplies only
        # the configured conservative estimate used by local sizing/telemetry;
        # it must never override the SDK's signed amounts.
        fee_bps = self._default_fee_bps
        self._cache[token_id] = _FeeCache(fee_bps=fee_bps, ts=now)
        return fee_bps
