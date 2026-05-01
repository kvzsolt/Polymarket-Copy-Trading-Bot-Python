"""CLOB market-data adapter — ``AbstractMarketClient`` implementation.

Wraps ``utils.createClobClient.ClobClientWrapper`` (which owns the metadata
cache and the async executor offloading for the sync ``py_clob_client_v2``
SDK). Services depend on the ABC, not on the wrapper, so the wrapper can
be swapped out or rebuilt for testing without touching consumers.
"""

from __future__ import annotations

import asyncio
from typing import Any

from domain.ports.clients import AbstractMarketClient
from utils.createClobClient import ClobClientWrapper


class PolymarketClobClient(AbstractMarketClient):
    def __init__(self, wrapper: ClobClientWrapper):
        self._wrapper = wrapper

    async def get_order_book(self, token_id: str) -> Any:
        return await self._wrapper.get_order_book(token_id)

    async def get_market_meta(self, condition_id: str) -> Any:
        return await self._wrapper.get_market_meta(condition_id)

    async def get_last_trade_price(self, token_id: str) -> Any:
        loop = asyncio.get_running_loop()
        client = self._wrapper.client
        return await loop.run_in_executor(
            None, lambda: client.get_last_trade_price(token_id)
        )

    async def get_midpoint(self, token_id: str) -> Any:
        loop = asyncio.get_running_loop()
        client = self._wrapper.client
        return await loop.run_in_executor(
            None, lambda: client.get_midpoint(token_id)
        )
