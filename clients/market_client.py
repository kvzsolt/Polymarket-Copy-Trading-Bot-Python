"""Unified-SDK market-data adapter."""

from __future__ import annotations

from typing import Any

from domain.ports.clients import AbstractMarketClient
from integrations.polymarket.sdk_client import PolymarketClient


class PolymarketClobClient(AbstractMarketClient):
    def __init__(self, wrapper: PolymarketClient):
        self._wrapper = wrapper

    async def get_order_book(self, token_id: str) -> Any:
        return await self._wrapper.get_order_book(token_id)

    async def get_market_meta(self, condition_id: str) -> Any:
        return await self._wrapper.get_market_meta(condition_id)

    async def get_last_trade_price(self, token_id: str) -> Any:
        return await self._wrapper.public_client.get_last_trade_price(token_id=token_id)

    async def get_midpoint(self, token_id: str) -> Any:
        return await self._wrapper.public_client.get_midpoint(token_id=token_id)
