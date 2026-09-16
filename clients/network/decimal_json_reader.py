"""Read public financial JSON without passing numbers through binary floats."""

from __future__ import annotations

import json
from collections.abc import Callable
from decimal import Decimal

from clients.network.http_client import AsyncHttpClient


class DecimalJsonReader:
    """Read through an injected shared client with exact decimal decoding."""

    def __init__(self, client_provider: Callable[[], AsyncHttpClient]) -> None:
        self._client_provider = client_provider

    async def get(self, url: str) -> object:
        """Read a JSON document without logging raw payloads or transport errors.

        Args:
            url: Public endpoint URL constructed by its data adapter.

        Returns:
            Decoded JSON with Decimal for non-integral numbers. Boundary parsers
            must still validate field types, units and finite values.
        """
        return await self._client_provider().get_json(url, silent=True, json_decoder=self.decode)

    @staticmethod
    def decode(payload: str) -> object:
        """Decode exact numeric values; retain invalid constants for rejection."""
        decoded: object = json.loads(payload, parse_float=Decimal, parse_constant=Decimal)
        return decoded
