from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.createClobClient import ClobClientWrapper


class _FakeResponse:
    def __init__(self, payload, status: int = 200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
        return False


class _FakeSession:
    def __init__(self, payload, status: int = 200):
        self._payload = payload
        self._status = status
        self.urls: list[str] = []

    def get(self, url: str):
        self.urls.append(url)
        return _FakeResponse(self._payload, status=self._status)


@pytest.mark.asyncio
async def test_get_price_buy_requests_sell_side_for_best_ask():
    wrapper = ClobClientWrapper.__new__(ClobClientWrapper)
    session = _FakeSession({"price": 0.45})

    with patch("utils.createClobClient._get_session", return_value=session):
        quote = await wrapper.get_price("token-1", "BUY")

    assert quote == {"price": "0.45", "size": "0"}
    assert len(session.urls) == 1
    assert "side=SELL" in session.urls[0]


@pytest.mark.asyncio
async def test_get_price_sell_requests_buy_side_for_best_bid():
    wrapper = ClobClientWrapper.__new__(ClobClientWrapper)
    session = _FakeSession({"price": 0.41, "size": 250})

    with patch("utils.createClobClient._get_session", return_value=session):
        quote = await wrapper.get_price("token-1", "SELL")

    assert quote == {"price": "0.41", "size": "250"}
    assert len(session.urls) == 1
    assert "side=BUY" in session.urls[0]
