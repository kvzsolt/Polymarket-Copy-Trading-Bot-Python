from __future__ import annotations

import pytest

from services.onchain_detection.condition_id_resolver import GammaConditionIdResolver


@pytest.mark.asyncio
async def test_gamma_resolver_caches_both_clob_tokens(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_fetch_data(url: str, *, silent: bool = False):
        calls.append(url)
        assert silent is True
        return [
            {
                "conditionId": "0xcond",
                "clobTokenIds": '["token-a", "token-b"]',
            }
        ]

    monkeypatch.setattr(
        "services.onchain_detection.condition_id_resolver.fetch_data",
        fake_fetch_data,
    )
    resolver = GammaConditionIdResolver(base_url="https://gamma.example/markets")

    assert await resolver.resolve("token-a") == "0xcond"
    assert await resolver.resolve("token-b") == "0xcond"
    assert len(calls) == 1
    assert calls[0].startswith("https://gamma.example/markets?")
    assert "clob_token_ids=token-a" in calls[0]


@pytest.mark.asyncio
async def test_gamma_resolver_returns_blank_on_lookup_failure(monkeypatch) -> None:
    async def fake_fetch_data(url: str, *, silent: bool = False):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "services.onchain_detection.condition_id_resolver.fetch_data",
        fake_fetch_data,
    )
    resolver = GammaConditionIdResolver()

    assert await resolver.resolve("token-a") == ""
