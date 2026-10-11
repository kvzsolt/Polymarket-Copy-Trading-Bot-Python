"""Composition tests for production leader SELL fraction resolution."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import app


def test_builds_historical_and_data_fallback_resolver_from_runtime_config() -> None:
    """The composition root binds both read-only authorities to the resolver."""

    config = SimpleNamespace(
        polymarket=SimpleNamespace(
            rpc_url="https://polygon.example.invalid/private",
            fallback_rpc_url="https://fallback.example.invalid/private",
        )
    )
    historical = object()
    data_fallback = object()
    buy_scope_guard = object()
    resolver = object()
    provider_runtime = object()

    with (
        patch(
            "adapters.polygon.historical_erc1155_balance."
            "Web3HistoricalErc1155BalanceReader.from_rpc_url",
            return_value=historical,
        ) as build_historical,
        patch(
            "integrations.polymarket.leader_sell_fraction_data_api.DataApiLeaderSellFractionReader",
            return_value=data_fallback,
        ) as build_fallback,
        patch(
            "services.onchain_detection.leader_sell_fraction_resolver.LeaderSellFractionResolver",
            return_value=resolver,
        ) as build_resolver,
    ):
        result = app._build_leader_sell_fraction_resolver(
            config,
            buy_scope_guard=buy_scope_guard,
            provider_runtime=provider_runtime,
        )

    assert result is resolver
    build_historical.assert_called_once_with(
        "https://polygon.example.invalid/private",
        "https://fallback.example.invalid/private",
        request_timeout_seconds=2.5,
        provider_runtime=provider_runtime,
    )
    assert callable(build_fallback.call_args.kwargs["fetch_json"])
    build_resolver.assert_called_once_with(
        historical_balances=historical,
        fallback_fractions=data_fallback,
        buy_scope_guard=buy_scope_guard,
    )


def test_onchain_dispatch_receives_the_production_resolver() -> None:
    """The app source passes the composed resolver into the signal adapter."""

    source = Path(app.__file__).read_text(encoding="utf-8")

    assert "sell_fraction_resolver=_build_leader_sell_fraction_resolver(" in source
    assert "buy_scope_guard=leader_sell_scope_guard" in source
    assert 'context.extra["leader_sell_scope_guard"] = leader_sell_scope_guard' in source


@pytest.mark.asyncio
async def test_fallback_runtime_reader_preserves_decimal_json_values() -> None:
    config = SimpleNamespace(
        polymarket=SimpleNamespace(rpc_url="https://example.invalid", fallback_rpc_url="")
    )
    body = '{"size":0.1234567890123456789012345678}'

    async def read_json(url, **kwargs):
        return kwargs["json_decoder"](body)

    client = SimpleNamespace(get_json=AsyncMock(side_effect=read_json))
    with (
        patch(
            "adapters.polygon.historical_erc1155_balance.Web3HistoricalErc1155BalanceReader.from_rpc_url"
        ),
        patch(
            "integrations.polymarket.leader_sell_fraction_data_api.DataApiLeaderSellFractionReader"
        ) as fallback,
        patch("utils.fetchData._get_http_client", return_value=client),
        patch("utils.fetchData.fetch_data", AsyncMock(return_value=json.loads(body))),
    ):
        app._build_leader_sell_fraction_resolver(config)
        payload = await fallback.call_args.kwargs["fetch_json"]("https://example.invalid/data")
    assert payload == {"size": Decimal("0.1234567890123456789012345678")}
    client.get_json.assert_awaited_once()
