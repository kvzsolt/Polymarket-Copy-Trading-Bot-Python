"""Composition tests for production leader SELL fraction resolution."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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
        )

    assert result is resolver
    build_historical.assert_called_once_with(
        "https://polygon.example.invalid/private",
        "https://fallback.example.invalid/private",
        request_timeout_seconds=2.5,
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
