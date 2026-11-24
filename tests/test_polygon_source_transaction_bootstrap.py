"""Tests for Polygon source-transaction dependency composition."""

from __future__ import annotations

from datetime import timedelta

from pmcopy.orchestrators.bootstrap.polygon_bootstrap import PolygonSourceRuntimeConfig


def test_polygon_runtime_config_uses_sixty_second_confirmation_timeout() -> None:
    """The agreed timeout is explicit and deterministic in composition."""

    config = PolygonSourceRuntimeConfig(
        chain_id=137,
        websocket_url="wss://polygon.example.invalid/v2/redacted",
        exchange_addresses=("0x" + "11" * 20,),
        watched_wallets=frozenset({"0x" + "22" * 20}),
    )

    assert config.confirmation_timeout == timedelta(seconds=60)
    assert config.pending_targets == ("0x" + "11" * 20,)


def test_polygon_runtime_config_deduplicates_normalized_pending_targets() -> None:
    """Direct exchange and Safe targets form one normalized provider filter."""

    address = "0x" + "ab" * 20
    config = PolygonSourceRuntimeConfig(
        chain_id=137,
        websocket_url="wss://polygon.example.invalid/v2/redacted",
        exchange_addresses=(address.upper(),),
        watched_wallets=frozenset({address}),
    )

    assert config.pending_targets == (address,)
