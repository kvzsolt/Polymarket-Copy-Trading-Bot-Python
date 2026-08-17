"""CCXT-only runtime cutover release-gate contracts.

The official Polymarket SDK is gone. CCXT is the only trading transport. These
contracts pin the final state so a partial revert cannot silently reintroduce
an SDK, a backend selector, a hybrid gateway, or a shadow mode.
"""

from __future__ import annotations

import asyncio
import importlib
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.paper_execution import PaperExecutionGateway
from integrations.polymarket.venue_runtime import build_polymarket_runtime
from services.executionService import ExecutionService

ROOT = Path(__file__).resolve().parents[1]


class _Exchange:
    """Deterministic CCXT exchange double used by composition tests."""

    def __init__(self, settings: dict[str, object]) -> None:
        self.settings = settings
        self.options = dict(settings.get("options") or {})
        self.privateKey = settings.get("privateKey")
        self.walletAddress = settings.get("walletAddress")
        self.has = {
            "prediction": True,
            "createOrder": True,
            "fetchOrderBook": True,
            "fetchOrder": True,
            "fetchOpenOrders": True,
            "fetchMyTrades": True,
            "cancelOrder": True,
        }
        self.close = MagicMock()
        self.load_api_credentials = MagicMock(return_value={"apiKey": "dk"})

    def clob_private_get_data_orders(self, _params: object) -> dict:
        return {"data": [], "next_cursor": ""}

    def clob_private_get_data_trades(self, _params: object) -> dict:
        return {"data": [], "next_cursor": ""}

    def polymarket_order_raw_amounts(self, *_args: object) -> dict[str, str]:
        return {"makerAmount": "0", "takerAmount": "0"}


def _runtime_user() -> SimpleNamespace:
    return SimpleNamespace(
        user_id="operator",
        wallet=SimpleNamespace(
            funder_address="0x" + "b" * 40,
            private_key="0x" + "a" * 64,
        ),
    )


def _config(mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(trading_mode=mode),
        wallet=SimpleNamespace(
            funder_address="0x" + "b" * 40,
            private_key="0x" + "a" * 64,
        ),
    )


def test_packaged_execution_config_is_ccxt_only_paper_safe() -> None:
    """Ship paper mode with no venue selector and no SDK keys."""

    payload = yaml.safe_load((ROOT / "config" / "execution.yml").read_text(encoding="utf-8"))

    assert payload["trading_mode"] == "paper"
    assert "venue_backend" not in payload
    assert "allow_experimental_ccxt_live" not in payload
    assert "shadow_ccxt" not in payload


def test_paper_runtime_is_ccxt_market_reads_plus_simulated_writes() -> None:
    """Paper mode uses CCXT for reads and the simulator for writes."""

    runtime = asyncio.run(
        build_polymarket_runtime(
            runtime_user=_runtime_user(),
            config=_config("paper"),
            exchange_factory=_Exchange,
            ccxt_version="4.5.70",
        )
    )

    assert isinstance(runtime.client, CcxtPredictionClient)
    assert runtime.client.authenticated is False
    assert isinstance(runtime.gateway, PaperExecutionGateway)
    assert isinstance(runtime.gateway.market_data_gateway, CcxtPredictionGateway)


def test_live_runtime_is_authenticated_ccxt_gateway() -> None:
    """Live mode uses the authenticated CCXT gateway for reads and writes."""

    runtime = asyncio.run(
        build_polymarket_runtime(
            runtime_user=_runtime_user(),
            config=_config("live"),
            exchange_factory=_Exchange,
            ccxt_version="4.5.70",
        )
    )

    assert runtime.client.authenticated is True
    assert isinstance(runtime.gateway, CcxtPredictionGateway)


def test_execution_service_requires_a_gateway() -> None:
    """Reject an unusable legacy client when no gateway is injected."""

    with pytest.raises(TypeError, match="clob_gateway"):
        ExecutionService(
            clob_client=object(),
            order_repo=MagicMock(),
            config=SimpleNamespace(
                execution=SimpleNamespace(
                    execution_submit_concurrency=1,
                    circuit_failure_threshold=2,
                )
            ),
        )


def test_no_sdk_dependency_exists_in_packaging() -> None:
    """polymarket-client and py-clob-client must be absent from dependencies."""

    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "dependencies"
    ]

    for banned in ("polymarket-client", "py-clob-client"):
        assert not any(banned in dep for dep in dependencies), f"{banned} still pinned"


def test_no_sdk_module_exists() -> None:
    """The SDK and hybrid adapter modules must not exist."""

    for removed in (
        "integrations/polymarket/sdk_client.py",
        "integrations/polymarket/clob_v2_gateway.py",
        "integrations/polymarket/hybrid_gateway.py",
        "integrations/polymarket/backend_factory.py",
        "integrations/polymarket/runtime_gateway.py",
        "integrations/polymarket/ccxt_precision.py",
    ):
        assert not (ROOT / removed).exists(), f"removed module still present: {removed}"


def test_no_backend_selector_or_sdk_imports_in_production() -> None:
    """Production source must not import the removed SDK/selector surface."""

    app_source = (ROOT / "app.py").read_text(encoding="utf-8")
    bootstrap_source = (ROOT / "orchestrators" / "runtime_bootstrap.py").read_text(encoding="utf-8")
    package_source = (ROOT / "integrations" / "polymarket" / "__init__.py").read_text(
        encoding="utf-8"
    )

    for banned in (
        "create_polymarket_client",
        "PolymarketClient",
        "clob_v2_gateway",
        "sdk_client",
        "hybrid_gateway",
        "backend_factory",
        "VenueBackend",
        "build_user_venue_backend",
        "build_runtime_polymarket_gateway",
    ):
        assert banned not in app_source, f"app.py still references {banned}"
        assert banned not in bootstrap_source, f"runtime_bootstrap still references {banned}"
        assert banned not in package_source, f"package init still references {banned}"


def test_no_venue_selector_imports_resolvable() -> None:
    """Importing the removed modules must fail."""

    for removed in (
        "integrations.polymarket.sdk_client",
        "integrations.polymarket.clob_v2_gateway",
        "integrations.polymarket.hybrid_gateway",
        "integrations.polymarket.backend_factory",
        "integrations.polymarket.runtime_gateway",
    ):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(removed)


def test_ccxt_is_required_in_paper_and_live() -> None:
    """Missing CCXT fails startup in both modes."""

    from integrations.polymarket.ccxt_compat.exceptions import CcxtPredictionUnavailable

    def _raise(*_args, **_kwargs):
        raise CcxtPredictionUnavailable("CCXT prediction support is unavailable")

    import integrations.polymarket.ccxt_prediction_client as client_module

    original = client_module._load_default_factory
    client_module._load_default_factory = _raise  # type: ignore[assignment]
    try:
        for mode in ("paper", "live"):
            with pytest.raises(CcxtPredictionUnavailable):
                asyncio.run(
                    build_polymarket_runtime(
                        runtime_user=_runtime_user(),
                        config=_config(mode),
                        exchange_factory=None,
                        ccxt_version=None,
                    )
                )
    finally:
        client_module._load_default_factory = original  # type: ignore[assignment]


def test_paper_mode_cannot_perform_live_writes() -> None:
    """The paper gateway has no code path to a CCXT private write method."""

    runtime = asyncio.run(
        build_polymarket_runtime(
            runtime_user=_runtime_user(),
            config=_config("paper"),
            exchange_factory=_Exchange,
            ccxt_version="4.5.70",
        )
    )

    # The paper gateway delegates writes to the simulator; the CCXT exchange
    # double's create_order/cancel_order must never be invoked.
    exchange = runtime.client.exchange
    exchange.create_order = MagicMock(side_effect=AssertionError("live write attempted"))
    exchange.cancel_order = MagicMock(side_effect=AssertionError("live cancel attempted"))

    # Drive a simulated write through the gateway to prove the transport is
    # never reached.
    from decimal import Decimal

    from integrations.polymarket.models import MarketableOrderRequest

    result = asyncio.run(
        runtime.gateway.submit_marketable_order(
            MarketableOrderRequest(
                token_id="123",
                side="BUY",
                size=Decimal("10"),
                price_limit=Decimal("0.40"),
                condition_id="condition",
            )
        )
    )
    assert result.success is True
    exchange.create_order.assert_not_called()
    exchange.cancel_order.assert_not_called()


def test_ci_mypy_gate_no_longer_lists_sdk_gateway() -> None:
    """The CI mypy allowlist must not reference the deleted SDK gateway."""

    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "clob_v2_gateway.py" not in workflow
    assert "integrations/polymarket/ccxt_prediction_client.py" in workflow
    assert "integrations/polymarket/venue_runtime.py" in workflow
