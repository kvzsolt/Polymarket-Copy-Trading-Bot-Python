"""SDK-default and CCXT-optional runtime composition contracts.

The runtime keeps paper mode as the shipped default and constructs no CCXT
transport unless an operator explicitly selects the optional adapter.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import tomllib

import pytest
import yaml

from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from orchestrators.runtime_bootstrap import build_user_venue_backend
from services.executionService import ExecutionService


class _Exchange:
    """Deterministic CCXT exchange double used by composition tests."""

    def __init__(self, settings: dict[str, object]) -> None:
        self.settings = settings
        self.options = settings["options"]
        self.close = AsyncMock()
        self.load_api_credentials = AsyncMock(return_value={"apiKey": "derived"})

    def polymarket_order_raw_amounts(self, *_args: object) -> dict[str, object]:
        """Expose the precision hook expected by the client factory."""

        return {}


def _runtime_user() -> SimpleNamespace:
    """Return one valid runtime-user fixture."""

    return SimpleNamespace(
        user_id="operator",
        wallet=SimpleNamespace(
            funder_address="0x" + "b" * 40,
            private_key="0x" + "a" * 64,
        ),
    )


def _config(*, mode: str, allow_live: bool) -> SimpleNamespace:
    """Return the narrow typed settings surface required by composition."""

    return SimpleNamespace(
        execution=SimpleNamespace(
            trading_mode=mode,
            allow_experimental_ccxt_live=allow_live,
        ),
        wallet=SimpleNamespace(
            funder_address="0x" + "b" * 40,
            private_key="0x" + "a" * 64,
        ),
    )


def test_packaged_execution_config_is_sdk_paper_safe() -> None:
    """Ship the official SDK default without arming live trading."""

    root = Path(__file__).resolve().parents[1]
    payload = yaml.safe_load((root / "config" / "execution.yml").read_text(encoding="utf-8"))

    assert payload["venue_backend"] == "sdk"
    assert payload["trading_mode"] == "paper"
    assert payload["allow_experimental_ccxt_live"] is False


def test_paper_runtime_builds_sdk_without_ccxt_transport() -> None:
    """Do not initialize optional CCXT for the default paper runtime."""

    factory = MagicMock()

    bundle = asyncio.run(
        build_user_venue_backend(
            _runtime_user(),
            object(),
            config=_config(mode="paper", allow_live=False),
            exchange_factory=factory,
            ccxt_version="4.5.70",
        )
    )

    assert isinstance(bundle.gateway, PolymarketGateway)
    assert bundle.ccxt_client is None
    factory.assert_not_called()


def test_live_runtime_refuses_ccxt_before_transport_without_explicit_gate() -> None:
    """Reject live CCXT startup before creating an authenticated transport."""

    factory = MagicMock()

    with pytest.raises(RuntimeError, match="explicit"):
        asyncio.run(
            build_user_venue_backend(
                _runtime_user(),
                object(),
                config=_config(mode="live", allow_live=False),
                backend="ccxt",
                exchange_factory=factory,
                ccxt_version="4.5.70",
            )
        )

    factory.assert_not_called()


def test_execution_service_requires_a_gateway_or_sdk_compatible_client() -> None:
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


def test_runtime_and_dependencies_retain_the_official_sdk_path() -> None:
    """Keep the official adapter installed and reachable from composition."""

    root = Path(__file__).resolve().parents[1]
    app_source = (root / "app.py").read_text(encoding="utf-8")
    bootstrap_source = (root / "orchestrators" / "runtime_bootstrap.py").read_text(encoding="utf-8")
    package_source = (root / "integrations" / "polymarket" / "__init__.py").read_text(
        encoding="utf-8"
    )
    utility_source = (root / "utils" / "createClobClient.py").read_text(encoding="utf-8")
    dependencies = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "dependencies"
    ]

    assert "create_polymarket_client" in app_source
    assert "clob_v2_gateway" in bootstrap_source
    assert "sdk_client" in package_source
    assert "clob_v2_gateway" in package_source
    assert "from polymarket" not in utility_source
    assert "polymarket-client==0.1.0b18" in dependencies
    for required in (
        "integrations/polymarket/sdk_client.py",
        "integrations/polymarket/clob_v2_gateway.py",
        "integrations/polymarket/hybrid_gateway.py",
        "integrations/polymarket/backend_factory.py",
    ):
        assert (root / required).exists(), f"required venue path missing: {required}"


def test_blocking_money_path_type_gate_targets_both_adapters() -> None:
    """CI must type-check both production-capable adapter paths."""

    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "integrations/polymarket/clob_v2_gateway.py" in workflow
    assert "integrations/polymarket/ccxt_prediction_gateway.py" in workflow
    assert "integrations/polymarket/ccxt_prediction_client.py" in workflow
    assert "PMCOPY__WALLET__PRIVATE_KEY=" not in workflow
