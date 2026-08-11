"""CCXT-only runtime composition regression contracts.

The v1 cutover keeps paper mode as the shipped default and constructs exactly
one CCXT prediction client/gateway per runtime user.  No official SDK client is
created as a hidden primary or compatibility fallback.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
import tomllib

import pytest
import yaml

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
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


def test_packaged_execution_config_is_ccxt_paper_safe() -> None:
    """Ship CCXT as the only backend without arming live trading."""

    root = Path(__file__).resolve().parents[1]
    payload = yaml.safe_load((root / "config" / "execution.yml").read_text(encoding="utf-8"))

    assert payload["venue_backend"] == "ccxt"
    assert payload["trading_mode"] == "paper"
    assert payload["allow_experimental_ccxt_live"] is False


def test_paper_runtime_builds_public_ccxt_without_sdk_client() -> None:
    """Create a public-only CCXT gateway when the runtime is in paper mode."""

    bundle = asyncio.run(
        build_user_venue_backend(
            _runtime_user(),
            config=_config(mode="paper", allow_live=False),
            exchange_factory=_Exchange,
            ccxt_version="4.5.70",
        )
    )

    assert isinstance(bundle.gateway, CcxtPredictionGateway)
    assert bundle.client.authenticated is False
    assert "privateKey" not in bundle.client.exchange.settings
    assert "walletAddress" not in bundle.client.exchange.settings
    asyncio.run(bundle.close())
    bundle.client.exchange.close.assert_awaited_once_with(True)


def test_live_runtime_refuses_ccxt_before_transport_without_explicit_gate() -> None:
    """Reject live CCXT startup before creating an authenticated transport."""

    factory = MagicMock()

    with pytest.raises(RuntimeError, match="explicit"):
        asyncio.run(
            build_user_venue_backend(
                _runtime_user(),
                config=_config(mode="live", allow_live=False),
                exchange_factory=factory,
                ccxt_version="4.5.70",
            )
        )

    factory.assert_not_called()


def test_execution_service_requires_injected_gateway() -> None:
    """Forbid the execution layer from constructing a hidden SDK adapter."""

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


def test_runtime_and_dependencies_have_no_official_sdk_path() -> None:
    """Keep the production composition and dependency set CCXT-only."""

    root = Path(__file__).resolve().parents[1]
    app_source = (root / "app.py").read_text(encoding="utf-8")
    bootstrap_source = (root / "orchestrators" / "runtime_bootstrap.py").read_text(encoding="utf-8")
    package_source = (root / "integrations" / "polymarket" / "__init__.py").read_text(
        encoding="utf-8"
    )
    cli_source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((root / "bot_cli").rglob("*.py"))
    )
    utility_source = (root / "utils" / "createClobClient.py").read_text(encoding="utf-8")
    dependencies = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"][
        "dependencies"
    ]

    assert "create_polymarket_client" not in app_source
    assert "clob_v2_gateway" not in bootstrap_source
    assert "sdk_client" not in package_source
    assert "clob_v2_gateway" not in package_source
    assert "sdk_client" not in cli_source
    assert "clob_v2_gateway" not in cli_source
    assert "polymarket-client" not in cli_source
    assert "from polymarket" not in utility_source
    assert all(not dependency.startswith("polymarket-client") for dependency in dependencies)
    for obsolete in (
        "integrations/polymarket/sdk_client.py",
        "integrations/polymarket/clob_v2_gateway.py",
        "integrations/polymarket/hybrid_gateway.py",
        "integrations/polymarket/backend_factory.py",
    ):
        assert not (root / obsolete).exists(), f"obsolete venue path remains: {obsolete}"


def test_blocking_money_path_type_gate_targets_ccxt_files() -> None:
    """CI must type-check the active adapter instead of a deleted SDK file."""

    root = Path(__file__).resolve().parents[1]
    workflow = (root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "integrations/polymarket/clob_v2_gateway.py" not in workflow
    assert "integrations/polymarket/ccxt_prediction_gateway.py" in workflow
    assert "integrations/polymarket/ccxt_prediction_client.py" in workflow
    assert "PMCOPY__WALLET__PRIVATE_KEY=" not in workflow
