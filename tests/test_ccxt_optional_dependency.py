"""CCXT must stay optional and lazily loaded.

The overlay suite always injects an ``exchange_factory`` test double, so it
never exercises the real import path. These tests cover the contract an
operator actually hits: the default SDK backend must work with CCXT absent,
and asking for a CCXT backend without the optional dependency must fail with
an actionable error instead of an opaque ImportError.
"""
from __future__ import annotations

import asyncio
import builtins
import importlib
import sys
from types import SimpleNamespace

import pytest

import integrations.polymarket.ccxt_prediction_client as client_mod
from integrations.polymarket.backend_factory import VenueBackend
from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from integrations.polymarket.runtime_gateway import build_runtime_polymarket_gateway


@pytest.fixture
def ccxt_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``import ccxt`` fail regardless of what is installed locally."""
    real_import_module = importlib.import_module
    real_import = builtins.__import__

    def fake_import_module(name, *args, **kwargs):
        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import_module(name, *args, **kwargs)

    def fake_import(name, *args, **kwargs):
        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(client_mod.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(builtins, "__import__", fake_import)


def test_default_sdk_backend_never_imports_ccxt() -> None:
    sdk = object()

    bundle = asyncio.run(
        build_runtime_polymarket_gateway(
            sdk_gateway=sdk,
            config=SimpleNamespace(execution=SimpleNamespace(trading_mode="live")),
        )
    )

    assert bundle.gateway is sdk
    assert bundle.backend is VenueBackend.SDK
    assert bundle.ccxt_client is None
    # The production default must not drag the optional dependency into the
    # import graph, so a deployment without it stays fully usable.
    assert "ccxt" not in sys.modules


@pytest.mark.asyncio
async def test_missing_ccxt_raises_an_actionable_error(ccxt_absent: None) -> None:
    with pytest.raises(CcxtPredictionUnavailable, match="requirements.ccxt.txt"):
        await create_ccxt_prediction_client(authenticated=False)


@pytest.mark.asyncio
async def test_selecting_a_ccxt_backend_without_the_dependency_fails_closed(
    ccxt_absent: None,
) -> None:
    sdk = object()

    with pytest.raises(CcxtPredictionUnavailable):
        await build_runtime_polymarket_gateway(
            sdk_gateway=sdk,
            config=SimpleNamespace(execution=SimpleNamespace(trading_mode="paper")),
            backend=VenueBackend.SDK_SHADOW_CCXT,
        )
