"""Fail-closed contracts for an unavailable mandatory CCXT runtime."""

from __future__ import annotations

import builtins
import importlib
from types import SimpleNamespace

import pytest

import pmcopy.integrations.polymarket.ccxt_prediction_client as client_mod
from pmcopy.integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime


@pytest.fixture
def ccxt_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make all dynamic CCXT imports fail deterministically."""

    real_import_module = importlib.import_module
    real_import = builtins.__import__

    def fake_import_module(name: str, *args: object, **kwargs: object) -> object:
        """Reject CCXT while delegating every unrelated import."""

        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import_module(name, *args, **kwargs)

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        """Reject direct CCXT imports while preserving Python import behavior."""

        if name == "ccxt" or name.startswith("ccxt."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(client_mod.importlib, "import_module", fake_import_module)
    monkeypatch.setattr(builtins, "__import__", fake_import)


@pytest.mark.asyncio
async def test_missing_ccxt_raises_actionable_error(ccxt_absent: None) -> None:
    """Identify a broken runtime dependency without an opaque import error."""

    with pytest.raises(CcxtPredictionUnavailable, match="runtime requirements"):
        await create_ccxt_prediction_client(authenticated=False)


@pytest.mark.asyncio
async def test_runtime_factory_fails_closed_when_ccxt_is_unavailable(
    ccxt_absent: None,
) -> None:
    """CCXT is mandatory: paper and live startup fail closed without it."""

    runtime_user = SimpleNamespace(
        user_id="operator",
        wallet=SimpleNamespace(private_key="", funder_address="0x" + "b" * 40),
    )
    config = SimpleNamespace(
        execution=SimpleNamespace(trading_mode="paper"),
        wallet=SimpleNamespace(private_key="", funder_address="0x" + "b" * 40),
    )
    with pytest.raises(CcxtPredictionUnavailable):
        await build_polymarket_runtime(runtime_user=runtime_user, config=config)


@pytest.mark.asyncio
async def test_live_runtime_fails_closed_when_ccxt_is_unavailable(
    ccxt_absent: None,
) -> None:
    """Live startup also fails closed when CCXT is unavailable."""

    runtime_user = SimpleNamespace(
        user_id="operator",
        wallet=SimpleNamespace(
            private_key="0x" + "a" * 64,
            funder_address="0x" + "b" * 40,
        ),
    )
    config = SimpleNamespace(
        execution=SimpleNamespace(trading_mode="live"),
        wallet=SimpleNamespace(
            private_key="0x" + "a" * 64,
            funder_address="0x" + "b" * 40,
        ),
    )
    with pytest.raises(CcxtPredictionUnavailable):
        await build_polymarket_runtime(runtime_user=runtime_user, config=config)
