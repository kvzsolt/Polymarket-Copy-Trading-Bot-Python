"""Regression coverage for balance-reader configuration during bootstrap."""

from __future__ import annotations

from types import TracebackType

import pytest

import pmcopy.bootstrap as bootstrap_module
from pmcopy.config.app_config import AppConfig, DatabaseConfig, PolymarketConfig
from pmcopy.utils import getMyBalance as balance_module


class _FakeAcquireContext:
    """Provide the asynchronous pool-acquire protocol without a database."""

    async def __aenter__(self) -> object:
        """Return a deterministic fake connection."""
        return object()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Leave the fake acquisition context without suppressing errors."""


class _FakePool:
    """Expose only the pool operation used by the application bootstrap."""

    def acquire(self) -> _FakeAcquireContext:
        """Return a deterministic asynchronous acquisition context."""
        return _FakeAcquireContext()


@pytest.mark.asyncio
async def test_build_app_configures_balance_reader_before_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Apply the typed RPC setting before the returned registry can refresh."""
    expected_rpc_url = "https://polygon-rpc.example.invalid/private"
    config = AppConfig(
        database=DatabaseConfig(dsn="postgresql://test:test@localhost/test"),
        polymarket=PolymarketConfig(rpc_url=expected_rpc_url),
    )

    async def _fake_init_pool(*args: object, **kwargs: object) -> _FakePool:
        """Return an in-memory pool seam without opening a connection."""
        del args, kwargs
        return _FakePool()

    async def _fake_apply_migrations(connection: object) -> None:
        """Accept the fake connection without mutating external state."""
        del connection

    monkeypatch.setattr(bootstrap_module, "init_pool", _fake_init_pool)
    monkeypatch.setattr(bootstrap_module, "apply_migrations", _fake_apply_migrations)
    monkeypatch.setattr(balance_module, "_RPC_URL", "")

    application = await bootstrap_module.build_app(config=config)

    assert balance_module._RPC_URL == expected_rpc_url
    assert application.registry.config is config
