"""Phase 3.12 — typed venue_backend config is the primary selector source.

The audit found ``ExecutionConfig`` had no ``venue_backend`` /
``allow_experimental_ccxt_live`` fields, so only the single-underscore env
vars (``PMCOPY_VENUE_BACKEND`` / ``PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE``)
worked, bypassing all layered validation. Now the typed fields are the primary
source and the env vars are a backward-compatible fallback.
"""

from __future__ import annotations

from types import SimpleNamespace

from integrations.polymarket.backend_factory import (
    VenueBackend,
    resolve_backend_settings,
)


def _config(*, venue_backend: str = "sdk", allow_live: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        execution=SimpleNamespace(
            venue_backend=venue_backend,
            allow_experimental_ccxt_live=allow_live,
        )
    )


def test_typed_config_sdk_is_default(monkeypatch) -> None:
    monkeypatch.delenv("PMCOPY_VENUE_BACKEND", raising=False)
    monkeypatch.delenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE", raising=False)

    backend, allow_live = resolve_backend_settings(_config(venue_backend="sdk"))

    assert backend is VenueBackend.SDK
    assert allow_live is False


def test_typed_config_ccxt_shadow(monkeypatch) -> None:
    monkeypatch.delenv("PMCOPY_VENUE_BACKEND", raising=False)

    backend, _ = resolve_backend_settings(_config(venue_backend="sdk-shadow-ccxt"))

    assert backend is VenueBackend.SDK_SHADOW_CCXT


def test_typed_config_allow_experimental_live_gate() -> None:
    backend, allow_live = resolve_backend_settings(_config(venue_backend="ccxt", allow_live=True))

    assert backend is VenueBackend.CCXT
    assert allow_live is True


def test_env_var_falls_back_when_typed_config_absent(monkeypatch) -> None:
    monkeypatch.setenv("PMCOPY_VENUE_BACKEND", "ccxt")
    monkeypatch.setenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE", "true")

    backend, allow_live = resolve_backend_settings(None)

    assert backend is VenueBackend.CCXT
    assert allow_live is True


def test_typed_config_takes_precedence_over_env(monkeypatch) -> None:
    monkeypatch.setenv("PMCOPY_VENUE_BACKEND", "ccxt")
    monkeypatch.setenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE", "true")

    backend, allow_live = resolve_backend_settings(_config(venue_backend="sdk", allow_live=False))

    assert backend is VenueBackend.SDK
    assert allow_live is False


def test_unsupported_backend_raises() -> None:
    import pytest

    with pytest.raises(ValueError, match="unsupported Polymarket venue backend"):
        resolve_backend_settings(_config(venue_backend="bogus"))
