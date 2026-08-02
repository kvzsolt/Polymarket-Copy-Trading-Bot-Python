"""Fail-closed backend selection for the hybrid Polymarket migration."""

from __future__ import annotations

import os
from enum import StrEnum
from typing import Any

from integrations.polymarket.hybrid_gateway import HybridClobGateway, MismatchSink
from interfaces.clob_gateway import ClobGateway


class VenueBackend(StrEnum):
    SDK = "sdk"
    CCXT = "ccxt"
    SDK_SHADOW_CCXT = "sdk-shadow-ccxt"


def _attr(source: Any, name: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def resolve_backend_settings(config: Any = None) -> tuple[VenueBackend, bool]:
    """Resolve optional settings from typed config, falling back to env.

    Typed config (``execution.venue_backend`` /
    ``execution.allow_experimental_ccxt_live``) is the primary source so the
    selector participates in layered validation. The single-underscore env
    vars (``PMCOPY_VENUE_BACKEND`` / ``PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE``)
    remain a backward-compatible fallback for operators that have not migrated
    their config files yet.
    """

    execution = _attr(config, "execution")
    raw_backend = (
        _attr(execution, "venue_backend")
        or os.getenv("PMCOPY_VENUE_BACKEND")
        or VenueBackend.SDK.value
    )
    allow_live = _attr(execution, "allow_experimental_ccxt_live")
    if allow_live is None:
        allow_live = os.getenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE")
    try:
        backend = VenueBackend(str(raw_backend).strip().lower())
    except ValueError as exc:
        raise ValueError(f"unsupported Polymarket venue backend: {raw_backend!r}") from exc
    return backend, _bool(allow_live)


def select_polymarket_gateway(
    *,
    sdk_gateway: ClobGateway,
    ccxt_gateway: ClobGateway | None = None,
    backend: VenueBackend | str = VenueBackend.SDK,
    trading_mode: str = "paper",
    allow_experimental_ccxt_live: bool = False,
    mismatch_sink: MismatchSink | None = None,
) -> ClobGateway:
    """Select a backend while preserving SDK-primary production behavior."""

    selected = backend if isinstance(backend, VenueBackend) else VenueBackend(str(backend).lower())
    is_live = str(trading_mode).strip().lower() == "live"

    if selected is VenueBackend.SDK:
        return sdk_gateway
    if ccxt_gateway is None:
        raise RuntimeError(f"{selected.value} requires an initialized CCXT gateway")
    if selected is VenueBackend.CCXT:
        if is_live and not allow_experimental_ccxt_live:
            raise RuntimeError(
                "CCXT cannot become the live execution primary without the explicit "
                "allow_experimental_ccxt_live gate"
            )
        return ccxt_gateway
    if selected is VenueBackend.SDK_SHADOW_CCXT:
        return HybridClobGateway(
            primary=sdk_gateway,
            shadow=ccxt_gateway,
            mismatch_sink=mismatch_sink,
        )
    raise AssertionError(f"unreachable backend: {selected}")


__all__ = [
    "VenueBackend",
    "resolve_backend_settings",
    "select_polymarket_gateway",
]
