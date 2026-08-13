"""Fail-closed backend selection for the hybrid Polymarket migration."""

from __future__ import annotations

import os
from collections.abc import Mapping
from enum import StrEnum

from integrations.polymarket.hybrid_gateway import HybridClobGateway, MismatchSink
from interfaces.clob_gateway import ClobGateway


class VenueBackend(StrEnum):
    """Supported production backend compositions."""

    SDK = "sdk"
    CCXT = "ccxt"
    SDK_SHADOW_CCXT = "sdk-shadow-ccxt"


class BackendSettingsResolver:
    """Resolve typed backend settings with backward-compatible env fallback."""

    def resolve(self, config: object | None = None) -> tuple[VenueBackend, bool]:
        """Return selected backend and experimental live-CCXT gate.

        Args:
            config: Optional typed application configuration.

        Returns:
            Backend selection and whether CCXT may be a live primary.

        Raises:
            ValueError: If the configured backend is unsupported.
        """

        execution = self._attribute(config, "execution")
        raw_backend = (
            self._attribute(execution, "venue_backend")
            or os.getenv("PMCOPY_VENUE_BACKEND")
            or VenueBackend.SDK.value
        )
        allow_live = self._attribute(execution, "allow_experimental_ccxt_live")
        if allow_live is None:
            allow_live = os.getenv("PMCOPY_ALLOW_EXPERIMENTAL_CCXT_LIVE")
        try:
            backend = VenueBackend(str(raw_backend).strip().lower())
        except ValueError as exc:
            raise ValueError(f"unsupported Polymarket venue backend: {raw_backend!r}") from exc
        return backend, self._boolean(allow_live)

    @staticmethod
    def _attribute(source: object | None, name: str) -> object | None:
        if source is None:
            return None
        if isinstance(source, Mapping):
            return source.get(name)
        return getattr(source, name, None)

    @staticmethod
    def _boolean(value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


class PolymarketGatewaySelector:
    """Select the venue gateway while preserving SDK production primacy."""

    def select(
        self,
        *,
        sdk_gateway: ClobGateway,
        ccxt_gateway: ClobGateway | None = None,
        backend: VenueBackend | str = VenueBackend.SDK,
        trading_mode: str = "paper",
        allow_experimental_ccxt_live: bool = False,
        mismatch_sink: MismatchSink | None = None,
    ) -> ClobGateway:
        """Return the configured gateway composition.

        Args:
            sdk_gateway: Official first-party SDK gateway.
            ccxt_gateway: Optional isolated CCXT gateway.
            backend: Explicit backend selector; defaults to the SDK.
            trading_mode: Explicit paper or live trading mode.
            allow_experimental_ccxt_live: Separate opt-in for CCXT live primary.
            mismatch_sink: Optional structured shadow comparison consumer.

        Returns:
            Selected venue-neutral CLOB gateway.

        Raises:
            RuntimeError: If CCXT is required but unavailable or live-gated.
            ValueError: If ``backend`` or ``trading_mode`` is unsupported.
        """

        selected = (
            backend if isinstance(backend, VenueBackend) else VenueBackend(str(backend).lower())
        )
        mode = str(trading_mode).strip().lower()
        if mode not in {"paper", "live"}:
            raise ValueError(f"unsupported execution trading mode: {trading_mode!r}")
        if selected is VenueBackend.SDK:
            return sdk_gateway
        if ccxt_gateway is None:
            raise RuntimeError(f"{selected.value} requires an initialized CCXT gateway")
        if selected is VenueBackend.CCXT:
            if mode == "live" and not allow_experimental_ccxt_live:
                raise RuntimeError(
                    "CCXT cannot become the live execution primary without the explicit "
                    "allow_experimental_ccxt_live gate"
                )
            return ccxt_gateway
        return HybridClobGateway(
            primary=sdk_gateway,
            shadow=ccxt_gateway,
            mismatch_sink=mismatch_sink,
        )


def resolve_backend_settings(config: object | None = None) -> tuple[VenueBackend, bool]:
    """Compatibility facade for typed backend setting resolution."""

    return BackendSettingsResolver().resolve(config)


def select_polymarket_gateway(
    *,
    sdk_gateway: ClobGateway,
    ccxt_gateway: ClobGateway | None = None,
    backend: VenueBackend | str = VenueBackend.SDK,
    trading_mode: str = "paper",
    allow_experimental_ccxt_live: bool = False,
    mismatch_sink: MismatchSink | None = None,
) -> ClobGateway:
    """Compatibility facade for fail-closed gateway selection."""

    return PolymarketGatewaySelector().select(
        sdk_gateway=sdk_gateway,
        ccxt_gateway=ccxt_gateway,
        backend=backend,
        trading_mode=trading_mode,
        allow_experimental_ccxt_live=allow_experimental_ccxt_live,
        mismatch_sink=mismatch_sink,
    )


__all__ = [
    "BackendSettingsResolver",
    "PolymarketGatewaySelector",
    "VenueBackend",
    "resolve_backend_settings",
    "select_polymarket_gateway",
]
