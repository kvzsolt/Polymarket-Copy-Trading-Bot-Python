"""Composition helper for the optional hybrid Polymarket backend.

The existing runtime can keep constructing the official SDK gateway exactly as
before, then pass it here.  The helper avoids importing CCXT at all for the
default ``sdk`` mode and returns ownership information for clean shutdown.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from integrations.polymarket.backend_factory import (
    VenueBackend,
    resolve_backend_settings,
    select_polymarket_gateway,
)
from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionClient,
    create_ccxt_prediction_client,
)
from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.hybrid_gateway import MismatchSink
from interfaces.clob_gateway import ClobGateway


@dataclass(slots=True)
class RuntimeGatewayBundle:
    gateway: ClobGateway
    backend: VenueBackend
    ccxt_client: CcxtPredictionClient | None = None
    ccxt_gateway: CcxtPredictionGateway | None = None

    async def close_optional_backend(self) -> None:
        """Close only resources created by this helper.

        The existing runtime remains the owner of the official SDK gateway.
        """

        if self.ccxt_client is not None:
            await self.ccxt_client.close()

    async def close(self) -> None:
        """Shutdown alias used by the composition root's client registry.

        Phase 4.15 — drains the hybrid shadow comparison queue (bounded) and
        closes the optional CCXT transport. The SDK gateway remains owned by
        the runtime and is closed separately.
        """
        if self.ccxt_gateway is not None:
            close = getattr(self.gateway, "close", None)
            if callable(close):
                try:
                    await close()
                except Exception:
                    pass
        await self.close_optional_backend()


def _attr(source: Any, name: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _trading_mode(config: Any) -> str:
    execution = _attr(config, "execution")
    return str(_attr(execution, "trading_mode", "paper") or "paper").lower()


async def build_runtime_polymarket_gateway(
    *,
    sdk_gateway: ClobGateway,
    config: Any = None,
    wallet_config: Any = None,
    backend: VenueBackend | str | None = None,
    allow_experimental_ccxt_live: bool | None = None,
    mismatch_sink: MismatchSink | None = None,
    exchange_factory: Callable[[dict[str, Any]], Any] | None = None,
    ccxt_version: str | None = None,
) -> RuntimeGatewayBundle:
    """Build the selected gateway without changing SDK-default startup."""

    configured_backend, configured_live_gate = resolve_backend_settings(config)
    selected = configured_backend if backend is None else VenueBackend(str(backend).lower())
    live_gate = (
        configured_live_gate
        if allow_experimental_ccxt_live is None
        else bool(allow_experimental_ccxt_live)
    )
    mode = _trading_mode(config)

    if selected is VenueBackend.SDK:
        return RuntimeGatewayBundle(gateway=sdk_gateway, backend=selected)

    # Reject an unsafe live-primary selection before constructing, authenticating,
    # or opening any optional CCXT transport. This keeps configuration errors
    # side-effect free and makes the explicit experimental gate auditable.
    if selected is VenueBackend.CCXT and mode == "live" and not live_gate:
        raise RuntimeError(
            "CCXT cannot become the live execution primary without the explicit "
            "allow_experimental_ccxt_live gate"
        )

    # Paper startup always gets a public-only CCXT transport, even when a private
    # key is present. Live shadow needs auth for private order/fill comparisons;
    # live CCXT primary needs auth to submit.
    authenticated = mode == "live"
    ccxt_client = await create_ccxt_prediction_client(
        wallet_config=wallet_config,
        config=config,
        authenticated=authenticated,
        exchange_factory=exchange_factory,
        ccxt_version=ccxt_version,
    )
    ccxt_gateway = CcxtPredictionGateway(ccxt_client)
    try:
        gateway = select_polymarket_gateway(
            sdk_gateway=sdk_gateway,
            ccxt_gateway=ccxt_gateway,
            backend=selected,
            trading_mode=mode,
            allow_experimental_ccxt_live=live_gate,
            mismatch_sink=mismatch_sink,
        )
    except BaseException:
        await ccxt_client.close()
        raise
    return RuntimeGatewayBundle(
        gateway=gateway,
        backend=selected,
        ccxt_client=ccxt_client,
        ccxt_gateway=ccxt_gateway,
    )


__all__ = ["RuntimeGatewayBundle", "build_runtime_polymarket_gateway"]
