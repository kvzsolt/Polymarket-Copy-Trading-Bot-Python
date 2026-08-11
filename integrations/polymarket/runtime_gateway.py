"""CCXT-only Polymarket runtime composition.

This module owns construction and lifecycle of the one venue transport used by
each runtime user.  It preserves the explicit live-mode gate while ensuring a
paper runtime never receives private key material.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionClient,
    create_ccxt_prediction_client,
)
from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway


@dataclass(frozen=True, slots=True)
class RuntimeGatewayBundle:
    """Own one CCXT client and its venue-neutral gateway adapter."""

    gateway: CcxtPredictionGateway
    client: CcxtPredictionClient

    async def close(self) -> None:
        """Close the owned CCXT transport idempotently."""

        await self.client.close()


class CcxtRuntimeGatewayFactory:
    """Build CCXT prediction transports from validated runtime settings."""

    def __init__(
        self,
        *,
        exchange_factory: Callable[[dict[str, Any]], Any] | None = None,
        ccxt_version: str | None = None,
    ) -> None:
        self._exchange_factory = exchange_factory
        self._ccxt_version = ccxt_version

    async def build(
        self,
        *,
        config: object,
        wallet_config: object,
    ) -> RuntimeGatewayBundle:
        """Return the sole runtime gateway for one follower wallet.

        Args:
            config: Validated application settings containing execution mode.
            wallet_config: Validated follower wallet identity and secret.

        Returns:
            An owned CCXT client/gateway bundle.

        Raises:
            RuntimeError: If live mode is requested without its explicit gate.
            ValueError: If the configured trading mode is unsupported.
        """

        execution = getattr(config, "execution", None)
        mode = str(getattr(execution, "trading_mode", "paper") or "paper").strip().lower()
        if mode not in {"paper", "live"}:
            raise ValueError(f"unsupported execution trading mode: {mode!r}")
        allow_live = bool(getattr(execution, "allow_experimental_ccxt_live", False))
        if mode == "live" and not allow_live:
            raise RuntimeError(
                "CCXT cannot become the live execution primary without the explicit "
                "allow_experimental_ccxt_live gate"
            )

        client = await create_ccxt_prediction_client(
            wallet_config=wallet_config,
            config=config,
            authenticated=mode == "live",
            exchange_factory=self._exchange_factory,
            ccxt_version=self._ccxt_version,
        )
        return RuntimeGatewayBundle(
            gateway=CcxtPredictionGateway(client),
            client=client,
        )


__all__ = ["CcxtRuntimeGatewayFactory", "RuntimeGatewayBundle"]
