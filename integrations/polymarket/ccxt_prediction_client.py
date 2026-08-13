"""Lifecycle-safe client factory for ``ccxt.prediction.polymarket``.

CCXT is an optional Polymarket venue adapter on this branch. Paper startup
creates only a public transport; authenticated construction still requires
both the explicit live gate enforced by composition and a verified Deposit
Wallet.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from typing import Any, Callable

from integrations.polymarket.ccxt_precision import install_ccxt_polymarket_precision_patch

_EXPECTED_CHAIN_ID = 137
_REQUIRED_CCXT_VERSION = (4, 5, 70)


class CcxtPredictionUnavailable(RuntimeError):
    """Raised when the mandatory CCXT prediction dependency is unavailable."""


class CcxtPredictionConfigurationError(ValueError):
    """Raised when a live-capable CCXT client would be ambiguously configured."""


def _secret(value: Any) -> str:
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        value = getter()
    return str(value or "").strip()


def _attr(source: Any, name: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _version_tuple(value: Any) -> tuple[int, ...]:
    parts: list[int] = []
    for part in str(value or "").split("."):
        digits = "".join(ch for ch in part if ch.isdigit())
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def _load_default_factory() -> tuple[Callable[[dict[str, Any]], Any], str | None]:
    try:
        ccxt = importlib.import_module("ccxt")
        prediction = importlib.import_module("ccxt.prediction")
    except (ImportError, ModuleNotFoundError) as exc:
        raise CcxtPredictionUnavailable(
            "CCXT prediction support is unavailable; install the project runtime "
            "requirements (or requirements.ccxt.txt for a targeted install)"
        ) from exc

    factory = getattr(prediction, "polymarket", None)
    if factory is None:
        raise CcxtPredictionUnavailable(
            "installed CCXT does not expose ccxt.prediction.polymarket; CCXT >= 4.5.70 is required"
        )
    return factory, str(getattr(ccxt, "__version__", "") or "") or None


@dataclass(slots=True)
class CcxtPredictionClient:
    """Own one CCXT prediction exchange and close it exactly once."""

    exchange: Any
    authenticated: bool
    wallet_address: str | None
    signature_type: int | None
    ccxt_version: str | None = None
    _closed: bool = False

    @property
    def api_creds(self) -> dict[str, str]:
        """Return normalized in-memory L2 credentials for the user stream.

        Returns:
            A fixed-key mapping. Public-only clients return empty values.
        """

        if not self.authenticated:
            return {
                "api_key": "",
                "api_secret": "",
                "api_passphrase": "",
            }
        return {
            "api_key": str(getattr(self.exchange, "apiKey", "") or ""),
            "api_secret": str(getattr(self.exchange, "secret", "") or ""),
            "api_passphrase": str(getattr(self.exchange, "password", "") or ""),
        }

    async def close(self) -> None:
        """Close all CCXT transports and release instance-owned caches once."""

        if self._closed:
            return
        self._closed = True
        close = getattr(self.exchange, "close", None)
        if close is None:
            return
        result = close(True)
        if hasattr(result, "__await__"):
            await result

    async def __aenter__(self) -> "CcxtPredictionClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


async def create_ccxt_prediction_client(
    private_key: Any = None,
    *,
    wallet_address: str | None = None,
    wallet_config: Any = None,
    config: Any = None,
    authenticated: bool = True,
    exchange_factory: Callable[[dict[str, Any]], Any] | None = None,
    ccxt_version: str | None = None,
) -> CcxtPredictionClient:
    """Create a public or live-capable CCXT Polymarket prediction client.

    A type-3 Deposit Wallet cannot be guessed from an EOA. The client therefore
    requires an explicit, already verified funder address whenever
    ``authenticated=True``.
    """

    resolved_key = _secret(private_key)
    if not resolved_key:
        resolved_key = _secret(_attr(wallet_config, "private_key"))
    if not resolved_key:
        resolved_key = _secret(_attr(_attr(config, "wallet"), "private_key"))

    resolved_wallet = str(wallet_address or "").strip()
    if not resolved_wallet:
        resolved_wallet = str(_attr(wallet_config, "funder_address", "") or "").strip()
    if not resolved_wallet:
        resolved_wallet = str(_attr(_attr(config, "wallet"), "funder_address", "") or "").strip()

    detected_version = ccxt_version
    if exchange_factory is None:
        exchange_factory, detected_version = _load_default_factory()

    if detected_version and _version_tuple(detected_version) < _REQUIRED_CCXT_VERSION:
        raise CcxtPredictionUnavailable(
            f"CCXT {detected_version} is too old; >= 4.5.70 is required"
        )

    if authenticated:
        if not resolved_key:
            raise CcxtPredictionConfigurationError(
                "authenticated CCXT backend requires the EOA private key"
            )
        if not resolved_wallet:
            raise CcxtPredictionConfigurationError(
                "authenticated CCXT backend requires the verified Deposit Wallet "
                "funder_address; it is never inferred from the EOA"
            )

    options: dict[str, Any] = {
        "chainId": _EXPECTED_CHAIN_ID,
        "signatureType": 3,
    }
    settings: dict[str, Any] = {
        "enableRateLimit": True,
        "options": options,
    }
    if authenticated:
        settings["privateKey"] = resolved_key
        settings["walletAddress"] = resolved_wallet
        options["funder"] = resolved_wallet

    exchange = exchange_factory(settings)
    try:
        install_ccxt_polymarket_precision_patch(exchange)
        if authenticated:
            bootstrap = getattr(exchange, "load_api_credentials", None)
            if not callable(bootstrap):
                raise CcxtPredictionConfigurationError(
                    "authenticated CCXT backend does not expose load_api_credentials"
                )
            result = bootstrap()
            if inspect.isawaitable(result):
                await result
    except BaseException:
        close = getattr(exchange, "close", None)
        if callable(close):
            close_result = close(True)
            if inspect.isawaitable(close_result):
                await close_result
        raise

    return CcxtPredictionClient(
        exchange=exchange,
        authenticated=authenticated,
        wallet_address=resolved_wallet or None,
        signature_type=3 if authenticated else None,
        ccxt_version=detected_version,
    )


__all__ = [
    "CcxtPredictionClient",
    "CcxtPredictionConfigurationError",
    "CcxtPredictionUnavailable",
    "create_ccxt_prediction_client",
]
