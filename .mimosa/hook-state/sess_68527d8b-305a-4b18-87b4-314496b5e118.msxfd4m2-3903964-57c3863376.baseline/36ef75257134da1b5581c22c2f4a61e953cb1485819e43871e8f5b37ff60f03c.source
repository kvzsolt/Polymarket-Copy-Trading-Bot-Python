"""Lifecycle-safe client factory for ``ccxt.prediction.polymarket``.

CCXT is the mandatory Polymarket venue transport for this runtime. Paper
startup creates only a public transport; authenticated construction requires
an explicit live trading mode and a verified Deposit Wallet funder address.
"""

from __future__ import annotations

import importlib
import inspect
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from integrations.polymarket.ccxt_compat.credentials import extract_user_ws_credentials
from integrations.polymarket.ccxt_compat.exceptions import (
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
)
from integrations.polymarket.ccxt_compat.precision import (
    install_ccxt_polymarket_precision_patch,
)
from integrations.polymarket.ccxt_compat.version_guard import ensure_supported_ccxt_version

_EXPECTED_CHAIN_ID = 137


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


def _first(mapping: Any, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _tick_from_source(source: Any) -> Decimal | None:
    """Extract a positive finite tick size from one CCXT metadata mapping."""

    if not isinstance(source, dict):
        return None
    info = source.get("info")
    info = info if isinstance(info, dict) else {}
    raw = _first(source, "tick_size", "tickSize")
    raw = raw if raw is not None else _first(info, "tick_size", "tickSize")
    precision = source.get("precision")
    if raw is None and isinstance(precision, dict):
        raw = precision.get("price")
    if raw is None:
        raw = _first(info, "orderPriceMinTickSize", "minimumTickSize")
    if raw is None:
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


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

        Derived credentials are read from the exchange options first (where
        CCXT caches ``load_api_credentials`` output), then from the typed
        exchange fields. Public-only clients return empty values. Credential
        values are never logged.
        """

        if not self.authenticated:
            return {
                "api_key": "",
                "api_secret": "",
                "api_passphrase": "",
            }
        creds = extract_user_ws_credentials(self.exchange)
        return {
            "api_key": creds.api_key,
            "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase,
        }

    async def get_tick_size(self, token_id: str) -> Decimal | None:
        """Return the CCXT-resolved tick size for one outcome token.

        Reads CCXT's cached outcome metadata first, then the raw public CLOB
        book. Returns ``None`` when neither carries a usable tick so the
        tick-size resolver can apply its fallback; this method never defaults.
        """

        token = str(token_id)
        outcome: Any = None
        method = getattr(self.exchange, "load_outcome", None)
        if callable(method):
            try:
                result = method(token)
                outcome = await result if inspect.isawaitable(result) else result
            except Exception:
                outcome = None
        tick = _tick_from_source(outcome)
        if tick is not None:
            return tick

        book_method = None
        for name in ("clobPublicGetBook", "clob_public_get_book"):
            candidate = getattr(self.exchange, name, None)
            if callable(candidate):
                book_method = candidate
                break
        if book_method is None:
            return None
        try:
            result = book_method({"token_id": token})
            book = await result if inspect.isawaitable(result) else result
        except Exception:
            return None
        return _tick_from_source(book)

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

    ensure_supported_ccxt_version(detected_version)

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
