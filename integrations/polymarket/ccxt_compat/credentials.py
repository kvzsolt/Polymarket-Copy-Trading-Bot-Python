"""Polymarket L2 credential extraction from a CCXT exchange instance.

CCXT ``load_api_credentials`` caches derived credentials in
``exchange.options`` under ``l2ApiKey`` / ``l2Secret`` / ``l2Passphrase`` and
falls back to the typed ``apiKey`` / ``secret`` / ``password`` fields.  This
module mirrors that precedence for the authenticated user WebSocket.

Credential values must never be logged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class UserWsCredentials:
    """Normalized Polymarket L2 credentials for the user stream."""

    api_key: str
    api_secret: str
    api_passphrase: str

    @property
    def complete(self) -> bool:
        return all(
            (
                self.api_key,
                self.api_secret,
                self.api_passphrase,
            )
        )


def _option_or_attribute(
    exchange: Any,
    option_name: str,
    attribute_name: str,
) -> str:
    options = getattr(exchange, "options", None)
    if isinstance(options, Mapping):
        value = options.get(option_name)
        if value:
            return str(value)
    value = getattr(exchange, attribute_name, None)
    return str(value) if value else ""


def extract_user_ws_credentials(exchange: Any) -> UserWsCredentials:
    """Read derived L2 credentials from exchange options, then typed fields."""

    return UserWsCredentials(
        api_key=_option_or_attribute(exchange, "l2ApiKey", "apiKey"),
        api_secret=_option_or_attribute(exchange, "l2Secret", "secret"),
        api_passphrase=_option_or_attribute(exchange, "l2Passphrase", "password"),
    )


__all__ = [
    "UserWsCredentials",
    "extract_user_ws_credentials",
]
