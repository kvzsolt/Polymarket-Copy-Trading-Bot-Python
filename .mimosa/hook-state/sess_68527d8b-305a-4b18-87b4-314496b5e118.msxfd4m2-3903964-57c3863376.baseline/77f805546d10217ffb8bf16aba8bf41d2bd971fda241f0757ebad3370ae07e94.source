"""L2 credential extraction contracts for the CCXT-only venue.

CCXT ``load_api_credentials`` caches derived Polymarket L2 credentials in
``exchange.options['l2ApiKey' | 'l2Secret' | 'l2Passphrase']``. The user
WebSocket must read them from there first, falling back to the typed
``apiKey``/``secret``/``password`` exchange fields.

The fixtures below use two-character markers (option-sourced ``a*`` versus
field-sourced ``b*``) so the extraction order stays observable without
embedding credential-shaped literals in source.
"""

from __future__ import annotations

from types import SimpleNamespace

from integrations.polymarket.ccxt_compat.credentials import (
    UserWsCredentials,
    extract_user_ws_credentials,
)

_OPT_KEY = "a1"
_OPT_SECRET = "a2"
_OPT_PASS = "a3"
_FIELD_KEY = "b1"
_FIELD_SECRET = "b2"
_FIELD_PASS = "b3"


def _exchange(options: dict | None = None, **attrs: object) -> SimpleNamespace:
    return SimpleNamespace(options=options or {}, **attrs)


def test_reads_derived_l2_credentials_from_exchange_options() -> None:
    exchange = _exchange(
        {"l2ApiKey": _OPT_KEY, "l2Secret": _OPT_SECRET, "l2Passphrase": _OPT_PASS},
        apiKey=_FIELD_KEY,
        secret=_FIELD_SECRET,
        password=_FIELD_PASS,
    )

    creds = extract_user_ws_credentials(exchange)

    assert creds == UserWsCredentials(
        api_key=_OPT_KEY,
        api_secret=_OPT_SECRET,
        api_passphrase=_OPT_PASS,
    )
    assert creds.complete


def test_falls_back_to_typed_exchange_fields() -> None:
    exchange = _exchange(apiKey=_FIELD_KEY, secret=_FIELD_SECRET, password=_FIELD_PASS)

    creds = extract_user_ws_credentials(exchange)

    assert creds.api_key == _FIELD_KEY
    assert creds.api_secret == _FIELD_SECRET
    assert creds.api_passphrase == _FIELD_PASS
    assert creds.complete


def test_partial_options_fall_through_per_field() -> None:
    exchange = _exchange(
        {"l2ApiKey": _OPT_KEY},
        apiKey=_FIELD_KEY,
        secret=_FIELD_SECRET,
        password=_FIELD_PASS,
    )

    creds = extract_user_ws_credentials(exchange)

    assert creds.api_key == _OPT_KEY
    assert creds.api_secret == _FIELD_SECRET
    assert creds.api_passphrase == _FIELD_PASS


def test_missing_credentials_report_incomplete_without_raising() -> None:
    creds = extract_user_ws_credentials(_exchange())

    assert creds == UserWsCredentials(api_key="", api_secret="", api_passphrase="")
    assert not creds.complete


def test_none_options_is_tolerated() -> None:
    exchange = SimpleNamespace(options=None)

    creds = extract_user_ws_credentials(exchange)

    assert not creds.complete


def test_falsy_values_become_empty_strings() -> None:
    exchange = _exchange({"l2ApiKey": None, "l2Secret": 0, "l2Passphrase": False})

    creds = extract_user_ws_credentials(exchange)

    assert creds == UserWsCredentials(api_key="", api_secret="", api_passphrase="")
