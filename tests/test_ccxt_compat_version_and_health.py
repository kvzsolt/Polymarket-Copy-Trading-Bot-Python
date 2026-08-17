"""CCXT version guard and health-payload terminology contracts."""

from __future__ import annotations

import pytest

from integrations.polymarket.ccxt_compat.capabilities import (
    CCXT_CAPABILITY_METHODS,
    build_ccxt_health_payload,
)
from integrations.polymarket.ccxt_compat.version_guard import (
    ensure_supported_ccxt_version,
    parse_ccxt_version,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4.5.73", (4, 5, 73)),
        ("4.5.71.1", (4, 5, 71)),
        ("4.6.0", (4, 6, 0)),
        ("5.0.0", (5, 0, 0)),
        ("4.5.6", (4, 5, 6)),
        (None, ()),
        ("", ()),
        ("garbage", ()),
    ],
)
def test_parse_ccxt_version(value: str | None, expected: tuple[int, ...]) -> None:
    assert parse_ccxt_version(value) == expected


def test_minimum_supported_version_passes() -> None:
    ensure_supported_ccxt_version("4.5.73")


def test_newer_patch_version_passes() -> None:
    ensure_supported_ccxt_version("4.6.1")


def test_too_old_version_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="4.5.73"):
        ensure_supported_ccxt_version("4.5.72")


def test_missing_version_fails_closed() -> None:
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version(None)


def test_future_major_version_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="5"):
        ensure_supported_ccxt_version("5.1.0")


def test_health_payload_uses_ccxt_terminology_only() -> None:
    payload = build_ccxt_health_payload(
        available=True,
        ccxt_version="4.5.73",
        authenticated=True,
        live_execution_ready=True,
        capabilities={name: True for name in CCXT_CAPABILITY_METHODS},
    )

    assert payload["available"] is True
    assert payload["transport"] == "ccxt.prediction.polymarket"
    assert payload["ccxt_version"] == "4.5.73"
    assert payload["authenticated"] is True
    assert payload["live_execution_ready"] is True
    assert payload["precision_patch"] == "pmcopy-arbitrary-tick-v1"
    assert payload["capabilities"] == {
        "create_order": True,
        "cancel_order": True,
        "fetch_order": True,
        "fetch_open_orders": True,
        "fetch_my_trades": True,
    }
    assert payload["error"] is None


def test_health_payload_never_carries_sdk_field_names() -> None:
    payload = build_ccxt_health_payload(
        available=False,
        ccxt_version=None,
        authenticated=False,
        live_execution_ready=False,
        capabilities={},
        error="probe failed",
    )

    serialized = repr(payload)
    for banned in ("sdk_available", "sdk_ready", "sdk_client", "'sdk'"):
        assert banned not in serialized
    assert payload["error"] == "probe failed"


def test_health_payload_accepts_extra_diagnostics_without_secrets() -> None:
    payload = build_ccxt_health_payload(
        available=True,
        ccxt_version="4.5.73",
        authenticated=False,
        live_execution_ready=False,
        capabilities={},
        details={"chain_id": 137},
    )

    assert payload["chain_id"] == 137
