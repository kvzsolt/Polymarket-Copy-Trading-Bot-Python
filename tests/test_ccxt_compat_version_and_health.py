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
        ("4.5", (4, 5)),
        # Strict leading-numeric parsing: a suffixed component contributes
        # only its leading digits — never the digit-joined value.
        ("4.5.7rc3", (4, 5, 7)),
        ("4.5.73rc1", (4, 5, 73)),
        ("4.5.78rc2", (4, 5, 78)),
        ("4.5.73-rc1", (4, 5, 73)),
        ("4.5.73.dev1", (4, 5, 73)),
        ("4.5.73.post1", (4, 5, 73)),
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


def test_pinned_stable_version_passes() -> None:
    ensure_supported_ccxt_version("4.5.78")


def test_too_old_version_fails_closed() -> None:
    with pytest.raises(RuntimeError, match="4.5.73"):
        ensure_supported_ccxt_version("4.5.72")


def test_numeric_patch_comparison_is_not_digit_joined() -> None:
    # 8 must not combine with surrounding digits; 4.5.8 < 4.5.73.
    with pytest.raises(RuntimeError, match="4.5.73"):
        ensure_supported_ccxt_version("4.5.8")


def test_short_release_tuple_fails_closed() -> None:
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version("4.5")


def test_prerelease_of_older_release_fails_closed() -> None:
    # 4.5.7rc3 digit-joins to (4, 5, 73) under the old parser; it must
    # stay a pre-release of 4.5.7 and never satisfy the stable minimum.
    with pytest.raises(RuntimeError, match="stable"):
        ensure_supported_ccxt_version("4.5.7rc3")


def test_prerelease_of_minimum_release_fails_closed() -> None:
    # A pre-release of 4.5.73 sorts below the stable 4.5.73 release.
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version("4.5.73rc1")


def test_prerelease_of_newer_release_fails_closed() -> None:
    # Pre-releases never satisfy a stable minimum, even of a newer release.
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version("4.5.78rc1")


def test_dev_build_fails_closed() -> None:
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version("4.5.73.dev1")


def test_post_release_fails_closed() -> None:
    # Documented policy: stricter than PEP 440 — post-releases are not
    # accepted by the stable-minimum gate either.
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version("4.5.73.post1")


def test_missing_version_fails_closed() -> None:
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version(None)


@pytest.mark.parametrize("value", [None, "", "   ", "garbage"])
def test_unparseable_versions_fail_closed(value: str | None) -> None:
    with pytest.raises(RuntimeError):
        ensure_supported_ccxt_version(value)


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
    assert payload["precision_patch"] == "pmcopy-arbitrary-tick-v2"
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
