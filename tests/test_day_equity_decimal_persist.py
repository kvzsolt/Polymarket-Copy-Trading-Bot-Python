"""Regression: dayEquity must persist as Decimal string, not float.

Pre-fix: ``equity_val = float(current_equity) if isinstance(current_equity, Decimal)``
demoted Decimal → float before JSON serialization. A Decimal like
``100000.001`` would round-trip as ``100000.001`` for small values but
lose precision for values straddling float boundaries. The daily-loss
calculation that drives the risk-engine halt was built on this stale
starting equity.

Test pins:
  * High-precision Decimal round-trips losslessly through the JSON file.
  * On second-call same-day, the value read back equals the original Decimal exactly.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

import pmcopy.utils.dayEquity as day_eq


@pytest.fixture()
def isolated_state(tmp_path, monkeypatch):
    state_file = tmp_path / "day_equity.json"
    monkeypatch.setattr(day_eq, "_STATE_FILE", state_file)
    return state_file


def test_decimal_persist_roundtrip(isolated_state):
    """First call writes; second call reads back the exact Decimal."""
    precise = Decimal("100000.0000000001")
    recorded = day_eq.get_day_start_equity(precise)
    assert recorded == precise

    # Second call same day — must read back exactly, not via float coercion.
    read_back = day_eq.get_day_start_equity(Decimal("999"))  # current ignored
    assert read_back == precise, (
        f"persisted Decimal lost precision; got {read_back!r}, expected {precise!r}"
    )


def test_json_stores_string_form(isolated_state):
    """The on-disk JSON must keep equity as a string for lossless round-trip."""
    import json

    day_eq.get_day_start_equity(Decimal("123.456789"))
    payload = json.loads(isolated_state.read_text())
    # Stored as string keeps Decimal precision intact across JSON.
    assert isinstance(payload["equity"], str), (
        f"equity must persist as JSON string, got {type(payload['equity']).__name__}"
    )
    assert payload["equity"] == "123.456789"
