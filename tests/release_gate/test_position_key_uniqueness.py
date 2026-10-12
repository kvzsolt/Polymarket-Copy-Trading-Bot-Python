"""Release-gate: PositionKey identity is canonical (Issue #75 matrix item 1).

Asserts:
* identity is the triple ``(user_id, condition_id, token_id)`` regardless of
  the optional ``outcome`` label;
* hex ids are normalized so heterogeneous casing does not fork a single
  position into two scopes;
* the five factory constructors all produce keys that compare equal when fed
  the same underlying identity;
* same condition / different token == distinct keys (no condition-only
  collapse — see CLAUDE.md).
"""

from __future__ import annotations

import pytest

from pmcopy.domain.position_key import PositionKey


def test_outcome_does_not_affect_identity() -> None:
    a = PositionKey("u1", "0xCOND", "0xTOK", outcome="Yes")
    b = PositionKey("u1", "0xCOND", "0xTOK", outcome="No")
    assert a == b
    assert hash(a) == hash(b)


def test_hex_ids_are_normalized() -> None:
    upper = PositionKey("u1", "0xABCDEF", "0xDEADBEEF")
    lower = PositionKey("u1", "0xabcdef", "0xdeadbeef")
    assert upper == lower
    assert upper.condition_id == "0xabcdef"
    assert upper.token_id == "0xdeadbeef"


def test_same_condition_different_token_are_distinct() -> None:
    a = PositionKey("u1", "0xcond", "0xtokA")
    b = PositionKey("u1", "0xcond", "0xtokB")
    assert a != b


def test_signal_factory_yields_canonical_identity() -> None:
    event = {"condition_id": "0xcond", "asset": "0xtok", "outcome": "Yes"}
    key = PositionKey.from_signal_event(event, user_id="u1")
    assert key == PositionKey("u1", "0xcond", "0xtok")


def test_wss_factory_yields_canonical_identity() -> None:
    frame = {"market": "0xcond", "asset_id": "0xtok"}
    key = PositionKey.from_wss_event(frame, user_id="u1")
    assert key == PositionKey("u1", "0xcond", "0xtok")


def test_stored_factory_yields_canonical_identity() -> None:
    row = {"user_id": "u1", "condition_id": "0xcond", "asset": "0xtok"}
    key = PositionKey.from_stored_position(row)
    assert key == PositionKey("u1", "0xcond", "0xtok")


def test_execution_request_factory_yields_canonical_identity() -> None:
    req = {"user_id": "u1", "condition_id": "0xcond", "token_id": "0xtok"}
    key = PositionKey.from_execution_request(req)
    assert key == PositionKey("u1", "0xcond", "0xtok")


def test_reconciliation_factory_yields_canonical_identity() -> None:
    record = {"user_id": "u1", "condition_id": "0xcond", "asset": "0xtok"}
    key = PositionKey.from_reconciliation_record(record)
    assert key == PositionKey("u1", "0xcond", "0xtok")


def test_empty_identity_components_rejected() -> None:
    with pytest.raises(ValueError):
        PositionKey("", "0xcond", "0xtok")
    with pytest.raises(ValueError):
        PositionKey("u1", "", "0xtok")
    with pytest.raises(ValueError):
        PositionKey("u1", "0xcond", "")
