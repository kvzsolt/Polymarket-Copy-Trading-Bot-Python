"""Issue #67 — canonical PositionKey unit tests.

Covers: identity excludes outcome, normalization, validation, and the five
factory constructors required by the spec checklist.
"""

from __future__ import annotations

import pytest

from pmcopy.domain.position_key import PositionKey


# ---------------------------------------------------------------------------
# Identity, equality, hashing
# ---------------------------------------------------------------------------


def test_identity_excludes_outcome():
    a = PositionKey(user_id="u1", condition_id="cA", token_id="tA", outcome="YES")
    b = PositionKey(user_id="u1", condition_id="cA", token_id="tA", outcome="NO")
    assert a == b
    assert hash(a) == hash(b)
    assert a.identity() == b.identity() == ("u1", "cA", "tA")
    assert a.outcome == "YES"
    assert b.outcome == "NO"


def test_yes_no_token_ids_distinct_under_same_condition():
    yes = PositionKey(user_id="u1", condition_id="c1", token_id="t-yes")
    no = PositionKey(user_id="u1", condition_id="c1", token_id="t-no")
    assert yes != no
    assert hash(yes) != hash(no)


def test_different_users_distinct_under_same_token():
    a = PositionKey(user_id="alice", condition_id="c1", token_id="t1")
    b = PositionKey(user_id="bob", condition_id="c1", token_id="t1")
    assert a != b
    assert hash(a) != hash(b)


def test_position_key_is_hashable_and_set_safe():
    s = {
        PositionKey(user_id="u1", condition_id="cA", token_id="tA"),
        PositionKey(user_id="u1", condition_id="cA", token_id="tA", outcome="YES"),
        PositionKey(user_id="u1", condition_id="cA", token_id="tB"),
    }
    assert len(s) == 2


def test_position_key_is_frozen():
    k = PositionKey(user_id="u1", condition_id="c", token_id="t")
    with pytest.raises((AttributeError, Exception)):
        k.user_id = "u2"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def test_hex_token_id_lowercased():
    a = PositionKey(user_id="u1", condition_id="0xABCDEF", token_id="0xDEADBEEF")
    b = PositionKey(user_id="u1", condition_id="0xabcdef", token_id="0xdeadbeef")
    assert a == b
    assert a.token_id == "0xdeadbeef"
    assert a.condition_id == "0xabcdef"


def test_uppercase_hex_prefix_lowercased():
    # P1-k: condition_id / token_id are case-insensitive hashes and are
    # lowercased; user_id (an EIP-55 checksum wallet address) PRESERVES its
    # casing so a PositionKey matches the checksum-cased rows the repos store.
    a = PositionKey(user_id="0xAbCd", condition_id="0XABCDEF", token_id="0XDEADBEEF")
    assert a.user_id == "0xAbCd"
    assert a.condition_id == "0xabcdef"
    assert a.token_id == "0xdeadbeef"
    assert a.identity() == ("0xAbCd", "0xabcdef", "0xdeadbeef")


def test_non_hex_user_id_preserved_case():
    k = PositionKey(user_id="Alice", condition_id="c1", token_id="t1")
    assert k.user_id == "Alice"


def test_whitespace_stripped():
    a = PositionKey(user_id="  u1 ", condition_id=" 0xABC ", token_id=" 0xDEF ")
    b = PositionKey(user_id="u1", condition_id="0xabc", token_id="0xdef")
    assert a == b


def test_outcome_whitespace_stripped_to_none_when_empty():
    k = PositionKey(user_id="u1", condition_id="c", token_id="t", outcome="   ")
    assert k.outcome is None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "user_id,condition_id,token_id",
    [
        ("", "c", "t"),
        ("u", "", "t"),
        ("u", "c", ""),
        (None, "c", "t"),
        ("u", None, "t"),
        ("u", "c", None),
    ],
)
def test_empty_field_raises(user_id, condition_id, token_id):
    with pytest.raises(ValueError):
        PositionKey(user_id=user_id, condition_id=condition_id, token_id=token_id)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# log_fields
# ---------------------------------------------------------------------------


def test_log_fields_without_outcome():
    k = PositionKey(user_id="u1", condition_id="c", token_id="t")
    fields = k.log_fields()
    assert fields == {
        "position_key.user_id": "u1",
        "position_key.condition_id": "c",
        "position_key.token_id": "t",
    }


def test_log_fields_with_outcome():
    k = PositionKey(user_id="u1", condition_id="c", token_id="t", outcome="YES")
    fields = k.log_fields()
    assert fields["position_key.outcome"] == "YES"


# ---------------------------------------------------------------------------
# Factory constructors (spec checklist item 2)
# ---------------------------------------------------------------------------


def test_from_signal_event_dict():
    event = {
        "condition_id": "0xCONDITION",
        "asset": "0xTOKEN",
        "outcome": "YES",
    }
    key = PositionKey.from_signal_event(event, user_id="u1")
    assert key.identity() == ("u1", "0xcondition", "0xtoken")
    assert key.outcome == "YES"


def test_from_signal_event_attr_object():
    class Sig:
        condition_id = "0xCOND"
        token_id = "0xTOK"
        outcome = "NO"

    key = PositionKey.from_signal_event(Sig(), user_id="u1")
    assert key.identity() == ("u1", "0xcond", "0xtok")
    assert key.outcome == "NO"


def test_from_signal_event_camelcase_fallback():
    event = {"conditionId": "0xC", "tokenId": "0xT"}
    key = PositionKey.from_signal_event(event, user_id="u1")
    assert key.identity() == ("u1", "0xc", "0xt")


def test_from_wss_event_market_and_asset_id():
    event = {"market": "0xMKT", "asset_id": "0xAST", "outcome": "YES"}
    key = PositionKey.from_wss_event(event, user_id="u1")
    assert key.identity() == ("u1", "0xmkt", "0xast")
    assert key.outcome == "YES"


def test_from_stored_position_row():
    row = {
        "user_id": "u1",
        "condition_id": "0xC",
        "asset": "0xT",
        "outcome": "NO",
    }
    key = PositionKey.from_stored_position(row)
    assert key.identity() == ("u1", "0xc", "0xt")
    assert key.outcome == "NO"


def test_from_execution_request_dict():
    req = {"user_id": "u1", "condition_id": "0xC", "token_id": "0xT"}
    key = PositionKey.from_execution_request(req)
    assert key.identity() == ("u1", "0xc", "0xt")


def test_from_execution_request_attr_object():
    class Req:
        user_id = "u1"
        condition_id = "0xC"
        token_id = "0xT"

    key = PositionKey.from_execution_request(Req())
    assert key.identity() == ("u1", "0xc", "0xt")


def test_from_reconciliation_record():
    record = {
        "user_id": "u1",
        "condition_id": "0xCOND",
        "asset": "0xTOK",
    }
    key = PositionKey.from_reconciliation_record(record)
    assert key.identity() == ("u1", "0xcond", "0xtok")


def test_factory_validation_propagates():
    with pytest.raises(ValueError):
        PositionKey.from_reconciliation_record({"user_id": "u1", "condition_id": "", "asset": "t"})


# ---------------------------------------------------------------------------
# Cross-import sanity (PositionScope alias resolves to PositionKey)
# ---------------------------------------------------------------------------


def test_position_scope_alias_resolves_to_position_key():
    from pmcopy.services.positionActionCoordinator import PositionScope

    assert PositionScope is PositionKey
    a = PositionScope(user_id="u1", condition_id="c", token_id="t")
    b = PositionKey(user_id="u1", condition_id="c", token_id="t")
    assert a == b
    assert isinstance(a, PositionKey)


def test_coordinator_module_exposes_position_key_aliases():
    from pmcopy.services.positionActionCoordinator import (
        PositionActionCoordinator,
        PositionKey as PK,
        PositionScope as PS,
    )

    assert PK is PositionKey
    assert PS is PositionKey
    assert PositionActionCoordinator is not None
