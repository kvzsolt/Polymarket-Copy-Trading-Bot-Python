"""Release-gate: effective idempotency keys (Issue #75 item 7).

Per CLAUDE.md the trade key must collapse duplicates ONLY when every identity
component matches. ``utils.idempotency.build_trade_key`` keys are namespaced
by ``(user_id, leader_wallet, tx_hash, side, asset)``; same components ⇒ same
key, any one diverges ⇒ different key.
"""

from __future__ import annotations

from decimal import Decimal

from services.idempotency import compute_effective_key
from utils.idempotency import build_trade_key


def _trade(**overrides) -> dict:
    base = {
        "transactionHash": "0xtxn-1",
        "side": "BUY",
        "asset": "0xtoken-A",
        "user_id": "user-1",
        "conditionId": "0xcond-1",
    }
    base.update(overrides)
    return base


def test_same_identity_collapses_to_same_key() -> None:
    a = build_trade_key("0xleader-1", _trade())
    b = build_trade_key("0xleader-1", _trade())
    assert a == b


def test_different_user_yields_different_key() -> None:
    a = build_trade_key("0xleader-1", _trade(user_id="user-1"))
    b = build_trade_key("0xleader-1", _trade(user_id="user-2"))
    assert a != b


def test_different_side_yields_different_key() -> None:
    a = build_trade_key("0xleader-1", _trade(side="BUY"))
    b = build_trade_key("0xleader-1", _trade(side="SELL"))
    assert a != b


def test_different_tx_hash_yields_different_key() -> None:
    a = build_trade_key("0xleader-1", _trade(transactionHash="0xtxn-1"))
    b = build_trade_key("0xleader-1", _trade(transactionHash="0xtxn-2"))
    assert a != b


def test_different_asset_yields_different_key() -> None:
    a = build_trade_key("0xleader-1", _trade(asset="0xtoken-A"))
    b = build_trade_key("0xleader-1", _trade(asset="0xtoken-B"))
    assert a != b


def test_key_is_namespaced_by_user_when_present() -> None:
    key = build_trade_key("0xleader-1", _trade(user_id="user-1"))
    assert key.startswith("user-1|")


def test_key_falls_back_to_leader_namespace_when_no_user() -> None:
    trade = _trade()
    trade.pop("user_id")
    key = build_trade_key("0xleader-1", trade)
    assert key.startswith("0xleader-1|")


# ---------------------------------------------------------------------------
# Execution-layer key — the second idempotency surface mandated by
# CLAUDE.md: "Effective idempotency computed AFTER sizing+tick rounding,
# over (user, condition_id, token_id, side, eff_size, eff_price, tick,
# intent_id, policy_version)."
#
# ``utils.idempotency.build_trade_key`` above is the COPY-EVENT key (from
# raw on-chain trade); the gate below pins the EXECUTION-LAYER key from
# ``services.idempotency.compute_effective_key`` so a future refactor
# cannot drop or silently widen any of the discriminators.
# ---------------------------------------------------------------------------


def _exec_kwargs(**overrides):
    base = dict(
        user="u",
        condition_id="cond",
        token_id="tok",
        side="BUY",
        effective_size=Decimal("10"),
        effective_price=Decimal("0.42"),
        tick_size=Decimal("0.01"),
        intent_id="intent-1",
        policy_version="v1",
    )
    base.update(overrides)
    return base


def test_exec_key_stable_for_same_identity() -> None:
    assert compute_effective_key(**_exec_kwargs()) == compute_effective_key(**_exec_kwargs())


def test_exec_key_changes_with_effective_size() -> None:
    a = compute_effective_key(**_exec_kwargs(effective_size=Decimal("10")))
    b = compute_effective_key(**_exec_kwargs(effective_size=Decimal("11")))
    assert a != b, "effective_size must be part of the executable identity"


def test_exec_key_changes_with_effective_price() -> None:
    a = compute_effective_key(**_exec_kwargs(effective_price=Decimal("0.42")))
    b = compute_effective_key(**_exec_kwargs(effective_price=Decimal("0.43")))
    assert a != b, "effective_price must be part of the executable identity"


def test_exec_key_changes_with_tick_size() -> None:
    a = compute_effective_key(**_exec_kwargs(tick_size=Decimal("0.01")))
    b = compute_effective_key(**_exec_kwargs(tick_size=Decimal("0.001")))
    assert a != b, "tick_size at decision must be part of the identity"


def test_exec_key_changes_with_intent_id() -> None:
    a = compute_effective_key(**_exec_kwargs(intent_id="intent-1"))
    b = compute_effective_key(**_exec_kwargs(intent_id="intent-2"))
    assert a != b


def test_exec_key_changes_with_policy_version() -> None:
    a = compute_effective_key(**_exec_kwargs(policy_version="v1"))
    b = compute_effective_key(**_exec_kwargs(policy_version="v2"))
    assert a != b


def test_exec_key_changes_with_condition_id() -> None:
    a = compute_effective_key(**_exec_kwargs(condition_id="cond-A"))
    b = compute_effective_key(**_exec_kwargs(condition_id="cond-B"))
    assert a != b


def test_exec_key_changes_with_token_id() -> None:
    a = compute_effective_key(**_exec_kwargs(token_id="tok-YES"))
    b = compute_effective_key(**_exec_kwargs(token_id="tok-NO"))
    assert a != b, (
        "token_id must discriminate — two outcomes of the same condition "
        "with the same sizing must NOT collide on the executable key"
    )


def test_exec_key_changes_with_side() -> None:
    a = compute_effective_key(**_exec_kwargs(side="BUY"))
    b = compute_effective_key(**_exec_kwargs(side="SELL"))
    assert a != b


def test_exec_key_is_canonical_across_decimal_formatting() -> None:
    """Decimal("10") and Decimal("10.00") must produce the same key —
    the canonicalisation strips trailing zeros so benign formatting
    differences don't fork the identity."""
    a = compute_effective_key(**_exec_kwargs(effective_size=Decimal("10")))
    b = compute_effective_key(**_exec_kwargs(effective_size=Decimal("10.00")))
    assert a == b
