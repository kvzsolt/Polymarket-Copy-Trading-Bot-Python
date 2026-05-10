"""Release-gate: effective idempotency keys (Issue #75 item 7).

Per CLAUDE.md the trade key must collapse duplicates ONLY when every identity
component matches. ``utils.idempotency.build_trade_key`` keys are namespaced
by ``(user_id, leader_wallet, tx_hash, side, asset)``; same components ⇒ same
key, any one diverges ⇒ different key.
"""
from __future__ import annotations

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
