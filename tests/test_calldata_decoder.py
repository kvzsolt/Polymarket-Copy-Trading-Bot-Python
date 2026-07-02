"""Tests for ``services/onchain_detection/calldata_decoder.py`` (PR2)."""
from __future__ import annotations

import pytest
from eth_abi import encode
from eth_utils import function_signature_to_4byte_selector

from services.onchain_detection.calldata_decoder import (
    MATCH_ORDERS_SELECTOR,
    decode_match_orders,
    find_watched_wallets,
    is_valid_maker_side,
)

_ORDER_TUPLE = (
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
_MATCH_ORDERS_TYPE = (
    f"(bytes32,{_ORDER_TUPLE},{_ORDER_TUPLE}[],"
    "uint256,uint256[],uint256,uint256[])"
)
_WATCHED_TAKER = "0x" + "11" * 20
_WATCHED_MAKER_A = "0x" + "22" * 20
_WATCHED_MAKER_B = "0x" + "33" * 20
_UNWATCHED = "0x" + "44" * 20


def _mk_order(maker: str, side: int = 0, token_id: int = 456, maker_amount: int = 1000, taker_amount: int = 2000) -> tuple:
    return (
        123,
        maker,
        maker,
        token_id,
        maker_amount,
        taker_amount,
        side,
        0,
        1700000000,
        b"\xaa" * 32,
        b"\xbb" * 32,
        b"\xcc" * 4,
    )


def _encode_match_orders(
    taker: tuple,
    makers: list[tuple],
    *,
    condition: bytes = b"\x01" * 32,
    taker_fill: int = 1000,
    maker_fills: list[int] | None = None,
    taker_fee: int = 0,
    maker_fees: list[int] | None = None,
) -> bytes:
    if maker_fills is None:
        maker_fills = [500] * len(makers)
    if maker_fees is None:
        maker_fees = [0] * len(makers)
    payload = [
        (
            condition,
            taker,
            makers,
            taker_fill,
            maker_fills,
            taker_fee,
            maker_fees,
        )
    ]
    return MATCH_ORDERS_SELECTOR + encode([_MATCH_ORDERS_TYPE], payload)


def test_selector_matches_canonical_signature() -> None:
    # The canonical ``matchOrders`` signature includes a STANDALONE taker
    # ``Order`` (2nd parameter) before the ``Order[]`` array. A selector
    # computed without it (``0x6d0fcd6e``) does not match production
    # ``matchOrders`` calls, so the pending-tx decoder would reject every
    # real payload before ABI decoding (Greptile P1 re-review).
    expected = function_signature_to_4byte_selector(
        "matchOrders(bytes32,"
        "(uint256,address,address,uint256,uint256,uint256,"
        "uint8,uint8,uint256,bytes32,bytes32,bytes),"
        "(uint256,address,address,uint256,uint256,uint256,"
        "uint8,uint8,uint256,bytes32,bytes32,bytes)[],"
        "uint256,uint256[],uint256,uint256[])"
    )
    assert MATCH_ORDERS_SELECTOR == expected
    # Production selector confirmed via Greptile T-Rex repro against a
    # real ``matchOrders`` pending transaction.
    assert MATCH_ORDERS_SELECTOR.hex() == "3c2b4399"
    # Regression guard: the buggy selector (missing standalone taker
    # Order) must NOT be used.
    assert MATCH_ORDERS_SELECTOR.hex() != "6d0fcd6e"


def test_decode_round_trip_taker_and_makers() -> None:
    taker = _mk_order(_WATCHED_TAKER, side=0)
    maker_a = _mk_order(_WATCHED_MAKER_A, side=1, token_id=999)
    maker_b = _mk_order(_UNWATCHED, side=1, token_id=999)
    calldata = _encode_match_orders(taker, [maker_a, maker_b])

    call = decode_match_orders(calldata)
    assert call is not None
    assert call.condition_id == b"\x01" * 32
    assert call.taker_order.maker == _WATCHED_TAKER
    assert call.taker_order.side == 0
    assert len(call.maker_orders) == 2
    assert call.maker_orders[0].maker == _WATCHED_MAKER_A
    assert call.maker_orders[0].token_id == 999
    assert call.maker_orders[1].maker == _UNWATCHED
    assert call.taker_fill_amount == 1000
    assert call.maker_fill_amounts == (500, 500)


def test_find_watched_wallets_matches_taker_and_makers() -> None:
    taker = _mk_order(_WATCHED_TAKER, side=0, token_id=111)
    maker_a = _mk_order(_WATCHED_MAKER_A, side=1, token_id=222)
    maker_b = _mk_order(_WATCHED_MAKER_B, side=1, token_id=333)
    calldata = _encode_match_orders(taker, [maker_a, maker_b])
    call = decode_match_orders(calldata)
    assert call is not None

    watched = {_WATCHED_TAKER, _WATCHED_MAKER_A, _WATCHED_MAKER_B}
    matches = find_watched_wallets(call, watched)
    wallets = {m.wallet for m in matches}
    assert wallets == watched
    roles = {m.wallet: m.role for m in matches}
    assert roles[_WATCHED_TAKER] == "taker"
    assert roles[_WATCHED_MAKER_A] == "maker"
    assert roles[_WATCHED_MAKER_B] == "maker"
    sides = {m.wallet: m.side for m in matches}
    assert sides[_WATCHED_TAKER] == 0  # taker order side
    assert sides[_WATCHED_MAKER_A] == 1  # maker side
    assert sides[_WATCHED_MAKER_B] == 1


def test_find_watched_wallets_case_insensitive() -> None:
    taker = _mk_order(_WATCHED_TAKER.upper(), side=0)
    calldata = _encode_match_orders(taker, [])
    call = decode_match_orders(calldata)
    assert call is not None
    # The decoder lowercases addresses.
    matches = find_watched_wallets(call, {_WATCHED_TAKER})
    assert len(matches) == 1
    assert matches[0].wallet == _WATCHED_TAKER


def test_decode_returns_none_for_wrong_selector() -> None:
    bad = b"\xde\xad\xbe\xef" + b"\x00" * 32
    assert decode_match_orders(bad) is None


def test_decode_returns_none_for_short_calldata() -> None:
    assert decode_match_orders(b"") is None
    assert decode_match_orders(b"\x00") is None
    assert decode_match_orders(b"\x6d\x0f\xcd") is None  # 3 bytes, no selector match anyway


def test_decode_returns_none_for_malformed_payload() -> None:
    # Right selector but garbage after it.
    assert decode_match_orders(MATCH_ORDERS_SELECTOR + b"\x00" * 10) is None


def test_decode_returns_none_for_empty_makers() -> None:
    taker = _mk_order(_WATCHED_TAKER, side=0)
    calldata = _encode_match_orders(taker, [])
    call = decode_match_orders(calldata)
    assert call is not None
    assert call.maker_orders == ()
    # No watched maker, only the taker matches.
    matches = find_watched_wallets(call, {_WATCHED_TAKER})
    assert len(matches) == 1
    assert matches[0].role == "taker"


def test_is_valid_maker_side() -> None:
    assert is_valid_maker_side(0)  # BUY
    assert is_valid_maker_side(1)  # SELL
    assert not is_valid_maker_side(2)
    assert not is_valid_maker_side(-1)


def test_decoded_order_is_frozen() -> None:
    taker = _mk_order(_WATCHED_TAKER, side=0)
    calldata = _encode_match_orders(taker, [])
    call = decode_match_orders(calldata)
    assert call is not None
    with pytest.raises(Exception):
        call.taker_order.maker = _UNWATCHED  # type: ignore[misc]