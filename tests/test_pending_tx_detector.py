"""Tests for ``services/onchain_detection/pending_tx_detector.py`` (PR2)."""
from __future__ import annotations

from decimal import Decimal

from eth_abi import encode

from config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from services.onchain_detection.calldata_decoder import MATCH_ORDERS_SELECTOR
from services.onchain_detection.pending_tx_detector import (
    PendingTxConfig,
    build_signals_from_pending_tx,
    is_stale_pending,
    pending_subscription_params,
)
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
)

_ORDER_TUPLE = (
    "(uint256,address,address,uint256,uint256,uint256,"
    "uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
_MATCH_ORDERS_TYPE = (
    f"(bytes32,{_ORDER_TUPLE},{_ORDER_TUPLE}[],"
    "uint256,uint256[],uint256,uint256[])"
)
_WATCHED = "0x" + "ab" * 20
_WATCHED_MAKER = "0x" + "cd" * 20
_UNWATCHED = "0x" + "ef" * 20
_TX = "0x" + "12" * 32
_COND = b"\x02" * 32


def _mk_order(maker: str, side: int = MAKER_SIDE_BUY, token_id: int = 456, maker_amount: int = 2_000_000, taker_amount: int = 1_000_000) -> tuple:
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


def _encode(
    taker: tuple,
    makers: list[tuple],
    *,
    taker_fill: int = 1_000_000,
    maker_fills: list[int] | None = None,
) -> bytes:
    if maker_fills is None:
        maker_fills = [500_000] * len(makers)
    payload = [(_COND, taker, makers, taker_fill, maker_fills, 0, [0] * len(makers))]
    return MATCH_ORDERS_SELECTOR + encode([_MATCH_ORDERS_TYPE], payload)


def _tx(input_bytes: bytes, to: str = POLYMARKET_CTF_EXCHANGE_V2_ADDRESS, tx_hash: str = _TX) -> dict:
    return {
        "hash": tx_hash,
        "to": to,
        "from": "0x" + "00" * 20,
        "input": "0x" + input_bytes.hex(),
        "gas": "0x5208",
        "gasPrice": "0x3b9aca00",
        "nonce": "0x1",
        "value": "0x0",
        "blockHash": None,
        "blockNumber": None,
        "transactionIndex": None,
        "v": "0x0",
        "r": "0x0",
        "s": "0x0",
    }


def test_pending_subscription_params_covers_both_exchanges() -> None:
    params = pending_subscription_params()
    # JSON-RPC ``eth_subscribe`` expects params as a positional array:
    # ``["alchemy_pendingTransactions", options]``.
    assert isinstance(params, list)
    assert params[0] == "alchemy_pendingTransactions"
    options = params[1]
    to_addresses = [a.lower() for a in options["toAddress"]]
    assert POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() in to_addresses
    assert POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower() in to_addresses
    assert options["hashesOnly"] is False


def test_pending_subscription_params_hashes_only_flag() -> None:
    params = pending_subscription_params(hashes_only=True)
    assert isinstance(params, list)
    assert params[0] == "alchemy_pendingTransactions"
    assert params[1]["hashesOnly"] is True


def test_build_signals_watched_taker() -> None:
    taker = _mk_order(_WATCHED, side=MAKER_SIDE_BUY, token_id=789)
    calldata = _encode(taker, [_mk_order(_UNWATCHED, side=MAKER_SIDE_SELL)])
    tx = _tx(calldata)
    signals = build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1000)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.leader_wallet == _WATCHED
    assert sig.watched_role == "taker"
    assert sig.confidence == DetectionConfidence.PENDING_PROBABLE
    assert sig.provisional is True
    assert sig.log_index is None
    assert sig.tx_hash == _TX
    assert sig.exchange == POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower()
    # condition_id extracted from calldata first arg
    assert sig.condition_id == "0x" + _COND.hex()
    assert sig.token_id == "789"
    # taker BUY -> side BUY
    assert sig.side == "BUY"
    # default takerFillAmount partially fills half the order
    assert sig.size == Decimal("0.5")
    assert sig.usdc_size == Decimal("1")


def test_build_signals_watched_maker() -> None:
    taker = _mk_order(_UNWATCHED, side=MAKER_SIDE_BUY)
    maker = _mk_order(_WATCHED_MAKER, side=MAKER_SIDE_SELL, token_id=321, maker_amount=1_000_000, taker_amount=2_000_000)
    calldata = _encode(taker, [maker])
    tx = _tx(calldata)
    signals = build_signals_from_pending_tx(tx, {_WATCHED_MAKER}, recv_ts_ms=1000)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.leader_wallet == _WATCHED_MAKER
    assert sig.watched_role == "maker"
    assert sig.maker_side == MAKER_SIDE_SELL
    # maker SELL -> watched side SELL
    assert sig.side == "SELL"
    # default makerFillAmounts partially fill half the order
    assert sig.size == Decimal("0.5")
    assert sig.usdc_size == Decimal("1")


def test_build_signals_sizes_watched_taker_from_taker_fill_amount() -> None:
    taker = _mk_order(
        _WATCHED,
        side=MAKER_SIDE_BUY,
        maker_amount=10_000_000,
        taker_amount=5_000_000,
    )
    calldata = _encode(
        taker,
        [_mk_order(_UNWATCHED, side=MAKER_SIDE_SELL)],
        taker_fill=2_000_000,
    )
    tx = _tx(calldata)
    signals = build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1000)

    assert len(signals) == 1
    assert signals[0].usdc_size == Decimal("2")
    assert signals[0].size == Decimal("1")


def test_build_signals_sizes_watched_maker_from_indexed_maker_fill_amount() -> None:
    taker = _mk_order(_UNWATCHED, side=MAKER_SIDE_BUY)
    unfilled_watched = _mk_order(
        _WATCHED_MAKER,
        side=MAKER_SIDE_SELL,
        maker_amount=10_000_000,
        taker_amount=5_000_000,
    )
    calldata = _encode(
        taker,
        [_mk_order(_UNWATCHED, side=MAKER_SIDE_SELL), unfilled_watched],
        maker_fills=[3_000_000, 2_000_000],
    )
    tx = _tx(calldata)
    signals = build_signals_from_pending_tx(tx, {_WATCHED_MAKER}, recv_ts_ms=1000)

    assert len(signals) == 1
    assert signals[0].size == Decimal("2")
    assert signals[0].usdc_size == Decimal("1")


def test_build_signals_both_watched_emits_two() -> None:
    taker = _mk_order(_WATCHED, side=MAKER_SIDE_BUY)
    maker = _mk_order(_WATCHED_MAKER, side=MAKER_SIDE_SELL)
    calldata = _encode(taker, [maker])
    tx = _tx(calldata)
    signals = build_signals_from_pending_tx(tx, {_WATCHED, _WATCHED_MAKER}, recv_ts_ms=1)
    assert len(signals) == 2
    wallets = {s.leader_wallet for s in signals}
    assert wallets == {_WATCHED, _WATCHED_MAKER}


def test_build_signals_no_match_returns_empty() -> None:
    taker = _mk_order(_UNWATCHED, side=MAKER_SIDE_BUY)
    calldata = _encode(taker, [_mk_order("0x" + "99" * 20, side=MAKER_SIDE_SELL)])
    tx = _tx(calldata)
    assert build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1) == []


def test_build_signals_non_matchorders_tx_dropped() -> None:
    tx = _tx(b"\xde\xad\xbe\xef" + b"\x00" * 32)
    assert build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1) == []


def test_build_signals_unknown_exchange_dropped() -> None:
    taker = _mk_order(_WATCHED, side=MAKER_SIDE_BUY)
    calldata = _encode(taker, [])
    tx = _tx(calldata, to="0x" + "ff" * 20)
    assert build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1) == []


def test_build_signals_malformed_input_dropped() -> None:
    tx = _tx(b"\x6d\x0f\xcd\x6e" + b"\x00" * 10)  # right selector, garbage
    assert build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1) == []


def test_build_signals_neg_risk_exchange() -> None:
    taker = _mk_order(_WATCHED, side=MAKER_SIDE_BUY)
    calldata = _encode(taker, [])
    tx = _tx(calldata, to=POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS)
    signals = build_signals_from_pending_tx(tx, {_WATCHED}, recv_ts_ms=1)
    assert len(signals) == 1
    assert signals[0].exchange == POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower()


def test_is_stale_pending_true_for_old_signal() -> None:
    from services.onchain_detection.trade_detection_models import OnchainTradeSignal

    sig = OnchainTradeSignal(
        leader_wallet=_WATCHED,
        token_id="1",
        side="BUY",
        size=Decimal(1),
        price=Decimal(1),
        usdc_size=Decimal(1),
        tx_hash=_TX,
        confidence=DetectionConfidence.PENDING_PROBABLE,
        maker_side=0,
        watched_role="taker",
        event_ts=1000,
        log_index=None,
        condition_id="",
        provisional=True,
        exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        order_hash=None,
    )
    assert is_stale_pending(sig, now_ms=2000, max_age_ms=500) is True
    assert is_stale_pending(sig, now_ms=1100, max_age_ms=500) is False
    # max_age_ms <= 0 disables the gate
    assert is_stale_pending(sig, now_ms=100_000, max_age_ms=0) is False
    # non-provisional signals are never stale by this gate
    sig_mined = OnchainTradeSignal(
        leader_wallet=_WATCHED,
        token_id="1",
        side="BUY",
        size=Decimal(1),
        price=Decimal(1),
        usdc_size=Decimal(1),
        tx_hash=_TX,
        confidence=DetectionConfidence.MINED_AUTHORITATIVE,
        maker_side=0,
        watched_role="taker",
        event_ts=1000,
        log_index=5,
        condition_id="",
        provisional=False,
        exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        order_hash=None,
    )
    assert is_stale_pending(sig_mined, now_ms=100_000, max_age_ms=500) is False


def test_pending_config_defaults() -> None:
    cfg = PendingTxConfig()
    assert cfg.max_signal_age_ms == 500
    assert cfg.hashes_only is False
    assert cfg.confidence_required == DetectionConfidence.PENDING_PROBABLE
