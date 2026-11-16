"""Tests for ``services/onchain_detection/order_filled_detector.py`` (PR2)."""

from __future__ import annotations

from decimal import Decimal

from eth_utils import keccak

from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from pmcopy.services.onchain_detection.order_filled_detector import (
    ORDER_FILLED_TOPIC0,
    ONCHAIN_EXCHANGE_ADDRESSES,
    build_signal_from_log,
    decode_order_filled_log,
    log_subscription_params_for_watched,
)
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
)

_WATCHED = "0x" + "ab" * 20
_OTHER = "0x" + "cd" * 20
_TX = "0x" + "12" * 32


def _pad32(hexstr: str) -> str:
    if hexstr.startswith("0x") or hexstr.startswith("0X"):
        hexstr = hexstr[2:]
    return "0x" + hexstr.lower().rjust(64, "0")


def _topic_addr(addr: str) -> str:
    return _pad32(addr)


def _word(n: int) -> bytes:
    return n.to_bytes(32, "big")


def _build_log(
    *,
    exchange: str = POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    maker: str = _WATCHED,
    taker: str = _OTHER,
    maker_side: int = MAKER_SIDE_BUY,
    token_id: int = 12345,
    maker_amount_filled: int = 2_000_000,
    taker_amount_filled: int = 1_000_000,
    fee: int = 0,
    log_index: int = 7,
    tx_hash: str = _TX,
) -> dict:
    data = (
        _word(maker_side)
        + _word(token_id)
        + _word(maker_amount_filled)
        + _word(taker_amount_filled)
        + _word(fee)
        + _word(0)  # builder
        + _word(0)  # metadata
    )
    return {
        "address": exchange,
        "topics": [
            "0x" + ORDER_FILLED_TOPIC0.hex(),
            _pad32("aa"),  # orderHash
            _topic_addr(maker),
            _topic_addr(taker),
        ],
        "data": "0x" + data.hex(),
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
        "blockNumber": hex(123),
    }


def test_topic0_matches_canonical_signature() -> None:
    expected = keccak(
        text="OrderFilled(bytes32,address,address,uint8,uint256,uint256,"
        "uint256,uint256,bytes32,bytes32)"
    )
    assert ORDER_FILLED_TOPIC0 == expected


def test_both_exchange_addresses_in_subscription() -> None:
    maker_params, taker_params = log_subscription_params_for_watched(
        watched_wallets={_WATCHED}
    )
    for params in (maker_params, taker_params):
        assert isinstance(params, list)
        assert params[0] == "logs"
        filter_obj = params[1]
        addresses = [a.lower() for a in filter_obj["address"]]
        assert POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() in addresses
        assert POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower() in addresses
        assert filter_obj["topics"][0] == ["0x" + ORDER_FILLED_TOPIC0.hex()]


def test_onchain_exchange_addresses_covers_both() -> None:
    assert set(ONCHAIN_EXCHANGE_ADDRESSES) == {
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
    }


def test_decode_log_basic_fields() -> None:
    log = _build_log()
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    assert decoded.maker == _WATCHED
    assert decoded.taker == _OTHER
    assert decoded.maker_side == MAKER_SIDE_BUY
    assert decoded.token_id == 12345
    assert decoded.maker_amount_filled == 2_000_000
    assert decoded.taker_amount_filled == 1_000_000
    assert decoded.fee == 0
    assert decoded.tx_hash == _TX
    assert decoded.log_index == 7
    assert decoded.block_number == 123
    assert decoded.exchange == POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower()


def test_decode_log_from_neg_risk_exchange() -> None:
    log = _build_log(exchange=POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS)
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    assert decoded.exchange == POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower()


def test_decode_log_returns_none_for_unknown_address() -> None:
    log = _build_log(exchange="0x" + "ff" * 20)
    assert decode_order_filled_log(log) is None


def test_decode_log_returns_none_for_wrong_topic0() -> None:
    log = _build_log()
    log["topics"][0] = "0x" + "00" * 32
    assert decode_order_filled_log(log) is None


def test_decode_log_returns_none_for_short_data() -> None:
    log = _build_log()
    log["data"] = "0x" + "00" * 10
    assert decode_order_filled_log(log) is None


def test_decode_log_handles_decimal_log_index() -> None:
    log = _build_log()
    log["logIndex"] = 42
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    assert decoded.log_index == 42


def test_build_signal_watched_maker_buy() -> None:
    log = _build_log(
        maker=_WATCHED,
        maker_side=MAKER_SIDE_BUY,
        maker_amount_filled=2_000_000,
        taker_amount_filled=1_000_000,
    )
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    signal = build_signal_from_log(
        decoded, {_WATCHED}, event_ts=1700000000000, condition_id="cond-x"
    )
    assert signal is not None
    assert signal.leader_wallet == _WATCHED
    assert signal.watched_role == "maker"
    assert signal.maker_side == MAKER_SIDE_BUY
    assert signal.side == "BUY"
    assert signal.confidence == DetectionConfidence.MINED_AUTHORITATIVE
    assert signal.log_index == 7
    assert signal.block_number == 123
    assert signal.provisional is False
    assert signal.condition_id == "cond-x"
    assert signal.tx_hash == _TX
    # maker BUY -> size = taker_filled (tokens) = 1_000_000 / 1e6 = 1.0
    assert signal.size == Decimal("1")
    # usdc = maker_filled / 1e6 = 2.0
    assert signal.usdc_size == Decimal("2")
    # price = maker_filled / taker_filled = 2.0
    assert signal.price == Decimal("2")


def test_build_signal_watched_taker_side_inverted() -> None:
    # maker side BUY, watched wallet is the taker -> watched side is SELL.
    log = _build_log(maker=_OTHER, taker=_WATCHED, maker_side=MAKER_SIDE_BUY)
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    signal = build_signal_from_log(decoded, {_WATCHED}, event_ts=1)
    assert signal is not None
    assert signal.watched_role == "taker"
    assert signal.side == "SELL"
    assert signal.maker_side == MAKER_SIDE_BUY


def test_build_signal_watched_maker_sell_price_inverted() -> None:
    log = _build_log(
        maker=_WATCHED,
        maker_side=MAKER_SIDE_SELL,
        maker_amount_filled=1_000_000,
        taker_amount_filled=2_000_000,
    )
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    signal = build_signal_from_log(decoded, {_WATCHED}, event_ts=1)
    assert signal is not None
    assert signal.watched_role == "maker"
    assert signal.side == "SELL"
    # maker SELL -> size = maker_filled (tokens) = 1.0; usdc = taker_filled = 2.0
    assert signal.size == Decimal("1")
    assert signal.usdc_size == Decimal("2")
    # price = taker_filled / maker_filled = 2.0
    assert signal.price == Decimal("2")


def test_build_signal_returns_none_when_wallet_not_watched() -> None:
    log = _build_log(maker=_OTHER, taker="0x" + "ee" * 20)
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    assert build_signal_from_log(decoded, {_WATCHED}, event_ts=1) is None


def test_build_signal_log_index_always_set() -> None:
    log = _build_log(log_index=99)
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    signal = build_signal_from_log(decoded, {_WATCHED}, event_ts=1)
    assert signal is not None
    assert signal.log_index == 99
    assert signal.provisional is False


# ---------------------------------------------------------------------------
# Watched-wallet topic-filtered subscription params (logs_only confirmed mode)
# ---------------------------------------------------------------------------

_TOPIC0_HEX = "0x" + ORDER_FILLED_TOPIC0.hex()

_LEADER_A = "0x" + "11" * 20
_LEADER_B = "0xAB" + "cd" * 19  # mixed case on purpose
_EXCHANGE_FILTER = (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)


def test_watched_params_build_maker_and_taker_filters() -> None:
    """One subscription per indexed role: topic2 (maker) / topic3 (taker).

    A single ``logs`` filter cannot OR topic2 with topic3, so the runtime
    opens both subscriptions and keeps the client-side maker/taker match as
    the second guard.
    """
    from pmcopy.services.onchain_detection.order_filled_detector import (
        log_subscription_params_for_watched,
    )

    maker, taker = log_subscription_params_for_watched(
        exchange_addresses=_EXCHANGE_FILTER,
        watched_wallets={_LEADER_B, _LEADER_A},
    )

    expected_topics = sorted((_pad32(_LEADER_A), _pad32(_LEADER_B)))
    expected_addresses = [
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
    ]
    assert maker == [
        "logs",
        {
            "address": expected_addresses,
            "topics": [[_TOPIC0_HEX], None, expected_topics],
        },
    ]
    assert taker == [
        "logs",
        {
            "address": expected_addresses,
            "topics": [[_TOPIC0_HEX], None, None, expected_topics],
        },
    ]


def test_watched_params_pad_and_lowercase_the_address_topics() -> None:
    from pmcopy.services.onchain_detection.order_filled_detector import (
        log_subscription_params_for_watched,
    )

    (maker, _taker) = log_subscription_params_for_watched(
        exchange_addresses=_EXCHANGE_FILTER,
        watched_wallets={_LEADER_B},
    )

    topics = maker[1]["topics"]
    assert topics[2] == [_pad32(_LEADER_B)]
    assert topics[2][0] == "0x" + "0" * 24 + ("ab" + "cd" * 19)


def test_watched_params_cap_the_filter_and_warn_with_the_count_only() -> None:
    """Beyond the cap the filter keeps a deterministic prefix and the WARN
    names only counts (never wallet addresses)."""
    import logging

    from pmcopy.services.onchain_detection import order_filled_detector as detector_module
    from pmcopy.services.onchain_detection.order_filled_detector import (
        WATCHED_TOPIC_FILTER_CAP,
        log_subscription_params_for_watched,
    )

    roster = {f"0x{i:040x}" for i in range(WATCHED_TOPIC_FILTER_CAP + 50)}

    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = _Capture(level=logging.WARNING)
    detector_module.logger.addHandler(handler)
    previous_level = detector_module.logger.level
    detector_module.logger.setLevel(logging.WARNING)
    try:
        maker, _taker = log_subscription_params_for_watched(
            exchange_addresses=_EXCHANGE_FILTER,
            watched_wallets=roster,
        )
    finally:
        detector_module.logger.removeHandler(handler)
        detector_module.logger.setLevel(previous_level)

    assert len(maker[1]["topics"][2]) == WATCHED_TOPIC_FILTER_CAP
    assert records, "expected a WARN naming the capped count"
    warning = records[-1].getMessage()
    assert str(WATCHED_TOPIC_FILTER_CAP) in warning
    assert str(WATCHED_TOPIC_FILTER_CAP + 50) in warning
    for address in roster:
        assert address not in warning


def test_watched_params_with_an_empty_roster_open_no_subscriptions() -> None:
    """Zero watched leaders must not re-open the unfiltered logs feed.

    The address-only subscription costs ~40k CU/min — exactly what
    ``logs_only`` exists to avoid — so an empty roster (e.g. discovery
    rotating every leader out) opens nothing; the runtime idles until the
    roster delivers a leader again.
    """
    from pmcopy.services.onchain_detection.order_filled_detector import (
        log_subscription_params_for_watched,
    )

    for empty in (None, set()):
        assert (
            log_subscription_params_for_watched(
                exchange_addresses=_EXCHANGE_FILTER, watched_wallets=empty
            )
            == ()
        )
