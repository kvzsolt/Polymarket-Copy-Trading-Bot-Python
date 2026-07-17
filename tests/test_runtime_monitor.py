from __future__ import annotations

from orchestrators.runtime_monitor import _coalesce_trade_batches


def test_coalesce_preserves_trade_id_aliases_without_transaction_hash() -> None:
    first = [
        {
            "tradeID": "trade-a",
            "asset": "asset-1",
            "side": "BUY",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "trade_id": "trade-b",
            "asset": "asset-1",
            "side": "BUY",
            "timestamp": 2,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [first[0], second[0]]


def test_coalesce_preserves_distinct_anonymous_trades() -> None:
    """Rows carrying neither a transaction hash nor a trade id are distinct.

    Coalescing runs only under backlog pressure, so collapsing two different
    anonymous fills onto one empty identity silently drops a leader trade the
    copy pipeline would otherwise mirror.
    """
    first = [
        {
            "asset": "asset-1",
            "side": "BUY",
            "size": "10",
            "price": "0.40",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "asset": "asset-1",
            "side": "BUY",
            "size": "25",
            "price": "0.55",
            "timestamp": 2,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [first[0], second[0]]


def test_coalesce_dedupes_identical_anonymous_rows() -> None:
    """The same anonymous row seen in both batches still collapses to one."""
    row = {
        "asset": "asset-1",
        "side": "BUY",
        "size": "10",
        "price": "0.40",
        "timestamp": 1,
    }

    combined = _coalesce_trade_batches([dict(row)], [dict(row)])

    assert combined == [row]


def test_coalesce_still_dedupes_on_transaction_hash() -> None:
    """Keyed dedupe must stay intact: the later row wins for one identity."""
    first = [
        {
            "transactionHash": "0xabc",
            "asset": "asset-1",
            "side": "BUY",
            "size": "10",
            "timestamp": 1,
        }
    ]
    second = [
        {
            "transactionHash": "0xabc",
            "asset": "asset-1",
            "side": "BUY",
            "size": "11",
            "timestamp": 1,
        }
    ]

    combined = _coalesce_trade_batches(first, second)

    assert combined == [second[0]]
