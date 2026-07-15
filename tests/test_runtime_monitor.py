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
