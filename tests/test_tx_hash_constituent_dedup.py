"""Phase 2.10 (H6) — aggregated signals must dedup on ALL constituent tx hashes.

The audit found aggregated signals dedup on a single tx hash only
(``build_trade_key`` uses only the primary ``transactionHash``), and
constituent hashes are marked processed only on the ``executed`` branch. The
helper ``mark_processed_all_tx_hashes`` exists but is never called from
production. The moment ``onchain_ws_url`` is set (enabling a second detection
channel), the same underlying trade arrives via two hashes and is copied
twice.

Fix: claim/mark ALL constituent tx hashes at claim time, not just the
primary, so a second detection channel cannot re-trigger an already-processed
aggregated signal.
"""

from __future__ import annotations

from orchestrators.trade_pipeline import build_trade_key, all_tx_hashes_for_trade


def test_all_tx_hashes_returns_primary_and_constituents():
    raw = {
        "transactionHash": "0xprimary",
        "_txHashes": ["0xprimary", "0xsecond", "0xthird"],
        "side": "BUY",
        "tokenID": "0xasset",
    }
    hashes = all_tx_hashes_for_trade(raw)
    assert set(hashes) == {"0xprimary", "0xsecond", "0xthird"}


def test_all_tx_hashes_dedups_and_preserves_order():
    raw = {
        "transactionHash": "0xprimary",
        "_txHashes": ["0xprimary", "0xsecond", "0xsecond", "0xthird"],
    }
    hashes = all_tx_hashes_for_trade(raw)
    assert hashes == ["0xprimary", "0xsecond", "0xthird"]


def test_all_tx_hashes_falls_back_to_primary_only():
    raw = {"transactionHash": "0xprimary", "txHash": "0xprimary"}
    assert all_tx_hashes_for_trade(raw) == ["0xprimary"]


def test_all_tx_hashes_handles_alternate_keys():
    raw = {"txHash": "0xalt", "tx_hash": "0xalt"}
    assert all_tx_hashes_for_trade(raw) == ["0xalt"]


def test_all_tx_hashes_empty_when_no_hash():
    assert all_tx_hashes_for_trade({}) == []


def test_build_trade_key_uses_primary_hash():
    """The per-hash dedup key still uses a single tx hash so each constituent
    can be claimed independently."""
    raw = {"transactionHash": "0xprimary", "side": "BUY", "tokenID": "0xasset"}
    key = build_trade_key("u1", "l1", raw)
    assert "0xprimary" in key
