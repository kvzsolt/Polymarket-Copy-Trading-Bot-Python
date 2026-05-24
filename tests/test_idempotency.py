from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class TestIdempotencyStore:
    def test_build_trade_key(self):
        from utils.idempotency import build_trade_key

        trade = {
            "transactionHash": "0xABC",
            "side": "buy",
            "asset": "12345",
        }
        key = build_trade_key("0xLeader", trade)
        assert key == "0xleader|0xabc|BUY|12345"

    def test_mark_and_has_processed(self, tmp_path, monkeypatch):
        import utils.idempotency as idem

        monkeypatch.setitem(idem.ENV, "BOT_STATE_DB", str(tmp_path / "state.db"))
        idem.reset_cache_for_tests()

        trade = {
            "transactionHash": "0x1",
            "side": "BUY",
            "asset": "a1",
            "conditionId": "c1",
        }
        key = idem.build_trade_key("0xleader", trade)
        assert idem.has_processed(key) is False

        idem.mark_processed("0xleader", trade, status="executed")
        assert idem.has_processed(key) is True

    def test_mark_processed_all_tx_hashes_marks_aggregated(self, tmp_path, monkeypatch):
        import utils.idempotency as idem

        monkeypatch.setitem(idem.ENV, "BOT_STATE_DB", str(tmp_path / "state.db"))
        idem.reset_cache_for_tests()

        trade = {
            "transactionHash": "0xNEW",
            "side": "BUY",
            "asset": "a1",
            "conditionId": "c1",
            "_txHashes": ["0xOLD1", "0xOLD2"],
        }

        keys = idem.mark_processed_all_tx_hashes("0xleader", trade, status="executed")
        assert len(keys) >= 3

        for txh in ["0xnew", "0xold1", "0xold2"]:
            k = idem.build_trade_key("0xleader", {"transactionHash": txh, "side": "BUY", "asset": "a1"})
            assert idem.has_processed(k) is True
