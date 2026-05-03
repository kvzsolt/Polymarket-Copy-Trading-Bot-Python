"""Tests for services/observability/audit_logger.py"""

from __future__ import annotations

import json
import logging
import io

from services.observability import audit_logger


def _capture_audit_logs() -> io.StringIO:
    """Capture audit log output for assertion."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.INFO)
    audit_logger.logger.addHandler(handler)
    audit_logger.logger.setLevel(logging.INFO)
    return buf


def _strip_handler():
    """Remove the test handler from audit logger."""
    audit_logger.logger.handlers.clear()


class TestAuditLogger:
    def test_intent_accepted_emits_structured_json(self):
        buf = _capture_audit_logs()
        audit_logger.log_intent_accepted(
            intent_key="user|0xCOND|0xTOKEN|BUY|copy_buy",
            side="BUY",
            size="50.0",
            price="0.65",
            reason="copy_buy",
        )
        line = json.loads(buf.getvalue().strip())
        assert line["event"] == "intent_accepted"
        assert line["intent_key"] == "user|0xCOND|0xTOKEN|BUY|copy_buy"
        assert line["side"] == "BUY"
        assert line["size"] == "50.0"
        assert line["price"] == "0.65"
        assert "ts" in line
        _strip_handler()

    def test_duplicate_intent_suppressed(self):
        buf = _capture_audit_logs()
        audit_logger.log_duplicate_intent_suppressed(
            intent_key="user|0xCOND|0xTOKEN|SELL|copy_sell",
            existing_reservation_id="res_123",
        )
        line = json.loads(buf.getvalue().strip())
        assert line["event"] == "duplicate_intent_suppressed"
        assert line["existing_reservation_id"] == "res_123"
        _strip_handler()

    def test_all_events_have_timestamp(self):
        buf = _capture_audit_logs()

        audit_logger.log_intent_accepted("k", "BUY", "10", "0.5", "test")
        audit_logger.log_reservation_created("r1", "k", "5.0")
        audit_logger.log_order_submitted("r1", "o1", "v1")
        audit_logger.log_order_filled("r1", "10", "0.5")
        audit_logger.log_order_cancelled("r1", "manual")
        audit_logger.log_order_rejected("r1", "size_below_min")
        audit_logger.log_reservation_released("r1", "terminal")
        audit_logger.log_reconciliation_result("o1", "filled", "ok")
        audit_logger.log_geoblock_decision(True, "ok")
        audit_logger.log_wallet_validation(True, "eoa_match", "0xA", "0xA")
        audit_logger.log_kill_switch()
        audit_logger.log_startup("paper")
        audit_logger.log_shutdown("manual")

        for line_raw in buf.getvalue().strip().split("\n"):
            line = json.loads(line_raw)
            assert "ts" in line
            assert line["ts"].endswith("+00:00")
        _strip_handler()

    def test_no_secrets_in_audit_logs(self):
        buf = _capture_audit_logs()
        audit_logger.log_wallet_validation(
            True, "eoa_match",
            signer="0xAbCdEf0000000000000000000000000000000000",
            funder="0xAbCdEf0000000000000000000000000000000000",
        )
        raw = buf.getvalue()
        # Should only contain public addresses, not private keys
        assert "private" not in raw.lower()
        assert "secret" not in raw.lower()  # no secret field leakage
        # signer and funder are public addresses; they should be in output
        line = json.loads(buf.getvalue().strip())
        assert line["signer"].startswith("0x")
        assert len(line["signer"]) == 42
        _strip_handler()
