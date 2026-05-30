"""Structured logging for on-chain CTF redemption.

Mirrors ``test_order_lifecycle_logging.py``: assert the emitted record carries
the ``redemption`` structured payload, and that each status selects its own
panel theme (distinct coin/settlement icons, never the order/trade icons).
"""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from utils.logger import RichConsoleHandler, log_redemption


@pytest.fixture
def captured_logger(caplog):
    caplog.set_level(logging.INFO)
    return logging.getLogger("PMBot.Test.Redemption")


def test_log_emits_structured_payload(captured_logger, caplog):
    log_redemption(
        captured_logger,
        status="Success",
        condition_id="cond1",
        funder="0xfunder",
        positions=2,
        expected_value="42.50",
        tx_hash="0xdeadbeef",
    )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "redemption"]
    assert len(matching) == 1
    rec = matching[0]
    assert rec.status == "success"  # canonicalised lower
    assert rec.conditionId == "cond1"
    assert rec.funder == "0xfunder"
    assert rec.positions == 2
    assert rec.expectedValue == "42.50"
    assert rec.txHash == "0xdeadbeef"


def test_meta_merges_into_payload(captured_logger, caplog):
    log_redemption(
        captured_logger,
        status="blocked",
        funder="0xfunder",
        reason="signer_mismatch",
        meta={"signer": "0xsigner"},
    )
    rec = next(r for r in caplog.records if getattr(r, "type", None) == "redemption")
    assert rec.status == "blocked"
    assert rec.reason == "signer_mismatch"
    assert rec.signer == "0xsigner"


def test_no_dedup_every_event_emits(captured_logger, caplog):
    """Unlike order_lifecycle, redemption has no dedup — the service already
    guards duplicate submits, so repeats must remain operator-visible."""
    for _ in range(3):
        log_redemption(
            captured_logger,
            status="submit",
            condition_id="cond1",
            funder="0xfunder",
            positions=1,
        )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "redemption"]
    assert len(matching) == 3


def test_render_uses_correct_panel_for_each_status():
    handler = RichConsoleHandler()
    panels: list = []

    def _capture(*args, **kwargs):
        for arg in args:
            if hasattr(arg, "title") and hasattr(arg, "border_style"):
                panels.append(arg)

    with patch("utils.logger.console.print", side_effect=_capture):
        for status in ("submit", "success", "failed", "blocked", "online"):
            record = SimpleNamespace(
                status=status,
                conditionId="cond1",
                funder="0xfunder",
                txHash="0xtx",
                positions=1,
                expectedValue="10.00",
                reason=None,
                error="boom" if status == "failed" else None,
            )
            handler._render_redemption(record, ts="12:00:00 AM")

    assert len(panels) == 5
    titles = [str(p.title) for p in panels]
    borders = [p.border_style for p in panels]

    assert any("🪙" in t and "REDEEMING POSITION" in t for t in titles)
    assert any("💰" in t and "POSITION REDEEMED" in t for t in titles)
    assert "green" in borders
    assert any("❌" in t and "REDEMPTION FAILED" in t for t in titles)
    assert "red" in borders
    assert any("🛑" in t and "REDEMPTION BLOCKED" in t for t in titles)
    assert any("🟢" in t and "REDEMPTION ENGINE ONLINE" in t for t in titles)


def test_render_unknown_status_falls_back():
    handler = RichConsoleHandler()
    panels: list = []

    def _capture(*args, **kwargs):
        for arg in args:
            if hasattr(arg, "title") and hasattr(arg, "border_style"):
                panels.append(arg)

    with patch("utils.logger.console.print", side_effect=_capture):
        handler._render_redemption(
            SimpleNamespace(status="weird", conditionId=None, funder=None),
            ts="12:00:00 AM",
        )

    assert len(panels) == 1
    assert "REDEMPTION" in str(panels[0].title)
