"""PR 5: order lifecycle logging — structured payload, dedup, panel selection."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from utils.logger import (
    RichConsoleHandler,
    _ORDER_LIFECYCLE_LAST,
    _reset_lifecycle_dedup_for_tests,
    log_order_lifecycle,
)


@pytest.fixture(autouse=True)
def _clear_dedup():
    _reset_lifecycle_dedup_for_tests()
    yield
    _reset_lifecycle_dedup_for_tests()


@pytest.fixture
def captured_logger(caplog):
    caplog.set_level(logging.INFO)
    return logging.getLogger("PMBot.Test.OrderLifecycle")


def test_log_emits_structured_payload(captured_logger, caplog):
    log_order_lifecycle(
        captured_logger,
        status="LIVE",
        side="buy",
        condition_id="cond1",
        asset="0xtok",
        order_id="ord1",
        size=5.0,
        price=0.5,
        usdc_value=2.5,
        venue_status="live",
    )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "order_lifecycle"]
    assert len(matching) == 1
    rec = matching[0]
    assert rec.status == "live"
    assert rec.side == "BUY"
    assert rec.conditionId == "cond1"
    assert rec.asset == "0xtok"
    assert rec.copyOrderId == "ord1"
    assert rec.size == 5.0
    assert rec.price == 0.5
    assert rec.usdcValue == 2.5


def test_dedup_suppresses_repeat_status(captured_logger, caplog):
    for _ in range(3):
        log_order_lifecycle(
            captured_logger,
            status="live",
            side="BUY",
            condition_id="cond1",
            asset="0xtok",
            order_id="ord1",
        )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "order_lifecycle"]
    assert len(matching) == 1


def test_force_bypasses_dedup(captured_logger, caplog):
    log_order_lifecycle(
        captured_logger,
        status="live", side="BUY",
        condition_id="cond1", asset="0xtok", order_id="ord1",
    )
    log_order_lifecycle(
        captured_logger,
        status="live", side="BUY",
        condition_id="cond1", asset="0xtok", order_id="ord1",
        force=True,
    )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "order_lifecycle"]
    assert len(matching) == 2


def test_status_change_emits_again(captured_logger, caplog):
    log_order_lifecycle(
        captured_logger,
        status="live", side="BUY",
        condition_id="cond1", asset="0xtok", order_id="ord1",
    )
    log_order_lifecycle(
        captured_logger,
        status="confirmed", side="BUY",
        condition_id="cond1", asset="0xtok", order_id="ord1",
    )
    matching = [r for r in caplog.records if getattr(r, "type", None) == "order_lifecycle"]
    assert len(matching) == 2
    assert {r.status for r in matching} == {"live", "confirmed"}


def test_render_uses_correct_panel_color_for_each_status():
    handler = RichConsoleHandler()
    panels: list = []

    def _capture(*args, **kwargs):
        for arg in args:
            # Panel instances have a `.title` and `.border_style` attribute.
            if hasattr(arg, "title") and hasattr(arg, "border_style"):
                panels.append(arg)

    with patch("utils.logger.console.print", side_effect=_capture):
        for status in ("live", "confirmed", "failed", "delayed"):
            record = SimpleNamespace(
                status=status,
                side="BUY",
                conditionId="cond1",
                asset="0xtok",
                copyOrderId="ord1",
                size=5.0,
                price=0.5,
                usdcValue=2.5,
                venueStatus=status,
                submissionState=None,
                txHash=None,
            )
            handler._render_order_lifecycle(record, ts="12:00:00 AM")

    assert len(panels) == 4
    titles = [str(p.title) for p in panels]
    borders = [p.border_style for p in panels]
    assert any("📘" in t and "ORDER LIVE" in t for t in titles)
    assert "cyan" in borders
    assert any("✅" in t and "TRADE CONFIRMED" in t for t in titles)
    assert "green" in borders
    assert any("❌" in t and "ORDER FAILED" in t for t in titles)
    assert "red" in borders
    assert any("⏳" in t and "ORDER DELAYED" in t for t in titles)
    assert "yellow" in borders


def test_order_lifecycle_not_in_jsonl_only_types():
    assert "order_lifecycle" not in RichConsoleHandler._JSONL_ONLY_TYPES
