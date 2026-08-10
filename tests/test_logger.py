from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from unittest.mock import patch

import pytest
import sys
from rich.traceback import Traceback

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.logger import (
    APP_NAME,
    JsonFormatter,
    MonitorHeartbeat,
    RichConsoleHandler,
    _parse_api_error,
    _build_panel_lines,
    configure_logging,
    event_link,
    get_logger,
    get_wallet_type,
    log_api_response,
    log_monitor_heartbeat,
    log_trade_detected,
    log_trade_decision,
    log_trade_execution,
    log_trade_skip,
    log_trade_submitted,
    monitor_heartbeat_tick,
    print_api_error,
    print_api_response,
    print_balance_update,
    print_capital_info,
    print_clob_error,
    print_clob_client_status,
    print_db_status,
    print_error,
    print_executing_strategy,
    print_first_run_init,
    print_historical_done,
    print_info,
    print_monitor_start,
    print_new_trade_detected,
    print_order_skip,
    print_positions,
    print_service_start,
    print_skip_reason,
    print_startup_banner,
    print_status_checks,
    print_success,
    print_trade_queued,
    print_trade_result,
    print_traders_copying,
    print_warning,
    redact_error_detail,
    reset_heartbeat,
    shorten_addr,
    tx_link,
    _CONFIGURED_LOGGERS,
)


@pytest.fixture(autouse=True)
def _clean_loggers():
    _CONFIGURED_LOGGERS.clear()
    root = logging.getLogger(APP_NAME)
    root.handlers.clear()
    yield
    _CONFIGURED_LOGGERS.clear()
    root.handlers.clear()
    # Restore default APP_NAME logger config so cross-file test ordering
    # doesn't end with subsequent tests inheriting a handler-less /
    # WARNING-level logger. Without this, e.g.
    # ``test_integer_level`` (which sets level=WARNING) used to leak
    # the WARNING level into ``test_trade_pipeline_market_data_authority``
    # downstream, dropping every ``logger.info`` line the trade pipeline
    # tests assert on. Re-running ``configure_logging()`` with defaults
    # rebuilds the standard handler set + restores INFO level.
    configure_logging()


@pytest.fixture()
def tmp_log_dir(tmp_path):
    return tmp_path / "logs"


class TestConfigureLogging:
    def test_returns_logger(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir)
        assert isinstance(log, logging.Logger)
        assert log.name == APP_NAME

    def test_creates_log_directory(self, tmp_log_dir):
        configure_logging(log_dir=tmp_log_dir)
        assert tmp_log_dir.exists()

    def test_creates_jsonl_file(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir, level="DEBUG")
        log.info("test message")
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        assert jsonl.exists()
        assert "test message" in jsonl.read_text()

    def test_jsonl_is_valid_json(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir, level="DEBUG")
        log.info("hello world")
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        for line in jsonl.read_text().strip().splitlines():
            parsed = json.loads(line)
            assert "message" in parsed
            assert "level" in parsed
            assert "ts" in parsed

    def test_explicit_level(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir, level="WARNING")
        assert log.level == logging.WARNING

    def test_reconfigure_removes_old_handlers(self, tmp_log_dir):
        log1 = configure_logging(log_dir=tmp_log_dir)
        n1 = len(log1.handlers)
        log2 = configure_logging(log_dir=tmp_log_dir)
        n2 = len(log2.handlers)
        assert n1 == n2

    def test_has_two_managed_handlers(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir)
        managed = [h for h in log.handlers if getattr(h, "_pm_bot_managed", False)]
        assert len(managed) == 2

    def test_integer_level(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir, level=logging.WARNING)
        assert log.level == logging.WARNING


class TestGetLogger:
    def test_auto_configures(self):
        log = get_logger()
        assert APP_NAME in _CONFIGURED_LOGGERS
        assert log.name == APP_NAME

    def test_child_logger(self, tmp_log_dir):
        configure_logging(log_dir=tmp_log_dir)
        child = get_logger("mymodule")
        assert child.name == f"{APP_NAME}.mymodule"

    def test_same_name_returns_app_logger(self, tmp_log_dir):
        configure_logging(log_dir=tmp_log_dir)
        log = get_logger(APP_NAME)
        assert log.name == APP_NAME

    def test_none_name_returns_app_logger(self, tmp_log_dir):
        configure_logging(log_dir=tmp_log_dir)
        log = get_logger(None)
        assert log.name == APP_NAME


class TestJsonFormatter:
    def test_format_basic(self):
        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        result = json.loads(fmt.format(record))
        assert result["message"] == "hello world"
        assert result["level"] == "INFO"

    def test_format_extras(self):
        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="trade",
            args=(),
            exc_info=None,
        )
        record.custom_field = "custom_value"
        result = json.loads(fmt.format(record))
        assert result["custom_field"] == "custom_value"

    def test_format_discovery_candidate_eval_extras(self):
        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="candidate_eval_completed",
            args=(),
            exc_info=None,
        )
        record.type = "discovery_candidate_eval"
        record.event = "completed"
        record.wallet = "0x" + "a" * 40
        record.candidate_index = 1
        record.candidate_total = 250
        record.final_score = 0.82
        result = json.loads(fmt.format(record))
        assert result["type"] == "discovery_candidate_eval"
        assert result["event"] == "completed"
        assert result["candidate_index"] == 1
        assert result["final_score"] == pytest.approx(0.82)

    def test_format_exception(self):
        fmt = JsonFormatter()
        try:
            _ = 1 / 0
        except ZeroDivisionError:
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error",
            args=(),
            exc_info=exc_info,
        )
        result = json.loads(fmt.format(record))
        assert result["exc_type"] == "ZeroDivisionError"

    def test_non_serializable_extras_skipped(self):
        fmt = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.bad_field = object()
        result = json.loads(fmt.format(record))
        assert "bad_field" not in result


class TestRichConsoleHandler:
    def test_emit_does_not_raise(self):
        handler = RichConsoleHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        handler.emit(record)

    def test_emit_levels(self):
        handler = RichConsoleHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        for level in (
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ):
            record = logging.LogRecord(
                name="test",
                level=level,
                pathname="",
                lineno=0,
                msg=f"level {level}",
                args=(),
                exc_info=None,
            )
            handler.emit(record)

    def test_emit_exception_uses_record_exc_info(self):
        handler = RichConsoleHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))

        try:
            raise ValueError("record-backed error")
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=exc_info,
        )

        with patch("utils.logger.console.print") as print_mock:
            with patch.object(handler, "format", return_value="test message"):
                handler.emit(record)

        assert print_mock.call_count == 2
        traceback_renderable = print_mock.call_args_list[1].args[0]
        assert isinstance(traceback_renderable, Traceback)
        assert traceback_renderable.trace.stacks[-1].exc_type == "ValueError"
        assert "record-backed error" in str(traceback_renderable.trace.stacks[-1].exc_value)

    def test_emit_discovery_candidate_eval_structured_line(self):
        handler = RichConsoleHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="candidate_eval_completed",
            args=(),
            exc_info=None,
        )
        record.type = "discovery_candidate_eval"
        record.event = "completed"
        record.wallet = "0x" + "a" * 40
        record.candidate_index = 1
        record.candidate_total = 250
        record.concurrency = 4
        record.status = "live"
        record.final_score = 0.82
        record.eligible_live = True
        record.duration_seconds = 1.25

        with patch("utils.logger.console.print") as print_mock:
            handler.emit(record)

        assert print_mock.call_count == 1
        rendered = print_mock.call_args.args[0]
        assert "DISCOVERY CANDIDATE" in rendered
        assert "001/250" in rendered
        assert "concurrency=4" in rendered
        assert "score=0.820" in rendered


class TestUrlHelpers:
    def test_tx_link_polygon(self):
        assert tx_link("0xabc123") == "https://polygonscan.com/tx/0xabc123"

    def test_tx_link_ethereum(self):
        assert tx_link("0xabc123", chain="ethereum") == "https://etherscan.io/tx/0xabc123"

    def test_tx_link_empty(self):
        assert tx_link(None) == ""
        assert tx_link("") == ""

    def test_tx_link_unknown_chain_defaults_polygon(self):
        assert "polygonscan.com" in tx_link("0xabc", chain="unknown")

    def test_event_link(self):
        assert event_link("2026-nba-champion") == "https://polymarket.com/event/2026-nba-champion"

    def test_event_link_empty(self):
        assert event_link(None) == ""
        assert event_link("") == ""


class TestShortenAddr:
    def test_normal_address(self):
        addr = "0xdb27abc123def456789012345678901234c56e"
        result = shorten_addr(addr)
        assert result.startswith("0xdb27")
        assert result.endswith("c56e")
        assert "..." in result

    def test_short_address(self):
        assert shorten_addr("0xAB") == "0xAB"

    def test_none(self):
        assert shorten_addr(None) == "N/A"

    def test_empty(self):
        assert shorten_addr("") == "N/A"

    def test_custom_prefix_suffix(self):
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        result = shorten_addr(addr, prefix=10, suffix=6)
        assert result.startswith("0x12345678")
        assert result.endswith("345678")


class TestMonitorHeartbeat:
    def test_first_tick_logs(self):
        hb = MonitorHeartbeat(interval=30)
        hb.tick(trader_count=1)
        assert hb._last_log > 0

    def test_subsequent_ticks_throttled(self):
        hb = MonitorHeartbeat(interval=30)
        hb._last_log = time.time()
        initial_last = hb._last_log
        hb.tick(trader_count=1)
        assert hb._last_log == initial_last

    def test_tick_after_interval(self):
        hb = MonitorHeartbeat(interval=1)
        hb._last_log = time.time() - 2
        old_last = hb._last_log
        hb.tick(trader_count=2)
        assert hb._last_log > old_last

    def test_interval_zero_always_prints(self):
        hb = MonitorHeartbeat(interval=0)
        hb.tick(trader_count=1)
        first = hb._last_log
        time.sleep(0.01)
        hb.tick(trader_count=1)
        assert hb._last_log >= first

    def test_reset_heartbeat(self):
        reset_heartbeat()
        monitor_heartbeat_tick(trader_count=1)

    def test_trader_count_stored(self):
        hb = MonitorHeartbeat(interval=30)
        hb.tick(trader_count=5)
        assert hb._trader_count == 5


class TestBuildPanelLines:
    def test_filters_none_values(self):
        lines = _build_panel_lines(Side="BUY", Status=None, Message="test")
        assert len(lines) == 2
        assert any("Side" in line for line in lines)
        assert any("Message" in line for line in lines)

    def test_all_none(self):
        lines = _build_panel_lines(A=None, B=None)
        assert lines == []

    def test_all_present(self):
        lines = _build_panel_lines(A="1", B="2", C="3")
        assert len(lines) == 3


class TestRichDisplayHelpers:
    def test_print_startup_banner(self):
        print_startup_banner()

    def test_print_status_checks_healthy(self):
        print_status_checks(rpc_ok=True, balance=100.0, api_ok=True, database_ok=True)

    def test_print_status_checks_low_balance(self):
        print_status_checks(rpc_ok=True, balance=2.0, api_ok=True, database_ok=True)

    def test_print_status_checks_very_low_balance(self):
        print_status_checks(rpc_ok=False, balance=0.5, api_ok=False, database_ok=False)

    def test_print_clob_client_status(self):
        class DummyClient:
            signature_type = 0

        print_clob_client_status(DummyClient())

    def test_sdk_deposit_wallet_type_is_authoritative(self):
        class InnerClient:
            wallet_type = "DEPOSIT_WALLET"
            signature_type = 0

        class DummyClient:
            client = InnerClient()

        assert get_wallet_type(DummyClient()) == ("DEPOSIT_WALLET", 3)

    def test_legacy_signature_type_three_logs_as_deposit_wallet(self):
        class DummyClient:
            signature_type = 3

        assert get_wallet_type(DummyClient()) == ("DEPOSIT_WALLET", 3)

    def test_print_service_start(self):
        print_service_start(trader_count=3)

    def test_print_db_status_with_data(self):
        print_db_status({"0xabc...def": 42, "0x123...456": 0})

    def test_print_db_status_empty(self):
        print_db_status(None)

    def test_print_positions_with_data(self):
        print_positions(
            wallet="0xeFFA1234567890abcdef1234567890ab7f8F",
            available_cash=50.0,
            total_portfolio=150.0,
            positions=[
                {
                    "title": "Will BTC hit 100k?",
                    "outcome": "Yes",
                    "size": 10.0,
                    "avgPrice": 0.65,
                    "currentValue": 7.5,
                },
                {
                    "title": "US Election 2024",
                    "outcome": "No",
                    "size": 5.0,
                    "avgPrice": 0.30,
                    "currentValue": 2.0,
                },
            ],
        )

    def test_print_positions_empty(self):
        print_positions(
            wallet="0xeFFA...7f8F",
            available_cash=2.29,
            total_portfolio=2.29,
            positions=None,
        )

    def test_print_traders_copying(self):
        print_traders_copying(
            [
                {
                    "address": "0xdb27abc123def456789012345678901234c56e",
                    "positions": 5,
                    "leader_balance": 10.0,
                },
            ]
        )

    def test_print_traders_copying_empty(self):
        print_traders_copying(None)

    def test_print_monitor_start(self):
        print_monitor_start(trader_count=1, interval_s=1)

    def test_print_first_run_init(self):
        print_first_run_init()

    def test_print_historical_done(self):
        print_historical_done()

    def test_print_new_trade_detected(self):
        trade = {
            "proxyWallet": "0xdb27abc123def456789012345678901234c56e",
            "side": "BUY",
            "conditionId": "0x951403...8665",
            "asset": "0x951403abcdef1234567890abcdef1234567890ab8665",
            "usdcSize": 0.84392,
            "price": 0.014,
            "title": "2026 NBA Champion",
            "submarket": "Moneyline",
            "outcome": "Blackhawks",
            "eventSlug": "2026-nba-champion",
            "transactionHash": "0xa43d5164f7eb00b163bf37ab1016bf65381fcc38a96f2ea5ae81bb31fb0cd5bd",
        }
        print_new_trade_detected(trade)

    def test_print_new_trade_detected_minimal(self):
        print_new_trade_detected({"side": "SELL"})

    def test_print_trade_queued(self):
        print_trade_queued({"proxyWallet": "0xabc123"})

    def test_print_capital_info(self):
        print_capital_info(my_balance=2.29, leader_balance=145018.70, leader_addr="0xdb27...c56e")

    def test_print_executing_strategy(self):
        print_executing_strategy("BUY")
        print_executing_strategy("SELL")

    def test_print_balance_update(self):
        print_balance_update(42.50)

    def test_print_trade_result_success(self):
        print_trade_result(
            success=True,
            side="BUY",
            leader_tx="0xabc123",
            copy_tx="0xdef456",
            amount=0.84,
            price=0.014,
            title="Hurricanes vs Blackhawks",
            submarket="Moneyline",
            outcome="Blackhawks",
        )

    def test_print_trade_result_failure(self):
        print_trade_result(
            success=False,
            side="SELL",
            reason="proxy has no position in this market",
        )

    def test_print_skip_reason(self):
        print_skip_reason("order size is 0", "remaining=$0.00")

    def test_print_error(self):
        print_error("Something went wrong")

    def test_print_warning(self):
        print_warning("Low balance")

    def test_print_info(self):
        print_info("Starting up")

    def test_print_success(self):
        print_success("Trade executed")


class TestTradeLogging:
    def _make_logger(self, tmp_log_dir):
        return configure_logging(log_dir=tmp_log_dir, level="DEBUG")

    def test_log_trade_execution(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_execution(
            log,
            side="BUY",
            condition_id="cond123",
            asset="asset456",
            size=10.0,
            price=0.5,
            usdc_value=5.0,
            tx_hash="0xtx789",
            event_slug="test-event",
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        trade_lines = [
            json.loads(line)
            for line in jsonl.read_text().strip().splitlines()
            if "EXECUTED" in line
        ]
        assert len(trade_lines) >= 1
        entry = trade_lines[0]
        assert entry["type"] == "trade_execution"
        assert entry["side"] == "BUY"
        assert entry["conditionId"] == "cond123"
        assert "txHash" in entry

    def test_log_trade_detected(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_detected(
            log,
            side="SELL",
            condition_id="cond999",
            asset="asset111",
            size=5.0,
            price=0.3,
            usdc_value=1.5,
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        trade_lines = [
            json.loads(line)
            for line in jsonl.read_text().strip().splitlines()
            if "DETECTED" in line
        ]
        assert len(trade_lines) >= 1
        assert trade_lines[0]["type"] == "trade_detected"

    def test_log_trade_submitted(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_submitted(
            log,
            side="BUY",
            condition_id="cond555",
            asset="asset222",
            size=1.0,
            price=0.9,
            usdc_value=0.9,
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        trade_lines = [
            json.loads(line) for line in jsonl.read_text().strip().splitlines() if "SUBMIT" in line
        ]
        assert len(trade_lines) >= 1
        assert trade_lines[0]["type"] == "trade_submitted"

    def test_log_trade_skip_not_marked_executed(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_skip(
            log,
            side="BUY",
            condition_id="cond777",
            asset="asset777",
            size=1.0,
            price=0.9,
            usdc_value=0.9,
            skip_code="edge_decay_exceeded",
            reason="decision gate rejected",
            status="skipped",
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        lines = [json.loads(line) for line in jsonl.read_text().strip().splitlines()]
        assert any(e.get("type") == "trade_skip" for e in lines)
        assert not any(
            "EXECUTED" in e.get("message", "") for e in lines if e.get("type") == "trade_skip"
        )

    def test_log_trade_decision(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_decision(
            log,
            side="BUY",
            condition_id="cond101",
            asset="asset101",
            decision_score=0.82,
            allow=True,
            reason="decision-approved BUY",
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        entries = [json.loads(line) for line in jsonl.read_text().strip().splitlines()]
        decision_rows = [e for e in entries if e.get("type") == "trade_decision"]
        assert len(decision_rows) >= 1
        assert decision_rows[0]["allow"] is True

    def test_log_monitor_heartbeat(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_monitor_heartbeat(log, interval_s=30, checked_activities=42)
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        hb_lines = [
            json.loads(line)
            for line in jsonl.read_text().strip().splitlines()
            if "heartbeat" in line.lower()
        ]
        assert len(hb_lines) >= 1
        assert hb_lines[0]["type"] == "monitor_heartbeat"
        assert hb_lines[0]["checkedActivities"] == 42

    def test_log_trade_with_none_values(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_execution(
            log,
            side="BUY",
            condition_id=None,
            asset=None,
            size=None,
            price=None,
            usdc_value=None,
        )

    def test_log_trade_with_meta(self, tmp_log_dir):
        log = self._make_logger(tmp_log_dir)
        log_trade_execution(
            log,
            side="BUY",
            condition_id="cond",
            asset="asset",
            size=1.0,
            price=0.5,
            usdc_value=0.5,
            meta={"custom_key": "custom_value"},
        )
        for h in log.handlers:
            h.flush()
        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        trade_lines = [
            json.loads(line)
            for line in jsonl.read_text().strip().splitlines()
            if "EXECUTED" in line
        ]
        assert trade_lines[0].get("custom_key") == "custom_value"


class TestIntegrationFlow:
    def test_full_startup_flow(self, tmp_path):
        configure_logging(log_dir=tmp_path / "logs", level="DEBUG")
        print_startup_banner()
        print_status_checks(rpc_ok=True, balance=2.29, api_ok=True, database_ok=True)

        class DummyClient:
            signature_type = 0

        print_clob_client_status(DummyClient())
        print_service_start(trader_count=1)
        print_db_status({"0xdb27...c56e": 0})
        print_positions(wallet="0xeFFA...7f8F", available_cash=2.29, total_portfolio=2.29)
        print_traders_copying([{"address": "0xdb27...c56e", "positions": 0, "leader_balance": 0.0}])
        print_monitor_start(trader_count=1, interval_s=1)
        print_first_run_init()
        print_historical_done()

    def test_trade_detection_flow(self, tmp_path):
        log = configure_logging(log_dir=tmp_path / "logs", level="DEBUG")
        trade = {
            "proxyWallet": "0xdb27abc123def456789012345678901234c56e",
            "side": "BUY",
            "conditionId": "0x951403",
            "asset": "0x951403abcdef",
            "usdcSize": 0.84392,
            "price": 0.014,
            "title": "Test Market",
            "eventSlug": "test-market",
            "transactionHash": "0xa43d5164",
        }
        print_new_trade_detected(trade)
        log_trade_detected(
            log,
            side="BUY",
            condition_id=trade["conditionId"],
            asset=trade["asset"],
            size=float(trade["usdcSize"]) / float(trade["price"]),
            price=trade["price"],
            usdc_value=trade["usdcSize"],
            tx_hash=trade["transactionHash"],
            event_slug=trade["eventSlug"],
        )
        print_capital_info(my_balance=2.29, leader_balance=145018.70, leader_addr="0xdb27...c56e")
        print_executing_strategy("BUY")
        print_trade_result(
            success=True,
            side="BUY",
            leader_tx="0xa43d5164",
            copy_tx="0xresult123",
            amount=0.84,
            price=0.014,
        )
        print_balance_update(1.45)

    def test_heartbeat_throttle_integration(self):
        hb = MonitorHeartbeat(interval=2)
        hb.tick(trader_count=1)
        first_log_time = hb._last_log
        assert first_log_time > 0
        hb.tick(trader_count=1)
        assert hb._last_log == first_log_time
        hb._last_log = time.time() - 3
        hb.tick(trader_count=1)
        assert hb._last_log > first_log_time


class TestParseApiError:
    def test_parse_poly_api_exception(self):
        err = "PolyApiException[status_code=400, error_message={'error': 'invalid amount for a marketable BUY order ($0.1998), min size: $1'}]"
        parsed = _parse_api_error(err)
        assert parsed["status_code"] == "400"
        assert "invalid amount" in parsed["error_message"]

    def test_parse_no_status_code(self):
        parsed = _parse_api_error("some random error")
        assert "status_code" not in parsed

    def test_parse_status_code_only(self):
        parsed = _parse_api_error("status_code=500 something")
        assert parsed["status_code"] == "500"

    def test_parse_empty_string(self):
        parsed = _parse_api_error("")
        assert parsed["raw"] == ""

    def test_parse_dict_style_error(self):
        parsed = _parse_api_error("{'error': 'order too small'}")
        assert parsed.get("error_message") == "order too small"

    def test_parse_complex_poly_exception(self):
        parsed = _parse_api_error(
            "PolyApiException[status_code=429, error_message={'error': 'rate limited'}]"
        )
        assert parsed["status_code"] == "429"
        assert parsed["error_message"] == "rate limited"


class TestErrorRedaction:
    def test_redact_error_detail_status_code(self):
        value = redact_error_detail(
            "PolyApiException[status_code=429, error_message={'error':'rate limited'}]"
        )
        assert value == "rate_limited"

    def test_redact_error_detail_timeout(self):
        assert redact_error_detail(RuntimeError("request timeout while posting order")) == "timeout"

    def test_log_api_response_omits_raw_response_payload(self, tmp_log_dir):
        log = configure_logging(log_dir=tmp_log_dir, level="DEBUG")
        log_api_response(
            log,
            method="POST",
            url="https://example.test/order",
            status=400,
            data={"success": False, "error": {"message": "sensitive detail"}},
            error="PolyApiException[status_code=400, error_message={'error':'sensitive detail'}]",
        )
        for h in log.handlers:
            h.flush()

        jsonl = tmp_log_dir / f"{APP_NAME}.jsonl"
        rows = [json.loads(line) for line in jsonl.read_text().splitlines() if line.strip()]
        api_rows = [row for row in rows if row.get("type") == "api_response"]
        assert api_rows
        row = api_rows[-1]
        assert row.get("error") == "bad_request"
        assert "response" not in row
        assert row.get("response_shape") == "dict[2]"


class TestErrorDisplayHelpers:
    def test_print_api_error_with_poly_exception(self):
        print_api_error(
            side="BUY",
            error="PolyApiException[status_code=400, error_message={'error': 'invalid amount'}]",
            condition_id="0x785176d4",
            asset="0x951403ab",
            retry=3,
            max_retries=3,
        )

    def test_print_api_error_simple(self):
        print_api_error(side="SELL", error="Connection timeout")

    def test_print_api_error_none_error(self):
        print_api_error(side="BUY", error=None)

    def test_print_api_error_no_retries(self):
        print_api_error(side="BUY", error="some error", retry=0)

    def test_print_api_response_failed(self):
        print_api_response(
            side="BUY",
            response={"success": False, "error": "PolyApiException[status_code=400]"},
            condition_id="0x785176d4",
        )

    def test_print_api_response_success_skipped(self):
        print_api_response(side="BUY", response={"success": True, "data": {}})

    def test_print_api_response_none(self):
        print_api_response(side="BUY", response=None)

    def test_print_api_response_no_error(self):
        print_api_response(side="SELL", response={"success": False})

    def test_print_order_skip_full(self):
        print_order_skip(
            side="BUY",
            reason="BUY order rejected",
            condition_id="0x785176d4",
            asset="0x951403ab",
            amount=0.1998,
            price=0.5,
            detail="min size: $1",
        )

    def test_print_order_skip_minimal(self):
        print_order_skip(side="SELL", reason="no position")

    def test_print_clob_error_post_order(self):
        print_clob_error(
            operation="post_order",
            error="PolyApiException[status_code=400]",
            asset="0x951403ab",
            side="BUY",
            condition_id="0x785176d4",
        )

    def test_print_clob_error_get_price(self):
        print_clob_error(
            operation="get_price", error="HTTP 500 response", asset="0xabc123", side="SELL"
        )

    def test_print_clob_error_minimal(self):
        print_clob_error(operation="unknown", error="timeout")

    def test_print_clob_error_exception_object(self):
        print_clob_error(operation="post_order", error=ValueError("bad value"))


class TestErrorFlowIntegration:
    def test_buy_rejection_flow(self):
        poly_err = "PolyApiException[status_code=400, error_message={'error': 'invalid amount, min size: $1'}]"
        for _ in range(3):
            print_clob_error(
                operation="post_order",
                error=poly_err,
                asset="0x951403ab",
                side="BUY",
                condition_id="0x785176d4",
            )
        print_api_error(
            side="BUY",
            error=poly_err,
            condition_id="0x785176d4",
            asset="0x951403ab",
            retry=3,
            max_retries=3,
        )
        print_api_response(
            side="BUY",
            response={"success": False, "error": poly_err},
            condition_id="0x785176d4",
        )
