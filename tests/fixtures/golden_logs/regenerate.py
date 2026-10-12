"""Regenerate (or verify) the golden log fixtures for the structured logging sinks.

The fixtures under ``tests/fixtures/golden_logs/`` are the compatibility
contract for the standalone-refactor tasks that touch ``utils/logger.py``:
they pin, for each of the twelve order-lifecycle moments, exactly what the
Rich console sink renders and what the JSONL ``RotatingFileHandler`` sink
records when driven with synthetic events through the *real* production
helpers in ``utils/logger.py``.

What is pinned (contract):
  - event names (the ``type=`` extra / message prefixes),
  - levels,
  - required structured fields, reason/skip codes, and status vocabularies,
  - console-vs-JSONL routing (which events render a console panel, which are
    JSONL-only — including console silence as an explicit empty fixture),
  - rotation settings (see ``sink_contract.json``).

What is deliberately NOT pinned:
  - source module / function / line metadata (normalized to placeholders in
    the JSONL fixtures),
  - timestamps (frozen at emission and normalized to ``<TS>``),
  - latencies and order ids (fixtures use fixed synthetic identifiers).

Usage (from the repository root, with the project virtualenv active):

    python tests/fixtures/golden_logs/regenerate.py            # rewrite fixtures in place
    python tests/fixtures/golden_logs/regenerate.py --check    # verify fixtures match, exit 1 on drift
    python tests/fixtures/golden_logs/regenerate.py --output-dir DIR

Determinism measures (do not remove):
  - ``utils.logger.time`` is rebound to a frozen clock for the emission
    window, so console timestamps are constant regardless of wall clock,
    timezone, or locale;
  - the console is swapped for a fixed-width (100), no-color, non-terminal
    Rich ``Console`` writing to a buffer;
  - the JSONL lines are post-processed: ``ts`` -> ``<TS>``, ``module`` ->
    ``<module>``, ``funcName`` -> ``<funcName>``, ``lineno`` -> ``<lineno>``;
  - the module-level order-lifecycle dedup map is cleared before every event;
  - every identifier is a fixed synthetic constant defined below.

The synthetic identifiers are deliberately non-credential, obviously fake
values; no secret material is embedded in the fixtures.
"""

from __future__ import annotations

import argparse
import difflib
import io
import json
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from rich.console import Console  # noqa: E402

from pmcopy.utils import logger as logger_module  # noqa: E402
from pmcopy.utils.logger import (  # noqa: E402
    APP_NAME,
    DEFAULT_BACKUP_COUNT,
    DEFAULT_LOG_SUBDIR,
    DEFAULT_MAX_BYTES,
    JsonFormatter,
    RichConsoleHandler,
    configure_logging,
    log_order_lifecycle,
    log_position_drift,
    log_risk_exit,
    log_trade_decision,
    log_trade_detected,
    log_trade_execution,
    log_trade_skip,
    log_trade_submitted,
    print_monitor_start,
    print_new_trade_detected,
    print_order_skip,
    print_service_start,
    print_skip_reason,
    print_startup_banner,
    print_status_checks,
    print_trade_result,
    redact_error_detail,
    shorten_addr,
)

EVENT_NAMES = (
    "startup",
    "detection_event",
    "sizing_decision",
    "skip_decision",
    "submit",
    "ambiguous_order",
    "fill",
    "cancellation",
    "recovery",
    "position_drift",
    "protective_exit",
    "shutdown",
)

# Fixed synthetic identifiers (obviously fake; NOT credentials).
LEADER_WALLET = "0x1111111111111111111111111111111111111111"
USER_WALLET = "0x2222222222222222222222222222222222222222"
CONDITION_ID = "0x3333333333333333333333333333333333333333333333333333333333333333"
TOKEN_ID = "0x4444444444444444444444444444444444444444444444444444444444444444"
TX_HASH = "0x" + "ab" * 32
LEADER_TX_HASH = "0x" + "5a" * 32
ORDER_ID = "0x" + "cd" * 32
RESERVATION_ID = "res-golden-000001"
USER_ID = "user_golden"
LEADER_ID = "leader_golden"
EVENT_SLUG = "synthetic-golden-event"
MARKET_TITLE = "Synthetic Golden Market"
SUBMARKET = "Golden Submarket"
OUTCOME = "YES"

CONSOLE_WIDTH = 100
FROZEN_CLOCK_TEXT = "08:00:00 AM"


class _FrozenClock:
    """Stand-in for ``time`` inside utils.logger with fixed strftime output."""

    def strftime(self, fmt: str, *args: object, **kwargs: object) -> str:
        if fmt == "%I:%M:%S %p":
            return FROZEN_CLOCK_TEXT
        return "00:00:00"

    def time(self) -> float:  # pragma: no cover — heartbeats unused in fixtures
        return 0.0

    def monotonic(self) -> float:  # pragma: no cover — not used by fixtures
        return 0.0


def _normalize_console_line(line: str) -> str:
    return line.replace(f"[{FROZEN_CLOCK_TEXT}]", "[<TS>]")


def _normalize_jsonl_line(line: str) -> str:
    payload = json.loads(line)
    payload["ts"] = "<TS>"
    payload["module"] = "<module>"
    payload["funcName"] = "<funcName>"
    payload["lineno"] = "<lineno>"
    return json.dumps(payload, ensure_ascii=False)


def _build_startup_health_report():
    """Construct a synthetic StartupHealthReport via the production dataclasses."""
    from pmcopy.orchestrators.startup_health import (
        OrderStartupCleanupResult,
        PositionSyncResult,
        StartupHealthCheckItem,
        StartupHealthReport,
    )

    return StartupHealthReport(
        healthy=True,
        status="healthy",
        timestamp=1728614400,
        wallet=USER_WALLET,
        checks={
            "rpc": StartupHealthCheckItem(
                status="ok",
                message="RPC endpoint responding",
                details={"block_number": 65000000},
            ),
            "polymarket_api": StartupHealthCheckItem(
                status="ok",
                message="Data API responding",
                details={},
            ),
            "database": StartupHealthCheckItem(
                status="ok",
                message="Postgres reachable",
                details={},
            ),
        },
        position_sync=PositionSyncResult(
            persisted_before=2,
            persisted_after=2,
            live_count=2,
            pruned_positions=0,
            pruned_condition_ids=(),
            pruned_token_ids=(),
            fetch_failed=False,
            deferred_positions=0,
        ),
        order_status_counts={"submitted_unconfirmed": 1, "filled": 1},
        order_summary={"pending": 0, "terminal": 2},
        order_cleanup=OrderStartupCleanupResult(
            normalized_pending=0,
            normalized_delayed=0,
            normalized_retry_pending=0,
            normalized_total=0,
            purged_failed=0,
        ),
    )


def _startup_summary_payload() -> dict:
    """Mirror the production startup_summary extra payload (runtime_summary.py)."""
    return {
        "type": "startup_summary",
        "wallet": shorten_addr(USER_WALLET, 8, 4),
        "activeLeadersCount": 1,
        "availableCash": 250.0,
        "totalPortfolio": 325.0,
        "openPositionCount": 1,
        "settledPositionCount": 0,
        "livePositions": [
            {
                "title": MARKET_TITLE,
                "symbol": "SYN-GOLD",
                "outcome": OUTCOME,
                "size": 136.36,
                "value": 75.0,
            }
        ],
        "engineConfig": {"sizing": {"strategy": "proportional", "fraction": "0.10"}},
        "health": _build_startup_health_report().as_log_payload(),
    }


# ---------------------------------------------------------------------------
# Per-event drivers. Each drives ONLY public utils.logger helpers (plus the
# production dataclass payload builders) with synthetic events whose message
# shapes mirror the verified production call sites named in the docstrings.
# ---------------------------------------------------------------------------


def drive_startup(log) -> None:
    """app.py startup_stage / startup_health.py report / runtime_summary.py summary."""
    print_startup_banner()
    print_status_checks(
        rpc_ok=True, balance=250.0, api_ok=True, database_ok=True
    )
    print_service_start(monitor=True, executor=True, trader_count=2)
    print_monitor_start(trader_count=2, interval_s=30)

    log.info("startup_stage | stage=startup_health_started user=%s", USER_ID)
    report = _build_startup_health_report()
    log.info(
        "STARTUP HEALTH | status=%s | wallet=%s | live=%d | "
        "persisted_before=%d | persisted_after=%d | pruned=%d",
        report.status,
        report.wallet,
        report.position_sync.live_count,
        report.position_sync.persisted_before,
        report.position_sync.persisted_after,
        report.position_sync.pruned_positions,
        extra=report.as_log_payload(),
    )
    log.info(
        "STARTUP SUMMARY | wallet=%s | leaders=%d | cash=%.2f | total=%.2f | "
        "open_positions=%d | settled_holdings=%d",
        shorten_addr(USER_WALLET, 8, 4),
        1,
        250.0,
        325.0,
        1,
        0,
        extra=_startup_summary_payload(),
    )


def drive_detection_event(log) -> None:
    """services/tradeMonitor.py::_log_detected_trade (panel + JSONL record)."""
    trade = {
        "proxyWallet": LEADER_WALLET,
        "side": "BUY",
        "title": MARKET_TITLE,
        "submarket": SUBMARKET,
        "outcome": OUTCOME,
        "usdcSize": 120.0,
        "size": 150.0,
        "price": 0.8,
        "conditionId": CONDITION_ID,
        "asset": TOKEN_ID,
        "eventSlug": EVENT_SLUG,
        "transactionHash": TX_HASH,
        "_aggregatedCount": 2,
    }
    print_new_trade_detected(trade)
    log_trade_detected(
        log,
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=150.0,
        price=0.8,
        usdc_value=120.0,
        tx_hash=TX_HASH,
        event_slug=EVENT_SLUG,
        meta={
            "leaderWallet": LEADER_WALLET,
            "proxyWallet": LEADER_WALLET,
            "title": MARKET_TITLE,
            "submarket": SUBMARKET,
            "outcome": OUTCOME,
            "aggregatedCount": 2,
            "txHashes": [TX_HASH],
        },
    )


def drive_sizing_decision(log) -> None:
    """orchestrators/trade_pipeline.py sizing_audit / sizing_final / decision.

    sizing_audit, sizing_final and trade_decision are JSONL-only types: the
    console fixture for this event is intentionally EMPTY — that emptiness is
    part of the routing contract.
    """
    log.info(
        "sizing_audit | cond=%s leader=%s leader_notional=%.2f strategy=%s "
        "sizing_target=%.4f policy_enabled=%s policy_cap=%s policy_binding=%s "
        "max_for_decision=%.4f | %s",
        CONDITION_ID[:12],
        LEADER_ID,
        120.0,
        "proportional",
        75.0,
        True,
        "50.0000[daily_cap]",
        False,
        75.0,
        "proportional fraction of leader notional",
        extra={
            "type": "sizing_audit",
            "condition_id": CONDITION_ID,
            "leader_id": LEADER_ID,
            "leader_notional": 120.0,
            "strategy": "proportional",
            "sizing_target": 75.0,
            "policy_enabled": True,
            "policy_cap": 50.0,
            "policy_binding": False,
            "max_for_decision": 75.0,
        },
    )
    log.info(
        "sizing_final | cond=%s decision_gate adjusted %.4f -> %.4f (trust_mult=%.3f)",
        CONDITION_ID[:12],
        75.0,
        60.0,
        0.800,
        extra={
            "type": "sizing_final",
            "condition_id": CONDITION_ID,
            "leader_id": LEADER_ID,
            "original_notional": 75.0,
            "gated_notional": 60.0,
        },
    )
    log_trade_decision(
        log,
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        decision_score=0.8125,
        allow=True,
        reason="score_above_threshold",
        event_slug=EVENT_SLUG,
        meta={
            "user_id": USER_ID,
            "leader_id": LEADER_ID,
            "leader_score": 0.9,
            "trade_score": 0.8125,
            "price_drift_pct": 1.2,
            "edge_decay_pct": 0.5,
            "leader_notional": 120.0,
            "tx_hash": TX_HASH,
        },
    )


def drive_skip_decision(log) -> None:
    """trade_pipeline.py decision-gate skip (print_skip_reason + log_trade_skip)
    and execution-minimums skip (print_order_skip)."""
    print_skip_reason(
        "Decision gate rejected BUY", "signal age 91.0s exceeds max_age_s=90.0"
    )
    print_order_skip(
        side="BUY",
        reason="risk_cap_too_low_for_min_order",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        amount=1.2,
        price=0.55,
        detail="min order 1.00 USDC exceeds cap 0.80 USDC",
    )
    log_trade_skip(
        log,
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=2.0,
        price=0.55,
        usdc_value=1.1,
        skip_code="signal_too_old",
        reason="signal age 91.0s exceeds max_age_s=90.0",
        event_slug=EVENT_SLUG,
        meta={"user_id": USER_ID, "leader_id": LEADER_ID},
    )
    log_trade_skip(
        log,
        side="SELL",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=2.0,
        price=0.55,
        usdc_value=1.1,
        skip_code="no_local_position",
        reason="no local position to reduce",
        event_slug=EVENT_SLUG,
        meta={"user_id": USER_ID, "leader_id": LEADER_ID},
    )


def drive_submit(log) -> None:
    """trade_pipeline.py::log_trade_submitted after coordinator accept."""
    log_trade_submitted(
        log,
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        event_slug=EVENT_SLUG,
        meta={
            "user_id": USER_ID,
            "leader_id": LEADER_ID,
            "submissionState": "submitted_unconfirmed",
            "copyOrderId": ORDER_ID,
        },
    )


def drive_ambiguous_order(log) -> None:
    """executionService.py::_emit_post_lifecycle (submitted_unconfirmed) and
    reconciliation_service.py ambiguous_timeout transition."""
    log_order_lifecycle(
        log,
        status="submitted_unconfirmed",
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        venue_status="processed",
        submission_state="submitted_unconfirmed",
        signal_id=TX_HASH,
        meta={
            "sizeEstimated": True,
            "requestAmount": "75",
            "requestAmountUnit": "usdc",
        },
    )
    log_order_lifecycle(
        log,
        status="ambiguous_timeout",
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        venue_status="matched",
        signal_id=TX_HASH,
    )


def drive_fill(log) -> None:
    """reconciliation_service.py matched/confirmed lifecycle + trade_pipeline
    log_trade_execution + print_trade_result."""
    log_order_lifecycle(
        log,
        status="matched",
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        venue_status="matched",
        signal_id=TX_HASH,
    )
    log_order_lifecycle(
        log,
        status="confirmed",
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        venue_status="confirmed",
        signal_id=TX_HASH,
        tx_hash=TX_HASH,
    )
    log_trade_execution(
        log,
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        tx_hash=TX_HASH,
        event_slug=EVENT_SLUG,
        meta={
            "user_id": USER_ID,
            "leader_id": LEADER_ID,
            "copyOrderId": ORDER_ID,
            "order_id": ORDER_ID,
            "fee_paid": 0.0,
            "status": "filled",
            "title": MARKET_TITLE,
            "submarket": SUBMARKET,
            "outcome": OUTCOME,
        },
    )
    print_trade_result(
        success=True,
        side="BUY",
        leader_tx=LEADER_TX_HASH,
        copy_tx=TX_HASH,
        amount=75.0,
        price=0.55,
        title=MARKET_TITLE,
        submarket=SUBMARKET,
        outcome=OUTCOME,
    )


def drive_cancellation(log) -> None:
    """reconciliation_service.py _VENUE_TO_LIFECYCLE cancelled mapping."""
    log_order_lifecycle(
        log,
        status="cancelled",
        side="SELL",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.48,
        usdc_value=65.45,
        venue_status="order_status_canceled",
        signal_id=TX_HASH,
    )


def drive_recovery(log) -> None:
    """coordinator reservation lifecycle lines + reconciliation_service
    auto-release after ambiguous timeout."""
    log.info(
        "coordinator.reservation_pending reservation_id=%s status=%s — "
        "awaiting reconciliation",
        RESERVATION_ID,
        "submitted_unconfirmed",
    )
    log_order_lifecycle(
        log,
        status="auto_released_ambiguous",
        side="BUY",
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        order_id=ORDER_ID,
        size=136.36,
        price=0.55,
        usdc_value=75.0,
        venue_status="ambiguous_timeout",
        signal_id=TX_HASH,
    )
    log.info(
        "coordinator.reservation_released reservation_id=%s reason=%s",
        RESERVATION_ID,
        "cancelled",
    )


def drive_position_drift(log) -> None:
    """services/position_drift_service.py::_emit (clean + pause_scope drift)."""
    log_position_drift(
        log,
        status="clean",
        action="no_action",
        user_id=USER_ID,
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        local_size=136.36,
        venue_size=136.36,
        drift_size=0.0,
        reason="sizes agree within tolerance",
        reservation_ids=[],
    )
    log_position_drift(
        log,
        status="size_mismatch_positive",
        action="pause_scope",
        user_id=USER_ID,
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        local_size=100.0,
        venue_size=136.36,
        drift_size=36.36,
        reason="venue size exceeds local ledger by tolerance",
        reservation_ids=[RESERVATION_ID],
    )


def drive_protective_exit(log) -> None:
    """utils.logger.log_risk_exit (type=risk_exit) for a protective SELL."""
    log_risk_exit(
        log,
        condition_id=CONDITION_ID,
        asset=TOKEN_ID,
        size=136.36,
        price=0.48,
        usdc_value=65.45,
        reason="protective_stop_loss",
        event_slug=EVENT_SLUG,
        meta={"user_id": USER_ID, "leader_id": LEADER_ID},
    )


def drive_shutdown(log) -> None:
    """app.py::_shutdown_runtime / ::main teardown warning shapes."""
    log.warning(
        "onchain_runtime_shutdown_failed",
        extra={"error": redact_error_detail(RuntimeError("synthetic stop failure"))},
    )
    log.warning(
        "user_data_runtime_shutdown_failed",
        extra={"error": redact_error_detail(TimeoutError())},
    )
    log.warning(
        "failed_to_close_http_session",
        extra={"error": redact_error_detail(OSError("synthetic close failure"))},
    )
    log.warning(
        "db_pool_shutdown_failed",
        extra={"error": redact_error_detail(RuntimeError("synthetic pool close"))},
    )


DRIVERS = {
    "startup": drive_startup,
    "detection_event": drive_detection_event,
    "sizing_decision": drive_sizing_decision,
    "skip_decision": drive_skip_decision,
    "submit": drive_submit,
    "ambiguous_order": drive_ambiguous_order,
    "fill": drive_fill,
    "cancellation": drive_cancellation,
    "recovery": drive_recovery,
    "position_drift": drive_position_drift,
    "protective_exit": drive_protective_exit,
    "shutdown": drive_shutdown,
}


def _semantic_handler_class(handler) -> str:
    """Return the well-known base class name for a managed handler."""
    well_known = {"RichConsoleHandler", "RotatingFileHandler", "StreamHandler"}
    for cls in type(handler).__mro__:
        if cls.__name__ in well_known:
            return cls.__name__
    return type(handler).__name__


def _build_sink_contract() -> dict:
    """Introspect the configured logging sinks for the rotation/routing contract."""
    with tempfile.TemporaryDirectory(prefix="golden-sink-") as tmp:
        tmp_dir = Path(tmp)
        log = configure_logging(log_dir=tmp_dir, level="INFO")
        console_handler = next(
            h for h in log.handlers if isinstance(h, RichConsoleHandler)
        )
        file_handler = next(
            h
            for h in log.handlers
            if not isinstance(h, RichConsoleHandler)
        )
        for handler in log.handlers:
            handler.close()
        log.handlers.clear()
        return {
            "app_name": APP_NAME,
            "propagate": False,
            "defaults": {
                "log_subdir": DEFAULT_LOG_SUBDIR,
                "max_bytes": DEFAULT_MAX_BYTES,
                "backup_count": DEFAULT_BACKUP_COUNT,
            },
            "console_sink": {
                "handler_class": _semantic_handler_class(console_handler),
                "level": logging_level_name(console_handler.level),
                "formatter": "%(message)s",
            },
            "jsonl_sink": {
                "handler_class": _semantic_handler_class(file_handler),
                "level": logging_level_name(file_handler.level),
                "formatter_class": JsonFormatter.__name__,
                "max_bytes": DEFAULT_MAX_BYTES,
                "backup_count": DEFAULT_BACKUP_COUNT,
                "encoding": "utf-8",
                "filename": f"{APP_NAME}.jsonl",
            },
            "jsonl_only_types": sorted(RichConsoleHandler._JSONL_ONLY_TYPES),
            "jsonl_only_messages": sorted(RichConsoleHandler._JSONL_ONLY_MESSAGES),
        }


def logging_level_name(level: int) -> str:
    import logging

    return logging.getLevelName(level)


def capture_event(event_name: str, out_dir: Path) -> None:
    """Drive one lifecycle event and write its console + JSONL fixtures."""
    logger_module._ORDER_LIFECYCLE_LAST.clear()

    console_buf = io.StringIO()
    captured_console = Console(
        file=console_buf,
        width=CONSOLE_WIDTH,
        theme=logger_module._THEME,
        no_color=True,
        force_terminal=False,
        legacy_windows=False,
    )

    jsonl_path = out_dir / f"{event_name}.jsonl"
    # RotatingFileHandler opens in append mode — start each capture clean so
    # in-place regeneration never duplicates records.
    jsonl_path.unlink(missing_ok=True)
    original_console = logger_module.console
    original_time = logger_module.time
    logger_module.console = captured_console
    logger_module.time = _FrozenClock()
    try:
        log = configure_logging(
            level="INFO", log_dir=out_dir, jsonl_file=jsonl_path
        )
        DRIVERS[event_name](log)
        for handler in log.handlers:
            handler.flush()
            handler.close()
        log.handlers.clear()
    finally:
        logger_module.console = original_console
        logger_module.time = original_time

    console_text = "".join(
        _normalize_console_line(line) + "\n"
        for line in console_buf.getvalue().splitlines()
    )
    (out_dir / f"{event_name}.console.txt").write_text(
        console_text, encoding="utf-8"
    )

    jsonl_lines = [
        _normalize_jsonl_line(line)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    jsonl_path.write_text(
        "\n".join(jsonl_lines) + ("\n" if jsonl_lines else ""), encoding="utf-8"
    )


def regenerate(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    contract = _build_sink_contract()
    (output_dir / "sink_contract.json").write_text(
        json.dumps(contract, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    for event_name in EVENT_NAMES:
        capture_event(event_name, output_dir)
        console = (output_dir / f"{event_name}.console.txt").read_text(
            encoding="utf-8"
        )
        jsonl = (output_dir / f"{event_name}.jsonl").read_text(encoding="utf-8")
        print(
            f"captured {event_name}: console={len(console)} chars, "
            f"jsonl={len(jsonl.splitlines())} records"
        )


def check(fixtures_dir: Path) -> int:
    with tempfile.TemporaryDirectory(prefix="golden-check-") as tmp:
        generated = Path(tmp)
        regenerate(generated)
        non_fixture_names = {"README.md", "regenerate.py", "__pycache__"}
        mismatches = []
        expected = sorted(
            p.name
            for p in fixtures_dir.iterdir()
            if p.name not in non_fixture_names and p.suffix != ".pyc"
        )
        produced = sorted(p.name for p in generated.iterdir())
        for name in sorted(set(expected) | set(produced)):
            fixture_file = fixtures_dir / name
            generated_file = generated / name
            if not fixture_file.exists():
                mismatches.append(name)
                print(f"MISSING FIXTURE: {name}")
                continue
            if not generated_file.exists():
                mismatches.append(name)
                print(f"EXTRA FIXTURE (no longer generated): {name}")
                continue
            fixture_text = fixture_file.read_text(encoding="utf-8")
            generated_text = generated_file.read_text(encoding="utf-8")
            if fixture_text == generated_text:
                continue
            mismatches.append(name)
            diff = difflib.unified_diff(
                fixture_text.splitlines(),
                generated_text.splitlines(),
                fromfile=f"fixture/{name}",
                tofile=f"regenerated/{name}",
                lineterm="",
            )
            print(f"DRIFT: {name}")
            for line in diff:
                print(f"  {line}")
        if mismatches:
            print(f"\n{len(mismatches)} fixture(s) drifted: {', '.join(mismatches)}")
            return 1
        print(f"\nAll {len(produced)} golden log fixtures match.")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Directory to write fixtures into (default: alongside this script)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Regenerate into a temp dir and diff against the committed fixtures",
    )
    args = parser.parse_args()

    if args.check:
        return check(Path(__file__).resolve().parent)
    regenerate(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
