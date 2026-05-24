"""Tests for the new TRADERS YOU'RE COPYING + YOUR POSITIONS banner panels.

Captures the rich Console output via a StringIO console + asserts the
canonical sections appear with the expected wallet shortening, math,
and degraded-state markers.
"""

from __future__ import annotations

import json
from decimal import Decimal
from io import StringIO

import pytest
from rich.console import Console

from domain.discovery_models import PositionSnapshotRecord
from orchestrators import runtime_summary
from services.discovery.position_snapshot_service import SnapshotResult

TOK_A = "11111111111111111111111111111111111111111111111111111111111111111111111111111"
TOK_B = "22222222222222222222222222222222222222222222222222222222222222222222222222222"


@pytest.fixture
def capture_console(monkeypatch):
    """Patch the module-level console with a width-locked StringIO sink."""
    buf = StringIO()
    test_console = Console(file=buf, force_terminal=False, width=160, no_color=True)
    monkeypatch.setattr(runtime_summary, "console", test_console)
    return buf


def _snap(
    wallet: str,
    cond: str,
    *,
    size: str,
    avg: str,
    current_value: str,
    current_price: str,
    realized: str = "0",
    title: str | None = None,
    ts: int = 1700000000,
) -> PositionSnapshotRecord:
    return PositionSnapshotRecord(
        trader_wallet=wallet,
        condition_id=cond,
        token_id=TOK_A,
        size=Decimal(size),
        avg_price=Decimal(avg),
        current_value=Decimal(current_value),
        current_price=Decimal(current_price),
        realized_pnl=Decimal(realized),
        snapshot_ts=ts,
        raw_json=json.dumps({"title": title} if title else {}),
    )


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------


def test_safe_pct_zero_denom_returns_zero() -> None:
    assert runtime_summary._safe_pct(Decimal("5"), Decimal("0")) == Decimal("0")
    assert runtime_summary._safe_pct(Decimal("5"), None) == Decimal("0")  # type: ignore[arg-type]


def test_safe_pct_normal() -> None:
    assert runtime_summary._safe_pct(Decimal("10"), Decimal("100")) == Decimal("10")


def test_leader_aggregate_gain_math() -> None:
    snaps = [
        _snap("0xL", "c1", size="100", avg="0.40", current_value="65", current_price="0.65"),
        _snap("0xL", "c2", size="50", avg="0.70", current_value="30", current_price="0.60"),
    ]
    # invested = 100*0.40 + 50*0.70 = 75
    # current = 65 + 30 = 95
    # gain = (95 - 75) / 75 = 26.66...%
    invested, current, gain_pct = runtime_summary._leader_aggregate_gain(snaps)
    assert invested == Decimal("75")
    assert current == Decimal("95")
    assert gain_pct.quantize(Decimal("0.01")) == Decimal("26.67")


def test_leader_aggregate_gain_includes_realized_pnl() -> None:
    snaps = [
        _snap("0xL", "c", size="10", avg="0.5", current_value="5",
              current_price="0.5", realized="2"),
    ]
    # invested = 5, current = 5, realized = 2 → gain = (5+2-5)/5 = 40%
    _, _, gain_pct = runtime_summary._leader_aggregate_gain(snaps)
    assert gain_pct == Decimal("40")


# ---------------------------------------------------------------------------
# print_traders_youre_copying
# ---------------------------------------------------------------------------


def test_traders_panel_renders_canonical_header(capture_console) -> None:
    result = SnapshotResult(
        leader_snapshots={
            "0xleadera": [
                _snap("0xleadera", "c1", size="10", avg="0.5",
                      current_value="6", current_price="0.6",
                      title="Will the Fed cut?"),
            ],
        },
        own_snapshots=[],
        leaders_failed=[],
        own_failed=False,
    )
    runtime_summary.print_traders_youre_copying(result)
    out = capture_console.getvalue()
    assert "TRADERS YOU'RE COPYING" in out
    assert "Will the Fed cut?" in out
    assert "positions=1" in out
    # 6/5 - 1 = 20%
    assert "+20.00%" in out


def test_traders_panel_marks_failed_leader(capture_console) -> None:
    result = SnapshotResult(
        leader_snapshots={"0xbroken": []},
        own_snapshots=[],
        leaders_failed=["0xbroken"],
        own_failed=False,
    )
    runtime_summary.print_traders_youre_copying(result)
    out = capture_console.getvalue()
    assert "fetch failed" in out


def test_traders_panel_skipped_when_result_is_none(capture_console) -> None:
    runtime_summary.print_traders_youre_copying(None)
    assert capture_console.getvalue() == ""


def test_traders_panel_truncates_long_title(capture_console) -> None:
    long = "x" * 200
    result = SnapshotResult(
        leader_snapshots={
            "0xl": [_snap("0xl", "c", size="1", avg="0.5",
                          current_value="0.5", current_price="0.5", title=long)],
        },
        own_snapshots=[],
        leaders_failed=[],
        own_failed=False,
    )
    runtime_summary.print_traders_youre_copying(result)
    out = capture_console.getvalue()
    # Title is truncated to 42 chars; full long string must NOT appear verbatim.
    assert long not in out


def test_traders_panel_top_n_truncation(capture_console) -> None:
    snaps = [
        _snap("0xL", f"c{i}", size="10", avg="0.5",
              current_value=str(10 - i), current_price="0.5", title=f"M{i}")
        for i in range(8)
    ]
    result = SnapshotResult(
        leader_snapshots={"0xl": snaps},
        own_snapshots=[],
        leaders_failed=[],
        own_failed=False,
    )
    runtime_summary.print_traders_youre_copying(result, max_positions_per_leader=3)
    out = capture_console.getvalue()
    assert "+5 more" in out


# ---------------------------------------------------------------------------
# print_your_positions
# ---------------------------------------------------------------------------


def test_your_positions_panel_renders_aggregate_math(capture_console) -> None:
    result = SnapshotResult(
        leader_snapshots={},
        own_snapshots=[
            _snap("0xown", "c1", size="200", avg="0.30",
                  current_value="140", current_price="0.70", realized="5",
                  title="BTC>100k"),
            _snap("0xown", "c2", size="80", avg="0.60",
                  current_value="36", current_price="0.45",
                  title="ETH>5k"),
        ],
        leaders_failed=[],
        own_failed=False,
    )
    runtime_summary.print_your_positions(
        result, own_wallet="0xown", available_cash=1000.0, total_portfolio=1500.0
    )
    out = capture_console.getvalue()
    assert "YOUR POSITIONS" in out
    # invested = 60+48 = 108; current = 140+36 = 176; +5 realized → gain = 73/108 = 67.59%
    assert "+67.59%" in out
    assert "BTC>100k" in out
    assert "ETH>5k" in out
    assert "$1,000.00" in out
    assert "$1,500.00" in out


def test_your_positions_panel_marks_degraded_own_fetch(capture_console) -> None:
    result = SnapshotResult(
        leader_snapshots={},
        own_snapshots=[],
        leaders_failed=[],
        own_failed=True,
    )
    runtime_summary.print_your_positions(
        result, own_wallet="0xown", available_cash=100.0, total_portfolio=100.0
    )
    out = capture_console.getvalue()
    assert "snapshot fetch failed" in out


def test_your_positions_panel_no_positions(capture_console) -> None:
    result = SnapshotResult(
        leader_snapshots={},
        own_snapshots=[],
        leaders_failed=[],
        own_failed=False,
    )
    runtime_summary.print_your_positions(
        result, own_wallet="0xown", available_cash=0.0, total_portfolio=0.0
    )
    out = capture_console.getvalue()
    assert "No open positions" in out


def test_your_positions_panel_skipped_when_result_is_none(capture_console) -> None:
    runtime_summary.print_your_positions(
        None, own_wallet="0xown", available_cash=0.0, total_portfolio=0.0
    )
    assert capture_console.getvalue() == ""
