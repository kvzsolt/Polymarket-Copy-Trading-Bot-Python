"""Deterministic depth, cost, and closed-episode paper contracts."""

from datetime import UTC, datetime
from decimal import Decimal as D

import pytest

from domain.paper_evaluation import PaperBook, PaperFill, PaperLedger, PaperSimulation

NOW = datetime(2026, 9, 16, tzinfo=UTC)


def test_depth_limits_partial_fills_and_fees_are_exact():
    book = PaperBook(
        asks=((D(".5025"), D("2")), (D(".505"), D("3"))),
        bids=((D(".50"), D("4")),),
        tick=D(".0025"),
        minimum=D("1"),
        fee_rate=D(".05"),
    )
    fill = PaperSimulation().fill(book, side="BUY", size=D("10"), limit=D(".5025"))
    assert fill.shares == D("2")
    assert fill.notional == D("1.005")
    assert fill.fee == D(".02500")
    assert PaperSimulation().fill(book, side="SELL", size=D("9"), limit=D(".5")).shares == D("4")


def test_missing_metadata_or_off_tick_limits_cannot_be_simulated():
    with pytest.raises(ValueError):
        PaperBook(asks=(), bids=(), tick=D("0"), minimum=D("1"), fee_rate=D("0"))
    book = PaperBook(
        asks=((D(".5025"), D("2")),), bids=(), tick=D(".0025"), minimum=D("1"), fee_rate=D("0")
    )
    with pytest.raises(ValueError):
        PaperSimulation().fill(book, side="BUY", size=D("1"), limit=D(".501"))


def test_buy_cost_is_bounded_and_tiny_dust_is_not_an_episode():
    book = PaperBook(
        asks=((D(".5"), D("100")),), bids=(), tick=D(".005"), minimum=D("1"), fee_rate=D(".05")
    )
    fill = PaperSimulation().fill(book, side="BUY", size=D("1"), limit=D(".5"))
    assert fill.notional + fill.fee <= D("1")
    ledger = PaperLedger.start(D("100"), NOW)
    ledger = ledger.apply(token="t", condition="c", side="BUY", fill=fill, event="buy")
    assert ledger.cash == D("100") - fill.notional - fill.fee
    entry_cost = ledger.positions[0].gross_cost
    entry_shares = ledger.positions[0].shares
    partial = PaperFill(shares=D(".5"), notional=D(".30"), fee=D(".006"))
    ledger = ledger.apply(token="t", condition="c", side="SELL", fill=partial, event="partial")
    assert ledger.closed_episodes == 0
    assert ledger.positions[0].gross_cost == entry_cost * (entry_shares - D(".5")) / entry_shares
    rest = ledger.positions[0].shares
    ledger = ledger.apply(
        token="t",
        condition="c",
        side="SELL",
        fill=PaperFill(rest, rest * D(".6"), D(".01")),
        event="close",
    )
    assert ledger.closed_episodes == 1
    assert ledger.realized_pnl == ledger.cash - D("100")
    assert ledger.performance(NOW, "eval").watermark == "paper:close"
    assert not ledger.positions


def test_unowned_sell_and_negative_cash_fail_closed():
    ledger = PaperLedger.start(D("1"), NOW)
    with pytest.raises(ValueError):
        ledger.apply(
            token="t",
            condition="c",
            side="SELL",
            fill=PaperFill(D("1"), D(".5"), D("0")),
            event="sell",
        )
    with pytest.raises(ValueError):
        ledger.apply(
            token="t",
            condition="c",
            side="BUY",
            fill=PaperFill(D("3"), D("1.5"), D("0")),
            event="buy",
        )


def test_drawdown_requires_complete_marked_inventory_and_preserves_losses():
    ledger = PaperLedger.start(D("100"), NOW).apply(
        token="t", condition="c", side="BUY", fill=PaperFill(D("10"), D("5"), D("0")), event="buy"
    )
    assert not ledger.performance(NOW, "unknown").complete
    ledger = ledger.mark({"t": D(".2")})
    assert ledger.max_drawdown == D(".03")
    assert ledger.equity == D("97")
    assert ledger.performance(NOW, "known").complete
    assert not ledger.mark({}).performance(NOW, "missing").complete
