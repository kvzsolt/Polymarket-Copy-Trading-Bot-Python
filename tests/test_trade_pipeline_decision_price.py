"""Copied-price invariant for copy decision sizing and execution."""

from decimal import Decimal
from types import SimpleNamespace

from pmcopy.orchestrators.trade_pipeline import _decision_live_price


def _sig(side: str, price: str) -> SimpleNamespace:
    return SimpleNamespace(side=side, price=Decimal(price))


def _bundle(best_bid=None, best_ask=None) -> SimpleNamespace:
    return SimpleNamespace(quote=SimpleNamespace(best_bid=best_bid, best_ask=best_ask))


def test_buy_uses_copied_price_even_when_best_ask_moved() -> None:
    sig = _sig("BUY", "0.50")
    bundle = _bundle(best_bid=Decimal("0.60"), best_ask=Decimal("0.62"))
    assert _decision_live_price(sig, bundle) == 0.50


def test_sell_uses_copied_price_even_when_best_bid_moved() -> None:
    sig = _sig("SELL", "0.50")
    bundle = _bundle(best_bid=Decimal("0.60"), best_ask=Decimal("0.62"))
    assert _decision_live_price(sig, bundle) == 0.50


def test_uses_copied_price_without_quote() -> None:
    sig = _sig("BUY", "0.50")
    assert _decision_live_price(sig, None) == 0.50
    assert _decision_live_price(sig, SimpleNamespace(quote=None)) == 0.50


def test_ignores_nonpositive_book_price() -> None:
    sig = _sig("BUY", "0.50")
    assert _decision_live_price(sig, _bundle(best_ask=Decimal("0"))) == 0.50


def test_book_movement_does_not_create_a_local_price_drift_rejection() -> None:
    sig = _sig("BUY", "0.50")
    live = _decision_live_price(sig, _bundle(best_ask=Decimal("0.55")))
    drift = (live - float(sig.price)) / float(sig.price)
    assert drift == 0.0
