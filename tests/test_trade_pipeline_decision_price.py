"""P1-c regression: the copy-decision gate must score against the FRESH
executable quote, not the stale leader signal price.

``live_price = float(signal.price)`` made ``price_drift_pct`` /
``edge_decay_pct`` identically zero (the signal compared against itself) and
sized BUYs off the old price, so the protective drift gate was inert on every
copy. The decision price must come from the fresh bundle quote — best_ask for a
BUY, best_bid for a SELL — falling back to the signal price only when no fresh
quote is available (the degraded/REST path is gated separately upstream).
"""

from decimal import Decimal
from types import SimpleNamespace

from orchestrators.trade_pipeline import _decision_live_price


def _sig(side: str, price: str) -> SimpleNamespace:
    return SimpleNamespace(side=side, price=Decimal(price))


def _bundle(best_bid=None, best_ask=None) -> SimpleNamespace:
    return SimpleNamespace(quote=SimpleNamespace(best_bid=best_bid, best_ask=best_ask))


def test_buy_uses_fresh_best_ask():
    sig = _sig("BUY", "0.50")
    bundle = _bundle(best_bid=Decimal("0.60"), best_ask=Decimal("0.62"))
    assert _decision_live_price(sig, bundle) == 0.62


def test_sell_uses_fresh_best_bid():
    sig = _sig("SELL", "0.50")
    bundle = _bundle(best_bid=Decimal("0.60"), best_ask=Decimal("0.62"))
    assert _decision_live_price(sig, bundle) == 0.60


def test_falls_back_to_signal_price_without_quote():
    sig = _sig("BUY", "0.50")
    assert _decision_live_price(sig, None) == 0.50
    assert _decision_live_price(sig, SimpleNamespace(quote=None)) == 0.50


def test_falls_back_when_fresh_price_nonpositive():
    sig = _sig("BUY", "0.50")
    assert _decision_live_price(sig, _bundle(best_ask=Decimal("0"))) == 0.50


def test_drift_is_nonzero_when_quote_moved():
    # The whole point: with a fresh ask different from the signal price, a drift
    # gate computing (live - signal)/signal is no longer identically zero.
    sig = _sig("BUY", "0.50")
    live = _decision_live_price(sig, _bundle(best_ask=Decimal("0.55")))
    drift = (live - float(sig.price)) / float(sig.price)
    assert abs(drift) > 0.0
