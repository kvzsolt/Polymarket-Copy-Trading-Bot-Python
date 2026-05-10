"""Exit policy — evaluates whether a position should be exited.

Provides a standalone, stateless exit evaluator that mirrors the core
logic in ``services.positionManager`` but can be used independently
for pre-trade risk checks and monitoring hooks.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class ExitDecision:
    """Result of evaluating whether a position should be exited."""

    should_exit: bool
    reason: str
    size_fraction: float = 1.0


def evaluate_exit(
    entry_price: Decimal,
    current_price: Decimal,
    *,
    stop_loss_pct: float = 0.0,
    take_profit_pct: float = 0.0,
    trailing_stop_pct: float = 0.0,
    trailing_high: Decimal | None = None,
) -> ExitDecision:
    """Evaluate whether a position should be exited based on price movement.

    Parameters
    ----------
    entry_price:
        The position's average entry price.
    current_price:
        The current market price.
    stop_loss_pct:
        If positive, trigger exit when PnL falls to or below ``-stop_loss_pct``.
    take_profit_pct:
        If positive, trigger exit when PnL reaches ``take_profit_pct``.
    trailing_stop_pct:
        If positive and ``trailing_high`` is provided, trigger exit when
        the price drops ``trailing_stop_pct`` from the trailing high.
    trailing_high:
        The highest observed price since position entry (for trailing stop).

    Returns
    -------
    ExitDecision
        Whether to exit, the reason, and what fraction to sell.
    """
    if entry_price <= 0 or current_price <= 0:
        return ExitDecision(should_exit=False, reason="invalid_prices")

    pnl_pct = float((current_price - entry_price) / entry_price)

    # Stop loss
    if stop_loss_pct > 0 and pnl_pct <= -stop_loss_pct:
        return ExitDecision(should_exit=True, reason="stop_loss", size_fraction=1.0)

    # Trailing stop
    if trailing_stop_pct > 0 and trailing_high is not None and trailing_high > 0:
        drop_from_high = float((trailing_high - current_price) / trailing_high)
        if drop_from_high >= trailing_stop_pct:
            return ExitDecision(should_exit=True, reason="trailing_stop", size_fraction=1.0)

    # Take profit
    if take_profit_pct > 0 and pnl_pct >= take_profit_pct:
        return ExitDecision(should_exit=True, reason="take_profit", size_fraction=1.0)

    return ExitDecision(should_exit=False, reason="ok")
