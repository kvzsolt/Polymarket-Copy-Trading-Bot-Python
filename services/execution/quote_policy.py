"""Quote/spread validation policy before order submission.

Evaluates whether the order book spread is tight enough to justify
execution, complementing the existing ``services.execution.book_spread``
module which calculates spread metrics but does not gate execution.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class QuoteQuality:
    """Assessment of order book spread quality for execution gating."""

    acceptable: bool
    reason: str
    spread_bps: int = 0
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


def evaluate_quote_quality(
    *,
    best_bid: Decimal | None = None,
    best_ask: Decimal | None = None,
    max_spread_bps: int = 1000,
    min_liquidity_usdc: Decimal = Decimal("0"),
    bid_size: Decimal | None = None,
    ask_size: Decimal | None = None,
) -> QuoteQuality:
    """Evaluate whether the order book spread is acceptable for execution.

    Parameters
    ----------
    best_bid:
        Best bid price from the order book.
    best_ask:
        Best ask price from the order book.
    max_spread_bps:
        Maximum allowed spread in basis points.
    min_liquidity_usdc:
        Minimum liquidity required on the relevant side (not yet enforced;
        reserved for future depth checks).
    bid_size:
        Size at best bid (reserved for future depth checks).
    ask_size:
        Size at best ask (reserved for future depth checks).

    Returns
    -------
    QuoteQuality
        Whether the spread is acceptable and diagnostic metadata.
    """
    if best_bid is None or best_ask is None:
        return QuoteQuality(
            acceptable=False,
            reason="no_quotes",
            spread_bps=0,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    if best_bid <= 0 or best_ask <= 0:
        return QuoteQuality(
            acceptable=False,
            reason="invalid_quotes",
            best_bid=best_bid,
            best_ask=best_ask,
        )

    mid = (best_bid + best_ask) / Decimal("2")
    if mid <= 0:
        return QuoteQuality(
            acceptable=False,
            reason="invalid_mid_price",
            best_bid=best_bid,
            best_ask=best_ask,
        )

    spread = best_ask - best_bid
    spread_bps = int((spread / mid) * 10000)

    if spread_bps > max_spread_bps:
        return QuoteQuality(
            acceptable=False,
            reason="spread_too_wide",
            spread_bps=spread_bps,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    return QuoteQuality(
        acceptable=True,
        reason="ok",
        spread_bps=spread_bps,
        best_bid=best_bid,
        best_ask=best_ask,
    )
