"""Optional CCXT order-book capacity probe for discovery scoring."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from integrations.falcon.models import MarketSnapshot, TradeSnapshot
from integrations.polymarket.ccxt_prediction_market_data import CcxtPredictionMarketData


@dataclass(frozen=True, slots=True)
class CcxtCapacityProbe:
    """Read public CCXT order-book depth without venue mutations."""

    market_data: CcxtPredictionMarketData
    slippage_bps: int = 150

    async def visible_capacity_usdc(
        self,
        trade: TradeSnapshot,
        market: MarketSnapshot,
    ) -> Decimal | None:
        """Return exact visible notional within the configured slippage bound."""
        token_id = market.token_for_outcome(trade.outcome)
        if not token_id:
            return None
        side = trade.side.upper()
        if side not in {"BUY", "SELL"}:
            return None
        delta = Decimal(self.slippage_bps) / Decimal("10000")
        price_limit = (
            min(Decimal("0.9999"), trade.price * (Decimal("1") + delta))
            if side == "BUY"
            else max(Decimal("0.0001"), trade.price * (Decimal("1") - delta))
        )
        capacity = await self.market_data.visible_capacity(
            token_id,
            side=side,
            price_limit=price_limit,
        )
        return capacity.notional_usdc


__all__ = ["CcxtCapacityProbe"]
