"""Read paper execution depth and explicit fee evidence through the CCXT adapter."""

import asyncio
from decimal import Decimal, InvalidOperation

from pmcopy.domain.paper_evaluation import PaperBook
from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from pmcopy.integrations.polymarket.errors import VenueInvalidOrderError


class CcxtPaperMarketReader:
    """Expose a narrow read-only book port with no private execution methods."""

    def __init__(self, gateway: CcxtPredictionGateway) -> None:
        """Reuse the already composed public CCXT transport."""
        self._gateway = gateway

    async def get_paper_fee_rate(self, token_id: str, *, condition_id: str) -> Decimal:
        """Require the current documented price-dependent taker fee curve.

        Unsupported exponents fail closed rather than treating CCXT's base-fee
        endpoint as a market-specific taker fee. Zero-fee markets still require
        explicit fee evidence.
        """
        payload = await self._gateway._fetch_clob_market_metadata(condition_id)
        self._gateway._validate_metadata_token(token_id, payload)
        fee = payload.get("fd")
        if not isinstance(fee, dict):
            raise VenueInvalidOrderError("paper evaluation requires explicit fee metadata")
        try:
            rate = Decimal(str(fee["r"]))
        except (KeyError, InvalidOperation, ValueError) as exc:
            raise VenueInvalidOrderError("invalid paper fee metadata") from exc
        if not rate.is_finite() or not 0 <= rate <= 1 or (rate and fee.get("e") != 1):
            raise VenueInvalidOrderError("unsupported paper fee curve")
        return rate

    async def book(self, token_id: str, condition_id: str) -> PaperBook:
        """Return fresh validated execution depth and costs from public reads."""
        book, rate = await asyncio.gather(
            self._gateway.get_orderbook(token_id),
            self.get_paper_fee_rate(token_id, condition_id=condition_id),
        )
        if not isinstance(book.neg_risk, bool) or book.token_id != token_id:
            raise VenueInvalidOrderError("invalid paper book identity or negative-risk metadata")
        return PaperBook(
            asks=tuple((level.price, level.size) for level in book.asks),
            bids=tuple((level.price, level.size) for level in book.bids),
            tick=book.tick_size,
            minimum=book.min_order_size,
            fee_rate=rate,
        )
