"""Venue-neutral Polymarket SDK and optional CCXT adapter exports."""

from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionClient,
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.clob_v2_gateway import ClobV2Gateway, PolymarketGateway
from integrations.polymarket.errors import (
    RetryableVenueError,
    TerminalVenueError,
    VenueAuthError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    OrderBook,
    OrderBookLevel,
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from integrations.polymarket.sdk_client import PolymarketClient, create_polymarket_client

BUY = "BUY"
SELL = "SELL"

__all__ = [
    "BUY",
    "SELL",
    "CcxtPredictionClient",
    "CcxtPredictionConfigurationError",
    "CcxtPredictionGateway",
    "CcxtPredictionUnavailable",
    "ClobV2Gateway",
    "PolymarketClient",
    "PolymarketGateway",
    "create_ccxt_prediction_client",
    "create_polymarket_client",
    "LimitOrderRequest",
    "MarketableOrderRequest",
    "OrderBook",
    "OrderBookLevel",
    "RetryableVenueError",
    "TerminalVenueError",
    "VenueAuthError",
    "VenueCancelResult",
    "VenueFill",
    "VenueInvalidOrderError",
    "VenueOrderNotFoundError",
    "VenueOrderState",
    "VenueRateLimitError",
    "VenueSubmitResult",
    "VenueUnavailableError",
]
