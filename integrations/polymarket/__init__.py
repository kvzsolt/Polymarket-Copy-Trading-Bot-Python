"""CCXT-backed Polymarket venue integration exports."""

from integrations.polymarket.ccxt_prediction_client import (
    CcxtPredictionClient,
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
    create_ccxt_prediction_client,
)
from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
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

BUY = "BUY"
SELL = "SELL"

__all__ = [
    "BUY",
    "SELL",
    "CcxtPredictionClient",
    "CcxtPredictionConfigurationError",
    "CcxtPredictionGateway",
    "CcxtPredictionUnavailable",
    "create_ccxt_prediction_client",
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
