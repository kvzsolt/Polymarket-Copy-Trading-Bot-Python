"""Concrete client adapters implementing the ``domain.ports.clients`` ABCs.

Each adapter is constructor-injected — they accept their configuration
(``AppConfig`` section, HTTP session, SDK client) rather than reading
environment variables or resolving singletons. The composition root
(``bootstrap.py`` in Phase 5) is responsible for building them.
"""

from clients.analytics_client import AnalyticsClient
from clients.balance_client import BalanceClient
from clients.http_client import HttpClient
from clients.market_client import PolymarketClobClient
from clients.market_ws import MarketWebSocket
from clients.order_gateway import OrderGateway
from clients.user_ws import UserWebSocket

__all__ = [
    "AnalyticsClient",
    "BalanceClient",
    "HttpClient",
    "MarketWebSocket",
    "OrderGateway",
    "PolymarketClobClient",
    "UserWebSocket",
]

