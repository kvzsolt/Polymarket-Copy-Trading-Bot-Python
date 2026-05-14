"""Client package compatibility exports.

Keep this module lightweight.

Do not eagerly import client implementations here. Some low-level imports, for
example ``clients.network.http_client``, initialize the ``clients`` package
first. If this module imports ``clients.market_client`` eagerly, it can pull in
``utils.createClobClient`` which imports ``utils.fetchData`` while
``utils.fetchData`` is still initializing.

Use lazy exports instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clients.analytics_client import AnalyticsClient
    from clients.balance_client import BalanceClient
    from clients.market_client import PolymarketClobClient
    from clients.market_ws import MarketWebSocket
    from clients.network.http_client import AsyncHttpClient, HttpRetryPolicy
    from clients.order_gateway import OrderGateway
    from clients.user_ws import UserWebSocket

__all__ = [
    "AnalyticsClient",
    "BalanceClient",
    "AsyncHttpClient",
    "HttpRetryPolicy",
    "MarketWebSocket",
    "OrderGateway",
    "PolymarketClobClient",
    "UserWebSocket",
]


def __getattr__(name: str) -> Any:
    if name == "AnalyticsClient":
        from clients.analytics_client import AnalyticsClient

        return AnalyticsClient

    if name == "BalanceClient":
        from clients.balance_client import BalanceClient

        return BalanceClient

    if name == "AsyncHttpClient":
        from clients.network.http_client import AsyncHttpClient

        return AsyncHttpClient

    if name == "HttpRetryPolicy":
        from clients.network.http_client import HttpRetryPolicy

        return HttpRetryPolicy

    if name == "MarketWebSocket":
        from clients.market_ws import MarketWebSocket

        return MarketWebSocket

    if name == "OrderGateway":
        from clients.order_gateway import OrderGateway

        return OrderGateway

    if name == "PolymarketClobClient":
        from clients.market_client import PolymarketClobClient

        return PolymarketClobClient

    if name == "UserWebSocket":
        from clients.user_ws import UserWebSocket

        return UserWebSocket

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")