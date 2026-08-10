# clients/__init__.py

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from clients.market_ws import MarketWebSocket
    from clients.network.http_client import AsyncHttpClient, HttpRetryPolicy
    from clients.user_ws import UserWebSocket

__all__ = [
    "AsyncHttpClient",
    "HttpRetryPolicy",
    "MarketWebSocket",
    "UserWebSocket",
]


def __getattr__(name: str) -> Any:
    if name == "AsyncHttpClient":
        from clients.network.http_client import AsyncHttpClient
        return AsyncHttpClient

    if name == "HttpRetryPolicy":
        from clients.network.http_client import HttpRetryPolicy
        return HttpRetryPolicy

    if name == "MarketWebSocket":
        from clients.market_ws import MarketWebSocket
        return MarketWebSocket

    if name == "UserWebSocket":
        from clients.user_ws import UserWebSocket
        return UserWebSocket

    raise AttributeError(f"module 'clients' has no attribute {name!r}")
