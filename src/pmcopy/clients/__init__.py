# clients/__init__.py

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.clients.ws.market_ws import MarketWebSocket
    from pmcopy.clients.network.http_client import AsyncHttpClient, HttpRetryPolicy
    from pmcopy.clients.ws.user_ws import UserWebSocket

__all__ = [
    "AsyncHttpClient",
    "HttpRetryPolicy",
    "MarketWebSocket",
    "UserWebSocket",
]


def __getattr__(name: str) -> Any:
    if name == "AsyncHttpClient":
        from pmcopy.clients.network.http_client import AsyncHttpClient

        return AsyncHttpClient

    if name == "HttpRetryPolicy":
        from pmcopy.clients.network.http_client import HttpRetryPolicy

        return HttpRetryPolicy

    if name == "MarketWebSocket":
        from pmcopy.clients.ws.market_ws import MarketWebSocket

        return MarketWebSocket

    if name == "UserWebSocket":
        from pmcopy.clients.ws.user_ws import UserWebSocket

        return UserWebSocket

    raise AttributeError(f"module 'pmcopy.clients' has no attribute {name!r}")
