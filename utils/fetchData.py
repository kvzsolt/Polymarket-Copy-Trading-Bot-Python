from __future__ import annotations

import asyncio
import atexit
import warnings

import aiohttp

from network.http_client import AsyncHttpClient, HttpRetryPolicy
from utils.logger import get_logger, log_api_response

logger = get_logger("FetchData")

_session: aiohttp.ClientSession | None = None
_http_client: AsyncHttpClient | None = None


def _close_session_sync() -> None:
    """Best-effort shutdown hook for environments that do not call close_session."""
    global _session, _http_client
    session = _session
    if not session or session.closed:
        return

    close_coro = session.close()
    try:
        asyncio.run(close_coro)
    except Exception as exc:
        close_coro.close()
        # Do not raise during process shutdown, but make leaks observable.
        warnings.warn(f"Failed to close shared HTTP session during shutdown: {exc}")
    finally:
        _session = None
        _http_client = None


atexit.register(_close_session_sync)



def _get_session() -> aiohttp.ClientSession:
    """Shared aiohttp session for the process."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15, connect=5, sock_connect=5, sock_read=15),
            connector=aiohttp.TCPConnector(limit=100, keepalive_timeout=30),
            headers={"Accept": "application/json", "User-Agent": "polymarket-copy-bot/2.0"},
        )
    return _session



def _get_http_client() -> AsyncHttpClient:
    global _http_client
    session = _get_session()
    if (
        _http_client is None
        or _http_client.session.closed
        or _http_client.session is not session
    ):
        _http_client = AsyncHttpClient(
            session,
            retry_policy=HttpRetryPolicy(attempts=4, base_delay=0.25, max_delay=2.5, timeout_seconds=10.0),
            logger=logger,
        )
    return _http_client


async def fetch_data(url: str, *, silent: bool = False):
    """Fetch JSON from *url* with retries using true async IO.

    When ``silent`` is True, suppress the generic HTTP retry/response logs so
    the caller can emit a single higher-level error instead of log spam on
    every retry attempt.
    """
    try:
        data = await _get_http_client().get_json(url, silent=silent)
        if not silent:
            log_api_response(logger, method="GET", url=url, status=200, data=data)
        return data
    except Exception as exc:
        if not silent:
            log_api_response(logger, method="GET", url=url, status=0, error=str(exc))
        raise


async def close_session():
    global _session, _http_client
    if _session and not _session.closed:
        await _session.close()
    _session = None
    _http_client = None
