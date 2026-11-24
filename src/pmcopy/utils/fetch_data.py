from __future__ import annotations

import aiohttp

from pmcopy.clients.network.http_client import AsyncHttpClient, HttpRetryPolicy
from pmcopy.utils.logger import get_logger, log_api_response

logger = get_logger("FetchData")

_session: aiohttp.ClientSession | None = None
_http_client: AsyncHttpClient | None = None

# aiohttp's default header-size limit is 8190 bytes. Polymarket currently
# serves a Content-Security-Policy header of ~8.2 KB on endpoints such as
# https://polymarket.com/api/geoblock, so the default makes aiohttp fail the
# response as a client-side HTTP 400 before the body is ever read. Keep a
# generous ceiling for venue responses while still bounding memory per header.
_AIOHTTP_HEADER_SIZE_LIMIT = 64 * 1024


# INF-04: the prior ``atexit.register(_close_session_sync)`` ran
# ``asyncio.run()`` during interpreter shutdown. Under pytest-asyncio
# teardown a loop is still alive, and ``asyncio.run`` on top of a
# running loop raises ``RuntimeError`` — the session then leaked. The
# proper close path is the explicit ``await close_session()`` invoked
# from app shutdown. If a CLI script forgets to call it the connector
# will surface a warning during GC; that visibility is preferable to a
# silent atexit failure.


def _get_session() -> aiohttp.ClientSession:
    """Shared aiohttp session for the process."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15, connect=5, sock_connect=5, sock_read=15),
            connector=aiohttp.TCPConnector(limit=100, keepalive_timeout=30),
            headers={"Accept": "application/json", "User-Agent": "polymarket-copy-bot/2.0"},
            max_line_size=_AIOHTTP_HEADER_SIZE_LIMIT,
            max_field_size=_AIOHTTP_HEADER_SIZE_LIMIT,
        )
    return _session


def _get_http_client() -> AsyncHttpClient:
    global _http_client
    session = _get_session()
    if _http_client is None or _http_client.session.closed or _http_client.session is not session:
        _http_client = AsyncHttpClient(
            session,
            retry_policy=HttpRetryPolicy(
                attempts=4, base_delay=0.25, max_delay=2.5, timeout_seconds=10.0
            ),
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
