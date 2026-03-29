import json
import queue
import threading
import urllib.request

import aiohttp
from utils.logger import get_logger, log_api_response

logger = get_logger("FetchData")

_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    """Shared session for modules that opt-in to connection reuse."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10, connect=5, sock_connect=5, sock_read=10),
            connector=aiohttp.TCPConnector(limit=20, keepalive_timeout=30),
        )
    return _session


async def fetch_data(url: str, *, silent: bool = False):
    """Fetch JSON from *url*. Always logs to JSONL; console stays silent."""
    result_q: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

    def _worker():
        try:
            request = urllib.request.Request(url=url, headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=10) as resp:
                status = int(getattr(resp, "status", resp.getcode()))
                payload = resp.read()
            data = json.loads(payload.decode("utf-8")) if payload else None
            result_q.put(("ok", (status, data)))
        except Exception as exc:
            result_q.put(("err", exc))

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(10.0)

    if t.is_alive():
        exc = TimeoutError(f"fetch timed out after 10s: {url}")
        log_api_response(logger, method="GET", url=url, status=0, error=str(exc))
        raise exc

    if result_q.empty():
        exc = RuntimeError(f"fetch failed without result: {url}")
        log_api_response(logger, method="GET", url=url, status=0, error=str(exc))
        raise exc

    kind, payload = result_q.get_nowait()
    if kind == "err":
        log_api_response(logger, method="GET", url=url, status=0, error=str(payload))
        raise payload  # type: ignore[misc]

    status, data = payload  # type: ignore[misc]
    log_api_response(logger, method="GET", url=url, status=int(status), data=data)
    return data


async def close_session():
    global _session
    if _session and not _session.closed:
        await _session.close()
    _session = None
