"""Bounded read-only JSON-RPC recovery, isolated from transaction submission."""

import asyncio
from collections.abc import Callable
from email.utils import parsedate_to_datetime
from typing import Protocol

import aiohttp

from pmcopy.domain.redemption import SettlementUnavailable
from pmcopy.interfaces.settlement import SettlementClock


class SettlementRpcTransport(Protocol):
    """Make exactly one HTTP attempt without logging endpoint credentials."""

    async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool) -> object:
        """Return decoded JSON or raise the original transport/status exception."""
        ...


class SettlementRpcHttp:
    """Use a warm session with one request and status validation before JSON parsing."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        """Inject the shared session; this transport neither closes nor retries it."""
        self._session = session

    async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool) -> object:
        """Read once without logging; the caller applies its remaining time budget."""
        async with self._session.request(
            "POST", url, json=json_body, timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            response.raise_for_status()
            return await response.json(content_type=None)


class _RetryableRpcError(Exception):
    """Carry only an allowlisted availability reason from a JSON-RPC envelope."""


class SettlementRpcClient:
    """Try two endpoint passes within 45 seconds, preserving every request parameter."""

    _BUDGET_SECONDS = 45
    _READ_METHODS = frozenset(
        {
            "eth_chainId",
            "eth_getCode",
            "eth_call",
            "eth_getBlockByNumber",
            "eth_getTransactionReceipt",
        }
    )

    def __init__(
        self,
        http: SettlementRpcTransport,
        urls: tuple[str, ...],
        clock: SettlementClock,
        jitter: Callable[[float, float], float],
    ) -> None:
        """Inject single-attempt I/O, time, and randomness for replayable recovery."""
        if not urls or not all(urls):
            raise ValueError("A Polygon RPC endpoint is required for redemption")
        self._http = http
        self._urls = tuple(dict.fromkeys(urls))
        self._clock = clock
        self._jitter = jitter
        self._request_id = 0

    async def read(self, method: str, params: list[object]) -> object:
        """Read once logically; retry only transport and allowlisted availability errors.

        Args:
            method: An allowlisted read-only Ethereum RPC method.
            params: Immutable-in-use arguments, including any pinned block tag.

        Returns:
            The result of an identity-checked JSON-RPC response.

        Raises:
            SettlementUnavailable: On invalid evidence, terminal errors, or exhaustion.
            ValueError: If asked to invoke a write or unsupported method.
        """
        if method not in self._READ_METHODS:
            raise ValueError("Settlement RPC is read-only")
        self._request_id += 1
        request_id = self._request_id
        body: dict[str, object] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        deadline = self._clock.monotonic() + self._BUDGET_SECONDS
        ready_at: dict[str, float] = {}
        context = f"method={method}"
        reason = "unavailable"
        for pass_index in range(2):
            for index, url in enumerate(self._urls):
                endpoint = "primary" if index == 0 else "fallback"
                context = f"method={method} endpoint={endpoint}"
                remaining = deadline - self._clock.monotonic()
                delay = max(0.0, ready_at.get(url, 0) - self._clock.monotonic())
                if delay >= remaining:
                    continue
                if delay:
                    await self._clock.sleep(delay)
                remaining = deadline - self._clock.monotonic()
                if remaining <= 0:
                    break
                retry_after = 0.0
                try:
                    async with asyncio.timeout(min(10, remaining)):
                        raw = await self._http.post_json(url, json_body=body, silent=True)
                    if self._clock.monotonic() >= deadline:
                        raise SettlementUnavailable(
                            f"settlement_rpc_unavailable {context} reason=budget"
                        )
                    return self._result(raw, request_id, context)
                except _RetryableRpcError as error:
                    reason = str(error)
                except aiohttp.ClientResponseError as error:
                    if error.status not in (408, 429) and not 500 <= error.status < 600:
                        raise SettlementUnavailable(
                            f"settlement_rpc_http_error {context} status={error.status}"
                        ) from None
                    reason = f"http_{error.status}"
                    retry_after = self._retry_after(error)
                except (TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientPayloadError):
                    reason = "transport_unavailable"
                except SettlementUnavailable:
                    raise
                except Exception:
                    raise SettlementUnavailable(
                        f"invalid_settlement_rpc_response {context}"
                    ) from None
                backoff = 0.5 * 2**pass_index
                ready_at[url] = self._clock.monotonic() + max(
                    retry_after, backoff + self._jitter(0, backoff * 0.2)
                )
        if (
            any(ready >= deadline for ready in ready_at.values())
            or self._clock.monotonic() >= deadline
        ):
            reason = "budget"
        raise SettlementUnavailable(
            f"settlement_rpc_unavailable {context} reason={reason}"
        ) from None

    @staticmethod
    def _result(raw: object, request_id: int, context: str) -> object:
        if (
            not isinstance(raw, dict)
            or raw.get("jsonrpc") != "2.0"
            or type(raw.get("id")) is not int
            or raw["id"] != request_id
            or ("result" in raw) == ("error" in raw)
        ):
            raise SettlementUnavailable(f"invalid_settlement_rpc_response {context}")
        if "error" in raw:
            error = raw["error"]
            code = error.get("code") if isinstance(error, dict) else None
            if type(code) is int and code in (-32603, -32002, -32005):
                raise _RetryableRpcError(f"rpc_{code}")
            raise SettlementUnavailable(f"settlement_rpc_rejected {context}")
        return raw["result"]

    def _retry_after(self, error: aiohttp.ClientResponseError) -> float:
        value = (error.headers or {}).get("Retry-After", "")
        if value.isascii() and value.isdecimal():
            return float(min(int(value), self._BUDGET_SECONDS + 1))
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is not None:
                return max(0.0, date.timestamp() - self._clock.now())
        except (ValueError, TypeError, OverflowError):
            pass
        return 0.0
