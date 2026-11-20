"""RPC provider with automatic fallback for transient failures.

Wraps ``Web3.HTTPProvider`` creation so that a primary + fallback RPC
endpoint can be tried in sequence when the primary returns connection-level
or internal-RPC errors (e.g. ``-32603``).

The wrapper is **read-call only** — it does not attempt to failover on
*transaction* submissions because nonce + state continuity matters there.
For reads (balance, code, gas price, etc.) a failover is safe because there
is no side-effect.

Usage::

    from clients.network.rpc_fallback import FallbackWeb3Provider

    provider = FallbackWeb3Provider(primary_url, fallback_url)
    w3 = provider.get_web3()
    # ... use w3 normally; if the primary fails, fallback is used transparently.

If ``fallback_url`` is empty or identical to ``primary_url`` the wrapper
degenerates to a single-provider pass-through.
"""

from __future__ import annotations

from typing import Any

from web3 import Web3
from web3.providers.base import JSONBaseProvider
from web3.types import RPCResponse

from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("rpc_fallback")

# Web3.py accepts request timeout through HTTPProvider request_kwargs. Keep RPC
# read calls bounded so startup balance probes cannot look like a hung boot.
#
# This provider is *read-call only* (see module docstring): every call site is
# a balance / allowance / code / gas-price probe, none of which legitimately
# runs for seconds on Polygon, so a bounded default is safe. The default is
# Callers on slow or archive endpoints may pass ``request_timeout_seconds``.
_FALLBACK_REQUEST_TIMEOUT_SECONDS = 8.0
_DEFAULT_REQUEST_TIMEOUT_SECONDS = _FALLBACK_REQUEST_TIMEOUT_SECONDS

# JSON-RPC error codes that justify a failover attempt.
# -32603 = internal error (generic node failure / overload)
_FAILOVER_CODES: frozenset[int] = frozenset({-32603})
# Exceptions whose *message* contains these substrings also trigger failover.
_FAILOVER_MSG_FRAGMENTS: tuple[str, ...] = (
    "internal error",
    "request timeout",
    "connection refused",
    "max retries exceeded",
    "temporary failure",
)


class _FailoverHTTPProvider(JSONBaseProvider):
    """Provider that routes to primary first, fallback on specific errors."""

    def __init__(
        self,
        primary_url: str,
        fallback_url: str | None = None,
        *,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__()
        self._request_timeout_seconds = float(request_timeout_seconds)
        request_kwargs = {"timeout": self._request_timeout_seconds}
        self._primary = Web3.HTTPProvider(primary_url, request_kwargs=request_kwargs)
        self._fallback: Web3.HTTPProvider | None = None
        if fallback_url and fallback_url.strip() and fallback_url.strip() != primary_url:
            self._fallback = Web3.HTTPProvider(
                fallback_url.strip(),
                request_kwargs=request_kwargs,
            )
            logger.info("RPC fallback configured")
        else:
            logger.debug("RPC fallback not configured (single provider)")

    def make_request(self, method: str, params: Any) -> RPCResponse:
        """Perform an RPC request through the bounded fallback policy."""
        primary_exc: Exception | None = None
        primary_error_response: RPCResponse | None = None

        # Try primary first.
        try:
            raw = self._primary.make_request(method, params)
            if not _is_failover_response(raw):
                return raw
            primary_error_response = raw
            # Primary returned a failover-worthy error code; log and continue.
            logger.warning(
                "Primary RPC returned failover-worthy error for %s: %s",
                method,
                redact_error_detail(raw.get("error")),
            )
        except Exception as exc:
            if _is_failover_exception(exc):
                logger.warning(
                    "Primary RPC failed for %s: %s",
                    method,
                    redact_error_detail(exc),
                )
                primary_exc = exc
            else:
                # Non-failover exception (e.g. invalid params) — raise immediately.
                raise

        # If we have no fallback, preserve the primary result shape: exceptions
        # are re-raised, JSON-RPC error responses flow back to Web3 for normal
        # error handling.
        if self._fallback is None:
            if primary_exc is not None:
                raise primary_exc
            if primary_error_response is not None:
                return primary_error_response
            raise RuntimeError(f"Primary RPC failed for {method} without fallback")

        # Try fallback.
        logger.info("Failing over to secondary RPC for %s", method)
        return self._fallback.make_request(method, params)


def _is_failover_response(raw: RPCResponse) -> bool:
    """Return True if a JSON-RPC response object justifies failover."""
    error = raw.get("error")
    if not isinstance(error, dict):
        return False
    code = error.get("code")
    if isinstance(code, int) and code in _FAILOVER_CODES:
        return True
    message = str(error.get("message", "")).lower()
    return any(fragment in message for fragment in _FAILOVER_MSG_FRAGMENTS)


def _is_failover_exception(exc: Exception) -> bool:
    """Return True if an exception justifies trying the fallback."""
    msg = str(exc).lower()
    return any(fragment in msg for fragment in _FAILOVER_MSG_FRAGMENTS)


class FallbackWeb3Provider:
    """Factory that returns a ``Web3`` instance backed by primary+fallback."""

    def __init__(
        self,
        primary_url: str,
        fallback_url: str | None = None,
        *,
        request_timeout_seconds: float = _DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self._primary_url = primary_url.strip()
        self._fallback_url = (fallback_url or "").strip()
        self._request_timeout_seconds = float(request_timeout_seconds)

    def get_web3(self) -> Web3:
        """Return a ``Web3`` instance using the failover provider."""
        provider = _FailoverHTTPProvider(
            primary_url=self._primary_url,
            fallback_url=self._fallback_url,
            request_timeout_seconds=self._request_timeout_seconds,
        )
        return Web3(provider)

    @property
    def primary_url(self) -> str:
        """Return the configured primary RPC URL."""

        return self._primary_url

    @property
    def fallback_url(self) -> str | None:
        """Return the distinct configured fallback URL, if any."""

        # Same-as-primary is treated as no fallback; mirror the logic in
        # _FailoverHTTPProvider.__init__ so callers see a consistent view.
        if not self._fallback_url:
            return None
        if self._fallback_url == self._primary_url:
            return None
        return self._fallback_url

    @property
    def request_timeout_seconds(self) -> float:
        """Return the per-request timeout bound."""

        return self._request_timeout_seconds
