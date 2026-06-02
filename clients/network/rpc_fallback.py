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

import logging
from typing import Any

from web3 import Web3
from web3.providers.base import JSONBaseProvider

logger = logging.getLogger("rpc_fallback")

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
    ) -> None:
        super().__init__()
        self._primary = Web3.HTTPProvider(primary_url)
        self._fallback: Web3.HTTPProvider | None = None
        if fallback_url and fallback_url.strip() and fallback_url.strip() != primary_url:
            self._fallback = Web3.HTTPProvider(fallback_url.strip())
            logger.info(
                "RPC fallback configured: primary=%s fallback=%s",
                _mask_url(primary_url),
                _mask_url(fallback_url),
            )
        else:
            logger.debug("RPC fallback not configured (single provider)")

    def make_request(self, method: str, params: Any) -> dict[str, Any]:
        # Track the primary outcome so the no-fallback branch can pass it
        # through rather than crash. ``_primary_exc`` stays None on the
        # failover-worthy *response* path; ``_primary_response`` stays None on
        # the *exception* path.
        _primary_exc: Exception | None = None
        _primary_response: dict[str, Any] | None = None

        # Try primary first.
        try:
            raw = self._primary.make_request(method, params)
            if not _is_failover_response(raw):
                return raw
            # Primary returned a failover-worthy error code; log and continue.
            logger.warning(
                "Primary RPC returned failover-worthy error for %s: %s",
                method,
                raw.get("error"),
            )
            _primary_response = raw
        except Exception as exc:
            if _is_failover_exception(exc):
                logger.warning(
                    "Primary RPC failed for %s: %s",
                    method,
                    exc,
                )
                # Store the exception to re-raise if no fallback is available.
                _primary_exc = exc
            else:
                # Non-failover exception (e.g. invalid params) — raise immediately.
                raise

        # No fallback configured: pass the primary outcome through instead of
        # crashing with UnboundLocalError. Re-raise a stored exception; on the
        # response path, return the (failover-worthy) primary response so the
        # caller sees the real JSON-RPC error rather than an unrelated crash.
        if self._fallback is None:
            if _primary_exc is not None:
                raise _primary_exc
            if _primary_response is None:
                # Unreachable: we only fall through here from the
                # failover-worthy *response* path (which sets _primary_response)
                # or the *exception* path (handled above). Guard the invariant
                # explicitly so a future edit can't silently return None.
                raise RuntimeError(
                    "rpc_fallback: no primary response or exception recorded"
                )
            return _primary_response

        # Try fallback.
        logger.info("Failing over to secondary RPC for %s", method)
        return self._fallback.make_request(method, params)


def _mask_url(url: str) -> str:
    """Strip API key / credential from URL for safe logging."""
    if not url:
        return ""
    # Heuristic: anything after the last path segment that looks like a key.
    if "/v3/" in url:
        return url.rsplit("/", 1)[0] + "/***"
    if "?" in url:
        return url.split("?", 1)[0] + "?***"
    return url


def _is_failover_response(raw: dict[str, Any]) -> bool:
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
    ) -> None:
        self._primary_url = primary_url.strip()
        self._fallback_url = (fallback_url or "").strip()

    def get_web3(self) -> Web3:
        """Return a ``Web3`` instance using the failover provider."""
        provider = _FailoverHTTPProvider(
            primary_url=self._primary_url,
            fallback_url=self._fallback_url,
        )
        return Web3(provider)

    @property
    def primary_url(self) -> str:
        return self._primary_url

    @property
    def fallback_url(self) -> str | None:
        # Same-as-primary is treated as no fallback; mirror the logic in
        # _FailoverHTTPProvider.__init__ so callers see a consistent view.
        if not self._fallback_url:
            return None
        if self._fallback_url == self._primary_url:
            return None
        return self._fallback_url
