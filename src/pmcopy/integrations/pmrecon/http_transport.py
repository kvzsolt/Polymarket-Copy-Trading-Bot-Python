"""Bounded server-to-server evidence writes to one configured private origin."""

from __future__ import annotations

import asyncio
from contextvars import ContextVar
import ipaddress
import json
import logging
from urllib.parse import urlsplit

import httpx

from pmcopy.domain.evidence import EvidenceError, EvidenceHttpResponse


_PRIVATE_DELIVERY: ContextVar[bool] = ContextVar("private_evidence_delivery", default=False)


class _PrivateDeliveryLogFilter(logging.Filter):
    """Suppress only this async delivery context; leave unrelated HTTP logs unchanged."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Prevent library INFO/DEBUG messages from disclosing configured origins."""
        return not _PRIVATE_DELIVERY.get()


_LOG_FILTER = _PrivateDeliveryLogFilter()


class PmreconHttpTransport:
    """Never follow redirects, trust proxy environment, log secrets or retry here."""

    def __init__(
        self,
        origin: str,
        token: str,
        *,
        timeout: float = 5.0,
        max_response_bytes: int = 65536,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Validate configuration and inject an optional network-free HTTPX transport."""
        try:
            parts = urlsplit(origin)
            _ = parts.port
            if (
                parts.scheme not in ("http", "https")
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.path not in ("", "/")
                or parts.query
                or parts.fragment
                or any(c.isspace() for c in origin)
            ):
                raise ValueError
            if parts.scheme == "http" and parts.hostname != "localhost":
                address = ipaddress.ip_address(parts.hostname)
                if not address.is_private or address.is_unspecified:
                    raise ValueError
        except (ValueError, TypeError):
            raise EvidenceError("evidence_invalid_origin") from None
        if (
            not isinstance(token, str)
            or not 32 <= len(token) <= 512
            or any(not 33 <= ord(c) <= 126 for c in token)
        ):
            raise EvidenceError("evidence_invalid_credential")
        if (
            not 0 < timeout <= 30
            or type(max_response_bytes) is not int
            or not 1024 <= max_response_bytes <= 1048576
        ):
            raise EvidenceError("evidence_invalid_transport_bound")
        self._timeout, self._max_response_bytes = timeout, max_response_bytes
        for name in (
            "httpx",
            "httpcore",
            "httpcore.connection",
            "httpcore.http11",
            "httpcore.http2",
        ):
            logger = logging.getLogger(name)
            if _LOG_FILTER not in logger.filters:
                logger.addFilter(_LOG_FILTER)
        self._url = origin.rstrip("/") + "/api/v1/evidence"
        self._client = httpx.AsyncClient(
            transport=transport,
            timeout=timeout,
            follow_redirects=False,
            trust_env=False,
            headers={
                "Authorization": "Bearer " + token,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Encoding": "identity",
            },
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    async def post(self, body: bytes) -> EvidenceHttpResponse:
        """Bound total duration and uncompressed reply bytes; suppress upstream details."""
        if not isinstance(body, bytes) or len(body) > 1_000_000:
            raise EvidenceError("evidence_request_too_large")
        context = _PRIVATE_DELIVERY.set(True)
        try:
            async with asyncio.timeout(self._timeout):
                async with self._client.stream("POST", self._url, content=body) as response:
                    if response.headers.get("content-encoding", "identity") != "identity":
                        raise EvidenceError("evidence_invalid_response_encoding")
                    received = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(received) + len(chunk) > self._max_response_bytes:
                            raise EvidenceError("evidence_response_too_large")
                        received.extend(chunk)
                    payload = json.loads(received) if received else None
                    return EvidenceHttpResponse(response.status_code, payload)
        except (httpx.HTTPError, OSError, TimeoutError, ValueError, RecursionError):
            raise EvidenceError("evidence_transport_unavailable") from None
        finally:
            _PRIVATE_DELIVERY.reset(context)

    async def aclose(self) -> None:
        """Release the private HTTP connection pool."""
        await self._client.aclose()
