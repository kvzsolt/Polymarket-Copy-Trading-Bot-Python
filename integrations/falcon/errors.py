"""Typed errors for the Falcon API boundary."""

from __future__ import annotations


class FalconError(RuntimeError):
    """Base error carrying a redacted request identifier when available."""

    retryable = False

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        self.request_id = request_id
        suffix = f" [request_id={request_id}]" if request_id else ""
        super().__init__(message + suffix)


class FalconConfigurationError(FalconError):
    """Report invalid or incomplete Falcon adapter configuration."""


class FalconBadRequestError(FalconError):
    """Report a request rejected by the Falcon analytics endpoint."""


class FalconAuthenticationError(FalconError):
    """Report a missing or invalid Falcon bearer credential."""


class FalconPermissionError(FalconError):
    """Report an endpoint unavailable to the configured paid plan."""


class FalconRateLimitError(FalconError):
    """Report exhaustion of the provider request allowance."""

    retryable = True


class FalconUnavailableError(FalconError):
    """Report a retryable Falcon transport or server outage."""

    retryable = True


class FalconProtocolError(FalconError):
    """Report a malformed or internally inconsistent provider response."""


__all__ = [
    "FalconAuthenticationError",
    "FalconBadRequestError",
    "FalconConfigurationError",
    "FalconError",
    "FalconPermissionError",
    "FalconProtocolError",
    "FalconRateLimitError",
    "FalconUnavailableError",
]
