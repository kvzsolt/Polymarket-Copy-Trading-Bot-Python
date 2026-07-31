"""Typed errors for the Heisenberg API boundary."""

from __future__ import annotations


class HeisenbergError(RuntimeError):
    """Base error carrying a redacted request identifier when available."""

    retryable = False

    def __init__(self, message: str, *, request_id: str | None = None) -> None:
        self.request_id = request_id
        suffix = f" [request_id={request_id}]" if request_id else ""
        super().__init__(message + suffix)


class HeisenbergConfigurationError(HeisenbergError):
    pass


class HeisenbergBadRequestError(HeisenbergError):
    pass


class HeisenbergAuthenticationError(HeisenbergError):
    pass


class HeisenbergPermissionError(HeisenbergError):
    pass


class HeisenbergRateLimitError(HeisenbergError):
    retryable = True


class HeisenbergUnavailableError(HeisenbergError):
    retryable = True


class HeisenbergProtocolError(HeisenbergError):
    pass


__all__ = [
    "HeisenbergAuthenticationError",
    "HeisenbergBadRequestError",
    "HeisenbergConfigurationError",
    "HeisenbergError",
    "HeisenbergPermissionError",
    "HeisenbergProtocolError",
    "HeisenbergRateLimitError",
    "HeisenbergUnavailableError",
]
