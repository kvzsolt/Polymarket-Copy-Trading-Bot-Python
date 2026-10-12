"""Typed errors for the Falcon API boundary."""

from __future__ import annotations

from typing import Self


class FalconError(RuntimeError):
    """Provider error carrying optional request and trusted agent identifiers."""

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        agent_id: int | None = None,
    ) -> None:
        """Attach adapter diagnostics to a provider failure.

        Args:
            message: Error description supplied by the adapter or application.
            request_id: Optional provider request identifier.
            agent_id: Positive agent identifier from the outbound request.

        Raises:
            ValueError: If the agent identifier is not a positive integer.
        """
        if agent_id is not None and (type(agent_id) is not int or agent_id <= 0):
            raise ValueError("Falcon agent_id must be a positive integer")
        self.request_id = request_id
        self.agent_id = agent_id
        suffix = f" [request_id={request_id}]" if request_id else ""
        if agent_id is not None:
            suffix += f" [agent_id={agent_id}]"
        super().__init__(message + suffix)

    def with_context(self, message: str) -> Self:
        """Replace untrusted error details while retaining the requested agent.

        Args:
            message: Application-owned diagnostic text, never provider content.

        Returns:
            The same error type without the original message or response headers.
        """
        return type(self)(message, agent_id=self.agent_id)


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
