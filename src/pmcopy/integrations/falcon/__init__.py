"""Falcon prediction-intelligence integration (analytics/discovery only)."""

from __future__ import annotations

from pmcopy.integrations.falcon.client import FalconClient, FalconPage
from pmcopy.integrations.falcon.errors import (
    FalconAuthenticationError,
    FalconBadRequestError,
    FalconConfigurationError,
    FalconPermissionError,
    FalconProtocolError,
    FalconRateLimitError,
    FalconUnavailableError,
)

__all__ = [
    "FalconAuthenticationError",
    "FalconBadRequestError",
    "FalconClient",
    "FalconConfigurationError",
    "FalconPage",
    "FalconPermissionError",
    "FalconProtocolError",
    "FalconRateLimitError",
    "FalconUnavailableError",
]
