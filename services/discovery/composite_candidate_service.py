"""Failover wrapper preserving the existing discovery provider."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from integrations.heisenberg.errors import (
    HeisenbergRateLimitError,
    HeisenbergUnavailableError,
)


class CompositeCandidateService:
    """Prefer Heisenberg evidence and fail closed on a valid primary result.

    The fallback provider is an availability fallback, not a policy bypass.  A
    clean Heisenberg result with no ranked candidates means the primary source
    completed successfully and its gates rejected every candidate.  Falling
    back in that situation can re-promote the same unsafe or uncopyable wallets
    through a weaker legacy policy, so the default is to return the empty result
    unchanged.

    ``fallback_on_clean_empty`` exists only as an explicit compatibility escape
    hatch for operators who consciously accept that risk.  It must never be
    enabled implicitly by configuration defaults.
    """

    def __init__(
        self,
        *,
        primary: Any,
        fallback: Any,
        primary_enabled: bool = True,
        fallback_on_clean_empty: bool = False,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._primary_enabled = primary_enabled
        self._fallback_on_clean_empty = fallback_on_clean_empty

    async def discover(self, funds: Decimal):
        if not self._primary_enabled or self._primary is None:
            return await self._fallback.discover(funds)
        try:
            result = await self._primary.discover(funds)
        except (KeyboardInterrupt, SystemExit):
            raise
        except (HeisenbergUnavailableError, HeisenbergRateLimitError):
            return await self._fallback.discover(funds)

        if result.ranked or not self._fallback_on_clean_empty:
            return result
        return await self._fallback.discover(funds)


__all__ = ["CompositeCandidateService"]
