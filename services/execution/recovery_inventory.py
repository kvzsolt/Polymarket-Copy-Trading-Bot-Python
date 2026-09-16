"""Bind startup recovery to fresh, complete, wallet-validated inventory evidence."""

from collections.abc import Mapping
from decimal import Decimal
import re
from typing import Protocol

from utils.logger import get_logger

_log = get_logger("RecoveryInventory")


class StartupInventoryRecoveryUnavailable(RuntimeError):
    """Startup cannot declare readiness while recovered SELL inventory is unknown."""


class CompleteHoldingsReader(Protocol):
    """Read exact shares only after complete wallet-validated pagination."""

    async def shares(self, wallet: str, token_id: str) -> Decimal | None:
        """Return zero only for proven absence; return None for unknown evidence."""
        ...


class RecoveryInventoryReader(Protocol):
    """Prove current inventory absence for a configured follower position."""

    async def zero_inventory(self, user_id: str, condition_id: str, token_id: str) -> bool:
        """Return True only after a fresh complete snapshot proves zero shares."""
        ...


class CompleteRecoveryInventoryReader:
    """Use the complete holdings port without retaining stale absence results."""

    def __init__(
        self, holdings_reader: CompleteHoldingsReader, *, wallet_by_user: Mapping[str, str]
    ) -> None:
        """Bind configured user identities to their verified profile/funder wallets."""
        self._holdings_reader = holdings_reader
        self._wallet_by_user = dict(wallet_by_user)

    async def zero_inventory(self, user_id: str, condition_id: str, token_id: str) -> bool:
        """Fetch current holdings and accept only exact zero for the bound wallet.

        Args:
            user_id: Internal follower identity with an explicit wallet binding.
            condition_id: Non-empty market identity of the recovered order.
            token_id: Raw decimal outcome token identity.

        Returns:
            False for unknown accounts, malformed identities, positive shares,
            incomplete evidence, or provider failures. No result is cached.
        """
        wallet = self._wallet_by_user.get(user_id)
        if (
            not isinstance(wallet, str)
            or re.fullmatch(r"0x[0-9a-fA-F]{40}", wallet) is None
            or not isinstance(condition_id, str)
            or not condition_id.strip()
            or not isinstance(token_id, str)
            or re.fullmatch(r"[0-9]+", token_id) is None
            or int(token_id) <= 0
        ):
            return False
        try:
            shares = await self._holdings_reader.shares(wallet.lower(), token_id)
        except Exception as exc:
            _log.warning("recovery_inventory_unavailable error_type=%s", type(exc).__name__)
            return False
        return isinstance(shares, Decimal) and shares.is_finite() and shares == 0
