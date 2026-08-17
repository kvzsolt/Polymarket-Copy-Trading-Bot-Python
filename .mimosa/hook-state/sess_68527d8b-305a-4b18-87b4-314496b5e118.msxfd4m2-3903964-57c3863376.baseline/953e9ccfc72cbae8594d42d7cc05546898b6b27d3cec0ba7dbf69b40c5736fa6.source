"""Build qualified source reservations from Alchemy pending notifications."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta

from domain.source_transactions import (
    PendingSourceMatch,
    PendingTransaction,
    SourceSide,
    WatchedRole,
)
from interfaces.source_transactions import Clock
from services.onchain_detection.calldata_decoder import (
    SourceTransactionCalldataDecoder,
    find_watched_wallets,
    is_valid_maker_side,
)


class PendingTransactionFactory:
    """Qualify full Alchemy pending objects without reading wall-clock time."""

    def __init__(
        self,
        chain_id: int,
        calldata_decoder: SourceTransactionCalldataDecoder,
        clock: Clock,
        confirmation_timeout: timedelta,
    ) -> None:
        """Initialize the pending notification boundary.

        Args:
            chain_id: Positive EVM chain identifier.
            calldata_decoder: Direct and Safe-wrapped selector decoder.
            clock: Deterministic observation clock.
            confirmation_timeout: Reservation lifetime evaluated on head events.

        Raises:
            ValueError: If chain id or timeout is not positive.
        """

        if chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if confirmation_timeout <= timedelta(0):
            raise ValueError("confirmation_timeout must be positive")
        self._chain_id = chain_id
        self._calldata_decoder = calldata_decoder
        self._clock = clock
        self._confirmation_timeout = confirmation_timeout

    def from_notification(
        self,
        notification: Mapping[str, object],
        watched_wallets: frozenset[str],
    ) -> PendingTransaction | None:
        """Return one qualified pending transaction or ``None``.

        Args:
            notification: Full Alchemy pending transaction object.
            watched_wallets: Wallet addresses currently eligible for copying.

        Returns:
            Immutable pending transaction when target, selector, calldata, and
            at least one watched order all qualify; otherwise ``None``.
        """

        transaction_hash = str(notification.get("hash") or "").strip().lower()
        sender = str(notification.get("from") or "").strip().lower()
        target = str(notification.get("to") or "").strip().lower()
        calldata = self._parse_calldata(notification.get("input"))
        nonce = self._parse_quantity(notification.get("nonce"))
        if calldata is None or nonce is None:
            return None
        decoded = self._calldata_decoder.decode(
            transaction_target=target,
            calldata=calldata,
        )
        if decoded is None:
            return None
        watched_matches = find_watched_wallets(
            decoded.match_orders,
            set(watched_wallets),
        )
        matches: list[PendingSourceMatch] = []
        condition_id = "0x" + decoded.match_orders.condition_id.hex()
        for watched_match in watched_matches:
            if not is_valid_maker_side(watched_match.side):
                continue
            matches.append(
                PendingSourceMatch(
                    leader_wallet=watched_match.wallet,
                    watched_role=WatchedRole(watched_match.role),
                    exchange_address=decoded.exchange_address,
                    order_position=watched_match.maker_index,
                    order_hash=None,
                    token_id=str(watched_match.token_id),
                    condition_id=condition_id,
                    side=(SourceSide.BUY if watched_match.side == 0 else SourceSide.SELL),
                )
            )
        if not matches:
            return None
        observed_at = self._clock.now()
        try:
            return PendingTransaction(
                chain_id=self._chain_id,
                transaction_hash=transaction_hash,
                sender=sender,
                nonce=nonce,
                target=target,
                observed_at=observed_at,
                expires_at=observed_at + self._confirmation_timeout,
                matches=tuple(matches),
            )
        except ValueError:
            return None

    @staticmethod
    def _parse_calldata(value: object) -> bytes | None:
        if not isinstance(value, str):
            return None
        normalized = value[2:] if value.lower().startswith("0x") else value
        if not normalized:
            return None
        try:
            return bytes.fromhex(normalized)
        except ValueError:
            return None

    @staticmethod
    def _parse_quantity(value: object) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value if value >= 0 else None
        if isinstance(value, str):
            try:
                parsed = int(value, 16) if value.lower().startswith("0x") else int(value)
            except ValueError:
                return None
            return parsed if parsed >= 0 else None
        return None


__all__ = ["PendingTransactionFactory"]
