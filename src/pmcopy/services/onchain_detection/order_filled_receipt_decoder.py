"""Decode successful Polygon receipts into qualified source fills."""

from __future__ import annotations

from pmcopy.domain.source_transactions import (
    ConfirmedSourceFill,
    EvmReceipt,
    PendingSourceMatch,
    ReservedTransaction,
    SourceFillKey,
    SourceSide,
    WatchedRole,
)
from pmcopy.services.onchain_detection.order_filled_detector import (
    DecodedOrderFilledLog,
    build_signal_from_log,
    decode_order_filled_log,
)


class OrderFilledReceiptDecoder:
    """Match receipt OrderFilled logs against pending qualifications."""

    def decode_confirmed_fills(
        self,
        reservation: ReservedTransaction,
        receipt: EvmReceipt,
    ) -> tuple[ConfirmedSourceFill, ...]:
        """Return unique fills that satisfy ``reservation``.

        Args:
            reservation: Qualified pending source reservation.
            receipt: Successfully mined transaction receipt.

        Returns:
            Exact Decimal source fills whose transaction, order, wallet, role,
            token, and side agree with a pending match.
        """

        transaction = reservation.transaction
        if (
            receipt.chain_id != transaction.chain_id
            or receipt.transaction_hash != transaction.transaction_hash
        ):
            return ()
        fills: list[ConfirmedSourceFill] = []
        identities: set[SourceFillKey] = set()
        for raw_log in receipt.logs:
            decoded = decode_order_filled_log(raw_log)
            if decoded is None or decoded.tx_hash != transaction.transaction_hash:
                continue
            for pending_match in transaction.matches:
                fill = self._fill_for_match(
                    pending_match=pending_match,
                    receipt=receipt,
                    decoded_log=decoded,
                )
                if fill is None or fill.identity in identities:
                    continue
                identities.add(fill.identity)
                fills.append(fill)
        return tuple(fills)

    @staticmethod
    def _fill_for_match(
        *,
        pending_match: PendingSourceMatch,
        receipt: EvmReceipt,
        decoded_log: DecodedOrderFilledLog,
    ) -> ConfirmedSourceFill | None:
        order_hash_bytes = getattr(decoded_log, "order_hash", b"")
        if not isinstance(order_hash_bytes, bytes):
            return None
        order_hash = "0x" + order_hash_bytes.hex()
        exchange = getattr(decoded_log, "exchange", "")
        if exchange != pending_match.exchange_address:
            return None
        if pending_match.order_hash is not None and pending_match.order_hash != order_hash:
            return None
        signal = build_signal_from_log(
            decoded_log,
            {pending_match.leader_wallet},
            event_ts=receipt.block_number,
            condition_id=pending_match.condition_id,
        )
        if signal is None:
            return None
        if (
            signal.watched_role != pending_match.watched_role.value
            or signal.token_id != pending_match.token_id
            or signal.side != pending_match.side.value
        ):
            return None
        try:
            return ConfirmedSourceFill(
                chain_id=receipt.chain_id,
                transaction_hash=receipt.transaction_hash,
                log_index=signal.log_index if signal.log_index is not None else 0,
                order_hash=order_hash,
                leader_wallet=signal.leader_wallet,
                watched_role=WatchedRole(signal.watched_role),
                exchange_address=signal.exchange,
                token_id=signal.token_id,
                condition_id=pending_match.condition_id,
                side=SourceSide(signal.side),
                shares=signal.size,
                notional=signal.usdc_size,
                price=signal.price,
                block_number=receipt.block_number,
                block_hash=receipt.block_hash,
            )
        except ValueError:
            return None


__all__ = ["OrderFilledReceiptDecoder"]
