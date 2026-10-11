"""Strict native V2 owner-log decoder preserving removals and inclusion evidence."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal, localcontext

from domain.grouped_detection import NativeOwnerFill
from services.grouped_detection import EXCHANGES, V2_ORDER_FILLED_TOPIC


class NativeOwnerLogDecoder:
    """Normalize only reviewed native exchange events, never the opposite wallet leg."""

    def decode(self, log: Mapping[str, object]) -> NativeOwnerFill:
        """Return validated mined or removed evidence without guessing coordinates.

        Args:
            log: One provider log with four topics and seven non-indexed words.

        Returns:
            Immutable native owner evidence matching the pmrecon action identity.

        Raises:
            ValueError: Unknown exchange, malformed ABI, identity, or economics.
        """
        exchange = self._hex(log.get("address"), 20, "exchange")
        if exchange not in EXCHANGES:
            raise ValueError("unsupported exchange")
        tx_hash = self._hex(log.get("transactionHash"), 32, "transaction hash")
        block_hash = self._hex(log.get("blockHash"), 32, "block hash")
        log_index = self._quantity(log.get("logIndex"), "log index")
        block_number = self._quantity(log.get("blockNumber"), "block number")
        topics = log.get("topics")
        if not isinstance(topics, (list, tuple)) or len(topics) != 4:
            raise ValueError("native OrderFilled requires four topics")
        topic_words = tuple(self._hex(value, 32, "topic") for value in topics)
        if topic_words[0] != V2_ORDER_FILLED_TOPIC:
            raise ValueError("unsupported event topic")
        if any(value[2:26] != "0" * 24 for value in topic_words[2:]):
            raise ValueError("indexed owner address padding must be zero")
        owner, counterparty = ("0x" + value[-40:] for value in topic_words[2:])
        data = self._hex(log.get("data"), 224, "seven-word ABI data")
        words = tuple(int(data[start : start + 64], 16) for start in range(2, 450, 64))
        side, token_id, maker_amount, taker_amount, fee, _builder, _metadata = words
        if side not in (0, 1) or maker_amount <= 0 or taker_amount <= 0:
            raise ValueError("invalid native fill economics")
        removed = log.get("removed", False)
        if type(removed) is not bool:
            raise ValueError("removed must be a boolean")
        with localcontext() as context:
            context.prec = 160
            shares = Decimal(taker_amount if side == 0 else maker_amount) / Decimal(1_000_000)
            notional = Decimal(maker_amount if side == 0 else taker_amount) / Decimal(1_000_000)
            price = notional / shares
            amount_fee = Decimal(fee) / Decimal(1_000_000)
        if not Decimal("0") < price < Decimal("1"):
            raise ValueError("native price must be in (0, 1)")
        return NativeOwnerFill(
            owner=owner,
            counterparty=counterparty,
            exchange=exchange,
            transaction_hash=tx_hash,
            log_index=log_index,
            block_number=block_number,
            block_hash=block_hash,
            order_hash=topic_words[1],
            token_id=str(token_id),
            side="BUY" if side == 0 else "SELL",
            shares=shares,
            notional=notional,
            price=price,
            fee=amount_fee,
            native_taker=counterparty == exchange,
            removed=removed,
        )

    @staticmethod
    def _hex(value: object, length: int, label: str) -> str:
        if (
            not isinstance(value, str)
            or re.fullmatch(r"0x[0-9a-fA-F]{" + str(length * 2) + "}", value) is None
        ):
            raise ValueError(f"invalid {label}")
        return value.lower()

    @staticmethod
    def _quantity(value: object, label: str) -> int:
        if type(value) is int:
            result = value
        elif isinstance(value, str) and re.fullmatch(r"0x[0-9a-fA-F]+", value):
            result = int(value, 16)
        else:
            raise ValueError(f"invalid {label}")
        if result < 0:
            raise ValueError(f"invalid {label}")
        return result
