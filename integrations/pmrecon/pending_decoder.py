"""Bounded direct V2 matchOrders decoder retaining all native signed orders."""

import re
from typing import Literal

from eth_abi.abi import decode, encode
from eth_abi.exceptions import DecodingError, EncodingError

from config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from domain.pending_evaluation import PendingEvaluationError, PendingOrder

EXCHANGES = frozenset(
    (POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(), POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower())
)
ORDER_ABI = (
    "(uint256,address,address,uint256,uint256,uint256,uint8,uint8,uint256,bytes32,bytes32,bytes)"
)
MATCH_ARGUMENTS = (
    "bytes32",
    ORDER_ABI,
    ORDER_ABI + "[]",
    "uint256",
    "uint256[]",
    "uint256",
    "uint256[]",
)


class V2PendingCalldataDecoder:
    """Decode direct exchange calls only; wrappers require their own reviewed evidence."""

    def __init__(self, *, max_orders: int = 64, max_calldata_bytes: int = 262144) -> None:
        """Fix memory/work bounds before accepting any provider input."""
        if type(max_orders) is not int or not 1 <= max_orders <= 256:
            raise PendingEvaluationError("pending_invalid_order_bound")
        if type(max_calldata_bytes) is not int or not 1024 <= max_calldata_bytes <= 524288:
            raise PendingEvaluationError("pending_invalid_calldata_bound")
        self._maximum, self._byte_limit = max_orders, max_calldata_bytes

    def decode(self, target: str, calldata: str) -> tuple[PendingOrder, ...]:
        """Preserve every native owner order after validating flat ABI and array bounds."""
        if not isinstance(target, str) or target.lower() not in EXCHANGES:
            raise PendingEvaluationError("pending_unsupported_target")
        if (
            not isinstance(calldata, str)
            or len(calldata) > self._byte_limit * 2 + 2
            or not re.fullmatch(r"0x(?:[0-9a-fA-F]{2})+", calldata)
        ):
            raise PendingEvaluationError("pending_invalid_calldata")
        raw = bytes.fromhex(calldata[2:])
        if raw[:4] != bytes.fromhex("3c2b4399") or len(raw) < 228:
            raise PendingEvaluationError("pending_unsupported_selector")
        body = raw[4:]
        # Array lengths are checked before the ABI library allocates dynamic tuples.
        try:
            for word in (2, 4, 6):
                offset = int.from_bytes(body[word * 32 : (word + 1) * 32], "big")
                if offset < 224 or offset % 32 or offset + 32 > len(body):
                    raise PendingEvaluationError("pending_invalid_array_offset")
                count = int.from_bytes(body[offset : offset + 32], "big")
                if not 1 <= count < self._maximum:
                    raise PendingEvaluationError("pending_array_exceeds_bound")
            values = decode(list(MATCH_ARGUMENTS), body, strict=True)
            if encode(list(MATCH_ARGUMENTS), values) != body:
                raise PendingEvaluationError("pending_noncanonical_calldata")
            makers = values[2]
            if len(makers) != len(values[4]) or len(makers) != len(values[6]):
                raise PendingEvaluationError("pending_mismatched_arrays")
            orders = [self._order(values[1], "taker", 0)]
            orders.extend(self._order(value, "maker", index) for index, value in enumerate(makers))
            return tuple(orders)
        except (DecodingError, EncodingError, ValueError, TypeError, OverflowError, IndexError):
            raise PendingEvaluationError("pending_invalid_calldata") from None

    @staticmethod
    def _order(value: tuple, role: Literal["maker", "taker"], index: int) -> PendingOrder:
        """Translate ABI values without dropping signature bytes or owner identity."""
        return PendingOrder(
            salt=value[0],
            owner=value[1],
            signer=value[2],
            token_id=value[3],
            maker_amount=value[4],
            taker_amount=value[5],
            side=value[6],
            signature_type=value[7],
            timestamp=value[8],
            metadata=value[9],
            builder=value[10],
            signature=value[11],
            role=role,
            index=index,
        )
