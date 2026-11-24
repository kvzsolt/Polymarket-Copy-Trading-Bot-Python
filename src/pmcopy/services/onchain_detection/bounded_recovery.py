"""Finite subscription recovery; historical data never creates fresh entries."""

from __future__ import annotations

import re
from hashlib import sha256
from typing import cast

from pmcopy.interfaces.grouped_detection import (
    AdmittedRpcReader,
    DetectionStateStore,
    NativeObservationSink,
    RawObservationSink,
)
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.onchain_detection.grouped_detection import DetectionSubscription
from pmcopy.services.onchain_detection.native_owner import NativeOwnerLogDecoder
from pmcopy.utils.logger import get_logger

logger = get_logger("NativeRecovery")


class RecoveryUnavailable(RuntimeError):
    """The recent source window could not be validated against provider evidence."""


class BoundedLogRecovery:
    """Recover recent evidence once after each acknowledged subscription.

    There is no timer, REST trade endpoint, head subscription, or unbounded retry.
    Long downtime starts a new recent window and reports the historical gap.
    Checkpoints advance only after that window is validated and ingested.
    """

    def __init__(
        self,
        *,
        state: DetectionStateStore,
        gateway: AdmittedRpcReader,
        observer: NativeObservationSink,
        clock: ReceiptClock,
        raw: RawObservationSink,
        provider: str,
        overlap: int = 8,
        maximum_gap: int = 5000,
    ) -> None:
        """Inject local evidence and a metered Alchemy read gateway."""
        if provider != "alchemy" or not 1 <= overlap <= 100 or not overlap <= maximum_gap <= 5000:
            raise ValueError("invalid bounded recovery configuration")
        self._state, self._gateway, self._observer = state, gateway, observer
        self._clock, self._raw, self._provider = clock, raw, provider
        self._overlap, self._maximum = overlap, maximum_gap
        self._decoder = NativeOwnerLogDecoder()

    @staticmethod
    def source_key(group: DetectionSubscription) -> str:
        """Bind a checkpoint to the exact provider and owner roster."""
        return sha256((group.provider + "|" + "|".join(group.wallets)).encode()).hexdigest()

    async def recover(self, group: DetectionSubscription) -> int:
        """Validate recent history before enabling fresh events at the returned head.

        Old checkpoints beyond the recovery window cannot permanently stop live
        detection. Retained identities still suppress duplicates; skipped history
        is reported explicitly and is never replayed into fresh copy entries.
        """
        if group.kind != "mined" or not group.wallets:
            raise RecoveryUnavailable("recovery requires a nonempty mined owner group")
        key = self.source_key(group)
        head = self._quantity(await self._gateway.call(self._provider, "eth_blockNumber", []))
        previous = self._state.checkpoint(key)
        if previous is not None and previous > head:
            raise RecoveryUnavailable("provider head precedes source checkpoint")
        start = max(0, (head if previous is None else previous) - self._overlap)
        historical_gap = head - start + 1 > self._maximum
        if historical_gap:
            start = max(0, head - min(self._overlap, self._maximum - 1))
        for number, old_hash in self._state.included_blocks(start, head):
            block = await self._gateway.call(
                self._provider, "eth_getBlockByNumber", [hex(number), False]
            )
            receipt = self._clock.capture()
            self._raw.record(self._provider, "block_header", block, receipt, recovered=True)
            if (
                not isinstance(block, dict)
                or self._quantity(block.get("number")) != number
                or not isinstance(block.get("hash"), str)
                or re.fullmatch(r"0x[0-9a-fA-F]{64}", block["hash"]) is None
            ):
                raise RecoveryUnavailable("canonical block evidence unavailable")
            if block["hash"].lower() != old_hash:
                self._state.invalidate_block(number, old_hash)
        base_filter = cast(dict[str, object], group.subscribe_params()[1])
        for chunk_start in range(start, head + 1, 10):
            chunk_end = min(chunk_start + 9, head)
            logs = await self._gateway.call(
                self._provider,
                "eth_getLogs",
                [
                    base_filter | {"fromBlock": hex(chunk_start), "toBlock": hex(chunk_end)},
                ],
            )
            receipt = self._clock.capture()
            if not isinstance(logs, list) or len(logs) > 10_000:
                raise RecoveryUnavailable("recovery log response exceeds bound or is malformed")
            ordered: list[tuple[int, int, dict[str, object]]] = []
            for payload in logs:
                self._raw.record(self._provider, "mined", payload, receipt, recovered=True)
                if not isinstance(payload, dict):
                    raise RecoveryUnavailable("malformed recovered log")
                fill = self._decoder.decode(payload)
                if (
                    fill.owner not in group.wallets
                    or not chunk_start <= fill.block_number <= chunk_end
                ):
                    raise RecoveryUnavailable("recovered log outside requested owner interval")
                ordered.append((fill.block_number, fill.log_index, payload))
            for _, _, payload in sorted(ordered, key=lambda row: row[:2]):
                await self._observer.observe(payload, receipt, recovered=True)
        if historical_gap:
            logger.warning(
                "native_detection_recovery_gap | previous_checkpoint=%s "
                "resume_from_block=%d head=%d historical_gap_replayed=false",
                previous,
                start,
                head,
            )
        self._state.advance_checkpoint(key, head)
        return head

    @staticmethod
    def _quantity(value: object) -> int:
        if not isinstance(value, str) or re.fullmatch(r"0x[0-9a-fA-F]+", value) is None:
            raise RecoveryUnavailable("invalid recovery block quantity")
        return int(value, 16)
