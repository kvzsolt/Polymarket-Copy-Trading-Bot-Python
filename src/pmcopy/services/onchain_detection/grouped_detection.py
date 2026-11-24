"""Plan bounded provider groups using only native order-owner topic filters."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from pmcopy.domain.grouped_detection import TraderDetectionPolicy

V2_ORDER_FILLED_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
EXCHANGES = (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower(),
)


@dataclass(frozen=True, slots=True)
class DetectionSubscription:
    """One bounded upstream subscription shared by its complete owner group."""

    provider: str
    kind: Literal["mined", "pending"]
    wallets: tuple[str, ...]

    def subscribe_params(self) -> list[object]:
        """Return a fresh JSON request without exposing or copying credentials."""
        if self.kind == "pending":
            return [
                "alchemy_pendingTransactions",
                {"toAddress": list(EXCHANGES), "hashesOnly": True},
            ]
        return [
            "logs",
            {
                "address": list(EXCHANGES),
                "topics": [
                    V2_ORDER_FILLED_TOPIC,
                    None,
                    ["0x" + wallet[2:].rjust(64, "0") for wallet in self.wallets],
                ],
            },
        ]


class GroupedDetectionPlanner:
    """Build one mined source per bounded owner group and one optional pending feed."""

    def __init__(
        self,
        *,
        pending_enabled: bool = False,
    ) -> None:
        """Store explicit feed permissions without opening network connections."""
        if type(pending_enabled) is not bool:
            raise ValueError("feed verification flags must be booleans")
        self._pending_enabled = pending_enabled

    def plan(self, policies: Sequence[TraderDetectionPolicy]) -> tuple[DetectionSubscription, ...]:
        """Return complete owner groups or reject unsafe configuration."""
        grouped: dict[str, list[str]] = {}
        observed: set[str] = set()
        pending: list[str] = []
        if len({policy.wallet for policy in policies}) != len(policies):
            raise ValueError("duplicate trader policy")
        for policy in policies:
            if policy.wallet in observed:
                raise ValueError("duplicate trader policy")
            observed.add(policy.wallet)
            if not policy.enabled:
                continue
            if policy.provider != "alchemy":
                raise ValueError("only the Alchemy provider is supported")
            grouped.setdefault(policy.provider, []).append(policy.wallet)
            if policy.mode == "pending":
                pending.append(policy.wallet)
        result: list[DetectionSubscription] = []
        for provider, wallets in sorted(grouped.items()):
            ordered = sorted(wallets)
            for start in range(0, len(ordered), 100):
                result.append(
                    DetectionSubscription(provider, "mined", tuple(ordered[start : start + 100]))
                )
        if pending and self._pending_enabled:
            result.append(DetectionSubscription("alchemy", "pending", tuple(sorted(pending))))
        return tuple(result)
