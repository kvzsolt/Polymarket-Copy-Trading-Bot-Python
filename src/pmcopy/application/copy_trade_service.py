"""Copy-trade event identity rules shared by legacy and migrated pipelines."""

from __future__ import annotations

from collections.abc import Mapping


class CopyTradeIdentityBuilder:
    """Build composite copy identities without collapsing distinct fills."""

    def build(
        self,
        user_id: str,
        leader_id: str,
        trade: Mapping[str, object],
    ) -> str:
        """Return the canonical follower/source/fill identity.

        Args:
            user_id: Follower account identity.
            leader_id: Source leader identity.
            trade: Normalized or provider-shaped source event.

        Returns:
            Stable identity including a mined fill discriminator when present.
        """

        transaction_hash = self._first(
            trade,
            "transactionHash",
            "txHash",
            "transaction_hash",
            "tx_hash",
        )
        side = str(trade.get("side") or "").strip().upper()
        token_id = self._first(trade, "tokenID", "token_id", "asset")
        components = [
            self._normalize(user_id),
            self._normalize(leader_id),
            self._normalize(transaction_hash),
            side,
            self._normalize(token_id),
        ]
        fill_identity = self._fill_identity(trade)
        if fill_identity:
            components.append(fill_identity)
        return "|".join(components)

    @classmethod
    def _fill_identity(cls, trade: Mapping[str, object]) -> str:
        raw_index = trade.get("log_index")
        if raw_index is None:
            raw_index = trade.get("logIndex")
        normalized_index = cls._log_index(raw_index)
        if normalized_index is not None:
            return f"log:{normalized_index}"
        order_hash = cls._first(trade, "order_hash", "orderHash")
        if order_hash:
            return f"order:{cls._normalize(order_hash)}"
        return ""

    @staticmethod
    def _log_index(value: object | None) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            if isinstance(value, str):
                return int(value, 0)
            if isinstance(value, int):
                return value
        except ValueError:
            return None
        return None

    @staticmethod
    def _first(trade: Mapping[str, object], *keys: str) -> object:
        for key in keys:
            value = trade.get(key)
            if value is not None and str(value).strip():
                return value
        return ""

    @staticmethod
    def _normalize(value: object) -> str:
        return str(value or "").strip().lower()


__all__ = ["CopyTradeIdentityBuilder"]
