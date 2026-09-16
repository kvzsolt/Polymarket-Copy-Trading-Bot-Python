"""Resolve leader SELL fractions from Polymarket's public Data API.

This is a read-only fallback for authoritative on-chain signals. It rewinds
newer token activity from the latest position snapshot until it reaches the
canonical transaction, yielding the balance immediately before that SELL.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

from services.onchain_detection.trade_detection_models import OnchainTradeSignal

_ZERO = Decimal("0")
_ONE = Decimal("1")


class DataApiLeaderSellFractionReader:
    """Reconstruct one SELL fraction from public activity and positions."""

    _ACTIVITY_URL = "https://data-api.polymarket.com/activity"
    _POSITIONS_URL = "https://data-api.polymarket.com/positions"

    def __init__(
        self,
        *,
        fetch_json: Callable[[str], Awaitable[object]],
        activity_limit: int = 500,
    ) -> None:
        """Bind the injected public JSON reader.

        Args:
            fetch_json: Asynchronous GET-by-URL JSON reader.
            activity_limit: Maximum recent activity rows used for rewind.

        Raises:
            ValueError: If ``activity_limit`` is not positive.
        """

        if activity_limit <= 0:
            raise ValueError("activity_limit must be positive")
        self._fetch_json = fetch_json
        self._activity_limit = activity_limit

    async def fraction_for(self, signal: OnchainTradeSignal) -> Decimal | None:
        """Return the leader's fraction sold by ``signal`` when provable.

        Args:
            signal: Authoritative canonical SELL signal.

        Returns:
            An exact fraction in ``(0, 1]`` or ``None`` when either payload
            cannot prove the target transaction and its pre-sell balance.
        """

        if signal.provisional or str(signal.side or "").upper() != "SELL":
            return None
        leader = signal.leader_wallet.strip().lower()
        condition_id = str(signal.condition_id or "").strip()
        activity_params = {
            "user": leader,
            "type": "TRADE",
            "limit": str(self._activity_limit),
            "offset": "0",
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        positions_params = {
            "user": leader,
            "sizeThreshold": "0",
            "limit": "500",
            "offset": "0",
        }
        if condition_id:
            activity_params["market"] = condition_id
            positions_params["market"] = condition_id
        activity_url = self._url(
            self._ACTIVITY_URL,
            activity_params,
        )
        positions_url = self._url(self._POSITIONS_URL, positions_params)
        activity_payload, positions_payload = await asyncio.gather(
            self._fetch_json(activity_url),
            self._fetch_json(positions_url),
        )
        activities = self._rows(activity_payload)
        positions = self._rows(positions_payload)
        if activities is None or positions is None or len(positions) >= 500:
            return None
        token_id = str(signal.token_id)
        balance = self._position_balance(positions, token_id)
        if balance is None:
            return None
        target_hash = signal.tx_hash.strip().lower()
        target_rows = [
            row
            for row in activities
            if self._token_id(row) == token_id and self._transaction_hash(row) == target_hash
        ]
        if len(target_rows) != 1 or self._decimal(target_rows[0].get("size")) != signal.size:
            return None
        target_seen = False
        token_activities = [row for row in activities if self._token_id(row) == token_id]
        previous_timestamp: Decimal | None = None
        for row in token_activities:
            if len(token_activities) > 1 or "timestamp" in row:
                timestamp = self._decimal(row.get("timestamp"))
                if timestamp is None or timestamp < 0 or timestamp != timestamp.to_integral_value():
                    return None
                if previous_timestamp is not None and timestamp >= previous_timestamp:
                    return None
                previous_timestamp = timestamp
            size = self._decimal(row.get("size"))
            if size is None or size <= _ZERO:
                return None
            side = str(row.get("side") or "").strip().upper()
            row_hash = self._transaction_hash(row)
            if side == "SELL":
                balance += size
            elif side == "BUY":
                balance -= size
                if balance < _ZERO:
                    return None
            else:
                return None
            if row_hash == target_hash and side == "SELL":
                target_seen = True
                break
        if not target_seen or balance <= _ZERO:
            return None
        fraction = signal.size / balance
        if not fraction.is_finite() or fraction <= _ZERO:
            return None
        return fraction if fraction <= _ONE else None

    @staticmethod
    def _url(base: str, params: Mapping[str, str]) -> str:
        return f"{base}?{urlencode(params)}"

    @staticmethod
    def _transaction_hash(row: Mapping[str, object]) -> str:
        return (
            str(row.get("transactionHash") or row.get("txHash") or row.get("tx_hash") or "")
            .strip()
            .lower()
        )

    @classmethod
    def _rows(cls, payload: object) -> list[Mapping[str, object]] | None:
        if isinstance(payload, Mapping):
            for key in ("data", "activities", "results"):
                if key in payload:
                    payload = payload[key]
                    break
        if not isinstance(payload, list) or any(not isinstance(row, Mapping) for row in payload):
            return None
        return list(payload)

    @classmethod
    def _position_balance(
        cls,
        positions: Sequence[Mapping[str, object]],
        token_id: str,
    ) -> Decimal | None:
        balance = _ZERO
        seen: set[str] = set()
        for position in positions:
            asset = cls._token_id(position)
            size = cls._decimal(position.get("size"))
            if not asset or asset in seen or size is None or size < _ZERO:
                return None
            seen.add(asset)
            if asset == token_id:
                balance = size
        return balance

    @staticmethod
    def _token_id(row: Mapping[str, object]) -> str:
        return str(row.get("asset") or row.get("tokenID") or row.get("token_id") or "")

    @staticmethod
    def _decimal(value: object) -> Decimal | None:
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            return None
        try:
            parsed = Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            return None
        return parsed if parsed.is_finite() else None


__all__ = ["DataApiLeaderSellFractionReader"]
