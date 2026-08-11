"""Durable per-fill protective-risk lot application model.

The domain aggregate keeps exact entry, remaining-share, trailing-peak, and
policy-snapshot state independent of venue and PostgreSQL adapters.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol

from services.copy_tasks.protective_exits import FillRiskLot, ProtectiveRiskPolicy

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HEX = frozenset("0123456789abcdef")


class RiskLotInvariantError(RuntimeError):
    """Raised when durable fill evidence contradicts existing lot state."""


class RiskLotStatus(StrEnum):
    """Persistent lifecycle of one entry-fill lot."""

    OPEN = "open"
    CLOSED = "closed"


class RiskLotValueParser:
    """Validate exact values at the durable-lot application boundary."""

    @staticmethod
    def positive_decimal(value: object, *, name: str) -> Decimal:
        """Return a finite positive decimal.

        Args:
            value: External decimal-like value.
            name: Actionable field name for validation errors.

        Returns:
            A finite positive ``Decimal``.
        """

        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"{name} must be decimal-like") from exc
        if not parsed.is_finite() or parsed <= _ZERO:
            raise ValueError(f"{name} must be finite and positive")
        return parsed

    @staticmethod
    def wallet(value: object, *, name: str) -> str:
        """Return a canonical 20-byte hexadecimal wallet.

        Args:
            value: External wallet-like value.
            name: Actionable field name for validation errors.

        Returns:
            Lowercase EVM address.
        """

        normalized = str(value or "").strip().lower()
        if (
            len(normalized) != 42
            or not normalized.startswith("0x")
            or any(character not in _HEX for character in normalized[2:])
        ):
            raise ValueError(f"{name} must be a 20-byte hexadecimal wallet")
        return normalized


@dataclass(frozen=True, slots=True)
class DurableRiskLot:
    """Restart-safe lot aggregate created from one authoritative BUY fill."""

    follower_account: str
    leader_wallet: str
    entry_fill_id: str
    lot: FillRiskLot
    policy: ProtectiveRiskPolicy
    status: RiskLotStatus = RiskLotStatus.OPEN
    version: int = 1

    def __post_init__(self) -> None:
        follower = str(self.follower_account or "").strip()
        fill_id = str(self.entry_fill_id or "").strip()
        if not follower:
            raise ValueError("follower_account is required")
        if not fill_id:
            raise ValueError("entry_fill_id is required")
        if int(self.version) < 1:
            raise ValueError("risk lot version must be positive")
        expected_status = (
            RiskLotStatus.CLOSED if self.lot.remaining_shares == _ZERO else RiskLotStatus.OPEN
        )
        if self.status is not expected_status:
            raise ValueError("risk lot status must match remaining shares")
        object.__setattr__(self, "follower_account", follower)
        object.__setattr__(
            self,
            "leader_wallet",
            RiskLotValueParser.wallet(self.leader_wallet, name="leader_wallet"),
        )
        object.__setattr__(self, "entry_fill_id", fill_id)
        object.__setattr__(self, "version", int(self.version))


@dataclass(frozen=True, slots=True)
class ProtectiveLotExitFill:
    """Authoritative protective SELL fill applied to one durable lot."""

    fill_id: str
    lot_id: str
    shares: Decimal
    price: Decimal
    occurred_at: datetime
    level_id: str | None = None

    def __post_init__(self) -> None:
        fill_identity = str(self.fill_id or "").strip()
        lot_identity = str(self.lot_id or "").strip()
        if not fill_identity or not lot_identity:
            raise ValueError("protective fill and lot identities are required")
        shares = RiskLotValueParser.positive_decimal(self.shares, name="shares")
        price = RiskLotValueParser.positive_decimal(self.price, name="price")
        if price > _ONE:
            raise ValueError("price must be <= 1")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        level = str(self.level_id).strip() if self.level_id is not None else None
        if level == "":
            level = None
        object.__setattr__(self, "fill_id", fill_identity)
        object.__setattr__(self, "lot_id", lot_identity)
        object.__setattr__(self, "shares", shares)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "level_id", level)


@dataclass(frozen=True, slots=True)
class RiskLotExitApplication:
    """Exactly-once result of applying a protective exit fill."""

    created: bool
    lot: DurableRiskLot


class RiskLotRepository(Protocol):
    """Narrow durable repository required by risk-lot orchestration."""

    async def create_if_absent(self, lot: DurableRiskLot) -> bool:
        """Create an entry-fill lot exactly once."""

        ...

    async def get(self, lot_id: str) -> DurableRiskLot | None:
        """Return one durable lot by identity."""

        ...

    async def list_open(self, follower_account: str) -> tuple[DurableRiskLot, ...]:
        """Return stable open-lot state for startup rehydration."""

        ...

    async def list_all_open(self) -> tuple[DurableRiskLot, ...]:
        """Return every open lot during single-process startup restoration."""

        ...

    async def observe_peak(self, lot_id: str, price: Decimal) -> DurableRiskLot:
        """Raise a lot's high-water mark monotonically."""

        ...

    async def record_exit_fill(
        self,
        fill: ProtectiveLotExitFill,
    ) -> RiskLotExitApplication:
        """Apply one protective fill exactly once."""

        ...


class InMemoryRiskLotRepository:
    """Deterministic risk-lot repository for unit tests and paper runtime."""

    def __init__(self) -> None:
        self._lots: dict[str, DurableRiskLot] = {}
        self._exit_fills: dict[str, ProtectiveLotExitFill] = {}
        self._lock = asyncio.Lock()

    async def create_if_absent(self, lot: DurableRiskLot) -> bool:
        """Create an immutable entry-fill lot idempotently."""

        async with self._lock:
            existing = self._lots.get(lot.lot.lot_id)
            if existing is not None:
                if existing == lot:
                    return False
                raise RiskLotInvariantError("risk lot identity conflicts with existing state")
            if any(item.entry_fill_id == lot.entry_fill_id for item in self._lots.values()):
                raise RiskLotInvariantError("entry fill already belongs to another risk lot")
            self._lots[lot.lot.lot_id] = lot
            return True

    async def get(self, lot_id: str) -> DurableRiskLot | None:
        """Return one immutable lot snapshot."""

        async with self._lock:
            return self._lots.get(str(lot_id))

    async def list_open(self, follower_account: str) -> tuple[DurableRiskLot, ...]:
        """Return open lots in stable identity order."""

        follower = str(follower_account or "").strip()
        if not follower:
            raise ValueError("follower_account is required")
        async with self._lock:
            return tuple(
                lot
                for _, lot in sorted(self._lots.items())
                if lot.follower_account == follower and lot.status is RiskLotStatus.OPEN
            )

    async def list_all_open(self) -> tuple[DurableRiskLot, ...]:
        """Return every open lot in stable identity order."""

        async with self._lock:
            return tuple(
                lot for _, lot in sorted(self._lots.items()) if lot.status is RiskLotStatus.OPEN
            )

    async def observe_peak(self, lot_id: str, price: Decimal) -> DurableRiskLot:
        """Persist a strictly higher trailing peak and ignore older prices."""

        observed = RiskLotValueParser.positive_decimal(price, name="price")
        if observed > _ONE:
            raise ValueError("price must be <= 1")
        async with self._lock:
            identity = str(lot_id or "").strip()
            current = self._lots.get(identity)
            if current is None:
                raise LookupError(f"risk lot does not exist: {identity}")
            if current.status is RiskLotStatus.CLOSED or observed <= current.lot.peak_price:
                return current
            updated = replace(
                current,
                lot=replace(current.lot, peak_price=observed),
                version=current.version + 1,
            )
            self._lots[identity] = updated
            return updated

    async def record_exit_fill(
        self,
        fill: ProtectiveLotExitFill,
    ) -> RiskLotExitApplication:
        """Apply one authoritative protective fill atomically and idempotently."""

        async with self._lock:
            existing_fill = self._exit_fills.get(fill.fill_id)
            if existing_fill is not None:
                if existing_fill != fill:
                    raise RiskLotInvariantError(
                        "protective fill identity conflicts with existing state"
                    )
                existing_lot = self._lots.get(fill.lot_id)
                if existing_lot is None:
                    raise RiskLotInvariantError("protective fill references a missing lot")
                return RiskLotExitApplication(created=False, lot=existing_lot)
            current = self._lots.get(fill.lot_id)
            if current is None:
                raise LookupError(f"risk lot does not exist: {fill.lot_id}")
            if current.status is RiskLotStatus.CLOSED:
                raise RiskLotInvariantError("cannot apply a new fill to a closed risk lot")
            if fill.shares > current.lot.remaining_shares:
                raise ValueError("protective fill shares exceed lot remaining shares")
            remaining = current.lot.remaining_shares - fill.shares
            levels = current.lot.triggered_levels
            if fill.level_id is not None and fill.level_id not in levels:
                levels = (*levels, fill.level_id)
            updated = replace(
                current,
                lot=replace(
                    current.lot,
                    remaining_shares=remaining,
                    triggered_levels=levels,
                ),
                status=(RiskLotStatus.CLOSED if remaining == _ZERO else RiskLotStatus.OPEN),
                version=current.version + 1,
            )
            self._exit_fills[fill.fill_id] = fill
            self._lots[fill.lot_id] = updated
            return RiskLotExitApplication(created=True, lot=updated)


class RiskLotService:
    """Application service for restart-safe per-fill protective state."""

    def __init__(self, repository: RiskLotRepository) -> None:
        self._repository = repository

    async def create_from_entry_fill(self, lot: DurableRiskLot) -> bool:
        """Create one durable lot from authoritative BUY fill evidence."""

        return await self._repository.create_if_absent(lot)

    async def get(self, lot_id: str) -> DurableRiskLot | None:
        """Return one durable lot by identity."""

        return await self._repository.get(lot_id)

    async def list_open(self, follower_account: str) -> tuple[DurableRiskLot, ...]:
        """Rehydrate all open lots for one follower."""

        return await self._repository.list_open(follower_account)

    async def list_all_open(self) -> tuple[DurableRiskLot, ...]:
        """Rehydrate all open lots during startup."""

        return await self._repository.list_all_open()

    async def observe_price(self, lot_id: str, price: Decimal) -> DurableRiskLot:
        """Advance a lot's trailing high-water mark monotonically."""

        return await self._repository.observe_peak(lot_id, price)

    async def record_exit_fill(
        self,
        fill: ProtectiveLotExitFill,
    ) -> RiskLotExitApplication:
        """Apply one authoritative protective SELL fill exactly once."""

        return await self._repository.record_exit_fill(fill)


__all__ = [
    "DurableRiskLot",
    "InMemoryRiskLotRepository",
    "ProtectiveLotExitFill",
    "RiskLotExitApplication",
    "RiskLotInvariantError",
    "RiskLotRepository",
    "RiskLotService",
    "RiskLotStatus",
]
