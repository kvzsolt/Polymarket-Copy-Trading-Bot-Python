"""Immutable source-transaction lifecycle values.

The domain models contain no WebSocket, RPC, persistence, or exchange SDK
dependencies. Monetary fields use exact decimal arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import IntEnum, StrEnum


class TransactionState(StrEnum):
    """Lifecycle states for one observed source transaction."""

    OBSERVED = "observed"
    REJECTED = "rejected"
    RESERVED = "reserved"
    CONFIRMED = "confirmed"
    REVERTED = "reverted"
    REPLACED = "replaced"
    EXPIRED = "expired"
    RESYNC = "resync"
    CONSUMED = "consumed"


class SourceSide(StrEnum):
    """Copyable source fill direction."""

    BUY = "BUY"
    SELL = "SELL"


class WatchedRole(StrEnum):
    """Role held by the watched wallet in a matched order."""

    MAKER = "maker"
    TAKER = "taker"


class ReceiptStatus(IntEnum):
    """EVM transaction execution status from a mined receipt."""

    REVERTED = 0
    SUCCESS = 1


class _SourceTransactionValidation:
    """Validate normalized values at the source-transaction boundary."""

    _HEX_CHARACTERS = frozenset("0123456789abcdef")

    @classmethod
    def address(cls, value: str, *, name: str) -> str:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 42 or not normalized.startswith("0x"):
            raise ValueError(f"{name} must be a 20-byte hexadecimal address")
        if any(character not in cls._HEX_CHARACTERS for character in normalized[2:]):
            raise ValueError(f"{name} must be a 20-byte hexadecimal address")
        return normalized

    @classmethod
    def bytes32(cls, value: str, *, name: str) -> str:
        normalized = str(value or "").strip().lower()
        if len(normalized) != 66 or not normalized.startswith("0x"):
            raise ValueError(f"{name} must be a 32-byte hexadecimal value")
        if any(character not in cls._HEX_CHARACTERS for character in normalized[2:]):
            raise ValueError(f"{name} must be a 32-byte hexadecimal value")
        return normalized

    @staticmethod
    def non_empty(value: str, *, name: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError(f"{name} is required")
        return normalized

    @staticmethod
    def aware(value: datetime, *, name: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must be timezone-aware")
        return value

    @staticmethod
    def positive_decimal(value: Decimal, *, name: str) -> Decimal:
        parsed = Decimal(value)
        if not parsed.is_finite() or parsed <= 0:
            raise ValueError(f"{name} must be finite and greater than zero")
        return parsed


@dataclass(frozen=True, slots=True)
class PendingSourceMatch:
    """One watched order qualification decoded from pending calldata."""

    leader_wallet: str
    watched_role: WatchedRole
    exchange_address: str
    order_position: int | None
    order_hash: str | None
    token_id: str
    condition_id: str
    side: SourceSide

    def __post_init__(self) -> None:
        """Normalize and validate the decoded match."""

        object.__setattr__(
            self,
            "leader_wallet",
            _SourceTransactionValidation.address(self.leader_wallet, name="leader_wallet"),
        )
        object.__setattr__(self, "watched_role", WatchedRole(self.watched_role))
        object.__setattr__(
            self,
            "exchange_address",
            _SourceTransactionValidation.address(
                self.exchange_address,
                name="exchange_address",
            ),
        )
        if self.order_position is not None and self.order_position < 0:
            raise ValueError("order_position must be non-negative")
        if self.order_hash is not None:
            object.__setattr__(
                self,
                "order_hash",
                _SourceTransactionValidation.bytes32(self.order_hash, name="order_hash"),
            )
        object.__setattr__(
            self,
            "token_id",
            _SourceTransactionValidation.non_empty(self.token_id, name="token_id"),
        )
        object.__setattr__(
            self,
            "condition_id",
            _SourceTransactionValidation.bytes32(self.condition_id, name="condition_id"),
        )
        object.__setattr__(self, "side", SourceSide(self.side))


@dataclass(frozen=True, slots=True)
class PendingTransaction:
    """Qualified pending source transaction awaiting chain inclusion."""

    chain_id: int
    transaction_hash: str
    sender: str
    nonce: int
    target: str
    observed_at: datetime
    expires_at: datetime
    matches: tuple[PendingSourceMatch, ...]

    def __post_init__(self) -> None:
        """Normalize and validate transaction identity and lifetime."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        object.__setattr__(
            self,
            "transaction_hash",
            _SourceTransactionValidation.bytes32(
                self.transaction_hash,
                name="transaction_hash",
            ),
        )
        object.__setattr__(
            self,
            "sender",
            _SourceTransactionValidation.address(self.sender, name="sender"),
        )
        if self.nonce < 0:
            raise ValueError("nonce must be non-negative")
        object.__setattr__(
            self,
            "target",
            _SourceTransactionValidation.address(self.target, name="target"),
        )
        object.__setattr__(
            self,
            "observed_at",
            _SourceTransactionValidation.aware(self.observed_at, name="observed_at"),
        )
        object.__setattr__(
            self,
            "expires_at",
            _SourceTransactionValidation.aware(self.expires_at, name="expires_at"),
        )
        if self.expires_at <= self.observed_at:
            raise ValueError("expires_at must be later than observed_at")
        if not self.matches:
            raise ValueError("matches must contain at least one qualified order")


@dataclass(frozen=True, slots=True)
class ReservedTransaction:
    """Unique atomic reservation for a pending source transaction."""

    reservation_id: str
    transaction: PendingTransaction
    reserved_at: datetime

    def __post_init__(self) -> None:
        """Validate the stable reservation identity."""

        object.__setattr__(
            self,
            "reservation_id",
            _SourceTransactionValidation.non_empty(
                self.reservation_id,
                name="reservation_id",
            ),
        )
        object.__setattr__(
            self,
            "reserved_at",
            _SourceTransactionValidation.aware(self.reserved_at, name="reserved_at"),
        )


@dataclass(frozen=True, slots=True)
class EvmHead:
    """One Polygon head notification that may make receipts available."""

    chain_id: int
    number: int
    block_hash: str
    parent_hash: str
    observed_at: datetime

    def __post_init__(self) -> None:
        """Validate block identity and timestamp."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if self.number < 0:
            raise ValueError("number must be non-negative")
        object.__setattr__(
            self,
            "block_hash",
            _SourceTransactionValidation.bytes32(self.block_hash, name="block_hash"),
        )
        object.__setattr__(
            self,
            "parent_hash",
            _SourceTransactionValidation.bytes32(self.parent_hash, name="parent_hash"),
        )
        object.__setattr__(
            self,
            "observed_at",
            _SourceTransactionValidation.aware(self.observed_at, name="observed_at"),
        )


@dataclass(frozen=True, slots=True)
class EvmReceipt:
    """Normalized EVM receipt used by confirmation logic."""

    chain_id: int
    transaction_hash: str
    status: ReceiptStatus
    block_number: int
    block_hash: str
    logs: tuple[dict[str, object], ...]

    def __post_init__(self) -> None:
        """Validate receipt identity and status."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        object.__setattr__(
            self,
            "transaction_hash",
            _SourceTransactionValidation.bytes32(
                self.transaction_hash,
                name="transaction_hash",
            ),
        )
        object.__setattr__(self, "status", ReceiptStatus(self.status))
        if self.block_number < 0:
            raise ValueError("block_number must be non-negative")
        object.__setattr__(
            self,
            "block_hash",
            _SourceTransactionValidation.bytes32(self.block_hash, name="block_hash"),
        )


@dataclass(frozen=True, slots=True)
class ConfirmedSourceFill:
    """Successfully included source fill suitable for correlation."""

    chain_id: int
    transaction_hash: str
    log_index: int
    order_hash: str
    leader_wallet: str
    watched_role: WatchedRole
    exchange_address: str
    token_id: str
    condition_id: str
    side: SourceSide
    shares: Decimal
    notional: Decimal
    price: Decimal
    block_number: int
    block_hash: str

    def __post_init__(self) -> None:
        """Normalize and validate fill identity and exact amounts."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        object.__setattr__(
            self,
            "transaction_hash",
            _SourceTransactionValidation.bytes32(
                self.transaction_hash,
                name="transaction_hash",
            ),
        )
        if self.log_index < 0:
            raise ValueError("log_index must be non-negative")
        object.__setattr__(
            self,
            "order_hash",
            _SourceTransactionValidation.bytes32(self.order_hash, name="order_hash"),
        )
        object.__setattr__(
            self,
            "leader_wallet",
            _SourceTransactionValidation.address(self.leader_wallet, name="leader_wallet"),
        )
        object.__setattr__(self, "watched_role", WatchedRole(self.watched_role))
        object.__setattr__(
            self,
            "exchange_address",
            _SourceTransactionValidation.address(
                self.exchange_address,
                name="exchange_address",
            ),
        )
        object.__setattr__(
            self,
            "token_id",
            _SourceTransactionValidation.non_empty(self.token_id, name="token_id"),
        )
        object.__setattr__(
            self,
            "condition_id",
            _SourceTransactionValidation.bytes32(self.condition_id, name="condition_id"),
        )
        object.__setattr__(self, "side", SourceSide(self.side))
        object.__setattr__(
            self,
            "shares",
            _SourceTransactionValidation.positive_decimal(self.shares, name="shares"),
        )
        object.__setattr__(
            self,
            "notional",
            _SourceTransactionValidation.positive_decimal(self.notional, name="notional"),
        )
        price = _SourceTransactionValidation.positive_decimal(self.price, name="price")
        if price > Decimal("1"):
            raise ValueError("price must be less than or equal to one")
        object.__setattr__(self, "price", price)
        if self.block_number < 0:
            raise ValueError("block_number must be non-negative")
        object.__setattr__(
            self,
            "block_hash",
            _SourceTransactionValidation.bytes32(self.block_hash, name="block_hash"),
        )

    @property
    def identity(self) -> SourceFillKey:
        """Return the durable composite identity for this source fill."""

        return SourceFillKey(
            chain_id=self.chain_id,
            transaction_hash=self.transaction_hash,
            log_index=self.log_index,
            order_hash=self.order_hash,
            leader_wallet=self.leader_wallet,
            watched_role=self.watched_role,
        )


@dataclass(frozen=True, slots=True)
class ConfirmedTransaction:
    """Confirmed source transaction and all matching copyable fills."""

    reservation: ReservedTransaction
    block_number: int
    block_hash: str
    fills: tuple[ConfirmedSourceFill, ...]

    def __post_init__(self) -> None:
        """Validate inclusion metadata and non-empty fills."""

        if self.block_number < 0:
            raise ValueError("block_number must be non-negative")
        object.__setattr__(
            self,
            "block_hash",
            _SourceTransactionValidation.bytes32(self.block_hash, name="block_hash"),
        )
        if not self.fills:
            raise ValueError("fills must contain at least one confirmed source fill")


@dataclass(frozen=True, slots=True)
class LeaderTradeCorrelation:
    """Feed event waiting to correlate with a confirmed source fill."""

    source_event_id: str
    follower_account: str
    copy_task_id: str
    chain_id: int
    transaction_hash: str
    leader_wallet: str
    token_id: str
    side: SourceSide
    order_hash: str | None = None
    log_index: int | None = None

    def __post_init__(self) -> None:
        """Normalize source and consumer identity."""

        object.__setattr__(
            self,
            "source_event_id",
            _SourceTransactionValidation.non_empty(
                self.source_event_id,
                name="source_event_id",
            ),
        )
        object.__setattr__(
            self,
            "follower_account",
            _SourceTransactionValidation.address(
                self.follower_account,
                name="follower_account",
            ),
        )
        object.__setattr__(
            self,
            "copy_task_id",
            _SourceTransactionValidation.non_empty(self.copy_task_id, name="copy_task_id"),
        )
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        object.__setattr__(
            self,
            "transaction_hash",
            _SourceTransactionValidation.bytes32(
                self.transaction_hash,
                name="transaction_hash",
            ),
        )
        object.__setattr__(
            self,
            "leader_wallet",
            _SourceTransactionValidation.address(self.leader_wallet, name="leader_wallet"),
        )
        object.__setattr__(
            self,
            "token_id",
            _SourceTransactionValidation.non_empty(self.token_id, name="token_id"),
        )
        object.__setattr__(self, "side", SourceSide(self.side))
        if self.order_hash is not None:
            object.__setattr__(
                self,
                "order_hash",
                _SourceTransactionValidation.bytes32(self.order_hash, name="order_hash"),
            )
        if self.log_index is not None and self.log_index < 0:
            raise ValueError("log_index must be non-negative")


@dataclass(frozen=True, slots=True)
class SourceFillKey:
    """Chain and event identity for one copyable source fill."""

    chain_id: int
    transaction_hash: str
    log_index: int
    order_hash: str
    leader_wallet: str
    watched_role: WatchedRole


@dataclass(frozen=True, slots=True)
class CopyConsumptionKey:
    """Follower-scoped idempotency key for one confirmed source fill."""

    follower_account: str
    copy_task_id: str
    source_fill: SourceFillKey


@dataclass(frozen=True, slots=True)
class ConfirmedExecutionClaim:
    """Atomic authorization to process one confirmed source fill once."""

    consumption_key: CopyConsumptionKey
    correlation: LeaderTradeCorrelation
    fill: ConfirmedSourceFill


@dataclass(frozen=True, slots=True)
class SourceTransactionRecordSnapshot:
    """Persistable state for one source transaction reservation."""

    reservation: ReservedTransaction
    state: TransactionState
    correlations: tuple[LeaderTradeCorrelation, ...]
    confirmed: ConfirmedTransaction | None
    terminal_reason: str | None


@dataclass(frozen=True, slots=True)
class SourceTransactionSnapshot:
    """Immutable crash-recovery image of source transaction state."""

    records: tuple[SourceTransactionRecordSnapshot, ...]
    waiting_correlations: tuple[LeaderTradeCorrelation, ...]
    claimed: tuple[CopyConsumptionKey, ...]

    @classmethod
    def empty(cls) -> SourceTransactionSnapshot:
        """Return an empty persisted state image."""

        return cls(records=(), waiting_correlations=(), claimed=())
