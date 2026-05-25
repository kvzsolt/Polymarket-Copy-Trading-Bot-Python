"""Repository ports (ABCs).

Each ABC captures the **service-facing** surface of a persistence adapter.
Concrete implementations in ``storage/postgres/`` inherit from these ABCs and
may expose additional helpers — services must only depend on the methods
declared here.

NOTE: the ABCs are declared synchronously here for historical reasons; the
concrete Postgres adapters are async. Treat each method as a coroutine
contract — callers must ``await`` them.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

# Record dataclasses live alongside the concrete repos. We import them under
# TYPE_CHECKING so the domain port module stays free of adapter imports at
# runtime — and so storage adapters can subclass these ABCs without creating
# a circular import.
if TYPE_CHECKING:
    from storage.postgres.cursors_repo import TradeCursor
    from storage.postgres.fills_repo import FillRecord
    from storage.postgres.leaders_repo import LeaderCandidateRecord, LeaderTierRecord
    from storage.postgres.orders_repo import OrderSubmissionRecord
    from storage.postgres.positions_repo import PositionRecord


class AbstractOrderRepository(ABC):
    """Order submission, idempotency, fills, and processed-trade tracking."""

    @abstractmethod
    def try_acquire_submission_lock(
        self,
        key: str,
        user_id: str,
        leader_id: str | None,
        request_payload: dict,
    ) -> bool: ...

    @abstractmethod
    def is_submission_locked(self, key: str) -> bool: ...

    @abstractmethod
    def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def save_submission(self, record: OrderSubmissionRecord) -> None: ...

    @abstractmethod
    def mark_fill(
        self,
        *,
        order_id: str,
        fill: dict[str, Any],
        user_id: str | None = None,
        leader_id: str | None = None,
    ) -> None: ...

    @abstractmethod
    def has_processed_trade(self, trade_key: str) -> bool: ...

    @abstractmethod
    def try_claim_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str = "in_progress",
        stale_claim_ttl_seconds: float = 300.0,
    ) -> bool: ...

    @abstractmethod
    def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def mark_processed_trade(self, *args: Any, **kwargs: Any) -> None: ...

    @abstractmethod
    def purge_old_processed_trades(self, max_age_seconds: float = 30 * 86400) -> int: ...


class AbstractPositionRepository(ABC):
    """Per-user position CRUD and fill application."""

    @abstractmethod
    def list_open_positions(self, user_id: str) -> list[PositionRecord]: ...

    @abstractmethod
    def get_position(
        self, user_id: str, condition_id: str, *, token_id: str | None = None
    ) -> PositionRecord | None: ...

    @abstractmethod
    def list_positions_for_condition(
        self, user_id: str, condition_id: str
    ) -> list[PositionRecord]: ...

    @abstractmethod
    def upsert_position(self, record: PositionRecord) -> None: ...

    @abstractmethod
    def delete_position(
        self, user_id: str, condition_id: str, *, token_id: str | None = None
    ) -> None: ...

    @abstractmethod
    def apply_fill(self, *args: Any, **kwargs: Any) -> Any: ...


class AbstractFillRepository(ABC):
    """Append-only fill event store."""

    @abstractmethod
    def append_fill(
        self,
        record: FillRecord,
        *,
        raw_payload: dict | None = None,
    ) -> int: ...

    @abstractmethod
    def list_fills_for_user(self, user_id: str) -> list[dict]: ...

    @abstractmethod
    def list_fills_for_user_condition(
        self, user_id: str, condition_id: str
    ) -> list[dict]: ...


class AbstractLeaderRepository(ABC):
    """Leader candidate and tier persistence."""

    @abstractmethod
    def upsert_candidates(self, records: list[LeaderCandidateRecord]) -> None: ...

    @abstractmethod
    def list_active(self) -> list[str]: ...

    @abstractmethod
    def get_tier(self, wallet: str) -> LeaderTierRecord: ...

    @abstractmethod
    def update_tier(
        self, wallet: str, follow_tier: str, tier_changed_at: float
    ) -> None: ...

    @abstractmethod
    def get_tier_for_leader(self, wallet: str) -> str: ...

    @abstractmethod
    def get_leaders_by_tier(self, tier: str) -> list[str]: ...


class AbstractCursorRepository(ABC):
    """Per-leader trade-ingestion cursor."""

    @abstractmethod
    def get_cursor(self, leader_id: str) -> TradeCursor: ...

    @abstractmethod
    def set_cursor(self, leader_id: str, cursor: TradeCursor) -> None: ...


class AbstractDiscoveryRepository(ABC):
    """Trader-discovery persistence surface.

    Only the methods actually consumed by services are declared here.
    Concrete `DiscoveryRepo` implementations may offer additional helpers.
    """

    @abstractmethod
    def upsert_trader(self, trader: Any) -> None: ...

    @abstractmethod
    def get_trader(self, wallet: str) -> Any | None: ...

    @abstractmethod
    def list_traders(self, *, limit: int = 100, offset: int = 0) -> list[Any]: ...

    @abstractmethod
    def upsert_market(self, market: Any) -> None: ...

    @abstractmethod
    def get_market(self, condition_id: str) -> Any | None: ...

    @abstractmethod
    def insert_trade(self, trade: Any) -> int | None: ...

    @abstractmethod
    def list_trades(self, *args: Any, **kwargs: Any) -> list[Any]: ...

    @abstractmethod
    def insert_position_snapshot(self, snapshot: Any) -> int | None: ...

    @abstractmethod
    def latest_position_snapshot(self, *args: Any, **kwargs: Any) -> Any | None: ...

    @abstractmethod
    def insert_orderbook_snapshot(self, snapshot: Any) -> int | None: ...
