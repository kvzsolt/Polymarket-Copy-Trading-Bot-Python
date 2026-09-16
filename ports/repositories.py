"""Repository ports (ABCs).

Each ABC captures the **service-facing** surface of a persistence adapter.
Concrete implementations in ``storage/postgres/`` inherit from these ABCs and
may expose additional helpers — services must only depend on the methods
declared here.

All abstract methods are declared with ``async def`` so static checkers
(mypy, pyright) and runtime ``inspect.iscoroutinefunction`` can enforce
the coroutine contract. Concrete Postgres adapters in
``storage/postgres/`` implement each method as ``async def``; legacy
sync mocks must wrap their implementations in ``AsyncMock`` so awaited
calls return a coroutine.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Record dataclasses live alongside the concrete repos. We import them under
# TYPE_CHECKING so the domain port module stays free of adapter imports at
# runtime — and so storage adapters can subclass these ABCs without creating
# a circular import.
if TYPE_CHECKING:
    from storage.postgres.cursors_repo import TradeCursor
    from storage.postgres.fills_repo import FillRecord
    from storage.postgres.leaders_repo import LeaderTierRecord
    from storage.postgres.orders_repo import OrderSubmissionRecord
    from storage.postgres.positions_repo import PositionRecord


class AbstractOrderRepository(ABC):
    """Order submission, idempotency, fills, and processed-trade tracking."""

    @abstractmethod
    async def try_acquire_submission_lock(
        self,
        key: str,
        user_id: str,
        leader_id: str | None,
        request_payload: dict,
    ) -> bool: ...

    @abstractmethod
    async def is_submission_locked(self, key: str) -> bool: ...

    @abstractmethod
    async def get_by_idempotency_key(self, key: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def save_submission(self, record: OrderSubmissionRecord) -> None: ...

    @abstractmethod
    async def mark_fill(
        self,
        *,
        order_id: str,
        fill: dict[str, Any],
        user_id: str | None = None,
        leader_id: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def has_processed_trade(self, trade_key: str) -> bool: ...

    @abstractmethod
    async def try_claim_processed_trade(
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
    async def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def mark_processed_trade(self, *args: Any, **kwargs: Any) -> None: ...

    @abstractmethod
    async def purge_old_processed_trades(self, max_age_seconds: float = 30 * 86400) -> int: ...


class AbstractPositionRepository(ABC):
    """Per-user position CRUD and fill application."""

    @abstractmethod
    async def list_open_positions(self, user_id: str) -> list[PositionRecord]: ...

    @abstractmethod
    async def get_position(
        self, user_id: str, condition_id: str, *, token_id: str | None = None
    ) -> PositionRecord | None: ...

    @abstractmethod
    async def list_positions_for_condition(
        self, user_id: str, condition_id: str
    ) -> list[PositionRecord]: ...

    @abstractmethod
    async def upsert_position(self, record: PositionRecord) -> None: ...

    @abstractmethod
    async def delete_position(
        self, user_id: str, condition_id: str, *, token_id: str | None = None
    ) -> None: ...

    @abstractmethod
    async def apply_fill(self, *args: Any, **kwargs: Any) -> Any: ...


class AbstractFillRepository(ABC):
    """Append-only fill event store."""

    @abstractmethod
    async def append_fill(
        self,
        record: FillRecord,
        *,
        raw_payload: dict | None = None,
    ) -> int: ...

    @abstractmethod
    async def list_fills_for_user(self, user_id: str) -> list[dict]: ...

    @abstractmethod
    async def list_fills_for_user_condition(
        self, user_id: str, condition_id: str
    ) -> list[dict]: ...


class AbstractLeaderRepository(ABC):
    """Active-roster lookup used by the copy-trading hot path."""

    @abstractmethod
    async def list_active(
        self,
        limit: int | None = None,
        config_hash: str | None = None,
    ) -> list[str]: ...

    @abstractmethod
    async def get_tier(self, wallet: str) -> LeaderTierRecord: ...


class AbstractCursorRepository(ABC):
    """Per-leader trade-ingestion cursor."""

    @abstractmethod
    async def get_cursor(self, leader_id: str) -> TradeCursor: ...

    @abstractmethod
    async def set_cursor(self, leader_id: str, cursor: TradeCursor) -> None: ...


class AbstractDiscoveryRepository(ABC):
    """Trader-discovery persistence surface.

    Only the methods actually consumed by services are declared here.
    Concrete `DiscoveryRepo` implementations may offer additional helpers.

    This mirrors the single-generation discovery pipeline: cycle lifecycle
    (``begin_cycle`` / ``persist_cycle`` / ``complete_cycle``), roster reads
    consumed by ``DiscoveryFunnel`` and the reporting scripts, and the
    longitudinal backfill store consumed by ``BackfillService``. The
    structural ``services.discovery.funnel.DiscoveryRepository`` Protocol
    covers the funnel's subset of this same surface.
    """

    @abstractmethod
    async def begin_cycle(self, config_hash: str) -> str: ...

    @abstractmethod
    async def persist_cycle(
        self,
        cycle_id: str,
        candidates: list[Any],
        config_hash: str,
    ) -> None: ...

    @abstractmethod
    async def complete_cycle(
        self,
        cycle_id: str,
        *,
        status: str,
        candidate_count: int,
        kept_count: int,
        error: str | None = None,
    ) -> None: ...

    @abstractmethod
    async def score_history(
        self,
        wallet: str,
        *,
        since_ts: float,
    ) -> list[Mapping[str, Any]]: ...

    @abstractmethod
    async def latest_candidates(
        self,
        *,
        config_hash: str | None = None,
    ) -> list[Any]:
        """Accepted candidates from the latest completed cycle.

        ``config_hash`` scopes the read to cycles scored under that config so
        callers that re-stamp rows with the current hash cannot publish a
        base score computed under an older one.
        """

    @abstractmethod
    async def persist_rescore(
        self,
        candidates: Sequence[Any],
        config_hash: str,
    ) -> None: ...

    @abstractmethod
    async def list_snapshots(self, *, limit: int = 100) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def upsert_pnl_daily(
        self,
        wallet: str,
        rows: Iterable[tuple[date, Decimal, Mapping[str, Any]]],
    ) -> None: ...

    @abstractmethod
    async def upsert_rank_history(
        self,
        wallet: str,
        rows: Iterable[tuple[float, int, int, float, Mapping[str, Any]]],
    ) -> None: ...

    @abstractmethod
    async def has_completed_backfill(self, wallet: str, config_hash: str) -> bool: ...

    @abstractmethod
    async def mark_backfill_complete(self, wallet: str, config_hash: str) -> None: ...

    @abstractmethod
    async def upsert_metric_timeseries(
        self,
        wallet: str,
        rows: Iterable[Mapping[str, Any]],
        config_hash: str,
    ) -> None: ...
