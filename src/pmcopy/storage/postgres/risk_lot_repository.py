"""PostgreSQL adapter for restart-safe per-fill protective-risk lots."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import TracebackType
from typing import Protocol

import asyncpg

from pmcopy.services.copy_tasks.lots import (
    DurableRiskLot,
    ProtectiveLotExitFill,
    RiskLotExitApplication,
    RiskLotExitReducer,
    RiskLotInvariantError,
    RiskLotStatus,
    RiskLotValueParser,
)
from pmcopy.services.copy_tasks.protective_exits import (
    FillRiskLot,
    LayeredLevel,
    PendingLayerExit,
    ProtectiveRiskPolicy,
)

_ZERO = Decimal("0")


class TransactionContext(Protocol):
    """Async transaction context used by the repository."""

    async def __aenter__(self) -> object:
        """Enter one transaction."""

        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> object:
        """Exit one transaction."""

        ...


class DatabaseConnection(Protocol):
    """Narrow asyncpg-compatible connection surface."""

    def transaction(self) -> TransactionContext:
        """Return a transaction context."""

        ...

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one row."""

        ...

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[Mapping[str, object]]:
        """Return ordered rows."""

        ...

    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""

        ...


class AcquireContext(Protocol):
    """Pool acquisition context manager."""

    async def __aenter__(self) -> DatabaseConnection:
        """Return an acquired connection."""

        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> object:
        """Release an acquired connection."""

        ...


class DatabasePool(Protocol):
    """Narrow asyncpg-compatible pool surface."""

    def acquire(self) -> AcquireContext:
        """Acquire one connection."""

        ...


class RiskLotCodec:
    """Translate durable lot aggregates to and from PostgreSQL rows."""

    def encode_policy(self, policy: ProtectiveRiskPolicy) -> dict[str, object]:
        """Encode an immutable protective policy as exact JSON values.

        Args:
            policy: Policy snapshot captured at entry fill time.

        Returns:
            JSON-compatible mapping with decimals represented as strings.
        """

        return {
            "enabled": policy.enabled,
            "stop_loss_percent": self._optional_decimal(policy.stop_loss_percent),
            "stop_loss_price": self._optional_decimal(policy.stop_loss_price),
            "take_profit_percent": self._optional_decimal(policy.take_profit_percent),
            "take_profit_price": self._optional_decimal(policy.take_profit_price),
            "trailing_stop_percent": self._optional_decimal(policy.trailing_stop_percent),
            "layered_stop_loss": [self._encode_level(level) for level in policy.layered_stop_loss],
            "layered_take_profit": [
                self._encode_level(level) for level in policy.layered_take_profit
            ],
        }

    def decode(self, row: Mapping[str, object]) -> DurableRiskLot:
        """Decode and validate one asyncpg risk-lot row.

        Args:
            row: Database mapping containing the canonical lot projection.

        Returns:
            Fully validated immutable aggregate.
        """

        policy_payload = self._json_mapping(row.get("policy_snapshot"), "policy_snapshot")
        levels_payload = self._json_list(row.get("triggered_levels"), "triggered_levels")
        return DurableRiskLot(
            follower_account=self._text(row, "follower_account"),
            leader_wallet=self._text(row, "leader_wallet"),
            entry_fill_id=self._text(row, "entry_fill_id"),
            lot=FillRiskLot(
                lot_id=self._text(row, "lot_id"),
                task_id=self._text(row, "task_id"),
                market_id=self._text(row, "market_id"),
                outcome_id=self._text(row, "outcome_id"),
                entry_price=self._decimal(row, "entry_price"),
                original_shares=self._decimal(row, "original_shares"),
                remaining_shares=self._decimal(row, "remaining_shares"),
                opened_at=self._datetime(row, "opened_at"),
                peak_price=self._decimal(row, "peak_price"),
                triggered_levels=tuple(self._level_id(value) for value in levels_payload),
                pending_exits=self.decode_pending(row.get("pending_exits", [])),
            ),
            policy=self.decode_policy(policy_payload),
            status=RiskLotStatus(self._text(row, "status")),
            version=self._integer(row, "version"),
        )

    @staticmethod
    def encode_pending(pending: tuple[PendingLayerExit, ...]) -> str:
        """Serialize outstanding layer shares without binary floating-point conversion."""
        return json.dumps(
            [
                {"level_id": item.level_id, "remaining_shares": str(item.remaining_shares)}
                for item in pending
            ]
        )

    def decode_pending(self, raw: object) -> tuple[PendingLayerExit, ...]:
        """Validate durable outstanding layer targets."""
        pending: list[PendingLayerExit] = []
        for item in self._json_list(raw, "pending_exits"):
            if not isinstance(item, Mapping):
                raise ValueError("Pending protective exit must be an object")
            pending.append(
                PendingLayerExit(
                    self._text(item, "level_id"),
                    self._decimal(item, "remaining_shares"),
                )
            )
        return tuple(pending)

    def decode_policy(self, payload: Mapping[str, object]) -> ProtectiveRiskPolicy:
        """Decode exact protective settings from a stored policy snapshot."""
        return ProtectiveRiskPolicy(
            enabled=self._boolean(payload, "enabled", default=True),
            stop_loss_percent=self._optional_payload_decimal(payload, "stop_loss_percent"),
            stop_loss_price=self._optional_payload_decimal(payload, "stop_loss_price"),
            take_profit_percent=self._optional_payload_decimal(payload, "take_profit_percent"),
            take_profit_price=self._optional_payload_decimal(payload, "take_profit_price"),
            trailing_stop_percent=self._optional_payload_decimal(
                payload,
                "trailing_stop_percent",
            ),
            layered_stop_loss=self._decode_levels(payload.get("layered_stop_loss", [])),
            layered_take_profit=self._decode_levels(payload.get("layered_take_profit", [])),
        )

    def _decode_levels(self, value: object) -> tuple[LayeredLevel, ...]:
        items = self._json_list(value, "layered levels")
        levels: list[LayeredLevel] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise ValueError("layered policy entries must be mappings")
            levels.append(
                LayeredLevel(
                    trigger_percent=self._payload_decimal(item, "trigger_percent"),
                    exit_fraction=self._payload_decimal(item, "exit_fraction"),
                )
            )
        return tuple(levels)

    @staticmethod
    def _encode_level(level: LayeredLevel) -> dict[str, str]:
        return {
            "trigger_percent": str(level.trigger_percent),
            "exit_fraction": str(level.exit_fraction),
        }

    @staticmethod
    def _optional_decimal(value: Decimal | None) -> str | None:
        return None if value is None else str(value)

    @staticmethod
    def _json_mapping(value: object, name: str) -> Mapping[str, object]:
        decoded = json.loads(value) if isinstance(value, str) else value
        if not isinstance(decoded, Mapping):
            raise ValueError(f"risk lot {name} must be a JSON object")
        return decoded

    @staticmethod
    def _json_list(value: object, name: str) -> list[object]:
        decoded = json.loads(value) if isinstance(value, str) else value
        if not isinstance(decoded, list):
            raise ValueError(f"risk lot {name} must be a JSON array")
        return decoded

    @staticmethod
    def _text(row: Mapping[str, object], name: str) -> str:
        value = row.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"risk lot {name} must be non-empty text")
        return value.strip()

    @staticmethod
    def _datetime(row: Mapping[str, object], name: str) -> datetime:
        value = row.get(name)
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise ValueError(f"risk lot {name} must be timezone-aware datetime")
        return value

    @staticmethod
    def _decimal(row: Mapping[str, object], name: str) -> Decimal:
        return RiskLotCodec._parse_decimal(row.get(name), name=name)

    @staticmethod
    def _parse_decimal(value: object, *, name: str) -> Decimal:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError(f"risk lot {name} must be decimal-like") from exc
        if not parsed.is_finite() or parsed < _ZERO:
            raise ValueError(f"risk lot {name} must be finite and non-negative")
        return parsed

    @staticmethod
    def _integer(row: Mapping[str, object], name: str) -> int:
        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            raise ValueError(f"risk lot {name} must be an integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"risk lot {name} must be an integer") from exc
        if parsed < 1:
            raise ValueError(f"risk lot {name} must be positive")
        return parsed

    @staticmethod
    def _payload_decimal(payload: Mapping[str, object], name: str) -> Decimal:
        return RiskLotCodec._parse_decimal(payload.get(name), name=name)

    @staticmethod
    def _optional_payload_decimal(
        payload: Mapping[str, object],
        name: str,
    ) -> Decimal | None:
        value = payload.get(name)
        return None if value is None else RiskLotCodec._parse_decimal(value, name=name)

    @staticmethod
    def _boolean(payload: Mapping[str, object], name: str, *, default: bool) -> bool:
        value = payload.get(name, default)
        if not isinstance(value, bool):
            raise ValueError(f"risk lot policy {name} must be boolean")
        return value

    @staticmethod
    def _level_id(value: object) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError("risk lot triggered level must be non-empty text")
        return result


class PostgresRiskLotRepository:
    """Persist protective-risk lots and exit fills atomically."""

    def __init__(self, pool: DatabasePool, *, codec: RiskLotCodec | None = None) -> None:
        self._pool = pool
        self._codec = codec or RiskLotCodec()

    async def create_if_absent(self, lot: DurableRiskLot) -> bool:
        """Create one entry-fill lot idempotently and detect conflicts."""

        async with self._pool.acquire() as connection:
            return await self.create_in_connection(connection, lot)

    async def create_in_connection(
        self,
        connection: DatabaseConnection | asyncpg.pool.PoolConnectionProxy,
        lot: DurableRiskLot,
    ) -> bool:
        """Append a lot inside the caller's fill-accounting transaction."""
        created = await connection.fetchrow(
            """
            INSERT INTO copy_task_risk_lots
                (lot_id, entry_fill_id, follower_account, task_id,
                 leader_wallet, market_id, outcome_id, entry_price,
                 original_shares, remaining_shares, peak_price,
                 policy_snapshot, triggered_levels, status, opened_at,
                 version, updated_at, pending_exits)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                 $12::jsonb, $13::jsonb, $14, $15, $16, NOW(), $17::jsonb)
            ON CONFLICT DO NOTHING
            RETURNING 1 AS created
            """,
            lot.lot.lot_id,
            lot.entry_fill_id,
            lot.follower_account,
            lot.lot.task_id,
            lot.leader_wallet,
            lot.lot.market_id,
            lot.lot.outcome_id,
            lot.lot.entry_price,
            lot.lot.original_shares,
            lot.lot.remaining_shares,
            lot.lot.peak_price,
            json.dumps(self._codec.encode_policy(lot.policy), sort_keys=True),
            json.dumps(list(lot.lot.triggered_levels)),
            lot.status.value,
            lot.lot.opened_at,
            lot.version,
            self._codec.encode_pending(lot.lot.pending_exits),
        )
        if created is not None:
            return True
        existing = await connection.fetchrow(self._select_sql(), lot.lot.lot_id)
        if existing is None:
            raise RiskLotInvariantError("risk lot conflict did not resolve to an existing row")
        if self._codec.decode(existing) == lot:
            return False
        raise RiskLotInvariantError("risk lot identity conflicts with existing state")

    async def get(self, lot_id: str) -> DurableRiskLot | None:
        """Return one durable lot by identity."""

        identity = self._required_text(lot_id, name="lot_id")
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(self._select_sql(), identity)
        return None if row is None else self._codec.decode(row)

    async def list_open(self, follower_account: str) -> tuple[DurableRiskLot, ...]:
        """Return all open follower lots in deterministic order."""

        follower = self._required_text(follower_account, name="follower_account")
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                f"{self._projection_sql()} "
                "WHERE follower_account = $1 AND status = 'open' "
                "ORDER BY opened_at, lot_id",
                follower,
            )
        return tuple(self._codec.decode(row) for row in rows)

    async def list_all_open(self) -> tuple[DurableRiskLot, ...]:
        """Return every open lot for startup restoration."""

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                f"{self._projection_sql()} "
                "WHERE status = 'open' ORDER BY follower_account, opened_at, lot_id"
            )
        return tuple(self._codec.decode(row) for row in rows)

    async def observe_peak(self, lot_id: str, price: Decimal) -> DurableRiskLot:
        """Raise a trailing high-water mark atomically and monotonically."""

        identity = self._required_text(lot_id, name="lot_id")
        observed = RiskLotValueParser.positive_decimal(price, name="price")
        if observed > Decimal("1"):
            raise ValueError("price must be <= 1")
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE copy_task_risk_lots
                   SET peak_price = $2, version = version + 1, updated_at = NOW()
                 WHERE lot_id = $1
                   AND status = 'open'
                   AND peak_price < $2
                RETURNING lot_id, entry_fill_id, follower_account, task_id,
                          leader_wallet, market_id, outcome_id, entry_price,
                          original_shares, remaining_shares, peak_price,
                          policy_snapshot, triggered_levels, pending_exits, status, opened_at,
                          version
                """,
                identity,
                observed,
            )
            if row is None:
                row = await connection.fetchrow(self._select_sql(), identity)
        if row is None:
            raise LookupError(f"risk lot does not exist: {identity}")
        return self._codec.decode(row)

    async def record_exit_fill(
        self,
        fill: ProtectiveLotExitFill,
    ) -> RiskLotExitApplication:
        """Apply one exit fill and lot mutation in a single transaction."""

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                return await self.record_exit_in_connection(connection, fill)

    async def record_exit_in_connection(
        self,
        connection: DatabaseConnection | asyncpg.pool.PoolConnectionProxy,
        fill: ProtectiveLotExitFill,
    ) -> RiskLotExitApplication:
        """Consume a lot within the caller's fill-accounting transaction."""
        existing_fill = await connection.fetchrow(
            """
            SELECT fill_id, lot_id, shares, price, level_id, occurred_at, level_target_shares
              FROM copy_task_protective_exit_fill_ledger
             WHERE fill_id = $1
             FOR UPDATE
            """,
            fill.fill_id,
        )
        lot_row = await connection.fetchrow(
            f"{self._select_sql()} FOR UPDATE",
            fill.lot_id,
        )
        if lot_row is None:
            raise LookupError(f"risk lot does not exist: {fill.lot_id}")
        current = self._codec.decode(lot_row)
        if existing_fill is not None:
            self._validate_existing_fill(existing_fill, fill)
            return RiskLotExitApplication(created=False, lot=current)
        expected = RiskLotExitReducer.apply(current, fill)
        levels = expected.lot.triggered_levels
        status = expected.status
        await connection.execute(
            """
            INSERT INTO copy_task_protective_exit_fill_ledger
                (fill_id, lot_id, shares, price, level_id, occurred_at, created_at, level_target_shares)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)
            """,
            fill.fill_id,
            fill.lot_id,
            fill.shares,
            fill.price,
            fill.level_id,
            fill.occurred_at,
            fill.level_target_shares,
        )
        updated_row = await connection.fetchrow(
            """
            UPDATE copy_task_risk_lots
               SET remaining_shares = remaining_shares - $2,
                   triggered_levels = $3::jsonb,
                   pending_exits = $6::jsonb,
                   status = $4,
                   closed_at = CASE WHEN $4 = 'closed' THEN $5::timestamptz ELSE NULL END,
                   version = version + 1,
                   updated_at = NOW()
             WHERE lot_id = $1
            RETURNING lot_id, entry_fill_id, follower_account, task_id,
                      leader_wallet, market_id, outcome_id, entry_price,
                      original_shares, remaining_shares, peak_price,
                      policy_snapshot, triggered_levels, pending_exits, status, opened_at,
                      version
            """,
            fill.lot_id,
            fill.shares,
            json.dumps(list(levels)),
            status.value,
            fill.occurred_at,
            self._codec.encode_pending(expected.lot.pending_exits),
        )
        if updated_row is None:
            raise RiskLotInvariantError("risk lot update returned no row")
        updated = self._codec.decode(updated_row)
        if updated != expected:
            raise RiskLotInvariantError("risk lot update contradicted expected state")
        return RiskLotExitApplication(created=True, lot=updated)

    def _validate_existing_fill(
        self,
        row: Mapping[str, object],
        fill: ProtectiveLotExitFill,
    ) -> None:
        existing = ProtectiveLotExitFill(
            fill_id=self._row_text(row, "fill_id"),
            lot_id=self._row_text(row, "lot_id"),
            shares=RiskLotCodec._decimal(row, "shares"),
            price=RiskLotCodec._decimal(row, "price"),
            level_id=self._optional_row_text(row, "level_id"),
            occurred_at=RiskLotCodec._datetime(row, "occurred_at"),
            level_target_shares=self._codec._optional_payload_decimal(row, "level_target_shares"),
        )
        if existing != fill:
            raise RiskLotInvariantError("protective fill identity conflicts with existing state")

    @staticmethod
    def _projection_sql() -> str:
        return """
            SELECT lot_id, entry_fill_id, follower_account, task_id,
                   leader_wallet, market_id, outcome_id, entry_price,
                   original_shares, remaining_shares, peak_price,
                   policy_snapshot, triggered_levels, pending_exits, status, opened_at,
                   version
              FROM copy_task_risk_lots
        """

    @classmethod
    def _select_sql(cls) -> str:
        return f"{cls._projection_sql()} WHERE lot_id = $1"

    @staticmethod
    def _required_text(value: object, *, name: str) -> str:
        result = str(value or "").strip()
        if not result:
            raise ValueError(f"{name} is required")
        return result

    @staticmethod
    def _row_text(row: Mapping[str, object], name: str) -> str:
        value = row.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"protective fill {name} must be non-empty text")
        return value.strip()

    @staticmethod
    def _optional_row_text(row: Mapping[str, object], name: str) -> str | None:
        value = row.get(name)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"protective fill {name} must be text or null")
        normalized = value.strip()
        return normalized or None


__all__ = ["PostgresRiskLotRepository", "RiskLotCodec"]
