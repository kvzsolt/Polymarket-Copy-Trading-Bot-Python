"""PostgreSQL adapter for durable copy-task configuration.

The adapter stores nested task policies as canonical JSON strings while
keeping task identity, lifecycle, and optimistic version fields queryable.
Business services depend on the repository protocol in
``services.copy_tasks.service`` and never import this adapter.
"""

from __future__ import annotations

from pmcopy.storage.postgres.risk_lot_repository import RiskLotCodec

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import time
from decimal import Decimal
from typing import Protocol

from pmcopy.services.copy_tasks.domain import (
    CopySideFilter,
    CopyTask,
    CopyTaskStatus,
    MonitoringMode,
    PriceOffsetDirection,
    PriceOffsetMode,
    ReactionPolicy,
    TaskMarketFilter,
    TaskPricePolicy,
    TaskSchedule,
    TaskSizingMode,
    TaskSizingPolicy,
    TaskSpendPolicy,
    TaskTriggerPolicy,
)
from pmcopy.services.copy_tasks.service import CopyTaskConflict, CopyTaskNotFound


class DatabasePool(Protocol):
    """Small async database surface required by this repository."""

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first column of the first row."""

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one database row."""

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Return a sequence of database rows."""

        ...


class CopyTaskCodec:
    """Translate immutable domain tasks to and from JSON-safe records."""

    def encode_configuration(self, task: CopyTask) -> str:
        """Serialize nested task policies without converting decimals to floats."""

        value: dict[str, object] = {
            "protective_risk": RiskLotCodec().encode_policy(task.protective_risk),
            "trigger": {
                "enabled": task.trigger.enabled,
                "min_notional": format(task.trigger.min_notional, "f"),
                "max_notional": format(task.trigger.max_notional, "f"),
            },
            "spend": {
                "enabled": task.spend.enabled,
                "max_total": format(task.spend.max_total, "f"),
                "max_daily": format(task.spend.max_daily, "f"),
                "max_per_market": format(task.spend.max_per_market, "f"),
                "one_entry_per_market": task.spend.one_entry_per_market,
                "global_one_entry_per_market": task.spend.global_one_entry_per_market,
                "one_entry_per_sports_type": task.spend.one_entry_per_sports_type,
            },
            "price": {
                "enabled": task.price.enabled,
                "min_price": format(task.price.min_price, "f"),
                "max_price": format(task.price.max_price, "f"),
                "offset_mode": task.price.offset_mode.value,
                "offset_direction": task.price.offset_direction.value,
                "offset": format(task.price.offset, "f"),
                "max_slippage_percent": format(task.price.max_slippage_percent, "f"),
            },
            "reaction": {
                "enabled": task.reaction.enabled,
                "follow_buys": task.reaction.follow_buys,
                "copy_sells": task.reaction.copy_sells,
                "buy_on_sells": task.reaction.buy_on_sells,
                "counter_trade": task.reaction.counter_trade,
                "copy_side": task.reaction.copy_side.value,
                "copy_merges": task.reaction.copy_merges,
                "copy_splits": task.reaction.copy_splits,
                "copy_conversions": task.reaction.copy_conversions,
            },
            "market_filter": {
                "enabled": task.market_filter.enabled,
                "include_keywords": list(task.market_filter.include_keywords),
                "exclude_keywords": list(task.market_filter.exclude_keywords),
                "categories": list(task.market_filter.categories),
                "tokens": list(task.market_filter.tokens),
                "timeframes": list(task.market_filter.timeframes),
                "min_liquidity_usdc": format(
                    task.market_filter.min_liquidity_usdc,
                    "f",
                ),
                "min_volume_24h_usdc": format(
                    task.market_filter.min_volume_24h_usdc,
                    "f",
                ),
                "max_days_until_close": format(
                    task.market_filter.max_days_until_close,
                    "f",
                ),
                "allow_expired_unresolved": task.market_filter.allow_expired_unresolved,
            },
            "schedule": {
                "enabled": task.schedule.enabled,
                "timezone_name": task.schedule.timezone_name,
                "start": task.schedule.start.isoformat(),
                "end": task.schedule.end.isoformat(),
            },
            "sizing": {
                "enabled": task.sizing.enabled,
                "mode": task.sizing.mode.value,
                "percentage": format(task.sizing.percentage, "f"),
                "fixed_amount": format(task.sizing.fixed_amount, "f"),
                "trade_multiplier": format(task.sizing.trade_multiplier, "f"),
                "weighted_multiplier": format(task.sizing.weighted_multiplier, "f"),
                "buy_min_amount": format(task.sizing.buy_min_amount, "f"),
                "buy_max_amount": format(task.sizing.buy_max_amount, "f"),
                "copyable_balance_percent": format(
                    task.sizing.copyable_balance_percent,
                    "f",
                ),
                "proportional_balance_cap_enabled": (task.sizing.proportional_balance_cap_enabled),
                "require_leader_balance": task.sizing.require_leader_balance,
                "strict_fixed_amount": task.sizing.strict_fixed_amount,
                "strict_exact_mirror": task.sizing.strict_exact_mirror,
            },
        }
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    def decode_row(self, row: Mapping[str, object]) -> CopyTask:
        """Build a validated task from one database row."""

        configuration = self._configuration(row.get("configuration"))
        trigger = self._mapping(configuration, "trigger")
        spend = self._mapping(configuration, "spend")
        price = self._mapping(configuration, "price")
        reaction = self._mapping(configuration, "reaction")
        market_filter = self._mapping(configuration, "market_filter")
        schedule = self._mapping(configuration, "schedule")
        sizing = self._optional_mapping(configuration, "sizing")
        sizing_policy = (
            TaskSizingPolicy(mode=TaskSizingMode(self._text(row, "sizing_mode")))
            if sizing is None
            else TaskSizingPolicy(
                enabled=self._boolean(sizing, "enabled"),
                mode=TaskSizingMode(self._text(sizing, "mode")),
                percentage=self._decimal(sizing, "percentage"),
                fixed_amount=self._decimal(sizing, "fixed_amount"),
                trade_multiplier=self._decimal(sizing, "trade_multiplier"),
                weighted_multiplier=self._decimal(sizing, "weighted_multiplier"),
                buy_min_amount=self._decimal(sizing, "buy_min_amount"),
                buy_max_amount=self._decimal(sizing, "buy_max_amount"),
                copyable_balance_percent=self._decimal(
                    sizing,
                    "copyable_balance_percent",
                ),
                proportional_balance_cap_enabled=self._boolean(
                    sizing,
                    "proportional_balance_cap_enabled",
                ),
                require_leader_balance=self._boolean(
                    sizing,
                    "require_leader_balance",
                ),
                strict_fixed_amount=self._boolean(
                    sizing,
                    "strict_fixed_amount",
                ),
                strict_exact_mirror=self._boolean(
                    sizing,
                    "strict_exact_mirror",
                ),
            )
        )

        return CopyTask(
            task_id=self._text(row, "task_id"),
            protective_risk=RiskLotCodec().decode_policy(
                self._optional_mapping(configuration, "protective_risk") or {}
            ),
            name=self._text(row, "name"),
            leader_wallet=self._text(row, "leader_wallet"),
            follower_account=self._text(row, "follower_account"),
            status=CopyTaskStatus(self._text(row, "status")),
            monitoring_mode=MonitoringMode(self._text(row, "monitoring_mode")),
            trigger=TaskTriggerPolicy(
                enabled=self._boolean(trigger, "enabled"),
                min_notional=self._decimal(trigger, "min_notional"),
                max_notional=self._decimal(trigger, "max_notional"),
            ),
            spend=TaskSpendPolicy(
                enabled=self._boolean(spend, "enabled"),
                max_total=self._decimal(spend, "max_total"),
                max_daily=self._decimal(spend, "max_daily"),
                max_per_market=self._decimal(spend, "max_per_market"),
                one_entry_per_market=self._boolean(spend, "one_entry_per_market"),
                global_one_entry_per_market=self._boolean(
                    spend,
                    "global_one_entry_per_market",
                ),
                one_entry_per_sports_type=self._boolean_default(
                    spend,
                    "one_entry_per_sports_type",
                    False,
                ),
            ),
            price=TaskPricePolicy(
                enabled=self._boolean(price, "enabled"),
                min_price=self._decimal(price, "min_price"),
                max_price=self._decimal(price, "max_price"),
                offset_mode=PriceOffsetMode(self._text(price, "offset_mode")),
                offset_direction=PriceOffsetDirection(self._text(price, "offset_direction")),
                offset=self._decimal(price, "offset"),
                max_slippage_percent=self._decimal(
                    price,
                    "max_slippage_percent",
                ),
            ),
            reaction=ReactionPolicy(
                enabled=self._boolean(reaction, "enabled"),
                follow_buys=self._boolean(reaction, "follow_buys"),
                copy_sells=self._boolean(reaction, "copy_sells"),
                buy_on_sells=self._boolean(reaction, "buy_on_sells"),
                counter_trade=self._boolean(reaction, "counter_trade"),
                copy_side=CopySideFilter(self._text(reaction, "copy_side")),
                copy_merges=self._boolean(reaction, "copy_merges"),
                copy_splits=self._boolean(reaction, "copy_splits"),
                copy_conversions=self._boolean(reaction, "copy_conversions"),
            ),
            market_filter=TaskMarketFilter(
                enabled=self._boolean(market_filter, "enabled"),
                include_keywords=self._strings(market_filter, "include_keywords"),
                exclude_keywords=self._strings(market_filter, "exclude_keywords"),
                categories=self._strings(market_filter, "categories"),
                tokens=self._strings_default(market_filter, "tokens", ()),
                timeframes=self._strings_default(market_filter, "timeframes", ()),
                min_liquidity_usdc=self._decimal_default(
                    market_filter,
                    "min_liquidity_usdc",
                    Decimal("0"),
                ),
                min_volume_24h_usdc=self._decimal_default(
                    market_filter,
                    "min_volume_24h_usdc",
                    Decimal("0"),
                ),
                max_days_until_close=self._decimal(
                    market_filter,
                    "max_days_until_close",
                ),
                allow_expired_unresolved=self._boolean(
                    market_filter,
                    "allow_expired_unresolved",
                ),
            ),
            schedule=TaskSchedule(
                enabled=self._boolean(schedule, "enabled"),
                timezone_name=self._text(schedule, "timezone_name"),
                start=self._time(schedule, "start"),
                end=self._time(schedule, "end"),
            ),
            sizing=sizing_policy,
            sizing_mode=self._text(row, "sizing_mode"),
            version=self._integer(row, "version"),
            notifications_muted=self._boolean(row, "notifications_muted"),
        )

    @staticmethod
    def _configuration(value: object) -> Mapping[str, object]:
        if isinstance(value, str):
            decoded = json.loads(value)
        else:
            decoded = value
        if not isinstance(decoded, Mapping):
            raise ValueError("copy task configuration must be a JSON object")
        return decoded

    @staticmethod
    def _mapping(source: Mapping[str, object], name: str) -> Mapping[str, object]:
        value = source.get(name)
        if not isinstance(value, Mapping):
            raise ValueError(f"copy task configuration.{name} must be an object")
        return value

    @staticmethod
    def _optional_mapping(
        source: Mapping[str, object],
        name: str,
    ) -> Mapping[str, object] | None:
        value = source.get(name)
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise ValueError(f"copy task configuration.{name} must be an object")
        return value

    @staticmethod
    def _text(source: Mapping[str, object], name: str) -> str:
        value = source.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"copy task {name} must be non-empty text")
        return value

    @staticmethod
    def _boolean(source: Mapping[str, object], name: str) -> bool:
        value = source.get(name)
        if not isinstance(value, bool):
            raise ValueError(f"copy task {name} must be boolean")
        return value

    @staticmethod
    def _decimal(source: Mapping[str, object], name: str) -> Decimal:
        value = source.get(name)
        if isinstance(value, bool) or value is None:
            raise ValueError(f"copy task {name} must be decimal text")
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError(f"copy task {name} must be finite")
        return result

    @staticmethod
    def _boolean_default(
        source: Mapping[str, object],
        name: str,
        default: bool,
    ) -> bool:
        """Return an optional boolean for backward-compatible task rows."""

        if name not in source:
            return default
        return CopyTaskCodec._boolean(source, name)

    @staticmethod
    def _decimal_default(
        source: Mapping[str, object],
        name: str,
        default: Decimal,
    ) -> Decimal:
        """Return an optional decimal for backward-compatible task rows."""

        if name not in source:
            return default
        return CopyTaskCodec._decimal(source, name)

    @staticmethod
    def _strings_default(
        source: Mapping[str, object],
        name: str,
        default: tuple[str, ...],
    ) -> tuple[str, ...]:
        """Return an optional string tuple for backward-compatible task rows."""

        if name not in source:
            return default
        return CopyTaskCodec._strings(source, name)

    @staticmethod
    def _integer(source: Mapping[str, object], name: str) -> int:
        value = source.get(name)
        if isinstance(value, bool):
            raise ValueError(f"copy task {name} must be integer")
        try:
            result = int(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"copy task {name} must be integer") from exc
        return result

    @staticmethod
    def _strings(source: Mapping[str, object], name: str) -> tuple[str, ...]:
        value = source.get(name)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            raise ValueError(f"copy task {name} must be a string list")
        return tuple(value)

    @classmethod
    def _time(cls, source: Mapping[str, object], name: str) -> time:
        value = cls._text(source, name)
        try:
            return time.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"copy task {name} must be ISO time") from exc


class PostgresCopyTaskRepository:
    """Store copy tasks with atomic creation and optimistic updates."""

    def __init__(
        self,
        pool: DatabasePool,
        *,
        codec: CopyTaskCodec | None = None,
        on_mutation: Callable[[], None] | None = None,
    ) -> None:
        self._pool = pool
        self._codec = codec or CopyTaskCodec()
        self._on_mutation = on_mutation

    def set_mutation_listener(self, callback: Callable[[], None] | None) -> None:
        """Register an in-process cache invalidation hook invoked after commits."""
        self._on_mutation = callback

    def _notify_mutation(self) -> None:
        if self._on_mutation is not None:
            self._on_mutation()

    async def create(self, task: CopyTask) -> None:
        """Create a task or raise when its identity already exists."""

        inserted = await self._pool.fetchval(
            """
            INSERT INTO copy_tasks
                (task_id, name, leader_wallet, follower_account, status,
                 monitoring_mode, sizing_mode, notifications_muted, version,
                 configuration, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb,
                    NOW(), NOW())
            ON CONFLICT (task_id) DO NOTHING
            RETURNING task_id
            """,
            task.task_id,
            task.name,
            task.leader_wallet.casefold(),
            task.follower_account,
            task.status.value,
            task.monitoring_mode.value,
            task.sizing_mode,
            task.notifications_muted,
            task.version,
            self._codec.encode_configuration(task),
        )
        if inserted is None:
            raise CopyTaskConflict(f"copy task already exists: {task.task_id}")
        self._notify_mutation()

    async def get(self, task_id: str) -> CopyTask | None:
        """Return one task including soft-deleted records."""

        row = await self._pool.fetchrow(
            """
            SELECT task_id, name, leader_wallet, follower_account, status,
                   monitoring_mode, sizing_mode, notifications_muted, version,
                   configuration
              FROM copy_tasks
             WHERE task_id = $1
            """,
            str(task_id),
        )
        return None if row is None else self._codec.decode_row(row)

    async def save(self, task: CopyTask, *, expected_version: int) -> None:
        """Persist a task only when its current version matches."""

        updated = await self._pool.fetchval(
            """
            UPDATE copy_tasks
               SET name = $2,
                   leader_wallet = $3,
                   follower_account = $4,
                   status = $5,
                   monitoring_mode = $6,
                   sizing_mode = $7,
                   notifications_muted = $8,
                   version = $9,
                   configuration = $10::jsonb,
                   deleted_at = CASE WHEN $5 = 'deleted' THEN NOW() ELSE NULL END,
                   updated_at = NOW()
             WHERE task_id = $1 AND version = $11
            RETURNING task_id
            """,
            task.task_id,
            task.name,
            task.leader_wallet.casefold(),
            task.follower_account,
            task.status.value,
            task.monitoring_mode.value,
            task.sizing_mode,
            task.notifications_muted,
            task.version,
            self._codec.encode_configuration(task),
            int(expected_version),
        )
        if updated is not None:
            self._notify_mutation()
            return
        current = await self.get(task.task_id)
        if current is None:
            raise CopyTaskNotFound(task.task_id)
        raise CopyTaskConflict(
            f"copy task version conflict: expected {expected_version}, found {current.version}"
        )

    async def list_active(self) -> tuple[CopyTask, ...]:
        """Return active and paused tasks in stable identity order."""

        rows = await self._pool.fetch(
            """
            SELECT task_id, name, leader_wallet, follower_account, status,
                   monitoring_mode, sizing_mode, notifications_muted, version,
                   configuration
              FROM copy_tasks
             WHERE status <> 'deleted'
             ORDER BY task_id
            """
        )
        return tuple(self._codec.decode_row(row) for row in rows)


__all__ = ["CopyTaskCodec", "PostgresCopyTaskRepository"]
