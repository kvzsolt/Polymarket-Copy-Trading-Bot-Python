"""Atomic automatic-roster storage with exact JSON evidence and audit receipts."""

import json
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg

from pmcopy.domain.auto_trader import AutoRoster, AutoTraderScope, CopyPerformance, Trader, TraderStatus


class AutoTraderCodec:
    """Encode exact immutable evidence without monetary floats."""

    @classmethod
    def encode(cls, roster: AutoRoster) -> str:
        """Serialize a validated roster into deterministic JSON."""
        return json.dumps(asdict(roster), default=cls.json_value, sort_keys=True)

    @staticmethod
    def json_value(value: object) -> str:
        """Serialize exact decimals and aware timestamps."""
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError("unsupported automatic trader JSON value")

    @classmethod
    def decode(cls, payload: str) -> AutoRoster:
        """Validate persisted evidence rather than silently repairing corrupt rows."""
        data = json.loads(payload)
        traders = []
        for raw in data["traders"]:
            raw["status"] = TraderStatus(raw["status"])
            raw["score"] = Decimal(raw["score"])
            for key in ("observed_at", "selected_at"):
                if raw[key] is not None:
                    raw[key] = datetime.fromisoformat(raw[key])
            for key in ("paper", "live"):
                if raw[key] is not None:
                    raw[key] = cls.performance(raw[key])
            traders.append(Trader(**raw))
        return AutoRoster(
            revision=data["revision"],
            initialized=data["initialized"],
            traders=tuple(traders),
            rotation_events=tuple(data.get("rotation_events", ())),
            entry_block_reason=data.get("entry_block_reason", ""),
            last_command_fingerprint=data.get("last_command_fingerprint", ""),
        )

    @staticmethod
    def performance(raw: dict[str, object]) -> CopyPerformance:
        """Decode a source-specific immutable performance record."""
        return CopyPerformance(
            evaluation_id=str(raw["evaluation_id"]),
            observed_at=datetime.fromisoformat(str(raw["observed_at"])),
            started_at=datetime.fromisoformat(str(raw["started_at"])),
            closed_episodes=int(str(raw["closed_episodes"])),
            realized_pnl=Decimal(str(raw["realized_pnl"])),
            recent_pnl=Decimal(str(raw["recent_pnl"])),
            drawdown=None if raw["drawdown"] is None else Decimal(str(raw["drawdown"])),
            watermark=str(raw["watermark"]),
            complete=raw["complete"] is True,
        )


class PostgresAutoTraderRepository:
    """Serialize membership updates without retaining locks during provider I/O."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Use the canonical pool; this adapter owns no connection lifecycle."""
        self._pool = pool

    async def schema_available(self) -> bool:
        """Probe an additive upgrade without migrating during a doctor check."""
        async with self._pool.acquire() as connection:
            return bool(
                await connection.fetchval("SELECT to_regclass('auto_trader_rosters') IS NOT NULL")
            )

    async def load(self, scope: AutoTraderScope) -> AutoRoster:
        """Load one account/mode, distinguishing absence from database failure."""
        async with self._pool.acquire() as connection:
            payload = await connection.fetchval(
                "SELECT snapshot::text FROM auto_trader_rosters WHERE follower_account=$1 AND trading_mode=$2",
                scope.follower_account,
                scope.trading_mode,
            )
        return AutoTraderCodec.decode(payload) if payload is not None else AutoRoster()

    async def save(
        self,
        scope: AutoTraderScope,
        expected_revision: int,
        roster: AutoRoster,
        *,
        event: str,
        actor: str = "auto-trader",
        receipt_key: str | None = None,
    ) -> bool:
        """Atomically compare-and-swap membership, audit evidence, and receipt."""
        revision = expected_revision + 1
        payload = AutoTraderCodec.encode(replace(roster, revision=revision))
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"auto-trader:{scope.follower_account}:{scope.trading_mode}",
            )
            current = await connection.fetchval(
                "SELECT revision FROM auto_trader_rosters WHERE follower_account=$1 AND trading_mode=$2",
                scope.follower_account,
                scope.trading_mode,
            )
            if (current or 0) != expected_revision:
                return False
            if receipt_key is not None and await connection.fetchval(
                "SELECT 1 FROM auto_trader_receipts WHERE follower_account=$1 AND trading_mode=$2 AND receipt_key=$3",
                scope.follower_account,
                scope.trading_mode,
                receipt_key,
            ):
                return False
            await connection.execute(
                """INSERT INTO auto_trader_rosters VALUES ($1,$2,$3,$4::jsonb)
                   ON CONFLICT (follower_account,trading_mode) DO UPDATE
                   SET revision=excluded.revision,snapshot=excluded.snapshot""",
                scope.follower_account,
                scope.trading_mode,
                revision,
                payload,
            )
            await connection.execute(
                """INSERT INTO auto_trader_events
                   (follower_account,trading_mode,revision,event,actor,snapshot)
                   VALUES ($1,$2,$3,$4,$5,$6::jsonb)""",
                scope.follower_account,
                scope.trading_mode,
                revision,
                event,
                actor,
                payload,
            )
            if receipt_key is not None:
                await connection.execute(
                    "INSERT INTO auto_trader_receipts VALUES ($1,$2,$3,$4::jsonb)",
                    scope.follower_account,
                    scope.trading_mode,
                    receipt_key,
                    payload,
                )
        return True

    async def receipt(self, scope: AutoTraderScope, key: str) -> AutoRoster | None:
        """Read an immutable command receipt scoped to account and mode."""
        async with self._pool.acquire() as connection:
            payload = await connection.fetchval(
                "SELECT snapshot::text FROM auto_trader_receipts "
                "WHERE follower_account=$1 AND trading_mode=$2 AND receipt_key=$3",
                scope.follower_account,
                scope.trading_mode,
                key,
            )
        return AutoTraderCodec.decode(payload) if payload is not None else None

    async def claim_discovery(self, scope: AutoTraderScope, event: str, now: datetime) -> bool:
        """Admit at most three attempts and recover abandoned read-only provider jobs."""
        async with self._pool.acquire() as connection:
            claimed = await connection.fetchval(
                """INSERT INTO auto_trader_discovery_jobs
                   (follower_account,trading_mode,event,status,lease_until)
                   VALUES ($1,$2,$3,'running',$4)
                   ON CONFLICT (follower_account,trading_mode,event) DO UPDATE
                   SET status='running',lease_until=excluded.lease_until,
                       attempts=auto_trader_discovery_jobs.attempts+1
                   WHERE auto_trader_discovery_jobs.status <> 'completed'
                     AND auto_trader_discovery_jobs.lease_until <= $5
                     AND auto_trader_discovery_jobs.attempts < 3
                   RETURNING 1""",
                scope.follower_account,
                scope.trading_mode,
                event,
                now + timedelta(hours=1),
                now,
            )
        return claimed is not None

    async def finish_discovery(
        self, scope: AutoTraderScope, event: str, *, succeeded: bool
    ) -> None:
        """Keep failures distinct from successful empty discovery."""
        async with self._pool.acquire() as connection:
            await connection.execute(
                """UPDATE auto_trader_discovery_jobs SET status=$4
                   WHERE follower_account=$1 AND trading_mode=$2 AND event=$3""",
                scope.follower_account,
                scope.trading_mode,
                event,
                "completed" if succeeded else "failed",
            )

    async def history(self, scope: AutoTraderScope, *, limit: int = 50) -> tuple[AutoRoster, ...]:
        """Return bounded immutable evaluation revisions for operator review."""
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """SELECT snapshot::text FROM auto_trader_events
                   WHERE follower_account=$1 AND trading_mode=$2 ORDER BY id DESC LIMIT $3""",
                scope.follower_account,
                scope.trading_mode,
                min(100, max(1, limit)),
            )
        return tuple(AutoTraderCodec.decode(row["snapshot"]) for row in rows)
