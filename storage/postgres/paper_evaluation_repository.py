"""Atomic source-isolated paper portfolios and economic-event receipts."""

import json
from dataclasses import asdict, replace
from datetime import datetime
from decimal import Decimal

import asyncpg

from domain.auto_trader import AutoTraderScope
from domain.paper_evaluation import PaperLedger, PaperPosition
from storage.postgres.auto_trader_repository import AutoTraderCodec


class PaperLedgerCodec:
    """Preserve exact financial values and open-episode state across restarts."""

    @staticmethod
    def encode(ledger: PaperLedger) -> str:
        """Encode a complete immutable portfolio."""
        return json.dumps(asdict(ledger), default=AutoTraderCodec.json_value, sort_keys=True)

    @staticmethod
    def decode(payload: str) -> PaperLedger:
        """Restore exact numeric and temporal types."""
        raw = json.loads(payload)
        for name in (
            "initial_cash",
            "cash",
            "realized_pnl",
            "equity",
            "peak_equity",
            "max_drawdown",
        ):
            raw[name] = Decimal(raw[name])
        raw["started_at"] = datetime.fromisoformat(raw["started_at"])
        raw["recent_pnls"] = tuple(Decimal(value) for value in raw["recent_pnls"])
        raw["positions"] = tuple(
            PaperPosition(
                token=p["token"],
                condition=p["condition"],
                shares=Decimal(p["shares"]),
                net_cost=Decimal(p["net_cost"]),
                gross_cost=Decimal(p["gross_cost"]),
            )
            for p in raw["positions"]
        )
        return PaperLedger(**raw)


class PostgresPaperLedgerRepository:
    """Commit paper fills and their dedup receipt in one transaction."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Inject the application database pool."""
        self._pool = pool

    async def load(self, scope: AutoTraderScope, wallet: str) -> PaperLedger | None:
        """Read one isolated portfolio, propagating database errors."""
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                "SELECT snapshot::text FROM auto_trader_paper_portfolios "
                "WHERE follower_account=$1 AND trading_mode=$2 AND leader_wallet=$3",
                scope.follower_account,
                scope.trading_mode,
                wallet,
            )
        return PaperLedgerCodec.decode(value) if value is not None else None

    async def claim_source(
        self, scope: AutoTraderScope, wallet: str, event: str, *, source: str, now: datetime
    ) -> bool:
        """Persist the first feed owner without merging distinct mined log identities."""
        if source not in {"polling", "on_chain"} or not event:
            raise ValueError("invalid paper source claim")
        key = "source:" + event
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "INSERT INTO auto_trader_paper_events VALUES ($1,$2,$3,$4,$5::jsonb,$6) "
                "ON CONFLICT DO NOTHING",
                scope.follower_account,
                scope.trading_mode,
                wallet,
                key,
                json.dumps({"source": source}),
                now,
            )
            owner = await connection.fetchval(
                "SELECT snapshot->>'source' FROM auto_trader_paper_events "
                "WHERE follower_account=$1 AND trading_mode=$2 AND leader_wallet=$3 AND event_key=$4",
                scope.follower_account,
                scope.trading_mode,
                wallet,
                key,
            )
        return owner == source

    async def seen(self, scope: AutoTraderScope, wallet: str, event: str) -> bool:
        """Check the economic event before repeating public market reads."""
        async with self._pool.acquire() as connection:
            value = await connection.fetchval(
                "SELECT 1 FROM auto_trader_paper_events "
                "WHERE follower_account=$1 AND trading_mode=$2 AND leader_wallet=$3 AND event_key=$4",
                scope.follower_account,
                scope.trading_mode,
                wallet,
                event,
            )
        return value is not None

    async def save(
        self, scope: AutoTraderScope, wallet: str, ledger: PaperLedger, *, event: str, now: datetime
    ) -> bool:
        """Compare-and-swap inventory and insert exactly one event receipt."""
        revision = ledger.revision + 1
        payload = PaperLedgerCodec.encode(replace(ledger, revision=revision))
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"auto-paper:{scope.follower_account}:{scope.trading_mode}:{wallet}",
            )
            current = await connection.fetchval(
                "SELECT revision FROM auto_trader_paper_portfolios "
                "WHERE follower_account=$1 AND trading_mode=$2 AND leader_wallet=$3",
                scope.follower_account,
                scope.trading_mode,
                wallet,
            )
            if (current or 0) != ledger.revision:
                return False
            inserted = await connection.fetchval(
                "INSERT INTO auto_trader_paper_events VALUES ($1,$2,$3,$4,$5::jsonb,$6) "
                "ON CONFLICT DO NOTHING RETURNING 1",
                scope.follower_account,
                scope.trading_mode,
                wallet,
                event,
                payload,
                now,
            )
            if inserted is None:
                return False
            await connection.execute(
                "INSERT INTO auto_trader_paper_portfolios VALUES ($1,$2,$3,$4,$5::jsonb) "
                "ON CONFLICT (follower_account,trading_mode,leader_wallet) DO UPDATE "
                "SET revision=excluded.revision,snapshot=excluded.snapshot",
                scope.follower_account,
                scope.trading_mode,
                wallet,
                revision,
                payload,
            )
        return True
