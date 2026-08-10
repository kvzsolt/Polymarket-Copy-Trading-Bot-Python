"""bot positions — list open positions from the Postgres positions table."""

from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal

from config.app_config import load_config, EnvValidationError


def register_positions_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("positions", help="List open positions")
    parser.add_argument("--user-id", default=None, help="Filter by user ID")
    parser.add_argument(
        "--json", action="store_true", dest="json_output", help="Output as JSON (default)"
    )


async def _run_positions_async(args: argparse.Namespace) -> int:
    try:
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    user_id = getattr(args, "user_id", None)

    base_query = """
        SELECT user_id, condition_id, token_id, leader_id, size_tokens,
               avg_entry_price, updated_at
        FROM positions
    """
    params: list = []
    if user_id is not None:
        base_query += " WHERE user_id = $1"
        params.append(user_id)
    base_query += " ORDER BY user_id, condition_id"

    from storage.postgres.connection import init_pool, shutdown_pool

    positions: list[dict] = []
    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(base_query, *params)
            for row in rows:
                size = Decimal(str(row["size_tokens"] or "0"))
                if abs(size) <= Decimal("1e-12"):
                    continue
                positions.append(
                    {
                        "user_id": row["user_id"],
                        "condition_id": row["condition_id"],
                        "token_id": row["token_id"],
                        "leader_id": row["leader_id"],
                        "size_tokens": str(row["size_tokens"]),
                        "avg_entry_price": str(row["avg_entry_price"]),
                        "updated_at": (
                            row["updated_at"].isoformat()
                            if row["updated_at"] is not None
                            and hasattr(row["updated_at"], "isoformat")
                            else row["updated_at"]
                        ),
                    }
                )
        finally:
            await shutdown_pool()
    except Exception as exc:
        print(json.dumps({"error": "db_error", "details": str(exc)}))
        return 1

    output = {
        "count": len(positions),
        "positions": positions,
    }
    print(json.dumps(output, indent=2))
    return 0


def run_positions(args: argparse.Namespace) -> int:
    return asyncio.run(_run_positions_async(args))
