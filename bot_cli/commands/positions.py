"""bot positions — list open positions from the SQLite positions table."""
from __future__ import annotations

import argparse
import asyncio
import json

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError
from storage.sqlite.connection import create_connection


def register_positions_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("positions", help="List open positions")
    parser.add_argument("--user-id", default=None, help="Filter by user ID")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Output as JSON (default)")


async def _run_positions_async(args: argparse.Namespace) -> int:
    try:
        load_runtime_env()
        rehydrate_env()
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    db_path = config.database.db_path
    user_id = getattr(args, "user_id", None)

    query = """
        SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price, updated_at
        FROM positions
    """
    params: list = []
    if user_id is not None:
        query += " WHERE user_id = ?"
        params.append(user_id)
    query += " ORDER BY user_id, condition_id"

    positions: list[dict] = []
    try:
        with create_connection(db_path) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            for row in rows:
                from decimal import Decimal
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
                        "updated_at": row["updated_at"],
                    }
                )
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
