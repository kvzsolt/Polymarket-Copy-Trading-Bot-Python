"""bot reservations — list active order reservations and totals."""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING

from config.app_config import EnvValidationError, load_config
from config.env import rehydrate_env
from config.runtime_env import load_runtime_env

if TYPE_CHECKING:
    import asyncpg


def register_reservations_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "reservations", help="List active order reservations"
    )
    res_sub = parser.add_subparsers(dest="reservations_subcommand", required=True)

    list_parser = res_sub.add_parser(
        "list", help="List active reservations and per-user totals"
    )
    list_parser.add_argument("--user-id", default=None, help="Filter by user ID")
    list_parser.add_argument(
        "--include-expired",
        action="store_true",
        help="Include rows where expires_at has passed",
    )


def _load_config_or_fail():
    try:
        load_runtime_env()
        rehydrate_env()
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


def _row_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "order_intent_key": row["order_intent_key"],
        "condition_id": row["condition_id"],
        "token_id": row["token_id"],
        "side": row["side"],
        "reason": row["reason"],
        "reserved_usdc": row["reserved_usdc"],
        "reserved_size": row["reserved_size"],
        "status": row["status"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "order_id": row["order_id"],
        "venue_order_id": row["venue_order_id"],
    }


async def _list_reservations(
    pool: "asyncpg.Pool", *, user_id: str | None, include_expired: bool
) -> list[dict]:
    """List active reservations. Takes the asyncpg pool so callers
    (CLI entry + tests) own pool lifecycle.
    """
    now_ts = int(time.time())
    query = "SELECT * FROM order_reservations WHERE status = 'active'"
    params: list = []
    if user_id is not None:
        params.append(user_id)
        query += f" AND user_id = ${len(params)}"
    if not include_expired:
        params.append(now_ts)
        query += f" AND expires_at > ${len(params)}"
    query += " ORDER BY created_at ASC"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [_row_to_dict(row) for row in rows]


def _summarize(reservations: list[dict]) -> dict:
    total_usdc = Decimal("0")
    total_size = Decimal("0")
    per_user: dict[str, dict[str, Decimal]] = defaultdict(
        lambda: {"buy_usdc": Decimal("0"), "sell_size": Decimal("0"), "count": Decimal("0")}
    )
    for res in reservations:
        usdc = Decimal(str(res["reserved_usdc"] or "0"))
        size = Decimal(str(res["reserved_size"] or "0"))
        total_usdc += usdc
        total_size += size
        bucket = per_user[res["user_id"]]
        if res["side"] == "BUY":
            bucket["buy_usdc"] += usdc
        elif res["side"] == "SELL":
            bucket["sell_size"] += size
        bucket["count"] += Decimal("1")

    return {
        "total_active": len(reservations),
        "total_buy_usdc": str(total_usdc),
        "total_sell_size": str(total_size),
        "per_user": {
            uid: {
                "count": int(values["count"]),
                "buy_usdc": str(values["buy_usdc"]),
                "sell_size": str(values["sell_size"]),
            }
            for uid, values in per_user.items()
        },
    }


async def _run_list_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    user_id = getattr(args, "user_id", None)
    include_expired = bool(getattr(args, "include_expired", False))

    from storage.postgres.connection import init_pool, shutdown_pool

    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            reservations = await _list_reservations(
                pool,
                user_id=user_id,
                include_expired=include_expired,
            )
        finally:
            await shutdown_pool()
    except Exception as exc:
        print(json.dumps({"error": "db_error", "details": str(exc)}))
        return 1

    output = {
        "summary": _summarize(reservations),
        "reservations": reservations,
    }
    print(json.dumps(output, indent=2))
    return 0


async def _run_reservations_async(args: argparse.Namespace) -> int:
    if args.reservations_subcommand == "list":
        return await _run_list_async(args)
    return 1


def run_reservations(args: argparse.Namespace) -> int:
    return asyncio.run(_run_reservations_async(args))
