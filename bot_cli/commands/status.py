"""bot status — show current bot runtime status."""
from __future__ import annotations

import argparse
import asyncio
import json
import time

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError
from storage.sqlite.connection import create_connection


def register_status_command(subparsers: argparse._SubParsersAction) -> None:
    subparsers.add_parser("status", help="Show current bot runtime status")


async def _gather_status() -> dict:
    """Collect and return the bot's current runtime status."""
    result: dict = {
        "running_mode": "unknown",
        "uptime_seconds": None,
        "open_positions": 0,
        "pending_orders": 0,
        "geoblock_last_check": None,
        "risk_last_check": None,
        "active_reservations": 0,
    }

    try:
        load_runtime_env()
        rehydrate_env()
        config = load_config()
    except EnvValidationError:
        result["config_error"] = "validation_failed"
        return result

    result["running_mode"] = "paper" if config.execution.paper_trade_only else "live"

    db_path = config.database.db_path

    try:
        with create_connection(db_path) as conn:
            # Open positions count
            row = conn.execute(
                "SELECT COUNT(*) as c FROM sqlite_master WHERE type='table' AND name='positions'"
            ).fetchone()
            if row and row["c"] > 0:
                pos_row = conn.execute("SELECT COUNT(*) as c FROM positions").fetchone()
                result["open_positions"] = pos_row["c"] if pos_row else 0

            # Pending orders count
            row = conn.execute(
                "SELECT COUNT(*) as c FROM sqlite_master WHERE type='table' AND name='orders'"
            ).fetchone()
            if row and row["c"] > 0:
                pending_row = conn.execute(
                    """
                    SELECT COUNT(*) as c FROM orders
                    WHERE status IN ('submitted_unconfirmed', 'delayed', 'pending',
                                     'pending_lock', 'submitting', 'submitted')
                    """
                ).fetchone()
                result["pending_orders"] = pending_row["c"] if pending_row else 0

            # Active reservations count
            row = conn.execute(
                "SELECT COUNT(*) as c FROM sqlite_master WHERE type='table' AND name='order_reservations'"
            ).fetchone()
            if row and row["c"] > 0:
                now_ts = int(time.time())
                res_row = conn.execute(
                    """
                    SELECT COUNT(*) as c FROM order_reservations
                    WHERE status = 'active' AND expires_at > ?
                    """,
                    (now_ts,),
                ).fetchone()
                result["active_reservations"] = res_row["c"] if res_row else 0

            # Geoblock last check — use risk_blocks as a proxy timestamp source
            row = conn.execute(
                "SELECT COUNT(*) as c FROM sqlite_master WHERE type='table' AND name='risk_blocks'"
            ).fetchone()
            if row and row["c"] > 0:
                gb_row = conn.execute(
                    """
                    SELECT MAX(triggered_at) as last_ts FROM risk_blocks
                    WHERE source = 'geoblock'
                    """
                ).fetchone()
                if gb_row and gb_row["last_ts"]:
                    result["geoblock_last_check"] = gb_row["last_ts"]

                risk_row = conn.execute(
                    """
                    SELECT MAX(triggered_at) as last_ts FROM risk_blocks
                    """
                ).fetchone()
                if risk_row and risk_row["last_ts"]:
                    result["risk_last_check"] = risk_row["last_ts"]

            # Uptime — best-effort: read earliest order created_at
            row = conn.execute(
                "SELECT COUNT(*) as c FROM sqlite_master WHERE type='table' AND name='orders'"
            ).fetchone()
            if row and row["c"] > 0:
                up_row = conn.execute(
                    "SELECT MIN(created_at) as first_ts FROM orders"
                ).fetchone()
                if up_row and up_row["first_ts"]:
                    try:
                        from datetime import datetime, timezone
                        first_dt = datetime.fromisoformat(up_row["first_ts"]).replace(tzinfo=timezone.utc)
                        result["uptime_seconds"] = int(
                            (datetime.now(timezone.utc) - first_dt).total_seconds()
                        )
                    except (ValueError, TypeError):
                        pass
    except Exception as exc:
        result["db_error"] = str(exc)

    return result


async def _run_status_async(_args: argparse.Namespace) -> int:
    output = await _gather_status()
    print(json.dumps(output, indent=2))
    return 0


def run_status(args: argparse.Namespace) -> int:
    return asyncio.run(_run_status_async(args))
