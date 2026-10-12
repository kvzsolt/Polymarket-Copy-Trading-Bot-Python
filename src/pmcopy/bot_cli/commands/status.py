"""bot status — show current bot runtime status."""

from __future__ import annotations

import argparse
import asyncio
import json
import time

from pmcopy.config.app_config import load_config, EnvValidationError


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
        config = load_config()
    except EnvValidationError:
        result["config_error"] = "validation_failed"
        return result

    result["running_mode"] = "paper" if config.execution.paper_trade_only else "live"

    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            async with pool.acquire() as conn:
                # Apply migrations is idempotent + cheap; skip here, the
                # bot process owns schema lifecycle. CLI is read-only.

                # Open positions count
                result["open_positions"] = (
                    await conn.fetchval("SELECT COUNT(*) FROM positions") or 0
                )

                # Pending orders count
                result["pending_orders"] = (
                    await conn.fetchval(
                        """
                    SELECT COUNT(*) FROM orders
                    WHERE status IN ('submitted_unconfirmed', 'delayed', 'pending',
                                     'pending_lock', 'submitting', 'submitted')
                    """
                    )
                    or 0
                )

                # Active reservations count
                now_ts = int(time.time())
                result["active_reservations"] = (
                    await conn.fetchval(
                        """
                    SELECT COUNT(*) FROM order_reservations
                    WHERE status = 'active' AND expires_at > $1
                    """,
                        now_ts,
                    )
                    or 0
                )

                # Geoblock last check — use risk_blocks as a proxy timestamp source
                result["geoblock_last_check"] = await conn.fetchval(
                    """
                    SELECT MAX(triggered_at) FROM risk_blocks
                    WHERE source = 'geoblock'
                    """
                )
                result["risk_last_check"] = await conn.fetchval(
                    "SELECT MAX(triggered_at) FROM risk_blocks"
                )

                # Uptime — best-effort: read earliest order created_at
                first_ts = await conn.fetchval("SELECT MIN(created_at) FROM orders")
                if first_ts is not None:
                    try:
                        from datetime import datetime, timezone

                        if isinstance(first_ts, str):
                            first_dt = datetime.fromisoformat(first_ts).replace(tzinfo=timezone.utc)
                        else:
                            first_dt = first_ts
                            if first_dt.tzinfo is None:
                                first_dt = first_dt.replace(tzinfo=timezone.utc)
                        result["uptime_seconds"] = int(
                            (datetime.now(timezone.utc) - first_dt).total_seconds()
                        )
                    except (ValueError, TypeError):
                        pass
        finally:
            await shutdown_pool()
    except Exception as exc:
        result["db_error"] = str(exc)

    return result


async def _run_status_async(_args: argparse.Namespace) -> int:
    output = await _gather_status()
    print(json.dumps(output, indent=2))
    return 0


def run_status(args: argparse.Namespace) -> int:
    return asyncio.run(_run_status_async(args))
