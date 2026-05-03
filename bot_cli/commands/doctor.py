import argparse
import asyncio
import json
import sys

from config.runtime_env import load_runtime_env
from config.env import rehydrate_env
from config.app_config import load_config, EnvValidationError
from services.compliance import check_geoblock
from orchestrators.startup_health import _check_rpc
from storage.sqlite.connection import create_connection


def register_doctor_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("doctor", help="Run diagnostic checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live", action="store_true", help="Check readiness for live trading")
    group.add_argument("--paper", action="store_true", help="Check readiness for paper trading")


async def run_doctor_checks(mode: str) -> dict:
    output = {
        "mode": mode,
        "status": "ok",
        "checks": {}
    }
    
    try:
        # Avoid reloading if already loaded in app.py, but safe to call multiple times
        load_runtime_env()
        rehydrate_env()
        config = load_config()
    except EnvValidationError as exc:
        output["status"] = "blocked"
        output["checks"]["config"] = {
            "status": "failed",
            "reason": "validation_error",
            "details": str(exc).split("\n")
        }
        return output

    # Check 1: Geoblock (always check, but definitely required for live)
    if config.network.enforce_geoblock or mode == "live":
        decision = await check_geoblock()
        if not decision.allowed:
            output["checks"]["geoblock"] = {
                "status": "failed",
                "reason": decision.reason
            }
            output["status"] = "blocked"
        else:
            output["checks"]["geoblock"] = {"status": "ok"}
    else:
        output["checks"]["geoblock"] = {"status": "skipped", "reason": "not_enforced"}

    # Check 2: Database writable
    try:
        with create_connection(config.database.db_path) as conn:
            conn.execute("PRAGMA user_version;")
        output["checks"]["db"] = {"status": "ok"}
    except Exception as exc:
        output["checks"]["db"] = {
            "status": "failed",
            "reason": "db_not_writable",
            "details": str(exc)
        }
        output["status"] = "blocked"

    # Check 3: RPC reachable
    rpc_check = await _check_rpc(config.network.rpc_url)
    if rpc_check.status == "error":
        output["checks"]["rpc"] = {
            "status": "failed",
            "reason": "rpc_unreachable",
            "details": rpc_check.details
        }
        output["status"] = "blocked"
    else:
        output["checks"]["rpc"] = {"status": "ok"}

    # Check 4: Secrets presence & Redaction
    if mode == "live":
        if not config.wallet.private_key:
            output["checks"]["secrets"] = {
                "status": "failed",
                "reason": "missing_private_key"
            }
            output["status"] = "blocked"
        else:
            output["checks"]["secrets"] = {"status": "ok"}
    else:
        output["checks"]["secrets"] = {"status": "ok", "reason": "paper_mode_allows_missing_key"}

    # Check 5: Pending orders
    try:
        with create_connection(config.database.db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as c
                FROM sqlite_master 
                WHERE type='table' AND name='orders'
                """
            ).fetchone()
            if row and row["c"] > 0:
                ttl_seconds = config.execution.pending_submission_ttl_seconds
                row_orders = conn.execute(
                    """
                    SELECT COUNT(*) as c 
                    FROM orders 
                    WHERE status IN ('submitted_unconfirmed', 'delayed', 'pending', 'pending_lock', 'submitting')
                    AND (strftime('%s', 'now') - strftime('%s', created_at)) > ?
                    """, (ttl_seconds,)
                ).fetchone()
                if row_orders and row_orders["c"] > 0:
                    output["checks"]["pending_orders"] = {
                        "status": "warning",
                        "reason": "stale_pending_orders_exist",
                        "count": row_orders["c"]
                    }
                else:
                    output["checks"]["pending_orders"] = {"status": "ok"}
            else:
                output["checks"]["pending_orders"] = {"status": "skipped", "reason": "no_orders_table"}
    except Exception as exc:
        output["checks"]["pending_orders"] = {"status": "failed", "reason": "db_error", "details": str(exc)}
    
    # Check 6: Risk Kill Switch
    if config.risk.kill_switch:
        output["checks"]["kill_switch"] = {"status": "failed", "reason": "kill_switch_active"}
        output["status"] = "blocked"
    else:
        output["checks"]["kill_switch"] = {"status": "ok"}

    # Check 7: Leaders configured
    if not config.leader.wallets and not config.leader.auto_discover_leaders:
        output["checks"]["leaders"] = {"status": "failed", "reason": "no_leaders_configured_or_discovered"}
        output["status"] = "blocked"
    else:
        output["checks"]["leaders"] = {"status": "ok"}

    # Check 8: Minimum order size valid
    if config.sizing.buy_min_amount < 5.0:
        output["checks"]["order_size"] = {"status": "warning", "reason": "buy_min_amount_very_low"}
    else:
        output["checks"]["order_size"] = {"status": "ok"}

    return output

async def _run_doctor_async(args: argparse.Namespace) -> int:
    mode = "live" if args.live else "paper"
    output = await run_doctor_checks(mode)
    print(json.dumps(output, indent=2))
    return 1 if output.get("status") == "blocked" else 0

def run_doctor(args: argparse.Namespace) -> int:
    return asyncio.run(_run_doctor_async(args))
