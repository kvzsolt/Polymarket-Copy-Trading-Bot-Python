"""bot reservations — list active order reservations and totals."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.config.app_config import EnvValidationError, load_config
from pmcopy.services.execution.reservation_expiry import (
    OrderRowLookup,
    ReservationExpiryDecision,
    classify_no_id_reservation_expiry,
)

if TYPE_CHECKING:
    import asyncpg


def register_reservations_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("reservations", help="List active order reservations")
    res_sub = parser.add_subparsers(dest="reservations_subcommand", required=True)

    list_parser = res_sub.add_parser("list", help="List active reservations and per-user totals")
    list_parser.add_argument("--user-id", default=None, help="Filter by user ID")
    list_parser.add_argument(
        "--include-expired",
        action="store_true",
        help="Include rows where expires_at has passed",
    )

    resolve_parser = res_sub.add_parser(
        "resolve", help="Resolve expired active reservations that are safe to release"
    )
    resolve_parser.add_argument("--user-id", default=None, help="Filter by user ID")
    mode = resolve_parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        dest="apply",
        action="store_false",
        default=False,
        help="Report actions without mutating rows (default)",
    )
    mode.add_argument(
        "--apply",
        dest="apply",
        action="store_true",
        help="Release expired active reservations with no linked order",
    )


def _load_config_or_fail():
    try:
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


def _classify_expired_reservation(row: dict, *, now_ts: int) -> dict:
    """First-tier classification from the reservation row alone.

    A no-link expired row is provisionally ``safe_to_release``; the caller
    must confirm with :func:`_demand_submission_evidence` before releasing.
    """
    linked_order = bool(row.get("order_id") or row.get("venue_order_id"))
    classified = {
        **row,
        "expired_seconds": max(0, now_ts - int(row["expires_at"] or now_ts)),
        "classification": (
            "deferred_reconciliation_required" if linked_order else "safe_to_release"
        ),
        "action": "defer" if linked_order else "release",
    }
    return classified


async def _demand_submission_evidence(row: dict, orders_repo: Any) -> dict:
    """Reclassify a no-link expired reservation per the typed expiry proof.

    ``safe_to_release`` survives only on POSITIVE proof of a pre-submit
    orphan: the orders lookup succeeded and found no submission row linked
    through ``recovery_metadata`` (the same link the runtime expiry
    classifier uses), so no venue POST can have occurred. A reservation
    backed by any submission row — especially an unresolved no-ID
    ``submitted_unconfirmed`` row — defers to the recovery machinery, and a
    failed or unavailable lookup fails closed. An explicit ``--apply`` can
    therefore never contradict the runtime's typed classification.
    """
    if row.get("classification") != "safe_to_release":
        return row
    lookup = getattr(orders_repo, "get_by_reservation_id", None)
    if not callable(lookup):
        return {
            **row,
            "classification": "recovery_pending",
            "action": "defer",
            "defer_reason": "orders_lookup_unavailable",
        }
    try:
        lookup_result = lookup(row["id"])
        order_row = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
    except Exception:
        return {
            **row,
            "classification": "recovery_pending",
            "action": "defer",
            "defer_reason": "orders_lookup_unavailable",
        }
    decision = classify_no_id_reservation_expiry(OrderRowLookup.from_lookup(order_row))
    if decision is ReservationExpiryDecision.RELEASE_PRE_SUBMIT_ORPHAN:
        return row
    defer_reason = (
        "unresolved_submission_row"
        if decision is ReservationExpiryDecision.KEEP_PENDING_RECOVERY
        or decision is ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
        else "orders_lookup_unavailable"
    )
    return {
        **row,
        "classification": "recovery_pending",
        "action": "defer",
        "defer_reason": defer_reason,
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


async def _list_expired_reservation_candidates(
    pool: "asyncpg.Pool", *, user_id: str | None, orders_repo: Any = None
) -> list[dict]:
    now_ts = int(time.time())
    query = "SELECT * FROM order_reservations WHERE status = 'active' AND expires_at < $1"
    params: list = [now_ts]
    if user_id is not None:
        params.append(user_id)
        query += f" AND user_id = ${len(params)}"
    query += " ORDER BY expires_at ASC"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    if orders_repo is None:
        from pmcopy.storage.postgres.orders_repo import OrdersRepo

        orders_repo = OrdersRepo(pool)
    candidates = []
    for row in rows:
        classified = _classify_expired_reservation(_row_to_dict(row), now_ts=now_ts)
        classified = await _demand_submission_evidence(classified, orders_repo)
        candidates.append(classified)
    return candidates


async def _resolve_expired_reservations(
    pool: "asyncpg.Pool", *, user_id: str | None, apply: bool
) -> dict:
    from pmcopy.storage.postgres.reservations_repo import ReservationsRepo

    candidates = await _list_expired_reservation_candidates(pool, user_id=user_id)
    safe_rows = [row for row in candidates if row["classification"] == "safe_to_release"]
    deferred_rows = [row for row in candidates if row["action"] == "defer"]
    released_rows: list[dict] = []
    if apply:
        repo = ReservationsRepo(pool)
        released_at = int(time.time())
        for row in safe_rows:
            await repo.mark_released(
                row["id"],
                reason="expired_orphan_no_order",
                released_at=released_at,
            )
            released = {**row, "status": "released", "classification": "released"}
            released["action"] = "released"
            released_rows.append(released)

    return {
        "applied": bool(apply),
        "released": released_rows,
        "would_release": [] if apply else safe_rows,
        "deferred_reconciliation_required": deferred_rows,
        "summary": {
            "released": len(released_rows),
            "would_release": 0 if apply else len(safe_rows),
            "deferred_reconciliation_required": len(deferred_rows),
        },
    }


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

    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

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


async def _run_resolve_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    user_id = getattr(args, "user_id", None)
    apply = bool(getattr(args, "apply", False))

    from pmcopy.storage.postgres.connection import init_pool, shutdown_pool

    try:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        try:
            output = await _resolve_expired_reservations(
                pool,
                user_id=user_id,
                apply=apply,
            )
        finally:
            await shutdown_pool()
    except Exception as exc:
        print(json.dumps({"error": "db_error", "details": str(exc)}))
        return 1

    print(json.dumps(output, indent=2))
    return 0


async def _run_reservations_async(args: argparse.Namespace) -> int:
    if args.reservations_subcommand == "list":
        return await _run_list_async(args)
    if args.reservations_subcommand == "resolve":
        return await _run_resolve_async(args)
    return 1


def run_reservations(args: argparse.Namespace) -> int:
    return asyncio.run(_run_reservations_async(args))
