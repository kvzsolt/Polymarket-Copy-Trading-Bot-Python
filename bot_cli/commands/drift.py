"""bot drift — detect off-bot/manual position drift between persisted and live state."""
from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal
from typing import TYPE_CHECKING

from config.app_config import EnvValidationError, load_config
from services.multiUserConfig import bind_resolved_deposit_wallet, load_runtime_users
from utils.logger import redact_error_detail

if TYPE_CHECKING:
    import asyncpg

    from config.app_config import AppConfig
    from services.multiUserConfig import RuntimeUser


def register_drift_command(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "drift", help="Detect off-bot/manual position drift"
    )
    sub = parser.add_subparsers(dest="drift_subcommand", required=True)

    check_parser = sub.add_parser("check", help="Compare persisted vs live holdings")
    check_parser.add_argument(
        "--user-id", default=None, help="Filter to a single user_id"
    )
    check_parser.add_argument(
        "--epsilon",
        type=str,
        default="0.000001",
        help="Decimal tolerance for size equality (default 1e-6)",
    )


def _load_config_or_fail():
    try:
        return load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return None


async def _list_persisted_positions(
    pool: "asyncpg.Pool", *, user_id: str | None
) -> list[dict]:
    """List persisted positions for the optional user filter.

    Takes the asyncpg pool so callers (CLI entry + tests) own pool
    lifecycle. The bot's long-running process owns schema lifecycle via
    ``connect_db()`` so no existence check is needed here.
    """
    query = """
        SELECT user_id, condition_id, token_id, leader_id, size_tokens, avg_entry_price
        FROM positions
    """
    params: list = []
    if user_id is not None:
        query += " WHERE user_id = $1"
        params.append(user_id)

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    persisted: list[dict] = []
    for row in rows:
        size = Decimal(str(row["size_tokens"] or "0"))
        if abs(size) <= Decimal("1e-12"):
            continue
        persisted.append(
            {
                "user_id": row["user_id"],
                "condition_id": row["condition_id"],
                "token_id": row["token_id"],
                "leader_id": row["leader_id"],
                "size_tokens": str(row["size_tokens"]),
                "avg_entry_price": str(row["avg_entry_price"]),
            }
        )
    return persisted


def _live_size_index(live_positions: list[dict]) -> dict[tuple[str, str], Decimal]:
    index: dict[tuple[str, str], Decimal] = {}
    for position in live_positions:
        condition_id = str(
            position.get("conditionId") or position.get("condition_id") or ""
        )
        token_id = str(
            position.get("tokenID")
            or position.get("token_id")
            or position.get("asset")
            or ""
        )
        if not condition_id or not token_id:
            continue
        try:
            size = Decimal(str(position.get("size") or "0"))
        except Exception:
            continue
        key = (condition_id, token_id)
        index[key] = index.get(key, Decimal("0")) + size
    return index


def _classify_drift(
    persisted_size: Decimal, live_size: Decimal, epsilon: Decimal
) -> str | None:
    delta = persisted_size - live_size
    if abs(delta) <= epsilon:
        return None
    if live_size <= epsilon:
        return "should_close"
    if persisted_size > live_size:
        return "shrink"
    return "phantom_grow"


async def _resolve_live_deposit_wallets(
    runtime_users: list[RuntimeUser], config: AppConfig
) -> None:
    """Bind each live user's SDK-resolved Deposit Wallet, in place.

    A live one-secret config legitimately omits ``wallet.funder_address`` and
    lets the authenticated SDK derive the wallet from ``private_key``. Without
    this pass every such user has an empty wallet, gets skipped by the holdings
    loop, and the check reports ``checked_users: 0`` on a wallet that may well
    be drifting. Mirrors the startup binding in ``app.py``.

    Paper never authenticates, so no secure client exists to resolve an address
    and the configured funder (which AppConfig requires in paper) stands
    unchanged — hence the early return. Only client construction and wallet
    reads happen here; no order or trading calls are made.

    Binding is unconditional. Past the paper guard a ``wallet_address`` of None
    means the SDK built no secure client for a *live* user, so binding rejects
    it and the check fails loudly; skipping it instead would leave that user's
    funder empty and report the same clean ``checked_users: 0``.
    """
    if config.execution.paper_trade_only:
        return

    from integrations.polymarket.sdk_client import create_polymarket_client

    for runtime_user in runtime_users:
        client = await create_polymarket_client(
            wallet_config=runtime_user.wallet,
            config=config,
            authenticated=True,
        )
        try:
            bind_resolved_deposit_wallet(runtime_user, client.wallet_address)
        finally:
            await client.close()


async def _run_check_async(args: argparse.Namespace) -> int:
    config = _load_config_or_fail()
    if config is None:
        return 1

    user_filter = getattr(args, "user_id", None)
    try:
        epsilon = Decimal(str(getattr(args, "epsilon", "0.000001")))
    except Exception:
        epsilon = Decimal("0.000001")

    runtime_users = load_runtime_users(config.leader.wallets or [], config)

    # Bind before filtering: binding rewrites an empty/"default" user_id to the
    # resolved wallet, so --user-id must be matched against the bound identity.
    try:
        await _resolve_live_deposit_wallets(runtime_users, config)
    except Exception as exc:
        # Never fall through to a silent ``checked_users: 0`` — an unresolvable
        # live wallet means the check did not run, which is not the same as
        # "no drift".
        print(
            json.dumps(
                {
                    "error": "wallet_resolution_failed",
                    "details": redact_error_detail(exc),
                }
            )
        )
        return 1

    if user_filter is not None:
        runtime_users = [ru for ru in runtime_users if ru.user_id == user_filter]

    if not runtime_users:
        output = {
            "checked_users": 0,
            "drift_count": 0,
            "drifts": [],
            "warning": "no_runtime_users_resolved",
        }
        print(json.dumps(output, indent=2))
        return 0

    drifts: list[dict] = []
    checked_users: list[str] = []
    skipped_users: list[str] = []

    from orchestrators.runtime_bootstrap import fetch_live_positions
    from storage.postgres.connection import init_pool, shutdown_pool

    pool = await init_pool(
        config.database.dsn,
        min_size=1,
        max_size=2,
        command_timeout_seconds=15.0,
    )
    try:
        for runtime_user in runtime_users:
            wallet_addr = runtime_user.wallet.funder_address or ""
            if not wallet_addr:
                # Unreachable for a valid config: paper requires a funder and
                # live binds one above. Recorded rather than dropped so a skip
                # can never be mistaken for a clean check.
                skipped_users.append(runtime_user.user_id)
                continue
            checked_users.append(runtime_user.user_id)

            persisted = await _list_persisted_positions(
                pool, user_id=runtime_user.user_id
            )
            try:
                live = await fetch_live_positions(wallet_addr)
            except Exception:
                live = []

            live_index = _live_size_index(live)
            seen_keys: set[tuple[str, str]] = set()

            for record in persisted:
                key = (record["condition_id"], record["token_id"])
                seen_keys.add(key)
                persisted_size = Decimal(record["size_tokens"])
                live_size = live_index.get(key, Decimal("0"))
                drift_kind = _classify_drift(persisted_size, live_size, epsilon)
                if drift_kind is None:
                    continue
                drifts.append(
                    {
                        "user_id": record["user_id"],
                        "condition_id": record["condition_id"],
                        "token_id": record["token_id"],
                        "leader_id": record["leader_id"],
                        "persisted_size": str(persisted_size),
                        "live_size": str(live_size),
                        "delta": str(persisted_size - live_size),
                        "kind": drift_kind,
                    }
                )

            for key, live_size in live_index.items():
                if key in seen_keys or live_size <= epsilon:
                    continue
                condition_id, token_id = key
                drifts.append(
                    {
                        "user_id": runtime_user.user_id,
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "leader_id": None,
                        "persisted_size": "0",
                        "live_size": str(live_size),
                        "delta": str(-live_size),
                        "kind": "untracked_live",
                    }
                )
    finally:
        await shutdown_pool()

    output = {
        "checked_users": len(checked_users),
        "user_ids": checked_users,
        "drift_count": len(drifts),
        "drifts": drifts,
    }
    if skipped_users:
        # An incomplete check must not exit 0: "we could not look" reads
        # identically to "nothing drifted" to any script checking drift_count.
        output["skipped_users"] = skipped_users
        output["error"] = "users_without_wallet_skipped"
    print(json.dumps(output, indent=2))
    return 1 if skipped_users else 0


async def _run_drift_async(args: argparse.Namespace) -> int:
    if args.drift_subcommand == "check":
        return await _run_check_async(args)
    return 1


def run_drift(args: argparse.Namespace) -> int:
    return asyncio.run(_run_drift_async(args))
