from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal  # noqa: F401 — used in stringified type hints
from typing import TYPE_CHECKING, Any

from aiohttp import ClientSession

if TYPE_CHECKING:
    from interfaces.clob_gateway import ClobGateway

from storage.postgres.positions_repo import PositionRecord, PositionsRepo
from utils.logger import get_logger

logger = get_logger("StartupHealth")

_PENDING_ORDER_STATUSES = {"pending", "pending_lock"}
_DELAYED_ORDER_STATUSES = {"delayed"}
_FAILED_ORDER_STATUSES = {
    "failed",
    "submission_failed",
    "created",
    "retryable_failed",
    "terminal_failed",
}
_SUCCESS_ORDER_STATUSES = {
    "submitted",
    "submitted_unconfirmed",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
    "duplicate_ignored",
}


@dataclass(frozen=True)
class StartupHealthCheckItem:
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PositionSyncResult:
    persisted_before: int
    persisted_after: int
    live_count: int
    pruned_positions: int
    pruned_condition_ids: tuple[str, ...]
    pruned_token_ids: tuple[str, ...]
    # True when the live-position snapshot could not be fetched. In that case
    # pruning is SKIPPED (a transient venue/data-API failure must never be
    # read as "the wallet holds nothing" and delete the local ledger).
    fetch_failed: bool = False


@dataclass(frozen=True)
class OrderStartupCleanupResult:
    normalized_pending: int
    normalized_delayed: int
    normalized_retry_pending: int
    normalized_total: int
    purged_failed: int


@dataclass(frozen=True)
class StartupHealthReport:
    healthy: bool
    status: str
    timestamp: int
    wallet: str
    checks: dict[str, StartupHealthCheckItem]
    position_sync: PositionSyncResult
    order_status_counts: dict[str, int]
    order_summary: dict[str, int]
    order_cleanup: OrderStartupCleanupResult

    def as_log_payload(self) -> dict[str, Any]:
        return {
            "type": "startup_health_check",
            "healthy": self.healthy,
            "status": self.status,
            "timestamp": self.timestamp,
            "wallet": self.wallet,
            "checks": {
                name: asdict(item)
                for name, item in self.checks.items()
            },
            "positionSync": asdict(self.position_sync),
            "orderStatusCounts": dict(self.order_status_counts),
            "orderSummary": dict(self.order_summary),
            "orderCleanup": asdict(self.order_cleanup),
        }


async def _check_rpc(rpc_url: str) -> StartupHealthCheckItem:
    try:
        async with ClientSession() as session:
            async with session.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1,
                },
                timeout=5,
            ) as response:
                if response.status != 200:
                    return StartupHealthCheckItem(
                        status="error",
                        message=f"HTTP {response.status}",
                        details={"http_status": response.status},
                    )
                payload = await response.json(content_type=None)
                result = payload.get("result")
                if not result:
                    return StartupHealthCheckItem(
                        status="error",
                        message="Invalid RPC response",
                        details={"payload": payload},
                    )
                return StartupHealthCheckItem(
                    status="ok",
                    message="RPC endpoint responding",
                    details={"block": result},
                )
    except Exception as exc:
        return StartupHealthCheckItem(
            status="error",
            message=f"RPC check failed: {exc}",
            details={"error": str(exc)},
        )


async def _check_polymarket_api() -> StartupHealthCheckItem:
    url = "https://data-api.polymarket.com/positions?user=0x0000000000000000000000000000000000000000"
    try:
        async with ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                if response.status != 200:
                    return StartupHealthCheckItem(
                        status="error",
                        message=f"HTTP {response.status}",
                        details={"http_status": response.status},
                    )
                payload = await response.json(content_type=None)
                if not isinstance(payload, list):
                    return StartupHealthCheckItem(
                        status="error",
                        message="Unexpected API payload",
                        details={"payload_type": type(payload).__name__},
                    )
                return StartupHealthCheckItem(
                    status="ok",
                    message="Polymarket API responding",
                    details={"sample_count": len(payload)},
                )
    except Exception as exc:
        return StartupHealthCheckItem(
            status="error",
            message=f"API check failed: {exc}",
            details={"error": str(exc)},
        )


def _balance_check(
    available_cash: "Decimal | float",
    *,
    warning_threshold: "Decimal | float" = 5.0,
) -> StartupHealthCheckItem:
    if available_cash <= 0:
        return StartupHealthCheckItem(
            status="error",
            message="Zero cash balance",
            details={"balance": round(float(available_cash), 8)},
        )
    if available_cash < warning_threshold:
        return StartupHealthCheckItem(
            status="warning",
            message=f"Low balance: ${available_cash:.2f}",
            details={"balance": round(float(available_cash), 8)},
        )
    return StartupHealthCheckItem(
        status="ok",
        message=f"Balance: ${available_cash:.2f}",
        details={"balance": round(float(available_cash), 8)},
    )


def _position_key_variants(condition_id: str, token_id: str) -> set[tuple[str, str]]:
    variants: set[tuple[str, str]] = set()
    if condition_id and token_id:
        variants.add((condition_id, token_id))
    if condition_id:
        variants.add((condition_id, "*"))
    if token_id:
        variants.add(("*", token_id))
    return variants


def _live_position_keys(live_positions: list[dict[str, Any]]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for position in live_positions:
        condition_id = str(position.get("conditionId") or position.get("condition_id") or "")
        token_id = str(position.get("tokenID") or position.get("token_id") or position.get("asset") or "")
        keys.update(_position_key_variants(condition_id, token_id))
    return keys


async def reconcile_persisted_positions_to_live(
    positions_repo: PositionsRepo,
    *,
    user_id: str,
    live_positions: list[dict[str, Any]] | None,
) -> PositionSyncResult:
    persisted = await positions_repo.list_open_positions(user_id)

    # Fail closed: a None snapshot means the live-position fetch failed. We must
    # not prune — deleting every persisted position because a transient API
    # error returned "nothing" would destroy the basis for protective exits,
    # SELL mirroring, and exposure caps.
    if live_positions is None:
        return PositionSyncResult(
            persisted_before=len(persisted),
            persisted_after=len(persisted),
            live_count=0,
            pruned_positions=0,
            pruned_condition_ids=(),
            pruned_token_ids=(),
            fetch_failed=True,
        )

    live_keys = _live_position_keys(live_positions)

    stale: list[PositionRecord] = []
    for record in persisted:
        record_keys = _position_key_variants(record.condition_id, record.token_id)
        if not record_keys.intersection(live_keys):
            stale.append(record)

    for record in stale:
        await positions_repo.delete_position(
            user_id,
            record.condition_id,
            token_id=record.token_id,
        )

    return PositionSyncResult(
        persisted_before=len(persisted),
        persisted_after=len(persisted) - len(stale),
        live_count=len(live_positions),
        pruned_positions=len(stale),
        pruned_condition_ids=tuple(record.condition_id for record in stale[:12]),
        pruned_token_ids=tuple(record.token_id for record in stale[:12]),
    )


async def normalize_startup_order_statuses(pool, *, user_id: str) -> OrderStartupCleanupResult:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status IN ('pending', 'pending_lock') THEN 1 ELSE 0 END), 0) AS pending_count,
                    COALESCE(SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END), 0) AS delayed_count,
                    COALESCE(SUM(CASE WHEN status LIKE 'retry_pending_%' THEN 1 ELSE 0 END), 0) AS retry_pending_count
                FROM orders
                WHERE user_id = $1
                """,
                user_id,
            )

            pending_count = int(row["pending_count"] if row else 0)
            delayed_count = int(row["delayed_count"] if row else 0)
            retry_pending_count = int(row["retry_pending_count"] if row else 0)

            await conn.execute(
                """
                UPDATE orders
                SET
                    status = 'terminal_failed',
                    last_error = COALESCE(last_error, 'startup_reset_after_restart'),
                    updated_at = now()
                WHERE user_id = $1
                  AND (
                    status IN ('pending', 'pending_lock', 'delayed')
                    OR status LIKE 'retry_pending_%'
                  )
                """,
                user_id,
            )

            purge_row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS purge_count
                FROM orders
                WHERE user_id = $1
                  AND (
                    status IN ('failed', 'submission_failed', 'created', 'retryable_failed', 'terminal_failed')
                    OR status LIKE 'retryable_failed_%'
                    OR status LIKE 'retry_pending_%'
                  )
                """,
                user_id,
            )
            purged_failed = int(purge_row["purge_count"] if purge_row else 0)

            await conn.execute(
                """
                DELETE FROM orders
                WHERE user_id = $1
                  AND (
                    status IN ('failed', 'submission_failed', 'created', 'retryable_failed', 'terminal_failed')
                    OR status LIKE 'retryable_failed_%'
                    OR status LIKE 'retry_pending_%'
                  )
                """,
                user_id,
            )

    return OrderStartupCleanupResult(
        normalized_pending=pending_count,
        normalized_delayed=delayed_count,
        normalized_retry_pending=retry_pending_count,
        normalized_total=pending_count + delayed_count + retry_pending_count,
        purged_failed=purged_failed,
    )


async def _load_order_status_counts(pool, *, user_id: str) -> dict[str, int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT status, COUNT(*) AS count
            FROM orders
            WHERE user_id = $1
            GROUP BY status
            """,
            user_id,
        )
    return {str(row["status"] or "unknown"): int(row["count"] or 0) for row in rows}


def _summarize_order_status_counts(order_status_counts: dict[str, int]) -> dict[str, int]:
    successful = 0
    failed = 0

    for status, count in order_status_counts.items():
        status_name = str(status or "").strip().lower()
        if status_name in _SUCCESS_ORDER_STATUSES:
            successful += count
            continue
        if (
            status_name in _PENDING_ORDER_STATUSES
            or status_name in _DELAYED_ORDER_STATUSES
            or status_name in _FAILED_ORDER_STATUSES
            or status_name.startswith("retry_pending_")
            or status_name.startswith("retryable_failed_")
        ):
            failed += count
            continue
        failed += count

    return {
        "successful": successful,
        "failed": failed,
    }


def _check_atomic_lock(orders_repo) -> StartupHealthCheckItem:
    """Verify the orders repo supports atomic submission locking (issue #95).

    Live trading without ``try_acquire_submission_lock`` can result in
    duplicate order submissions from concurrent tasks. This check gates
    startup if the lock is missing.

    PR #105 review: ``orders_repo=None`` previously degraded to a warning,
    which silently bypassed the gate when a caller forgot to wire the repo
    through. The check is the gate — a missing repo is an error, not a
    warning. Callers must supply ``orders_repo`` explicitly.
    """
    if orders_repo is None:
        return StartupHealthCheckItem(
            status="error",
            message=(
                "orders_repo not supplied to health check — atomic submission "
                "lock gate cannot be verified"
            ),
            details={"method": "try_acquire_submission_lock"},
        )
    has_lock = hasattr(orders_repo, "try_acquire_submission_lock")
    if not has_lock:
        return StartupHealthCheckItem(
            status="error",
            message=(
                "order repository lacks atomic submission lock — "
                "duplicate order submissions possible in live trading"
            ),
            details={"method": "try_acquire_submission_lock"},
        )
    return StartupHealthCheckItem(
        status="ok",
        message="atomic submission lock available",
        details={"method": "try_acquire_submission_lock"},
    )


def _orders_check(
    order_status_counts: dict[str, int],
    order_cleanup: OrderStartupCleanupResult,
) -> StartupHealthCheckItem:
    summary = _summarize_order_status_counts(order_status_counts)
    message = f"orders successful={summary['successful']} failed={summary['failed']}"
    details = {
        **summary,
        "normalized_pending": order_cleanup.normalized_pending,
        "normalized_delayed": order_cleanup.normalized_delayed,
        "normalized_retry_pending": order_cleanup.normalized_retry_pending,
        "purged_failed": order_cleanup.purged_failed,
    }
    if order_cleanup.purged_failed > 0 or order_cleanup.normalized_total > 0:
        return StartupHealthCheckItem(
            status="warning",
            message=(
                f"{message} | startup_purged_failed={order_cleanup.purged_failed}"
            ),
            details=details,
        )
    if summary["failed"] > 0:
        return StartupHealthCheckItem(
            status="warning",
            message=message,
            details=details,
        )
    return StartupHealthCheckItem(
        status="ok",
        message=message,
        details=details,
    )


def _position_sync_check(sync: PositionSyncResult) -> StartupHealthCheckItem:
    if sync.fetch_failed:
        return StartupHealthCheckItem(
            status="error",
            message=(
                f"live position snapshot fetch failed; skipped pruning "
                f"{sync.persisted_before} persisted position(s) (fail-closed)"
            ),
            details=asdict(sync),
        )
    if sync.pruned_positions > 0:
        return StartupHealthCheckItem(
            status="warning",
            message=(
                f"pruned {sync.pruned_positions} stale persisted position(s) "
                f"to match {sync.live_count} live position(s)"
            ),
            details=asdict(sync),
        )
    return StartupHealthCheckItem(
        status="ok",
        message=f"persisted state matches {sync.live_count} live position(s)",
        details=asdict(sync),
    )


async def _check_clob_gateway(gateway: ClobGateway | None) -> StartupHealthCheckItem:
    """Probe CLOB V2 gateway compatibility (issue #66).

    Always returns a check item — never raises. The caller decides whether to
    fail closed (live mode) or warn (paper mode).
    """
    if gateway is None:
        return StartupHealthCheckItem(
            status="warning",
            message="CLOB gateway not wired (paper mode without venue connectivity)",
        )
    probe = getattr(gateway, "check_compatibility", None)
    if probe is None:
        return StartupHealthCheckItem(
            status="warning",
            message="CLOB gateway does not implement check_compatibility",
        )
    try:
        report = await asyncio.wait_for(probe(), timeout=5.0)
    except asyncio.TimeoutError:
        return StartupHealthCheckItem(
            status="error",
            message="CLOB gateway compatibility probe timed out after 5s",
        )
    except Exception as exc:  # pragma: no cover — gateway should not raise
        return StartupHealthCheckItem(
            status="error",
            message=f"CLOB gateway compatibility probe raised: {exc!s}",
        )
    sdk_ok = bool(report.get("sdk_available"))
    if not sdk_ok:
        return StartupHealthCheckItem(
            status="error",
            message=str(report.get("error") or "CLOB SDK not available"),
            details=report,
        )
    return StartupHealthCheckItem(
        status="ok",
        message="CLOB V2 gateway ready",
        details=report,
    )


async def perform_startup_health_check(
    *,
    wallet: str,
    user_id: str,
    pool,
    positions_repo: PositionsRepo,
    orders_repo=None,  # type: ignore[assignment] | OrdersRepo | None
    live_positions: list[dict[str, Any]] | None,
    available_cash: "Decimal | float",
    database_ok: bool,
    rpc_url: str,
    clob_gateway: ClobGateway | None = None,
    require_live_clob: bool = False,
) -> StartupHealthReport:
    rpc_check, polymarket_check, clob_check = await asyncio.gather(
        _check_rpc(rpc_url),
        _check_polymarket_api(),
        _check_clob_gateway(clob_gateway),
    )
    # In live mode, downgrade clob_check warnings to errors so the report
    # refuses to mark the runtime healthy. Paper mode keeps the warning.
    if require_live_clob and clob_check.status != "ok":
        clob_check = StartupHealthCheckItem(
            status="error",
            message=clob_check.message,
            details=clob_check.details,
        )
    position_sync = await reconcile_persisted_positions_to_live(
        positions_repo,
        user_id=user_id,
        live_positions=live_positions,
    )
    order_cleanup = await normalize_startup_order_statuses(pool, user_id=user_id)
    order_status_counts = await _load_order_status_counts(pool, user_id=user_id)
    order_summary = _summarize_order_status_counts(order_status_counts)

    checks: dict[str, StartupHealthCheckItem] = {
        "database": StartupHealthCheckItem(
            status="ok" if database_ok else "error",
            message="Connected" if database_ok else "Database not ready",
        ),
        "rpc": rpc_check,
        "balance": _balance_check(available_cash),
        "polymarketApi": polymarket_check,
        "clobGateway": clob_check,
        "positionSync": _position_sync_check(position_sync),
        "orders": _orders_check(order_status_counts, order_cleanup),
        "atomicLock": _check_atomic_lock(orders_repo),
    }

    statuses = [item.status for item in checks.values()]
    if any(status == "error" for status in statuses):
        overall_status = "error"
    elif any(status == "warning" for status in statuses):
        overall_status = "warning"
    else:
        overall_status = "ok"

    report = StartupHealthReport(
        healthy=overall_status != "error",
        status=overall_status,
        timestamp=int(time.time()),
        wallet=wallet,
        checks=checks,
        position_sync=position_sync,
        order_status_counts=order_status_counts,
        order_summary=order_summary,
        order_cleanup=order_cleanup,
    )

    logger.info(
        (
            f"STARTUP HEALTH | status={report.status} | wallet={wallet} | "
            f"live={position_sync.live_count} | persisted_before={position_sync.persisted_before} | "
            f"persisted_after={position_sync.persisted_after} | pruned={position_sync.pruned_positions}"
        ),
        extra=report.as_log_payload(),
    )
    return report
