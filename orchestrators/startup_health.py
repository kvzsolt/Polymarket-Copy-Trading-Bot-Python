from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from typing import Any

from aiohttp import ClientSession

from storage.sqlite.connection import create_connection
from storage.sqlite.positions_repo import PositionRecord, PositionsRepo
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


@dataclass(frozen=True)
class OrderStartupCleanupResult:
    normalized_pending: int
    normalized_delayed: int
    normalized_retry_pending: int
    normalized_total: int


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


def _balance_check(available_cash: float, *, warning_threshold: float = 5.0) -> StartupHealthCheckItem:
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


def reconcile_persisted_positions_to_live(
    positions_repo: PositionsRepo,
    *,
    user_id: str,
    live_positions: list[dict[str, Any]],
) -> PositionSyncResult:
    persisted = positions_repo.list_open_positions(user_id)
    live_keys = _live_position_keys(live_positions)

    stale: list[PositionRecord] = []
    for record in persisted:
        record_keys = _position_key_variants(record.condition_id, record.token_id)
        if not record_keys.intersection(live_keys):
            stale.append(record)

    for record in stale:
        positions_repo.delete_position(
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


def normalize_startup_order_statuses(db_path: str, *, user_id: str) -> OrderStartupCleanupResult:
    with create_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status IN ('pending', 'pending_lock') THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END) AS delayed_count,
                SUM(CASE WHEN status LIKE 'retry_pending_%' THEN 1 ELSE 0 END) AS retry_pending_count
            FROM orders
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()

        pending_count = int((row["pending_count"] or 0) if row else 0)
        delayed_count = int((row["delayed_count"] or 0) if row else 0)
        retry_pending_count = int((row["retry_pending_count"] or 0) if row else 0)

        conn.execute(
            """
            UPDATE orders
            SET
                status = 'terminal_failed',
                last_error = COALESCE(last_error, 'startup_reset_after_restart'),
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
              AND (
                status IN ('pending', 'pending_lock', 'delayed')
                OR status LIKE 'retry_pending_%'
              )
            """,
            (user_id,),
        )
        conn.commit()

    return OrderStartupCleanupResult(
        normalized_pending=pending_count,
        normalized_delayed=delayed_count,
        normalized_retry_pending=retry_pending_count,
        normalized_total=pending_count + delayed_count + retry_pending_count,
    )


def _load_order_status_counts(db_path: str, *, user_id: str) -> dict[str, int]:
    with create_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM orders
            WHERE user_id = ?
            GROUP BY status
            """,
            (user_id,),
        ).fetchall()
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
    }
    if order_cleanup.normalized_total > 0:
        return StartupHealthCheckItem(
            status="warning",
            message=(
                f"{message} | startup_normalized={order_cleanup.normalized_total}"
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


async def perform_startup_health_check(
    *,
    wallet: str,
    user_id: str,
    db_path: str,
    positions_repo: PositionsRepo,
    live_positions: list[dict[str, Any]],
    available_cash: float,
    database_ok: bool,
    rpc_url: str,
) -> StartupHealthReport:
    rpc_check, polymarket_check = await _check_rpc(rpc_url), await _check_polymarket_api()
    position_sync = reconcile_persisted_positions_to_live(
        positions_repo,
        user_id=user_id,
        live_positions=live_positions,
    )
    order_cleanup = normalize_startup_order_statuses(db_path, user_id=user_id)
    order_status_counts = _load_order_status_counts(db_path, user_id=user_id)
    order_summary = _summarize_order_status_counts(order_status_counts)

    checks: dict[str, StartupHealthCheckItem] = {
        "database": StartupHealthCheckItem(
            status="ok" if database_ok else "error",
            message="Connected" if database_ok else "Database not ready",
        ),
        "rpc": rpc_check,
        "balance": _balance_check(available_cash),
        "polymarketApi": polymarket_check,
        "positionSync": _position_sync_check(position_sync),
        "orders": _orders_check(order_status_counts, order_cleanup),
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
