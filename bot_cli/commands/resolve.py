"""bot resolve — evidence-based manual terminal resolution of a stuck order.

Operator-facing replacement for direct DB edits: a read-only evidence report by
default, and an explicit ``--apply`` path that validates the operator's
evidence, writes the terminal status, releases the linked reservation, records a
``manual_resolution`` audit metadata entry and persists a backup of the affected
rows before any mutation.

The command still never submits or cancels an order directly. On a terminal
no-fill resolution it may, however, delegate one guarded, durably claimed
leader-SELL re-drive to the canonical ``LeaderSellTargetService`` — the same
one-shot path the reconciler uses for every other authoritative terminal
no-fill release. The delegation is best-effort: it can never fail, roll back or
mask the resolution it follows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

from config.app_config import EnvValidationError, load_config

# Statuses the reconciler still treats as unresolved. Anything else is terminal
# (or unknown) and must not be re-resolved manually.
_RESOLVABLE_STATUSES = frozenset(
    {
        "submitted_unconfirmed",
        "submitted",
        "matched",
        "matched_pending_confirmation",
        "order_status_matched",
        "submitted_delayed",
        "delayed",
        "order_live",
        "live",
        "ambiguous_timeout",
        "reconciliation_required",
        "unknown_unconfirmed",
    }
)

_TERMINAL_STATUS_BY_RESOLUTION = {
    "filled": "filled_reconciled",
    "canceled": "manually_canceled",
    "expired": "manually_expired",
    "confirmed_absent": "manually_confirmed_absent",
}

# Resolutions that prove the order freed its reserved shares without filling.
# Only these may delegate a leader-SELL re-drive; ``filled`` never can.
_NO_FILL_RESOLUTIONS = frozenset({"canceled", "expired", "confirmed_absent"})


class LeaderSellTargetRedriver(Protocol):
    """Re-drive a newer leader SELL target after terminal no-fill proof.

    Structurally identical to the reconciler's redriver port. Declared locally
    so the operator CLI never imports reconciliation or execution internals.
    """

    async def redrive_after_terminal_no_fill(
        self,
        record: Mapping[str, Any],
        *,
        reason: str,
    ) -> bool:
        """Attempt one durably claimed target re-drive."""

        ...


def _jsonable(value: Any) -> Any:
    """Render repository values into JSON-safe structures."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "__dict__"):
        return {key: _jsonable(item) for key, item in vars(value).items()}
    return str(value)


async def _fetch_order_row(orders_repo: Any, args: argparse.Namespace) -> dict[str, Any] | None:
    """Return the addressed orders row without applying the user guard.

    Args:
        orders_repo: Canonical orders repository.
        args: Parsed command arguments carrying the row selector.

    Returns:
        The orders row, or ``None`` when no row is addressed or found.
    """
    if getattr(args, "order_id", None):
        row = await orders_repo.get_by_order_id(args.order_id)
    elif getattr(args, "idempotency_key", None):
        row = await orders_repo.get_by_idempotency_key(args.idempotency_key)
    else:
        row = None
    return row if isinstance(row, dict) else None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


class ResolveCommand:
    """Render stuck-order evidence and, on explicit apply, persist a terminal decision."""

    def __init__(
        self,
        orders_repo: Any,
        reservations_repo: Any,
        *,
        backup_dir: str = "data",
        now: Callable[[], float] = time.time,
        emit: Callable[[str], None] = print,
        redriver: LeaderSellTargetRedriver | None = None,
        redriver_unavailable_reason: str | None = None,
    ) -> None:
        """Bind the persistence repositories and optional re-drive delegate.

        Args:
            orders_repo: Canonical orders repository.
            reservations_repo: Canonical reservations repository.
            backup_dir: Directory for the pre-write backup JSON.
            now: Monotonic-enough wall-clock source.
            emit: JSON sink for the command's single report line.
            redriver: Optional canonical leader-SELL target service. When
                absent the command stays completely venue-free.
            redriver_unavailable_reason: Why no re-driver could be composed;
                reported verbatim in the emitted JSON.
        """

        self._orders_repo = orders_repo
        self._reservations_repo = reservations_repo
        self._backup_dir = Path(backup_dir)
        self._now = now
        self._emit = emit
        self._redriver = redriver
        self._redriver_unavailable_reason = redriver_unavailable_reason

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------
    async def _load_order(self, args: argparse.Namespace) -> dict[str, Any] | None:
        row = await _fetch_order_row(self._orders_repo, args)
        if row is None:
            return None
        expected_user = getattr(args, "user_id", None)
        if expected_user and str(row.get("user_id") or "") != expected_user:
            return None
        return row

    async def _load_reservation(self, order: dict[str, Any]) -> Any | None:
        order_id = str(order.get("order_id") or "")
        if not order_id:
            return None
        try:
            return await self._reservations_repo.get_active_by_order_id(order_id)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Render
    # ------------------------------------------------------------------
    @staticmethod
    def _order_summary(order: dict[str, Any]) -> dict[str, Any]:
        request = order.get("request") if isinstance(order.get("request"), dict) else {}
        response = order.get("response") if isinstance(order.get("response"), dict) else {}
        return {
            "idempotency_key": order.get("idempotency_key"),
            "order_id": order.get("order_id"),
            "user_id": order.get("user_id"),
            "status": order.get("status"),
            "attempt_count": order.get("attempt_count"),
            "matched_total": order.get("matched_total"),
            "created_at": _jsonable(order.get("created_at")),
            "updated_at": _jsonable(order.get("updated_at")),
            "side": request.get("side"),
            "amount": request.get("amount"),
            "price_limit": request.get("price_limit"),
            "condition_id": request.get("condition_id"),
            "token_id": request.get("token_id"),
            "reservation_id": request.get("reservation_id"),
            "response_status": response.get("status"),
            "attempted_order_ids": _jsonable(response.get("attempted_order_ids")),
            "attributed_order_ids": _jsonable(response.get("attributed_order_ids")),
            "size_matched": response.get("size_matched"),
            "ambiguous_since": response.get("ambiguous_since"),
        }

    def _suggested_command(self, order: dict[str, Any]) -> str:
        order_id = str(order.get("order_id") or "<order_id>")
        return (
            f"python bot.py resolve --order-id {order_id} --apply --status filled "
            "--size-matched <filled_shares> --avg-price <avg_fill_price> "
            "--note '<evidence source>'"
        )

    # ------------------------------------------------------------------
    # Apply
    # ------------------------------------------------------------------
    def _validate(
        self,
        order: dict[str, Any],
        args: argparse.Namespace,
    ) -> tuple[str | None, Decimal | None, Decimal | None]:
        """Return ``(error, size_matched, avg_price)``; error None means apply may proceed."""
        order_id = str(order.get("order_id") or "")
        if not order_id:
            return "order_id_missing", None, None
        status = str(order.get("status") or "")
        if status not in _RESOLVABLE_STATUSES:
            return "order_status_terminal", None, None
        if args.status not in _TERMINAL_STATUS_BY_RESOLUTION:
            return "status_required", None, None
        if not str(getattr(args, "note", "") or "").strip():
            return "note_required", None, None
        size_matched = _decimal_or_none(getattr(args, "size_matched", None))
        avg_price = _decimal_or_none(getattr(args, "avg_price", None))
        if args.status == "filled":
            if size_matched is None or size_matched <= 0:
                return "invalid_size_matched", None, None
            if avg_price is None or not Decimal("0") < avg_price < Decimal("1"):
                return "invalid_avg_price", None, None
            return None, size_matched, avg_price
        if size_matched is not None and size_matched != 0:
            return "unexpected_size_matched_for_no_fill", None, None
        return None, None, None

    def _write_backup(self, order: dict[str, Any], reservation: Any | None) -> Path:
        self._backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.fromtimestamp(self._now(), tz=timezone.utc).strftime(
            "%Y%m%dT%H%M%S"
        )
        path = self._backup_dir / f"manual-resolution-backup-{timestamp}.json"
        payload = {
            "created": datetime.fromtimestamp(self._now(), tz=timezone.utc).isoformat(),
            "reservations": [_jsonable(reservation)] if reservation is not None else [],
            "orders": [_jsonable(order)],
        }
        path.write_text(json.dumps(payload, indent=1, sort_keys=True, default=str))
        return path

    async def _apply(
        self,
        order: dict[str, Any],
        reservation: Any | None,
        args: argparse.Namespace,
        size_matched: Decimal | None,
        avg_price: Decimal | None,
    ) -> dict[str, Any]:
        order_id = str(order["order_id"])
        prior_status = str(order.get("status") or "")
        target_status = _TERMINAL_STATUS_BY_RESOLUTION[str(args.status)]
        resolved_at = datetime.fromtimestamp(self._now(), tz=timezone.utc).isoformat()
        note = str(args.note).strip()

        backup_path = self._write_backup(order, reservation)

        manual_resolution = {
            "status": target_status,
            "prior_status": prior_status,
            "note": note,
            "operator": "bot_cli",
            "resolved_at": resolved_at,
            "size_matched": str(size_matched) if size_matched is not None else None,
            "avg_price": str(avg_price) if avg_price is not None else None,
        }
        response_patch: dict[str, Any] = {"manual_resolution": manual_resolution}
        if size_matched is not None:
            response_patch["size_matched"] = str(size_matched)
        if avg_price is not None:
            response_patch["avg_price"] = str(avg_price)

        await self._orders_repo.update_submission_status(
            order_id=order_id,
            status=target_status,
            response_patch=response_patch,
        )
        if args.status == "filled":
            await self._orders_repo.upsert_matched_total(
                order_id=order_id,
                matched=str(size_matched),
            )

        reservation_released = False
        if reservation is not None:
            reservation_id = str(getattr(reservation, "id", "") or "")
            if reservation_id:
                try:
                    await self._reservations_repo.record_manual_resolution(
                        reservation_id,
                        manual_resolution,
                    )
                except Exception:
                    pass
                await self._reservations_repo.mark_released(
                    reservation_id=reservation_id,
                    reason=f"manual_resolution:{args.status}",
                    released_at=int(self._now()),
                )
                reservation_released = True

        request = order.get("request") if isinstance(order.get("request"), dict) else {}
        trade_key = str(request.get("signal_id") or order.get("idempotency_key") or order_id)
        processed_action = "none"
        if args.status == "filled":
            await self._orders_repo.mark_processed_trade(
                trade_key=trade_key,
                user_id=str(order.get("user_id") or ""),
                leader_id=order.get("leader_id"),
                tx_hash=request.get("signal_id") or trade_key,
                side=str(request.get("side") or ""),
                asset=str(request.get("token_id") or ""),
                condition_id=str(request.get("condition_id") or ""),
                status="filled_reconciled",
                order_id=order_id,
            )
            processed_action = "marked_filled"
        else:
            try:
                await self._orders_repo.delete_processed_trade(trade_key)
                processed_action = "retry_unlocked"
            except Exception:
                processed_action = "unlock_failed"

        redrive, redrive_reason = await self._delegate_redrive(
            order,
            args,
            reservation_released=reservation_released,
        )

        result: dict[str, Any] = {
            "applied": True,
            "dry_run": False,
            "order_id": order_id,
            "status": target_status,
            "prior_status": prior_status,
            "backup": str(backup_path),
            "reservation_released": reservation_released,
            "processed_trade": processed_action,
            "manual_resolution": manual_resolution,
            "redrive": redrive,
        }
        if redrive_reason is not None:
            result["redrive_reason"] = redrive_reason
        return result

    async def _delegate_redrive(
        self,
        order: dict[str, Any],
        args: argparse.Namespace,
        *,
        reservation_released: bool,
    ) -> tuple[str, str | None]:
        """Hand one terminal no-fill release to the canonical target service.

        Mirrors the reconciler: the delegation runs only after an authoritative
        terminal no-fill release has actually been recorded, and the target
        service owns every guard (generation ordering, signal identity, the
        durable one-shot ``claim_redrive``, position read and sizing). This
        command adds no venue call of its own and never retries.

        Args:
            order: The order row loaded before the mutation.
            args: Parsed command arguments carrying the resolution status.
            reservation_released: Whether this apply released the reservation.

        Returns:
            ``(redrive, reason)`` where ``redrive`` is one of
            ``not_applicable`` / ``delegated`` / ``skipped`` / ``failed`` and
            ``reason`` is an optional operator-facing explanation.
        """

        status = str(args.status)
        if status not in _NO_FILL_RESOLUTIONS:
            return "not_applicable", None
        if self._redriver is None:
            return "skipped", self._redriver_unavailable_reason or "no_redriver_configured"
        if not reservation_released:
            return "skipped", "no_reservation_released"
        try:
            await self._redriver.redrive_after_terminal_no_fill(
                order,
                reason=f"manual_resolution:{status}",
            )
        except Exception as exc:
            # Fail-open on the resolution: the terminal state and the released
            # reservation are already durable and must stand on their own.
            return "failed", type(exc).__name__
        return "delegated", None

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    async def run(self, args: argparse.Namespace) -> int:
        try:
            order = await self._load_order(args)
        except Exception:
            self._emit(json.dumps({"error": "order_lookup_failed"}, indent=2))
            return 1
        if order is None:
            self._emit(json.dumps({"error": "order_not_found"}, indent=2))
            return 1

        reservation = await self._load_reservation(order)
        status = str(order.get("status") or "")
        resolvable = status in _RESOLVABLE_STATUSES

        if not getattr(args, "apply", False):
            report = {
                "dry_run": True,
                "resolvable": resolvable,
                "order": self._order_summary(order),
                "reservation": _jsonable(reservation),
                "suggested_command": self._suggested_command(order),
            }
            if not resolvable:
                report["reason"] = "order_status_terminal"
            self._emit(json.dumps(report, indent=2))
            return 0

        error, size_matched, avg_price = self._validate(order, args)
        if error is not None:
            self._emit(
                json.dumps(
                    {
                        "error": error,
                        "order": self._order_summary(order),
                    },
                    indent=2,
                )
            )
            return 1

        result = await self._apply(order, reservation, args, size_matched, avg_price)
        self._emit(json.dumps(result, indent=2))
        return 0


def register_resolve_command(subparsers: argparse._SubParsersAction) -> None:
    """Register the evidence-based manual resolution command."""
    parser = subparsers.add_parser(
        "resolve",
        help="Evidence-based manual terminal resolution of a stuck order",
    )
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--order-id", help="Local order hash (orders.order_id)")
    selector.add_argument("--idempotency-key", help="Orders row idempotency key")
    parser.add_argument("--user-id", default=None, help="Optional expected user id guard")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the resolution (default: read-only evidence report)",
    )
    parser.add_argument(
        "--status",
        choices=sorted(_TERMINAL_STATUS_BY_RESOLUTION),
        default=None,
        help="Terminal resolution to record (required with --apply)",
    )
    parser.add_argument(
        "--size-matched",
        default=None,
        help="Confirmed cumulative filled shares (required for --status filled)",
    )
    parser.add_argument(
        "--avg-price",
        default=None,
        help="Confirmed average fill price in (0,1) (required for --status filled)",
    )
    parser.add_argument(
        "--note",
        default=None,
        help="Operator evidence note (required with --apply)",
    )
    parser.add_argument(
        "--backup-dir",
        default="data",
        help="Directory for the pre-write backup JSON (default: data)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=True,
        help="Emit machine-readable JSON (default)",
    )


async def _compose_redriver(
    *,
    pool: Any,
    config: Any,
    args: argparse.Namespace,
    orders_repo: Any,
) -> tuple[LeaderSellTargetRedriver | None, str | None, Any]:
    """Compose the canonical leader-SELL re-driver for the addressed order.

    Reuses the production composition helpers so the re-drive runs through the
    exact same venue runtime, execution coordinator and target service the bot
    process uses. The re-driver is bound to one follower wallet, so it is only
    returned when that wallet owns the addressed order.

    Args:
        pool: Live database pool.
        config: Validated application configuration.
        args: Parsed command arguments.
        orders_repo: Canonical orders repository.

    Returns:
        ``(redriver, unavailable_reason, venue_runtime)``. ``redriver`` is
        ``None`` whenever the execution stack cannot be composed safely; the
        caller must close ``venue_runtime`` when it is not ``None``.
    """
    venue_runtime: Any = None
    try:
        from orchestrators.runtime_bootstrap import (
            build_user_state,
            build_user_venue_runtime,
            configured_leaders,
            ensure_runtime_follows,
        )
        from services.multiUserConfig import load_runtime_users

        order_row = await _fetch_order_row(orders_repo, args)
        if order_row is None:
            return None, "order_not_found", None
        order_user_id = str(order_row.get("user_id") or "").strip()
        if not order_user_id:
            return None, "order_user_unknown", None

        leaders = configured_leaders(config)
        runtime_users = load_runtime_users(leaders, config)
        # A05 — follows are explicit per-copier whitelists; the legacy
        # single-account default may populate the lone account's follows.
        # Follows are irrelevant to the order→account match below.
        ensure_runtime_follows(runtime_users, leaders, config)
        matches = [user for user in runtime_users if str(user.user_id) == order_user_id]
        if not matches:
            return None, "no_runtime_user_for_order", None
        if len(matches) > 1:
            return None, "ambiguous_runtime_user", None

        venue_runtime = await build_user_venue_runtime(matches[0], config)
        user_state = build_user_state(
            matches[0],
            venue_runtime.client,
            pool,
            config,
            clob_gateway=venue_runtime.gateway,
        )
        coordinator = getattr(user_state, "execution_coordinator", None)
        redriver = getattr(coordinator, "leader_sell_target_service", None)
        if redriver is None:
            return None, "execution_coordinator_unavailable", venue_runtime
        return redriver, None, venue_runtime
    except Exception as exc:
        # Fail-closed on submission, fail-open on the resolution: a missing
        # re-driver only downgrades the delegation, never the terminal write.
        return None, f"redriver_unavailable:{type(exc).__name__}", venue_runtime


def run_resolve(args: argparse.Namespace) -> int:
    """Build the Postgres repositories and run the resolution command."""
    try:
        config = load_config()
    except EnvValidationError as exc:
        print(json.dumps({"error": "config_validation_failed", "details": str(exc)}))
        return 1

    from storage.postgres.connection import init_pool, shutdown_pool
    from storage.postgres.orders_repo import OrdersRepo
    from storage.postgres.reservations_repo import ReservationsRepo

    async def _run() -> int:
        pool = await init_pool(
            config.database.dsn,
            min_size=1,
            max_size=2,
            command_timeout_seconds=15.0,
        )
        venue_runtime: Any = None
        try:
            orders_repo = OrdersRepo(pool)
            redriver: LeaderSellTargetRedriver | None = None
            redriver_reason: str | None = "redrive_not_attempted_for_dry_run"
            # A dry run mutates nothing, so it never needs an authenticated
            # venue runtime. Only an explicit --apply composes one.
            if getattr(args, "apply", False):
                redriver, redriver_reason, venue_runtime = await _compose_redriver(
                    pool=pool,
                    config=config,
                    args=args,
                    orders_repo=orders_repo,
                )
            command = ResolveCommand(
                orders_repo,
                ReservationsRepo(pool),
                backup_dir=getattr(args, "backup_dir", "data"),
                redriver=redriver,
                redriver_unavailable_reason=redriver_reason,
            )
            return await command.run(args)
        finally:
            if venue_runtime is not None:
                try:
                    await venue_runtime.close()
                except Exception:
                    pass
            await shutdown_pool()

    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 130
