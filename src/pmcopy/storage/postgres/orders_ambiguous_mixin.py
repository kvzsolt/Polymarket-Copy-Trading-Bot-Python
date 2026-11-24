"""Ambiguous submission recovery queries and legacy provenance ownership mapping."""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import logging

import asyncpg

from pmcopy.services.execution.ambiguous_recovery import (
    AmbiguousRecoveryProgress,
    AmbiguousSubmission,
    AuthoritativeRecoveryScan,
    RecoveryMatchSource,
)
from pmcopy.storage.postgres.orders_records import (
    _MAX_AMBIGUOUS_EVIDENCE_HISTORY,
    _MAX_AMBIGUOUS_RECOVERY_BATCH_SIZE,
    _MAX_AMBIGUOUS_REQUIRED_SCANS,
    LegacyOwnershipMappingReport,
    _affected,
    _bounded_positive,
    _json_str,
    _parse_jsonb,
    _row_to_ambiguous_submission,
)

logger = logging.getLogger(__name__)


class _OrdersAmbiguousMixin:
    """No-ID ambiguous recovery tracking and legacy ownership provenance mapping."""

    _pool: asyncpg.Pool

    def _quarantine_count_due(self, scope: str) -> bool:
        raise NotImplementedError

    async def list_no_id_ambiguous_submissions(
        self,
        *,
        user_id: str,
        limit: int,
    ) -> list[AmbiguousSubmission]:
        """Return a bounded oldest-first batch of one account's lost-POST rows.

        Only ``submitted_unconfirmed`` rows without a venue identity qualify.
        The ``user_id`` predicate is mandatory and applied BEFORE ``LIMIT`` so
        one account's recovery batch is structurally scoped to its own rows:
        with N follower accounts, N recovery loops can never race over the
        same batch.  Legacy rows with a blank/NULL ``user_id`` are excluded
        by the same predicate and counted into a logged quarantine reason
        pending provenance mapping
        (``map_legacy_unowned_rows_to_provenance_owners``) — an
        observability-only count throttled to at most once per TTL window,
        never a predicate on the returned batch.  No age predicate
        is applied here: the constructor-injected recovery clock owns the
        grace-period decision, which keeps that behavior fully deterministic
        in tests.  ``LIMIT`` still bounds the database and in-memory work for
        every recovery cycle.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to scope ambiguous recovery")
        bounded_limit = _bounded_positive(
            limit,
            field="limit",
            maximum=_MAX_AMBIGUOUS_RECOVERY_BATCH_SIZE,
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT idempotency_key, user_id, leader_id, order_id, status,
                       request_json, response_json, attempt_count, last_error,
                       created_at, updated_at,
                       COALESCE(matched_total, '0') AS matched_total,
                       token_id, amount_unit
                FROM orders
                WHERE status = 'submitted_unconfirmed'
                  AND order_id IS NULL
                  AND user_id = $1
                ORDER BY created_at ASC, idempotency_key ASC
                LIMIT $2
                """,
                owner,
                bounded_limit,
            )
            quarantined_legacy = 0
            if self._quarantine_count_due("ambiguous_orders"):
                quarantined_legacy = await conn.fetchval(
                    """
                    SELECT count(*) FROM orders
                    WHERE status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                      AND COALESCE(user_id, '') = ''
                    """
                )
        if quarantined_legacy:
            logger.warning(
                "ambiguous_recovery_legacy_rows_quarantined | count=%d "
                "reason=blank_user_id pending=provenance_mapping",
                int(quarantined_legacy),
            )

        submissions: list[AmbiguousSubmission] = []
        for row in rows:
            try:
                submissions.append(_row_to_ambiguous_submission(row))
            except (TypeError, ValueError) as exc:
                # Quarantine a malformed legacy row (e.g. blank ``user_id``)
                # instead of letting it poison the whole batch: the row keeps
                # blocking duplicates while the reason stays countable.
                logger.error(
                    "ambiguous_recovery_row_quarantined | key=%s reason=%s",
                    row["idempotency_key"],
                    type(exc).__name__,
                )
        return submissions

    async def record_ambiguous_scan_evidence(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        scan: AuthoritativeRecoveryScan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ) -> AmbiguousRecoveryProgress | None:
        """Atomically append a bounded evidence summary to an ambiguous row.

        Only consecutive scans that exhaust both private collections and find
        both account checks consistent with absence advance the proof.  Any
        incomplete or contradictory scan resets the counter to zero.  The
        counter saturates at the configured requirement and the retained scan
        tail is bounded independently, preventing response JSON growth.

        The statement compare-and-sets on the account identity: a scan
        recorded by any other account's recovery loop finds no row and is a
        complete no-op, so a foreign account's incomplete scan can never
        reset the owning account's absence proof.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to record ambiguous scan evidence")
        required = _bounded_positive(
            required_scan_count,
            field="required_scan_count",
            maximum=_MAX_AMBIGUOUS_REQUIRED_SCANS,
        )
        history_limit = _bounded_positive(
            evidence_history_limit,
            field="evidence_history_limit",
            maximum=_MAX_AMBIGUOUS_EVIDENCE_HISTORY,
        )
        if observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return None

                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                recovery = dict(existing_recovery) if isinstance(existing_recovery, dict) else {}
                existing_scans = recovery.get("scans")
                scans = (
                    [dict(item) for item in existing_scans if isinstance(item, dict)]
                    if isinstance(existing_scans, list)
                    else []
                )
                try:
                    previous_consecutive = max(
                        0,
                        int(recovery.get("consecutive_complete_absence_scans", 0)),
                    )
                except (TypeError, ValueError):
                    previous_consecutive = 0

                qualifies = scan.qualifies_as_complete_absence
                consecutive = min(previous_consecutive + 1, required) if qualifies else 0
                evidence = {
                    "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
                    "private_orders_complete": bool(scan.private_orders_complete),
                    "private_fills_complete": bool(scan.private_fills_complete),
                    "position_check": scan.position_check.value,
                    "balance_check": scan.balance_check.value,
                    "qualifies_as_complete_absence": qualifies,
                }
                if scan.match is not None:
                    evidence["match_source"] = scan.match.source.value
                    evidence["venue_order_id"] = scan.match.venue_order_id
                scans.append(evidence)
                scans = scans[-history_limit:]
                recovery.update(
                    {
                        "version": 1,
                        "consecutive_complete_absence_scans": consecutive,
                        "scans": scans,
                    }
                )
                response["ambiguous_recovery"] = recovery
                response["status"] = "submitted_unconfirmed"
                status = await conn.execute(
                    """
                    UPDATE orders
                    SET response_json = $3::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    """,
                    idempotency_key,
                    owner,
                    _json_str(response) or "{}",
                )
                if _affected(status) != 1:
                    return None

        return AmbiguousRecoveryProgress(
            consecutive_complete_absence_scans=consecutive,
            retained_evidence_entries=len(scans),
        )

    async def attach_recovered_venue_order_id(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        venue_order_id: str,
        source: RecoveryMatchSource,
        recovered_at: datetime,
    ) -> bool:
        """Atomically attach an authoritative identity without changing state.

        The row remains ``submitted_unconfirmed`` so normal order-id-based
        reconciliation, rather than this recovery service, determines fills
        and terminal status.  A concurrent resolution causes a no-op.  The
        compare-and-set includes the account identity: an identity recovered
        from one account's private history can only ever be attached to that
        same account's row, making cross-account attachment structurally
        impossible at the persistence layer.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to attach a recovered identity")
        order_id = str(venue_order_id).strip()
        if not order_id:
            raise ValueError("venue_order_id is required")
        if not isinstance(source, RecoveryMatchSource):
            raise TypeError("source must be a RecoveryMatchSource")
        if recovered_at.tzinfo is None:
            raise ValueError("recovered_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return False
                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                recovery = dict(existing_recovery) if isinstance(existing_recovery, dict) else {}
                recovery.update(
                    {
                        "version": 1,
                        "resolution": "recovered_order_id",
                        "match_source": source.value,
                        "recovered_at": recovered_at.astimezone(timezone.utc).isoformat(),
                    }
                )
                response.update(
                    {
                        "status": "submitted_unconfirmed",
                        "order_id": order_id,
                        "orderID": order_id,
                        "ambiguous_recovery": recovery,
                    }
                )
                returned = await conn.fetchval(
                    """
                    UPDATE orders
                    SET order_id = $3,
                        response_json = $4::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    RETURNING idempotency_key
                    """,
                    idempotency_key,
                    owner,
                    order_id,
                    _json_str(response) or "{}",
                )
        return returned is not None

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        """Resolve a no-ID ambiguity only after its persisted absence proof.

        The threshold and latest qualifying evidence are checked while holding
        the row lock, so a concurrent scan, recovered venue identity, or status
        transition cannot race an unsafe ``confirmed_absent`` update.  The
        compare-and-set also carries the account identity, so only the owning
        account's recovery loop can resolve its own row.
        """

        owner = str(user_id).strip()
        if not owner:
            raise ValueError("user_id is required to confirm an ambiguous absence")
        required = _bounded_positive(
            required_scan_count,
            field="required_scan_count",
            maximum=_MAX_AMBIGUOUS_REQUIRED_SCANS,
        )
        if confirmed_at.tzinfo is None:
            raise ValueError("confirmed_at must be timezone-aware")

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT request_json, response_json
                    FROM orders
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    FOR UPDATE
                    """,
                    idempotency_key,
                    owner,
                )
                if row is None:
                    return False
                request_doc = _parse_jsonb(row["request_json"])
                response = _parse_jsonb(row["response_json"])
                existing_recovery = response.get("ambiguous_recovery")
                if not isinstance(existing_recovery, dict):
                    return False
                recovery = dict(existing_recovery)
                try:
                    consecutive = int(recovery.get("consecutive_complete_absence_scans", 0))
                except (TypeError, ValueError):
                    return False
                scans = recovery.get("scans")
                latest = scans[-1] if isinstance(scans, list) and scans else None
                if (
                    consecutive < required
                    or not isinstance(latest, dict)
                    or latest.get("qualifies_as_complete_absence") is not True
                ):
                    return False

                confirmed_utc = confirmed_at.astimezone(timezone.utc)
                recovery.update(
                    {
                        "version": 1,
                        "resolution": "confirmed_absent",
                        "confirmed_at": confirmed_utc.isoformat(),
                        "required_scan_count": required,
                    }
                )
                response.update(
                    {
                        "success": False,
                        "status": "confirmed_absent",
                        "ambiguous_recovery": recovery,
                    }
                )
                returned = await conn.fetchval(
                    """
                    UPDATE orders
                    SET status = 'confirmed_absent',
                        response_json = $3::jsonb,
                        updated_at = now()
                    WHERE idempotency_key = $1
                      AND user_id = $2
                      AND status = 'submitted_unconfirmed'
                      AND order_id IS NULL
                    RETURNING idempotency_key
                    """,
                    idempotency_key,
                    owner,
                    _json_str(response) or "{}",
                )
                if returned is not None:
                    req_meta = request_doc.get("recovery_metadata")
                    task_res_id = (
                        str(req_meta.get("task_budget_reservation_id") or "").strip()
                        if isinstance(req_meta, dict)
                        else ""
                    )
                    if task_res_id:
                        with contextlib.suppress(Exception):
                            async with conn.transaction():
                                await conn.execute(
                                    """
                                    UPDATE copy_task_budget_reservations
                                    SET status = 'released',
                                        updated_at = $2
                                    WHERE reservation_id = $1
                                      AND status = 'reserved'
                                      AND COALESCE(settled_notional, 0) = 0
                                    """,
                                    task_res_id,
                                    confirmed_utc,
                                )
        return returned is not None


    async def map_legacy_unowned_rows_to_provenance_owners(
        self,
        *,
        apply: bool = False,
    ) -> LegacyOwnershipMappingReport:
        """One-shot maintenance mapping of legacy unowned rows to their owners.

        Audit A01 quarantines blank/NULL-``user_id`` rows from automatic
        recovery instead of guessing an owner.  This explicit maintenance
        helper resolves owners ONLY from durable provenance, and ONLY when
        every provenance source agrees:

        * ``orders`` rows: the persisted ``request_json->>'user_id'`` and the
          owner of the reservation named by
          ``request_json->'recovery_metadata'->>'reservation_id'`` (when
          present).  Disagreement or absence leaves the row quarantined.
        * ``processed_trades`` rows: the owning account of the single
          ``orders`` row sharing the trade's ``order_id``.

        With ``apply=False`` (default) nothing is written; the returned
        report counts what a subsequent ``apply=True`` call would map, skip
        as ambiguous, or leave unresolved.  Updates are compare-and-set on
        ``COALESCE(user_id, '') = ''`` so a concurrently written owner is
        never overwritten.  This is deliberately NOT wired into any runtime
        loop: an operator decides when (and whether) to run it.
        """

        orders_candidates = orders_mapped = orders_ambiguous = orders_unresolved = 0
        trades_candidates = trades_mapped = trades_ambiguous = trades_unresolved = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                legacy_orders = await conn.fetch(
                    """
                    SELECT idempotency_key,
                           COALESCE(request_json->>'user_id', '') AS request_user_id,
                           COALESCE(
                               request_json->'recovery_metadata'->>'reservation_id', ''
                           ) AS reservation_id
                    FROM orders
                    WHERE COALESCE(user_id, '') = ''
                    ORDER BY created_at ASC
                    """
                )
                for row in legacy_orders:
                    orders_candidates += 1
                    request_owner = str(row["request_user_id"] or "").strip()
                    reservation_owner: str | None = None
                    reservation_id = str(row["reservation_id"] or "").strip()
                    if reservation_id:
                        reservation_owner = await conn.fetchval(
                            """
                            SELECT COALESCE(user_id, '') FROM order_reservations
                            WHERE id = $1
                            """,
                            reservation_id,
                        )
                        reservation_owner = str(reservation_owner or "").strip() or None
                    owners = {
                        owner for owner in (request_owner or None, reservation_owner) if owner
                    }
                    if len(owners) > 1:
                        orders_ambiguous += 1
                        continue
                    if not owners:
                        orders_unresolved += 1
                        continue
                    owner = owners.pop()
                    if apply:
                        status = await conn.execute(
                            """
                            UPDATE orders
                            SET user_id = $2, updated_at = now()
                            WHERE idempotency_key = $1
                              AND COALESCE(user_id, '') = ''
                            """,
                            row["idempotency_key"],
                            owner,
                        )
                        if _affected(status) == 1:
                            orders_mapped += 1
                    else:
                        orders_mapped += 1

                legacy_trades = await conn.fetch(
                    """
                    SELECT trade_key, order_id
                    FROM processed_trades
                    WHERE COALESCE(user_id, '') = ''
                    ORDER BY processed_at ASC
                    """
                )
                for row in legacy_trades:
                    trades_candidates += 1
                    order_id = row["order_id"]
                    if not order_id:
                        trades_unresolved += 1
                        continue
                    owner_rows = await conn.fetch(
                        """
                        SELECT DISTINCT user_id FROM orders
                        WHERE order_id = $1 AND COALESCE(user_id, '') <> ''
                        """,
                        order_id,
                    )
                    owners = {
                        str(record["user_id"]).strip()
                        for record in owner_rows
                        if str(record["user_id"] or "").strip()
                    }
                    if len(owners) > 1:
                        trades_ambiguous += 1
                        continue
                    if not owners:
                        trades_unresolved += 1
                        continue
                    owner = owners.pop()
                    if apply:
                        status = await conn.execute(
                            """
                            UPDATE processed_trades
                            SET user_id = $2
                            WHERE trade_key = $1
                              AND COALESCE(user_id, '') = ''
                            """,
                            row["trade_key"],
                            owner,
                        )
                        if _affected(status) == 1:
                            trades_mapped += 1
                    else:
                        trades_mapped += 1

        return LegacyOwnershipMappingReport(
            orders_candidates=orders_candidates,
            orders_mapped=orders_mapped,
            orders_ambiguous=orders_ambiguous,
            orders_unresolved=orders_unresolved,
            trades_candidates=trades_candidates,
            trades_mapped=trades_mapped,
            trades_ambiguous=trades_ambiguous,
            trades_unresolved=trades_unresolved,
        )

