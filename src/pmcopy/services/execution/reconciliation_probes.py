"""Ambiguous auto-release probes, reservation release, lifecycle audit, and anomaly mixin."""

from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, cast

from pmcopy.services.execution.reconciliation_models import (
    _MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS,
    _TERMINAL_FILLED_SUBMISSION_STATUSES,
    _TERMINAL_NO_FILL_STATUSES,
    _ReconciliationMixinBase,
    _decimal_or_zero,
    _matched_amount_from_response,
    _normalize_trade_status,
    _normalize_venue_status,
    _order_id_candidates,
    _order_id_set,
    _probe_order_id_candidates,
    _record_created_at,
)
from pmcopy.utils.logger import get_logger, redact_error_detail

_log = get_logger("ReconciliationService")


class _ReconciliationProbesMixin(_ReconciliationMixinBase):
    """Auto-release probing, reservation lifecycle, timeout, and drift-anomaly helpers."""

    # ------------------------------------------------------------------
    # Manual-sync write-back
    # ------------------------------------------------------------------
    async def _record_funder_activity(
        self,
        *,
        record: dict[str, Any],
        side: str,
        order_id: str | None,
        size: Any,
        price: Any,
    ) -> None:
        """Write the copy-trade tx_hash into ``user_activities`` for the funder.

        The manual-sync detector dedups against ``user_activities`` by
        ``transaction_hash``; without this write-back the first manual-sync
        pass after a copy execution would mis-classify the bot's own fill
        as a manual trade. Idempotent via ``find_activity_by_tx_hash`` so
        retried reconciliations don't double-insert.
        """
        if not self.funder_address:
            return
        tx_hash = str(record.get("tx_hash") or "").strip()
        if not tx_hash:
            return
        try:
            from pmcopy.storage.postgres.user_history_repo import (
                find_activity_by_tx_hash,
                get_user_activity_model,
            )

            if await find_activity_by_tx_hash(self.funder_address, tx_hash):
                return

            payload = {
                "transactionHash": tx_hash,
                "side": side,
                "size": str(size) if size is not None else "0",
                "price": str(price) if price is not None else "0",
                "asset": record.get("asset"),
                "conditionId": record.get("condition_id"),
                "orderId": order_id,
                "leaderId": record.get("leader_id"),
                "bot": True,
                "recordedAt": int(__import__("time").time()),
            }
            await get_user_activity_model(self.funder_address).insert_many([payload])
        except Exception as exc:
            # Write-back is best-effort — never break reconciliation on a
            # logging-only side effect.
            _log.debug(
                "reconcile_funder_activity_writeback_failed | tx=%s error=%s",
                tx_hash,
                redact_error_detail(exc),
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _audit(self, event: str, payload: dict[str, Any]) -> None:
        logger = self.audit_logger
        if logger is None:
            return
        try:
            logger.log_event(event, payload)
        except Exception:
            # Audit logging must never break reconciliation.
            pass

    async def _release_reservation_if_present(
        self,
        record: dict[str, Any],
        *,
        reason: str,
    ) -> None:
        """Release a reservation when the record carries reservation_id.

        Coordinator-path orders populate this either at the top level of
        ``request`` or inside ``request.recovery_metadata`` (PR6). Falls back
        to the top-level ``record["reservation_id"]`` if neither is present.
        Safe no-op when ``reservation_service`` is unset.
        """
        if self.reservation_service is None:
            return
        request = record.get("request") if isinstance(record, dict) else None
        reservation_id: Any = None
        reservation_source: str | None = None
        if isinstance(request, dict):
            reservation_id = request.get("reservation_id")
            if reservation_id:
                reservation_source = "request"
            if not reservation_id:
                recovery = request.get("recovery_metadata")
                if isinstance(recovery, dict):
                    reservation_id = recovery.get("reservation_id")
                    if reservation_id:
                        reservation_source = "request.recovery_metadata"
        if not reservation_id:
            reservation_id = record.get("reservation_id")
            if reservation_id:
                reservation_source = "record"
        if not reservation_id:
            order_id = str(record.get("order_id") or "").strip()
            getter = getattr(self.orders_repo, "get_by_order_id", None)
            if order_id and callable(getter):
                try:
                    order_row = await cast(Callable[[str], Awaitable[Any]], getter)(order_id)
                except Exception:
                    order_row = None
                if isinstance(order_row, dict):
                    order_request = order_row.get("request")
                    if isinstance(order_request, dict):
                        reservation_id = order_request.get("reservation_id")
                        if reservation_id:
                            reservation_source = "orders.request"
                        if not reservation_id:
                            recovery = order_request.get("recovery_metadata")
                            if isinstance(recovery, dict):
                                reservation_id = recovery.get("reservation_id")
                                if reservation_id:
                                    reservation_source = "orders.request.recovery_metadata"
                    if not reservation_id:
                        reservation_id = order_row.get("reservation_id")
                        if reservation_id:
                            reservation_source = "orders.row"
        if not reservation_id:
            reservation_id = await self._lookup_active_reservation_id_for_order(
                record.get("order_id")
            )
            if reservation_id:
                reservation_source = "order_reservations.order_id"
        if not reservation_id:
            return
        try:
            _log.info(
                "reconciliation_reservation_release | reservation_id=%s reason=%s source=%s order_id=%s",
                reservation_id,
                reason,
                reservation_source or "unknown",
                record.get("order_id"),
            )
            self._audit(
                "reconciliation_reservation_release",
                {
                    "reservation_id": str(reservation_id),
                    "reason": reason,
                    "source": reservation_source or "unknown",
                    "order_id": record.get("order_id"),
                },
            )
            await self.reservation_service.release_terminal(str(reservation_id), reason=reason)
        except Exception as exc:
            _log.warning(
                "reconciliation_reservation_release_failed | reservation_id=%s reason=%s error=%s",
                reservation_id,
                reason,
                redact_error_detail(exc),
            )
            return
        if self.leader_sell_target_service is None:
            return
        try:
            await self.leader_sell_target_service.redrive_after_terminal_no_fill(
                record,
                reason=reason,
            )
        except Exception as exc:
            _log.warning(
                "leader_sell_target_redrive_failed | reservation_id=%s order_id=%s "
                "reason=%s error=%s",
                reservation_id,
                record.get("order_id"),
                reason,
                redact_error_detail(exc),
            )

    async def _lookup_active_reservation_id_for_order(self, order_id: Any) -> str | None:
        repo = getattr(self.reservation_service, "reservations_repo", None)
        lookup = getattr(repo, "get_active_by_order_id", None)
        if not callable(lookup):
            return None
        for candidate in _order_id_candidates(order_id):
            try:
                reservation = await cast(Callable[[str], Awaitable[Any]], lookup)(candidate)
            except Exception:
                continue
            reservation_id = getattr(reservation, "id", None)
            if reservation_id:
                return str(reservation_id)
        return None

    def _emit_lifecycle(
        self,
        *,
        status: str,
        side: str,
        record: dict[str, Any],
        order_id: str | None,
        venue_status: str | None = None,
        size: Any = None,
        price: Any = None,
        usdc_value: Any = None,
        signal_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Emit an order_lifecycle log via the canonical helper.

        Best-effort: never breaks reconciliation. The helper deduplicates by
        (order_id|signal_id|condition_id, status), scoped per account when no
        venue order id exists, so repeated reconciliation passes never
        re-render the same panel.
        """
        try:
            from pmcopy.utils.logger import log_order_lifecycle as _log_lc

            _log_lc(
                _log,
                status=status,
                side=side,
                condition_id=record.get("condition_id") or record.get("conditionId"),
                asset=record.get("asset") or record.get("token_id"),
                order_id=order_id,
                size=float(size) if size is not None else None,
                price=float(price) if price is not None else None,
                usdc_value=float(usdc_value) if usdc_value is not None else None,
                venue_status=venue_status,
                signal_id=signal_id or record.get("trade_key") or record.get("signal_id"),
                account=str(record.get("user_id") or "") or None,
                meta=meta,
            )
        except Exception:
            pass

    def _should_skip_for_delay(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
        last_known_venue_status: str | None = None,
    ) -> bool:
        """Return True if the reconciler should defer polling for this record.

        Only applies before the configured ``initial_delay_seconds`` elapses
        AND while no fill has been recorded locally. Once a venue status has
        been observed at least once (i.e. the record carries a non-empty
        response.status that's not the pending placeholder) the delay no
        longer applies.
        """
        if order_confirmation_cfg is None:
            return False

        created_at = _record_created_at(record)
        if created_at is None:
            return False

        token_id = str(record.get("asset") or record.get("token_id") or "")
        condition_id = str(record.get("condition_id") or record.get("conditionId") or "")
        delay = order_confirmation_cfg.resolve_initial_delay(
            token_id=token_id,
            condition_id=condition_id,
            venue_status=last_known_venue_status,
        )
        # Treat any existing matched-total > 0 as "already observed", skip the delay.
        previous_matched = _matched_amount_from_response(record.get("response"))
        if previous_matched > 0:
            return False
        elapsed = time.time() - created_at
        return elapsed < delay

    def _is_past_ambiguous_timeout(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        return (time.time() - created_at) >= float(order_confirmation_cfg.max_wait_seconds)

    def _is_past_auto_release_grace(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        if not getattr(order_confirmation_cfg, "auto_release_ambiguous", False):
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        threshold = float(order_confirmation_cfg.max_wait_seconds) + float(
            order_confirmation_cfg.auto_release_grace_seconds
        )
        return (time.time() - created_at) >= threshold

    def _is_past_baseline_missing_horizon(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        """Return whether an uncapturable baseline may stop blocking release.

        Args:
            record: The persisted submission record.
            order_confirmation_cfg: Confirmation policy, or ``None``.

        Returns:
            True once the record is older than the configured horizon. ``0``
            (the default) disables the escape entirely.
        """
        if order_confirmation_cfg is None:
            return False
        horizon = float(
            getattr(
                order_confirmation_cfg,
                "auto_release_baseline_missing_after_seconds",
                0.0,
            )
            or 0.0
        )
        if horizon <= 0:
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        return (time.time() - created_at) >= horizon

    @staticmethod
    def _classify_probe_exception(exc: Exception) -> str:
        """Classify an auto-release probe exception.

        ``clean``: the venue affirmatively knows nothing about the order (404).
        ``inconclusive``: transient venue trouble (5xx/rate-limit) — we cannot
        know right now; release stays blocked, but this is expected during an
        outage and must not count as a hard probe error.
        ``error``: anything else (fail-closed).
        """
        try:
            from pmcopy.integrations.polymarket.errors import (
                VenueOrderNotFoundError,
                VenueRateLimitError,
                VenueUnavailableError,
            )
        except Exception:
            return "error"
        if isinstance(exc, VenueOrderNotFoundError):
            return "clean"
        if isinstance(exc, (VenueRateLimitError, VenueUnavailableError)):
            return "inconclusive"
        return "error"

    async def _run_auto_release_probes(
        self,
        *,
        order_id: str,
        venue_order_id: str | None,
        market_id: str | None,
        trade_ids: list[str],
        user_id: str,
        condition_id: str,
        token_id: str,
        expected_size: Decimal,
        side: str = "",
        pre_submit_position_size: Decimal | None = None,
        order_id_candidates: list[str] | None = None,
        created_ts: float | None = None,
    ) -> dict[str, Any]:
        """Run every independent safety probe and classify its outcome.

        Each probe is classified as clean/inconclusive/error. The caller
        decides whether to auto-release based on the configured policy.
        """
        result: dict[str, Any] = {
            "errors": 0,
            "inconclusive": 0,
            "clean": 0,
            "details": {},
        }
        candidates: list[str] = []
        for candidate in [venue_order_id or order_id, order_id, *(order_id_candidates or [])]:
            text = str(candidate or "").strip()
            if text and text not in candidates:
                candidates.append(text)
        candidate_set = set(candidates)

        gw = self.clob_gateway
        if gw is None:
            result["errors"] += 1
            result["details"]["clob_gateway_missing"] = True
            return result

        # A backed-off venue source cannot produce an all-clean probe battery
        # anyway; defer without touching the gateway so the auto-release path
        # stays silent (instead of spamming errors) during an outage.
        for source in ("order", "open_orders", "trade_fills", "account_fills"):
            backed_off, reason = self._source_backed_off(source)
            if backed_off:
                result["deferred"] = True
                result["details"]["deferred_source"] = source
                result["details"]["deferred_reason"] = reason
                return result

        # Probe 1: get_order across every known candidate id.
        found_status: str | None = None
        absent_status: str | None = None
        unavailable_order = False
        order_error: Exception | None = None
        for candidate in candidates:
            try:
                venue_state = await gw.get_order(candidate)
                status = _normalize_venue_status(
                    getattr(venue_state, "status", None)
                    if not isinstance(venue_state, dict)
                    else venue_state.get("status")
                )
                if status in _TERMINAL_NO_FILL_STATUSES or status in {
                    "",
                    "absent",
                    "not_found",
                }:
                    absent_status = status or "absent"
                    continue
                found_status = status
                break
            except Exception as exc:
                kind = self._classify_probe_exception(exc)
                if kind == "clean":
                    absent_status = "absent"
                    continue
                if kind == "inconclusive":
                    self._record_poll_failure("order", order_id, exc)
                    unavailable_order = True
                    break
                order_error = exc
                break
        if found_status is not None:
            result["inconclusive"] += 1
            result["details"]["get_order"] = found_status
        elif unavailable_order:
            result["inconclusive"] += 1
            result["details"]["get_order"] = "venue_unavailable"
        elif order_error is not None:
            result["errors"] += 1
            result["details"]["get_order_error"] = redact_error_detail(order_error)
        else:
            result["clean"] += 1
            result["details"]["get_order"] = absent_status or "absent"

        # Probe 2: exact authenticated open-order lookup.
        list_orders = getattr(gw, "list_open_orders", None)
        if callable(list_orders):
            try:
                ids: set[str] = set()
                try:
                    for candidate in candidates:
                        orders = await cast(Callable[..., Awaitable[Any]], list_orders)(
                            order_id=candidate,
                            market=market_id,
                        )
                        ids.update(_order_id_set(orders))
                except TypeError:
                    orders = await cast(Callable[..., Awaitable[Any]], list_orders)()
                    ids = _order_id_set(orders)
                if ids & candidate_set:
                    result["inconclusive"] += 1
                    result["details"]["list_open_orders"] = "order_present"
                else:
                    result["clean"] += 1
                    result["details"]["list_open_orders"] = "absent"
            except Exception as exc:
                kind = self._classify_probe_exception(exc)
                if kind == "clean":
                    result["clean"] += 1
                    result["details"]["list_open_orders"] = "absent"
                elif kind == "inconclusive":
                    self._record_poll_failure("open_orders", order_id, exc)
                    result["inconclusive"] += 1
                    result["details"]["list_open_orders"] = "venue_unavailable"
                else:
                    result["errors"] += 1
                    result["details"]["list_open_orders_error"] = redact_error_detail(exc)
        else:
            result["inconclusive"] += 1
            result["details"]["list_open_orders"] = "not_supported"

        # Probe 3: each persisted trade ID. Any returned trade means settlement
        # evidence still exists and release is unsafe, regardless of its status.
        for trade_id in trade_ids:
            try:
                trade_fills = await gw.get_fills(
                    market=market_id,
                    trade_id=trade_id,
                    order_id=venue_order_id or order_id,
                )
                if trade_fills:
                    result["inconclusive"] += 1
                    result["details"][f"trade:{trade_id}"] = "present"
                else:
                    result["clean"] += 1
                    result["details"][f"trade:{trade_id}"] = "absent"
            except Exception as exc:
                kind = self._classify_probe_exception(exc)
                if kind == "clean":
                    result["clean"] += 1
                    result["details"][f"trade:{trade_id}"] = "absent"
                elif kind == "inconclusive":
                    self._record_poll_failure("trade_fills", order_id, exc)
                    result["inconclusive"] += 1
                    result["details"][f"trade:{trade_id}"] = "venue_unavailable"
                else:
                    result["errors"] += 1
                    result["details"][f"trade:{trade_id}:error"] = redact_error_detail(exc)

        # Probe 4: exact account fills for the order (every candidate id).
        try:
            try:
                fills = []
                for candidate in candidates:
                    fills.extend(
                        await gw.get_fills(
                            market=market_id,
                            order_id=candidate,
                        )
                    )
            except TypeError:
                fills = await gw.get_fills(market=market_id)
            related = [
                f
                for f in fills or []
                if str(f.get("order_id") if isinstance(f, dict) else getattr(f, "order_id", ""))
                in candidate_set
            ]
            if related:
                result["inconclusive"] += 1
                result["details"]["get_fills"] = f"fills_present:{len(related)}"
            else:
                result["clean"] += 1
                result["details"]["get_fills"] = "absent"
        except Exception as exc:
            kind = self._classify_probe_exception(exc)
            if kind == "clean":
                result["clean"] += 1
                result["details"]["get_fills"] = "absent"
            elif kind == "inconclusive":
                self._record_poll_failure("account_fills", order_id, exc)
                result["inconclusive"] += 1
                result["details"]["get_fills"] = "venue_unavailable"
            else:
                result["errors"] += 1
                result["details"]["get_fills_error"] = redact_error_detail(exc)

        try:
            pos = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
            local_size = Decimal(str(getattr(pos, "size_tokens", 0) or 0))
            if pre_submit_position_size is None:
                result["inconclusive"] += 1
                result["baseline_missing"] = True
                result["details"]["position_delta"] = "pre_submit_baseline_missing"
            elif local_size != pre_submit_position_size:
                # Other-order explanation (2026-09-19): a changed local position
                # is only a clean signal when every same-side venue leg in the
                # bounded submission window is claimed by a *different* known
                # orders row and their exact sum equals the delta — i.e. this
                # order provably did not move the position. Any unattributed,
                # own-order, malformed or unverifiable leg keeps the original
                # inconclusive fail-closed result.
                delta = abs(local_size - pre_submit_position_size)
                explained_by_other_orders = False
                if created_ts is not None:
                    try:
                        market_probe, market_fills, _ = await self._poll_source(
                            "market_fills",
                            order_id,
                            lambda: gw.get_fills(market=market_id),
                        )
                        if market_probe == "ok":
                            other_order_fills_total = Decimal("0")
                            has_unattributed = False
                            window_start = created_ts - _MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS
                            getter = getattr(self.orders_repo, "get_by_order_id", None)
                            for fill in market_fills or []:
                                if token_id:
                                    leg_token = str(
                                        self._fill_value(fill, "token_id", "") or ""
                                    ).strip()
                                    if leg_token and leg_token != token_id:
                                        continue
                                leg_side = str(self._fill_value(fill, "side", "") or "").upper()
                                if leg_side != side.upper():
                                    continue
                                if (
                                    _normalize_trade_status(self._fill_value(fill, "status", ""))
                                    == "failed"
                                ):
                                    # A failed trade moved no inventory.
                                    continue
                                try:
                                    fill_ts = float(self._fill_value(fill, "timestamp"))
                                except (TypeError, ValueError):
                                    has_unattributed = True
                                    break
                                if fill_ts < window_start:
                                    continue
                                leg_order_id = str(self._fill_value(fill, "order_id", "") or "")
                                if not leg_order_id or leg_order_id == order_id:
                                    has_unattributed = True
                                    break
                                claimed_by_other = False
                                if callable(getter):
                                    try:
                                        claimed_by_other = (
                                            await cast(Callable[[str], Awaitable[Any]], getter)(
                                                leg_order_id
                                            )
                                        ) is not None
                                    except Exception:
                                        claimed_by_other = False
                                if claimed_by_other:
                                    other_order_fills_total += _decimal_or_zero(
                                        self._fill_value(fill, "size", 0)
                                    )
                                else:
                                    has_unattributed = True
                                    break
                            if (
                                not has_unattributed
                                and other_order_fills_total > 0
                                and other_order_fills_total == delta
                            ):
                                explained_by_other_orders = True
                    except Exception:
                        explained_by_other_orders = False

                if explained_by_other_orders:
                    result["clean"] += 1
                    result["details"]["position_delta"] = (
                        f"sibling_attributed delta={delta} side={side.upper() or 'UNKNOWN'}"
                    )
                else:
                    result["inconclusive"] += 1
                    result["details"]["position_delta"] = (
                        f"side={side.upper() or 'UNKNOWN'} baseline={pre_submit_position_size} "
                        f"local_size={local_size}"
                    )
            else:
                result["clean"] += 1
                result["details"]["position_delta"] = (
                    f"unchanged={local_size} side={side.upper() or 'UNKNOWN'}"
                )
        except Exception as exc:
            result["errors"] += 1
            result["details"]["position_delta_error"] = redact_error_detail(exc)

        return result

    def _clear_auto_release_state(self, order_id: str) -> None:
        self._auto_release_next_probe_at.pop(order_id, None)
        self._auto_release_abort_counts.pop(order_id, None)
        self._ambiguous_stalled_last_audit.pop(order_id, None)

    def _maybe_emit_ambiguous_stalled(self, order_id: str, record: dict[str, Any]) -> None:
        """Surface a stuck ambiguous order at most once per hour.

        The per-attempt abort logs are throttled; this is the operator-facing
        heartbeat that a reservation is still parked behind an unresolvable
        order.
        """
        now = time.monotonic()
        last = self._ambiguous_stalled_last_audit.get(order_id)
        if last is not None and (now - last) < 3600.0:
            return
        self._ambiguous_stalled_last_audit[order_id] = now
        _log.warning(
            "reconciliation_ambiguous_stalled | order_id=%s status=%s "
            "reservation_id=%s abort_count=%d",
            order_id,
            record.get("status"),
            record.get("reservation_id"),
            self._auto_release_abort_counts.get(order_id, 0),
        )
        self._audit(
            "reconciliation_ambiguous_stalled",
            {
                "order_id": order_id,
                "status": record.get("status"),
                "reservation_id": record.get("reservation_id"),
                "abort_count": self._auto_release_abort_counts.get(order_id, 0),
            },
        )

    def _log_throttled_auto_release_abort(
        self,
        *,
        order_id: str,
        reason: str,
        probe: dict[str, Any],
        level: int,
        message: str,
    ) -> None:
        """Emit the abort log/audit on the 1st and every 10th streak occurrence.

        A stuck ambiguous order re-aborts on every probe attempt; unthrottled
        that floods the log and audit trail with identical lines.
        """
        count = self._auto_release_abort_counts.get(order_id, 0) + 1
        self._auto_release_abort_counts[order_id] = count
        emit = count == 1 or count % 10 == 0
        if not emit:
            level = logging.DEBUG
        _log.log(
            level,
            "%s | order_id=%s errors=%d abort_count=%d details=%s",
            message,
            order_id,
            probe["errors"],
            count,
            probe["details"],
        )
        if emit:
            self._audit(
                "reconciliation_auto_release_aborted",
                {
                    "order_id": order_id,
                    "reason": reason,
                    "probe": probe,
                    "abort_count": count,
                },
            )

    async def _maybe_auto_release_ambiguous(
        self,
        record: dict[str, Any],
        *,
        side: str,
        order_confirmation_cfg: Any,
    ) -> bool:
        """Run safety probes and auto-release the reservation when clean.

        Returns True if the auto-release fired.
        """
        order_id = str(record.get("order_id") or "")
        if not order_id:
            return False

        request = record.get("request") or {}
        venue_order_id = str(record.get("venue_order_id") or order_id)
        user_id = str(record.get("user_id") or request.get("user_id") or "")
        condition_id = str(record.get("condition_id") or request.get("condition_id") or "")
        market_id = (
            str(request.get("market") or request.get("market_id") or condition_id or "").strip()
            or None
        )
        token_id = str(record.get("asset") or request.get("token_id") or "")
        try:
            expected_size = Decimal(str(request.get("amount") or request.get("size") or "0"))
        except Exception:
            expected_size = Decimal("0")
        recovery_value = request.get("recovery_metadata")
        recovery = recovery_value if isinstance(recovery_value, dict) else {}
        pre_submit_position_size: Decimal | None = None
        raw_pre_submit = recovery.get("pre_submit_live_position_size")
        if raw_pre_submit is not None:
            try:
                candidate = Decimal(str(raw_pre_submit))
                if candidate.is_finite() and candidate >= 0:
                    pre_submit_position_size = candidate
            except (InvalidOperation, TypeError, ValueError):
                pre_submit_position_size = None
        trade_ids, _ = self._submission_sequences(record)

        probe = await self._run_auto_release_probes(
            order_id=order_id,
            venue_order_id=venue_order_id,
            market_id=market_id,
            trade_ids=trade_ids,
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            expected_size=expected_size,
            side=side,
            pre_submit_position_size=pre_submit_position_size,
            order_id_candidates=_probe_order_id_candidates(record, venue_order_id),
            created_ts=_record_created_at(record),
        )

        if probe.get("deferred"):
            _log.debug(
                "auto_release_deferred_backoff | order_id=%s source=%s reason=%s",
                order_id,
                probe["details"].get("deferred_source"),
                probe["details"].get("deferred_reason"),
            )
            return False

        max_errors = int(order_confirmation_cfg.auto_release_max_probe_errors)
        require_clean = bool(order_confirmation_cfg.auto_release_require_all_probes_clean)
        if probe["errors"] > max_errors:
            self._log_throttled_auto_release_abort(
                order_id=order_id,
                reason="probe_errors",
                probe=probe,
                level=logging.WARNING,
                message="auto_release_aborted_probe_errors",
            )
            return False
        if require_clean and probe["inconclusive"] > 0:
            # A baseline the coordinator never wrote is our own bookkeeping gap,
            # not venue doubt. Past the configured horizon, three clean venue
            # probes (the venue would report a fill in get_fills) outweigh it —
            # otherwise the reservation parks capital forever. Any *other*
            # inconclusive probe still blocks, so this never forgives real doubt.
            baseline_only = (
                bool(probe.get("baseline_missing"))
                and probe["inconclusive"] == 1
                and probe["errors"] == 0
                and self._is_past_baseline_missing_horizon(
                    record, order_confirmation_cfg=order_confirmation_cfg
                )
            )
            if not baseline_only:
                self._log_throttled_auto_release_abort(
                    order_id=order_id,
                    reason="inconclusive",
                    probe=probe,
                    level=logging.INFO,
                    message="auto_release_aborted_inconclusive",
                )
                return False
            _log.warning(
                "auto_release_baseline_missing_horizon_reached | order_id=%s "
                "reservation_id=%s probe=%s",
                order_id,
                record.get("reservation_id"),
                probe.get("details"),
            )
        self._auto_release_abort_counts.pop(order_id, None)

        # All probes clean — release.
        try:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status="auto_released_ambiguous",
                response_patch={"probe": probe},
            )
        except Exception:
            pass

        await self._release_reservation_if_present(
            record,
            reason="auto_release_after_ambiguous_timeout",
        )

        self._emit_lifecycle(
            status="auto_released_ambiguous",
            side=side,
            record=record,
            order_id=order_id,
            venue_status="ambiguous_timeout",
        )
        self._audit(
            "reconciliation_auto_release_ambiguous",
            {"order_id": order_id, "probe": probe},
        )
        try:
            from pmcopy.services.observability.metrics import (
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                labels={"side": (side or "unknown").upper()},
            )
        except Exception:
            pass
        if self._telemetry is not None:
            self._telemetry.ambiguous_released(order_id)
        return True

    async def _handle_unresolved_confirmation_timeout(
        self,
        record: dict[str, Any],
        *,
        side: str,
        fill_info: dict[str, Any],
    ) -> tuple[bool, bool]:
        """Persist timeout state and optionally run strict auto-release.

        Returns ``(timed_out, released)``. A timeout never implies a fill or a
        cancellation; reconciliation continues unless every configured safety
        probe is clean after the grace period.
        """
        cfg = self.order_confirmation_cfg
        if cfg is None:
            return False, False
        order_id = str(record.get("order_id") or "")
        hydrated = await self._hydrate_order_record(order_id)
        combined = {**record, **(hydrated or {})}
        for key in ("trade_key", "asset", "condition_id", "user_id", "reservation_id"):
            if not combined.get(key) and record.get(key):
                combined[key] = record[key]
        if not self._is_past_ambiguous_timeout(
            combined,
            order_confirmation_cfg=cfg,
        ):
            return False, False

        if str(combined.get("status") or "") in _TERMINAL_FILLED_SUBMISSION_STATUSES:
            # A skipped or empty poll must not reopen a sell that already settled.
            return False, False

        if str(combined.get("status") or "") != "ambiguous_timeout":
            try:
                await self.orders_repo.update_submission_status(
                    order_id=order_id,
                    status="ambiguous_timeout",
                    response_patch={
                        "last_confirmation_status": fill_info.get("status"),
                        "ambiguous_since": time.time(),
                    },
                )
            except Exception as exc:
                _log.warning(
                    "ambiguous_timeout_persist_failed | order_id=%s error=%s",
                    order_id,
                    redact_error_detail(exc),
                )
            combined["status"] = "ambiguous_timeout"
            self._emit_lifecycle(
                status="ambiguous_timeout",
                side=side,
                record=combined,
                order_id=order_id,
                venue_status=_normalize_venue_status(fill_info.get("status")),
            )

        if not self._is_past_auto_release_grace(
            combined,
            order_confirmation_cfg=cfg,
        ):
            return True, False

        now = time.monotonic()
        schedule = self._auto_release_next_probe_at.get(order_id)
        if schedule is not None and now < schedule[0]:
            # Escalating per-order pacing: a stuck ambiguous order is re-probed
            # on a widening schedule instead of every reconcile tick.
            self._maybe_emit_ambiguous_stalled(order_id, combined)
            return True, False

        released = await self._maybe_auto_release_ambiguous(
            combined,
            side=side,
            order_confirmation_cfg=cfg,
        )
        if released:
            self._clear_auto_release_state(order_id)
        else:
            base = float(getattr(cfg, "auto_release_retry_backoff_seconds", 30.0))
            cap = float(getattr(cfg, "auto_release_retry_backoff_max_seconds", 900.0))
            interval = schedule[1] * 2 if schedule is not None else base
            interval = min(max(interval, base), cap)
            self._auto_release_next_probe_at[order_id] = (now + interval, interval)
            self._maybe_emit_ambiguous_stalled(order_id, combined)
        if released and combined.get("trade_key"):
            try:
                await self.orders_repo.delete_processed_trade(combined["trade_key"])
            except Exception as exc:
                _log.warning(
                    "auto_release_processed_trade_unlock_failed | order_id=%s error=%s",
                    order_id,
                    redact_error_detail(exc),
                )
        return True, released

    def _record_negative_delta_anomaly(
        self,
        *,
        side: str,
        order_id: str,
        current_matched: Decimal,
        previous_matched: Decimal,
        logger: Any = None,
    ) -> None:
        """Surface a venue-state regression.

        ``current_matched < previous_matched`` means the venue reported a
        cumulative matched size smaller than what we have already applied
        locally. This should never happen in normal operation — Polymarket
        only increments cumulative matched. Clamp to zero (caller does that)
        but make the anomaly observable so operators can investigate.
        """
        emit_logger = logger if logger is not None else _log
        try:
            emit_logger.warning(
                "reconciliation_negative_delta_anomaly | order_id=%s side=%s "
                "current_matched=%s previous_matched=%s",
                order_id,
                side,
                str(current_matched),
                str(previous_matched),
                extra={
                    "type": "reconciliation_negative_delta_anomaly",
                    "order_id": order_id,
                    "side": side,
                    "current_matched": str(current_matched),
                    "previous_matched": str(previous_matched),
                },
            )
        except Exception:
            pass
        self._audit(
            "reconciliation_negative_delta_anomaly",
            {
                "order_id": order_id,
                "side": side,
                "current_matched": str(current_matched),
                "previous_matched": str(previous_matched),
            },
        )
        try:
            from pmcopy.services.observability.metrics import (
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                labels={"side": str(side or "unknown")},
            )
        except Exception:
            pass
        if self._telemetry is not None:
            self._telemetry.reconciliation_drift(order_id)

    @staticmethod
    async def _previous_matched_for_order(orders_repo: Any, order_id: str) -> Decimal:
        if not order_id:
            return Decimal("0")
        getter = getattr(orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            existing = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(existing, dict):
            return Decimal("0")
        return _matched_amount_from_response(existing.get("response"))

    async def _resolve_buy_requested_amount(self, order_id: str) -> Decimal:
        """Return the requested BUY notional from the persisted orders row.

        Used by the terminal-no-fill branch to release the eagerly recorded
        daily-cap spend back to the regime ledger. Reads ``request_json.amount``
        (falling back to ``size``) on the local orders row.
        """
        if not order_id:
            return Decimal("0")
        getter = getattr(self.orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            row = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(row, dict):
            return Decimal("0")
        request = row.get("request") or {}
        if not isinstance(request, dict):
            return Decimal("0")
        raw = request.get("amount") if request.get("amount") is not None else request.get("size")
        try:
            return Decimal(str(raw or 0))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _avg_price_from_fill(fill_info: dict[str, Any]) -> Decimal:
        raw = (
            fill_info.get("avg_price")
            or fill_info.get("avg_fill_price")
            or fill_info.get("avgFillPrice")
            or fill_info.get("price")
            or fill_info.get("average_price")
            or fill_info.get("averagePrice")
            or 0
        )
        try:
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

