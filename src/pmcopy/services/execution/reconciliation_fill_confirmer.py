"""User-order evidence hydration, poll source backoff, and fill confirmation mixin."""

from __future__ import annotations

import random
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, cast

from pmcopy.domain.performance import PerformanceUnits
from pmcopy.services.execution.reconciliation_models import (
    _MARKET_ATTRIBUTION_MIN_AGE_SECONDS,
    _MARKET_ATTRIBUTION_POST_WINDOW_SECONDS,
    _MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS,
    _TERMINAL_NO_FILL_STATUSES,
    _VENUE_ORDER_NOT_FOUND_THRESHOLD,
    _ReconciliationMixinBase,
    _decimal_or_zero,
    _fills_cover_order,
    _matched_amount_from_response,
    _normalize_trade_status,
    _normalize_venue_status,
    _order_is_fully_filled,
    _order_is_resting,
    _probe_order_id_candidates,
    _record_created_at,
)
from pmcopy.services.execution.user_order_evidence import (
    UserOrderEvidence,
)
from pmcopy.utils.logger import get_logger, redact_error_detail

_log = get_logger("ReconciliationService")


class _ReconciliationFillConfirmerMixin(_ReconciliationMixinBase):
    """Multi-source fill confirmation, user-WS evidence, and backoff handling."""

    async def record_user_order_evidence(self, event: dict[str, Any]) -> bool:
        """Persist one authenticated user-channel order fact durably.

        The existing submission status is retained so an unresolved order stays
        visible to startup and periodic reconciliation. Terminal settlement and
        reservation release remain owned by the normal reconcile methods.

        Args:
            event: Raw authenticated user-channel order or trade event.

        Returns:
            ``True`` when newer evidence was persisted; ``False`` when the event
            was malformed, unknown, stale, or not linked to a local order.
        """

        evidence = self._user_order_evidence_parser.parse(event)
        if evidence is None:
            return False
        row = await self._hydrate_order_record(evidence.order_id)
        if not row:
            return False
        response_value = row.get("response")
        response = response_value if isinstance(response_value, dict) else {}
        key = (
            "user_wss_order_evidence"
            if str(event.get("event_type") or "").lower() == "order"
            else "user_wss_trade_evidence"
        )
        prior_value = response.get(key)
        if isinstance(prior_value, dict):
            prior = UserOrderEvidence.from_payload(prior_value)
            if prior is not None and not self._is_newer_user_evidence(evidence, prior):
                return False
        current_status = str(row.get("status") or "submitted_unconfirmed")
        await self.orders_repo.update_submission_status(
            order_id=evidence.order_id,
            status=current_status,
            response_patch={key: evidence.as_payload()},
        )
        return True

    @staticmethod
    def _is_newer_user_evidence(
        candidate: UserOrderEvidence,
        prior: UserOrderEvidence,
    ) -> bool:
        """Return whether ``candidate`` may safely supersede ``prior``."""

        if prior.is_terminal and not candidate.is_terminal:
            return False
        if candidate.is_terminal and not prior.is_terminal:
            return True
        if candidate.timestamp is None:
            return prior.timestamp is None and candidate != prior
        if prior.timestamp is None:
            return True
        return candidate.timestamp > prior.timestamp

    @staticmethod
    def _terminal_user_order_evidence(record: dict[str, Any]) -> dict[str, Any] | None:
        """Return a normalized terminal result from persisted WSS evidence."""

        response_value = record.get("response")
        if not isinstance(response_value, dict):
            return None
        payload = response_value.get("user_wss_order_evidence")
        if not isinstance(payload, dict):
            return None
        evidence = UserOrderEvidence.from_payload(payload)
        if evidence is None or not evidence.is_terminal or evidence.size_matched != Decimal("0"):
            return None
        return {
            "status": "cancelled",
            "size_matched": "0",
            "confirmation_source": "user_wss_order",
        }

    # ------------------------------------------------------------------
    # Fill confirmation (venue poll)
    # ------------------------------------------------------------------
    async def _hydrate_order_record(self, order_id: str) -> dict[str, Any]:
        getter = getattr(self.orders_repo, "get_by_order_id", None)
        if not callable(getter):
            return {}
        try:
            row = await cast(Callable[[str], Awaitable[Any]], getter)(order_id)
        except Exception as exc:
            _log.debug(
                "confirm_fill order hydration failed | order_id=%s error=%s",
                order_id,
                redact_error_detail(exc),
            )
            return {}
        return row if isinstance(row, dict) else {}

    @staticmethod
    def _submission_sequences(record: dict[str, Any]) -> tuple[list[str], list[str]]:
        response = record.get("response")
        if not isinstance(response, dict):
            return [], []
        raw = response.get("raw")
        if not isinstance(raw, dict):
            raw = {}
        raw_data = raw.get("data")
        if not isinstance(raw_data, dict):
            raw_data = {}

        def _values(*keys: str) -> list[str]:
            value: Any = None
            for source in (response, raw, raw_data):
                for key in keys:
                    if source.get(key) is not None:
                        value = source.get(key)
                        break
                if value is not None:
                    break
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, (list, tuple, set)):
                return []
            return [str(item) for item in value if str(item or "").strip()]

        return (
            _values("trade_ids", "tradeIds"),
            _values("transactions_hashes", "transaction_hashes", "transactionHashes"),
        )

    @staticmethod
    def _venue_state_dict(venue_state: Any, order_id: str) -> dict[str, Any]:
        if venue_state is None:
            return {"status": "unknown", "size_matched": "0"}
        if isinstance(venue_state, dict):
            return dict(venue_state)
        matched = (
            getattr(venue_state, "size_matched", None)
            or getattr(venue_state, "filled_size", None)
            or getattr(venue_state, "matched_size", None)
            or getattr(venue_state, "matched_amount", None)
            or "0"
        )
        avg_price = (
            getattr(venue_state, "avg_price", None)
            or getattr(venue_state, "avg_fill_price", None)
            or getattr(venue_state, "average_price", None)
            or "0"
        )
        return {
            "status": getattr(venue_state, "status", "unknown"),
            "size_matched": str(matched),
            "avg_price": str(avg_price),
            "order_id": getattr(venue_state, "order_id", order_id),
        }

    @staticmethod
    def _fill_value(fill: Any, key: str, default: Any = None) -> Any:
        if isinstance(fill, dict):
            return fill.get(key, default)
        return getattr(fill, key, default)

    @classmethod
    def _aggregate_fills(
        cls,
        fills: list[Any],
        *,
        status: str,
        source: str,
    ) -> dict[str, Any]:
        total = Decimal("0")
        notional = Decimal("0")
        fees = Decimal("0")
        fees_known = True
        tx_hashes: list[str] = []
        fill_ids: list[str] = []
        for fill in fills:
            size = _decimal_or_zero(cls._fill_value(fill, "size", 0))
            price = _decimal_or_zero(cls._fill_value(fill, "price", 0))
            if size <= 0 or price <= 0:
                continue
            total += size
            notional += size * price
            fee = PerformanceUnits.finite(cls._fill_value(fill, "fee"))
            if fee is None:
                fees_known = False
            elif fee < 0:
                raise ValueError("Fill fee amount cannot be negative")
            else:
                fees += fee
            tx_hash = str(cls._fill_value(fill, "transaction_hash", "") or "")
            fill_id = str(cls._fill_value(fill, "fill_id", "") or "")
            if tx_hash and tx_hash not in tx_hashes:
                tx_hashes.append(tx_hash)
            if fill_id and fill_id not in fill_ids:
                fill_ids.append(fill_id)
        avg_price = notional / total if total > 0 else Decimal("0")
        return {
            "status": status,
            "size_matched": str(total),
            "avg_price": str(avg_price),
            "fee_amount": str(fees) if fees_known else None,
            "transaction_hashes": tx_hashes,
            "trade_ids": fill_ids,
            "confirmation_source": source,
        }

    def _source_backed_off(self, source: str) -> tuple[bool, str]:
        state = self._venue_poll_backoffs.get(source)
        if not state:
            return False, ""
        if time.monotonic() >= float(state.get("until", 0.0)):
            return False, ""
        return True, str(state.get("reason") or "venue_unavailable")

    def _record_poll_success(self, source: str) -> None:
        # Decay instead of reset: one lucky success (or a 404) during a
        # flapping outage must not wipe the exponential ladder that repeated
        # failures built up. The source becomes immediately pollable again,
        # but the next failure escalates from the decayed count.
        state = self._venue_poll_backoffs.get(source)
        if not state:
            return
        failures = int(state.get("failures", 0)) - 1
        if failures <= 0:
            self._venue_poll_backoffs.pop(source, None)
            return
        self._venue_poll_backoffs[source] = {
            "failures": failures,
            "until": 0.0,
            "reason": str(state.get("reason") or ""),
            "base_delay": float(state.get("base_delay", 0.0)),
        }

    def _record_poll_failure(self, source: str, order_id: str, exc: Exception) -> str:
        previous = self._venue_poll_backoffs.get(source, {})
        if previous and time.monotonic() < float(previous.get("until", 0.0)):
            # A backoff window is already active: concurrent callers that
            # slipped past the gate before it was set must not escalate the
            # failure count (thundering-herd guard).
            return str(previous.get("reason") or "venue_unavailable")
        failures = int(previous.get("failures", 0)) + 1
        base_delay = min(5.0 * (2 ** (failures - 1)), 60.0)
        delay = base_delay * random.uniform(0.8, 1.2)
        reason = redact_error_detail(exc)
        self._venue_poll_backoffs[source] = {
            "failures": failures,
            "until": time.monotonic() + delay,
            "reason": reason,
            "base_delay": base_delay,
        }
        _log.warning(
            "confirm_fill transient source failure; backing off CLOB polls | "
            "source=%s base_delay_s=%.0f retry_in_s=%.1f order_id=%s error=%s",
            source,
            base_delay,
            delay,
            order_id,
            reason,
        )
        return reason

    async def _poll_source(
        self,
        source: str,
        order_id: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> tuple[str, Any, str]:
        backed_off, reason = self._source_backed_off(source)
        if backed_off:
            return "unavailable", None, reason
        try:
            value = await operation()
        except Exception as exc:
            try:
                from pmcopy.integrations.polymarket.errors import (
                    VenueOrderNotFoundError,
                    VenueRateLimitError,
                    VenueUnavailableError,
                )

                if isinstance(exc, VenueOrderNotFoundError):
                    self._record_poll_success(source)
                    return "not_found", None, "order_not_found"
                if isinstance(exc, (VenueRateLimitError, VenueUnavailableError)):
                    return (
                        "unavailable",
                        None,
                        self._record_poll_failure(source, order_id, exc),
                    )
            except Exception:
                pass
            reason = redact_error_detail(exc)
            _log.warning(
                "confirm_fill source failed | source=%s order_id=%s error=%s",
                source,
                order_id,
                reason,
            )
            return "error", None, reason
        self._record_poll_success(source)
        return "ok", value, ""

    async def _attribute_market_trade_fills(
        self,
        record: dict[str, Any],
        market_id: str | None,
        order_id: str,
    ) -> dict[str, Any] | None:
        """Attribute a unique matching venue trade when direct probes are empty.

        Last-resort positive evidence for sibling-hash ambiguity: the venue
        trade history for the market is scanned for one trade that matches the
        requested side, respects the order's price bound, falls inside a bounded
        submission window, and sums to the exact requested amount (BUY cost in
        USDC with one share-step of rounding headroom; SELL shares exactly).
        Legs whose order hash belongs to any locally known orders row are never
        attributed. Zero or multiple surviving trades return ``None`` so the
        caller keeps its existing fail-closed behaviour untouched.
        """
        gateway = self.clob_gateway
        if gateway is None or not market_id:
            return None
        request_value = record.get("request")
        request = request_value if isinstance(request_value, dict) else {}
        side = str(request.get("side") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        try:
            requested = Decimal(str(request.get("amount") or ""))
            price_limit = Decimal(str(request.get("price_limit") or ""))
        except (InvalidOperation, TypeError, ValueError):
            return None
        if requested <= 0 or not price_limit.is_finite() or price_limit <= 0:
            return None
        created_ts = _record_created_at(record)
        if created_ts is None:
            return None
        if time.time() - created_ts < _MARKET_ATTRIBUTION_MIN_AGE_SECONDS:
            return None
        window_start = created_ts - _MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS
        window_end = created_ts + _MARKET_ATTRIBUTION_POST_WINDOW_SECONDS

        market_probe, market_fills, _market_reason = await self._poll_source(
            "market_fills",
            order_id,
            lambda: gateway.get_fills(market=market_id),
        )
        if market_probe != "ok":
            return None

        expected_token_id = str(
            record.get("asset") or request.get("token_id") or request.get("asset") or ""
        ).strip()
        claimed_cache: dict[str, bool] = {}
        candidates: dict[str, list[Any]] = {}
        for fill in market_fills or []:
            leg_order_id = str(self._fill_value(fill, "order_id", "") or "")
            if not leg_order_id or leg_order_id == order_id:
                continue
            if expected_token_id:
                leg_token = str(self._fill_value(fill, "token_id", "") or "").strip()
                if leg_token and leg_token != expected_token_id:
                    continue
            leg_side = str(self._fill_value(fill, "side", "") or "").upper()
            if leg_side != side:
                continue
            size = _decimal_or_zero(self._fill_value(fill, "size", 0))
            price = _decimal_or_zero(self._fill_value(fill, "price", 0))
            if size <= 0 or price <= 0:
                continue
            if side == "BUY" and price > price_limit:
                continue
            if side == "SELL" and price < price_limit:
                continue
            try:
                fill_ts = float(self._fill_value(fill, "timestamp"))
            except (TypeError, ValueError):
                continue
            if not window_start <= fill_ts <= window_end:
                continue
            fill_id = str(self._fill_value(fill, "fill_id", "") or "")
            if not fill_id:
                continue
            if leg_order_id not in claimed_cache:
                # Fail closed on ownership doubt: a leg whose hash is (or may
                # be) claimed by another orders row is never attributable.
                claimed = True
                getter = getattr(self.orders_repo, "get_by_order_id", None)
                if callable(getter):
                    try:
                        claimed = (
                            await cast(Callable[[str], Awaitable[Any]], getter)(leg_order_id)
                        ) is not None
                    except Exception:
                        claimed = True
                claimed_cache[leg_order_id] = claimed
            if claimed_cache[leg_order_id]:
                continue
            candidates.setdefault(fill_id, []).append(fill)

        matches: list[tuple[str, list[Any], Decimal, Decimal]] = []
        for fill_id, legs in candidates.items():
            total_size = Decimal("0")
            cost = Decimal("0")
            for leg in legs:
                size = _decimal_or_zero(self._fill_value(leg, "size", 0))
                price = _decimal_or_zero(self._fill_value(leg, "price", 0))
                total_size += size
                cost += size * price
            if side == "BUY":
                tolerance = Decimal("0.01") * price_limit + Decimal("1e-9")
                if abs(cost - requested) > tolerance:
                    continue
            elif total_size != requested:
                continue
            matches.append((fill_id, legs, total_size, cost))

        if len(matches) != 1:
            _log.debug(
                "confirm_fill market attribution inconclusive | order_id=%s market=%s "
                "candidates=%d matches=%d",
                order_id,
                market_id,
                len(candidates),
                len(matches),
            )
            return None
        _fill_id, legs, total_size, cost = matches[0]
        statuses = {_normalize_trade_status(self._fill_value(leg, "status", "")) for leg in legs}
        status = "confirmed" if statuses and statuses <= {"confirmed"} else "matched"
        aggregate = self._aggregate_fills(
            legs,
            status=status,
            source="market_trade_attribution",
        )
        aggregate["attributed_order_ids"] = sorted(
            {str(self._fill_value(leg, "order_id", "") or "") for leg in legs} - {""}
        )
        aggregate["attribution"] = {
            "side": side,
            "requested_amount": str(requested),
            "price_limit": str(price_limit),
            "matched_shares": str(total_size),
            "matched_cost": str(cost),
            "window_start": window_start,
            "window_end": window_end,
            "candidate_trades": len(candidates),
        }
        _log.info(
            "confirm_fill market trade attribution | order_id=%s market=%s side=%s "
            "trade_ids=%s matched_shares=%s matched_cost=%s requested=%s "
            "attributed_order_ids=%s",
            order_id,
            market_id,
            side,
            aggregate.get("trade_ids"),
            str(total_size),
            str(cost),
            str(requested),
            aggregate["attributed_order_ids"],
        )
        self._audit(
            "reconciliation_market_trade_attribution",
            {
                "order_id": order_id,
                "market": market_id,
                "side": side,
                "trade_ids": aggregate.get("trade_ids"),
                "attributed_order_ids": aggregate["attributed_order_ids"],
                "matched_shares": str(total_size),
                "matched_cost": str(cost),
                "requested_amount": str(requested),
                "price_limit": str(price_limit),
            },
        )
        return aggregate

    async def confirm_fill(self, order_id: str) -> dict[str, Any]:
        """Resolve an order from independent authenticated venue evidence.

        A failing single-order endpoint no longer blocks trade history and open
        order fallbacks. Missing evidence is always fail-closed: it keeps the
        order pending and never implies cancellation or permission to resubmit.
        """
        gateway = self.clob_gateway
        if gateway is None:
            return {"status": "unknown", "size_matched": "0"}

        record = await self._hydrate_order_record(order_id)
        terminal_user_evidence = self._terminal_user_order_evidence(record)
        if terminal_user_evidence is not None:
            return terminal_user_evidence
        cfg = self.order_confirmation_cfg
        response_value = record.get("response")
        response: dict[str, Any] = response_value if isinstance(response_value, dict) else {}
        last_status = _normalize_venue_status(response.get("status"))
        if self._should_skip_for_delay(
            record,
            order_confirmation_cfg=cfg,
            last_known_venue_status=last_status,
        ):
            return {"status": "poll_deferred", "size_matched": "0"}
        if cfg is not None:
            now = time.monotonic()
            last_poll = self._last_order_poll_at.get(order_id)
            interval = max(0.0, float(cfg.poll_interval_seconds))
            if last_poll is not None and now - last_poll < interval:
                return {"status": "poll_deferred", "size_matched": "0"}
            self._last_order_poll_at[order_id] = now

        trade_ids, _ = self._submission_sequences(record)

        request_value = record.get("request")
        request: dict[str, Any] = request_value if isinstance(request_value, dict) else {}
        market_id = (
            str(
                request.get("market")
                or request.get("market_id")
                or request.get("condition_id")
                or ""
            )
            or None
        )
        probe_candidates = _probe_order_id_candidates(record, order_id)
        unavailable: list[tuple[str, str]] = []
        direct: dict[str, Any] | None = None

        order_probe, venue_state, reason = await self._poll_source(
            "order",
            order_id,
            lambda: gateway.get_order(order_id),
        )
        if order_probe == "not_found":
            # Sibling-attempt ids (2026-09-19): the row's own hash is absent on
            # the venue, but a persisted attempted-order id may still resolve.
            # A miss on every candidate keeps the existing absent handling.
            for candidate in probe_candidates[1:]:
                (
                    order_probe,
                    venue_state,
                    reason,
                ) = await self._poll_source(
                    "order",
                    order_id,
                    lambda candidate_id=candidate: gateway.get_order(candidate_id),  # type: ignore[misc]
                )
                if order_probe != "not_found":
                    break
        if order_probe == "ok":
            self._venue_not_found_counts.pop(order_id, None)
            # Venue is answering for this order again: drop the escalated
            # auto-release pacing so the next grace check probes promptly.
            self._clear_auto_release_state(order_id)
            direct = self._venue_state_dict(venue_state, order_id)
            direct_status = _normalize_venue_status(direct.get("status"))
            direct_matched = _matched_amount_from_response(direct)
            if direct_status in _TERMINAL_NO_FILL_STATUSES and direct_matched <= 0:
                direct["confirmation_source"] = "order"
                return direct
            if direct_status in {"confirmed", "filled"} and direct_matched > 0:
                direct["confirmation_source"] = "order"
                return direct
        elif order_probe == "not_found":
            count = self._venue_not_found_counts.get(order_id, 0) + 1
            self._venue_not_found_counts[order_id] = count
            if count == 1 or count == _VENUE_ORDER_NOT_FOUND_THRESHOLD:
                _log.info(
                    "confirm_fill order lookup absent; checking fallback evidence | "
                    "order_id=%s consecutive=%d",
                    order_id,
                    count,
                )
        else:
            unavailable.append(("order", reason))

        confirmed_trade_fills: list[Any] = []
        trade_states: list[str] = []
        if trade_ids:
            for trade_id in trade_ids:

                async def _get_trade_fills(tid: str = trade_id) -> list[Any]:
                    related = await gateway.get_fills(
                        market=market_id,
                        trade_id=tid,
                        order_id=order_id,
                    )
                    if related:
                        return related
                    # Sibling-hash evidence (2026-09-19): a persisted trade id
                    # may belong to legs whose order hash is not this row's
                    # order_id (attributed or retried submissions). Retry
                    # without the order filter so the trade itself still
                    # counts as confirmation evidence; an absent trade stays
                    # missing exactly as before.
                    return await gateway.get_fills(
                        market=market_id,
                        trade_id=tid,
                    )

                probe, fills, reason = await self._poll_source(
                    "trade_fills", order_id, _get_trade_fills
                )
                if probe != "ok":
                    if probe in {"unavailable", "error"}:
                        unavailable.append(("trade_fills", reason))
                    trade_states.append("missing")
                    continue
                related = list(fills or [])
                if not related:
                    trade_states.append("missing")
                    continue
                statuses = {
                    _normalize_trade_status(self._fill_value(fill, "status", ""))
                    for fill in related
                }
                if "confirmed" in statuses:
                    trade_states.append("confirmed")
                    confirmed_trade_fills.extend(
                        fill
                        for fill in related
                        if _normalize_trade_status(self._fill_value(fill, "status", ""))
                        == "confirmed"
                    )
                elif statuses and statuses <= {"failed"}:
                    trade_states.append("failed")
                else:
                    pending = next(
                        (s for s in ("retrying", "mined", "matched") if s in statuses),
                        "matched",
                    )
                    trade_states.append(pending)

        async def _list_open_orders() -> list[Any]:
            try:
                for candidate in probe_candidates:
                    orders = await gateway.list_open_orders(
                        order_id=candidate,
                        market=market_id,
                    )
                    if orders:
                        return orders
                return []
            except TypeError:
                orders = await gateway.list_open_orders()
                candidate_set = set(probe_candidates)
                return [
                    item
                    for item in orders or []
                    if str(self._fill_value(item, "order_id", "")) in candidate_set
                ]

        open_probe, open_orders, reason = await self._poll_source(
            "open_orders", order_id, _list_open_orders
        )
        if open_probe != "ok":
            unavailable.append(("open_orders", reason))
            open_orders = []
        else:
            # An id-filtered orders query can still return a fully matched
            # order. That row is not resting size, so it must not keep a
            # confirmed fill pending forever.
            open_orders = [item for item in (open_orders or []) if _order_is_resting(item)]
        direct_state = venue_state if order_probe == "ok" else None
        directly_resting = _order_is_resting(direct_state)
        is_open = bool(open_orders) or directly_resting
        order_closed = (open_probe == "ok" and not is_open) or _order_is_fully_filled(direct_state)

        async def _get_order_fills() -> list[Any]:
            related: list[Any] = []
            try:
                for candidate in probe_candidates:
                    related.extend(
                        await gateway.get_fills(
                            market=market_id,
                            order_id=candidate,
                        )
                    )
                return related
            except TypeError:
                fills = await gateway.get_fills(market=market_id)
                candidate_set = set(probe_candidates)
                return [
                    fill
                    for fill in fills or []
                    if str(self._fill_value(fill, "order_id", "")) in candidate_set
                ]

        fill_probe, account_fills, reason = await self._poll_source(
            "account_fills", order_id, _get_order_fills
        )
        if fill_probe != "ok":
            unavailable.append(("account_fills", reason))
            account_fills = []

        related_fills = list(account_fills or [])
        pending_statuses = {
            _normalize_trade_status(self._fill_value(fill, "status", ""))
            for fill in related_fills
            if _normalize_trade_status(self._fill_value(fill, "status", ""))
            not in {"confirmed", "failed"}
        }
        confirmed = [
            fill
            for fill in related_fills
            if _normalize_trade_status(self._fill_value(fill, "status", "")) == "confirmed"
        ]
        if confirmed:
            aggregate = self._aggregate_fills(
                confirmed,
                status="live",
                source="account_trades",
            )
            confirmed_amount = _matched_amount_from_response(aggregate)
            # Confirmed trades that cover the order are terminal even when the
            # venue order text is still "live". A smaller confirmed size leaves
            # the remainder open.
            if not pending_statuses and (
                order_closed or _fills_cover_order(confirmed_amount, direct_state)
            ):
                aggregate["status"] = "confirmed"
            direct_matched = _matched_amount_from_response(direct or {})
            known_matched = (
                _matched_amount_from_response(
                    self._aggregate_fills(confirmed_trade_fills, status="live", source="trade_ids")
                )
                if confirmed_trade_fills
                else Decimal("0")
            )
            if confirmed_amount < max(direct_matched, known_matched):
                aggregate["status"] = "live"
            return aggregate
        if confirmed_trade_fills:
            # A row previously resolved through market attribution carries the
            # attributed sibling ids on its persisted response. When every
            # probed trade id resolved confirmed, the trade evidence is terminal
            # instead of eternally "live"; anything less keeps existing
            # behaviour. Trade ids that themselves cover the order size are
            # terminal too; a smaller confirmed slice is not.
            is_attributed = "attributed_order_ids" in response
            trade_evidence_terminal = bool(trade_states) and set(trade_states) <= {"confirmed"}
            aggregate = self._aggregate_fills(
                confirmed_trade_fills,
                status="live",
                source="trade_ids",
            )
            if trade_evidence_terminal and (
                is_attributed
                or _fills_cover_order(_matched_amount_from_response(aggregate), direct_state)
            ):
                aggregate["status"] = "confirmed"
            return aggregate
        if pending_statuses:
            pending = next(
                (s for s in ("retrying", "mined", "matched") if s in pending_statuses),
                "matched",
            )
            return {
                "status": pending,
                "size_matched": "0",
                "confirmation_source": "account_trades",
            }
        if (
            related_fills
            and all(
                _normalize_trade_status(self._fill_value(fill, "status", "")) == "failed"
                for fill in related_fills
            )
            and order_closed
        ):
            return {
                "status": "failed",
                "size_matched": "0",
                "confirmation_source": "account_trades",
            }
        if is_open:
            state = (
                self._venue_state_dict(open_orders[0], order_id)
                if open_orders
                else dict(direct or {})
            )
            state.update(
                {
                    "status": _normalize_venue_status(state.get("status")) or "live",
                    "size_matched": "0",
                    "confirmation_source": "open_orders",
                }
            )
            return state
        if trade_states:
            pending = next(
                (s for s in ("retrying", "mined", "matched") if s in trade_states),
                "matched",
            )
            return {
                "status": pending,
                "size_matched": "0",
                "confirmation_source": "trade_ids",
            }
        if direct is not None:
            if _matched_amount_from_response(direct) > 0:
                direct["status"] = "matched"
            direct["size_matched"] = "0"
            direct["confirmation_source"] = "order"
            return direct
        if (
            direct is None
            and not related_fills
            and not confirmed_trade_fills
            and not pending_statuses
            and not trade_states
            and not is_open
        ):
            # Sibling-hash ambiguity (2026-09-19): every direct probe is empty,
            # so positive evidence can only come from a unique match in the
            # venue trade history for the market. Failure keeps the existing
            # fail-closed fallthrough below.
            attributed = await self._attribute_market_trade_fills(record, market_id, order_id)
            if attributed is not None:
                return attributed
        if unavailable:
            return {
                "status": "poll_unavailable",
                "size_matched": "0",
                "poll_error": unavailable[0][1],
                "unavailable_sources": [source for source, _ in unavailable],
                "retryable": True,
            }
        return {"status": "unknown", "size_matched": "0"}

