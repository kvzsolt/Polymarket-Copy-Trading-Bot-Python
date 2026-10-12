"""Phase 4 — exchange-side reconciliation extensions.

Validates that the periodic reconciler now:

* detects partial fills (venue cumulative matched > local matched_total) and
  persists the new matched_total
* flags terminal venue fills that we have no local record of
* flags terminal venue cancellations as ``unexpected_cancellation`` when our
  local row was still believed open
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from pmcopy.services.reconciliation import ReconciliationService


def _make_clob(orders):
    clob = MagicMock()
    clob.get_orders.return_value = orders
    return clob


class TestPartialFillDetection:
    @pytest.mark.asyncio
    async def test_partial_fill_recorded(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-partial",
                "timestamp": now,
                "status": "live",
                "size_matched": "12",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-partial",
            "status": "submitted_unconfirmed",
            "matched_total": "5",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 1
        assert result.partial_fill_deltas == (("o-partial", "7"),)
        repo.upsert_matched_total.assert_called_once_with(order_id="o-partial", matched="12")

    @pytest.mark.asyncio
    async def test_no_partial_fill_when_unchanged(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-noop",
                "timestamp": now,
                "status": "live",
                "size_matched": "5",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-noop",
            "status": "submitted_unconfirmed",
            "matched_total": "5",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 0
        repo.upsert_matched_total.assert_not_called()

    @pytest.mark.asyncio
    async def test_filled_field_supported(self):
        """Some venue schemas use ``filled`` instead of ``size_matched``."""
        now = time.time()
        venue_orders = [
            {
                "id": "o-alt",
                "timestamp": now,
                "status": "live",
                "filled": "8",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-alt",
            "status": "submitted_unconfirmed",
            "matched_total": "3",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 1


class TestUnexpectedFillDetection:
    @pytest.mark.asyncio
    async def test_unexpected_fill_with_no_local_record(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-orphan",
                "timestamp": now,
                "status": "matched",
                "size_matched": "10",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = None

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_fills_observed == 1
        assert result.locally_unknown_orders == 1

    @pytest.mark.asyncio
    async def test_terminal_but_zero_matched_is_not_a_fill(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-zero",
                "timestamp": now,
                "status": "matched",
                "size_matched": "0",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = None

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_fills_observed == 0


class TestUnexpectedCancellationDetection:
    @pytest.mark.asyncio
    async def test_unexpected_cancellation_with_local_open(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-cancel",
                "timestamp": now,
                "status": "cancelled",
                "size_matched": "0",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-cancel",
            "status": "submitted_unconfirmed",
            "matched_total": "0",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_cancellations == 1

    @pytest.mark.asyncio
    async def test_locally_cancelled_not_flagged(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-known-cancel",
                "timestamp": now,
                "status": "canceled",
                "size_matched": "0",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-known-cancel",
            "status": "cancelled",
            "matched_total": "0",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_cancellations == 0


class TestTerminalFillWithLocalRecord:
    """N6 — a terminal venue fill against a known local record must classify
    as a *complete* fill (kind="filled"), not a partial, so downstream
    consumers don't double-apply the delta on the next pass."""

    @pytest.mark.asyncio
    async def test_local_record_with_terminal_venue_status_is_complete_fill(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-done",
                "timestamp": now,
                "status": "matched",  # terminal-filled status
                "size_matched": "10",
                "original_size": "10",
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-done",
            "status": "submitted_unconfirmed",
            "matched_total": "4",  # we'd only seen 4 so far
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.complete_fills_observed == 1
        assert result.partial_fills_observed == 0
        assert result.complete_fill_deltas == (("o-done", "6"),)

    @pytest.mark.asyncio
    async def test_cumulative_reaching_original_size_is_complete_even_if_live(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-full-live",
                "timestamp": now,
                "status": "live",  # non-terminal status …
                "size_matched": "10",
                "original_size": "10",  # … but cumulative == original
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-full-live",
            "status": "submitted_unconfirmed",
            "matched_total": "0",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.complete_fills_observed == 1
        assert result.partial_fills_observed == 0


class TestCancelAfterPartial:
    """H1/N4 — an order that filled partially and was then terminally
    cancelled must roll back only the *unfilled* remainder against the daily
    cap. Previously this state fell through every branch and returned None,
    leaking the remainder forever."""

    @pytest.mark.asyncio
    async def test_cancel_after_partial_refunds_unfilled_remainder(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-cap",
                "timestamp": now,
                "status": "cancelled",
                "size_matched": "4",  # == local matched_total → no new delta
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-cap",
            "status": "submitted_unconfirmed",
            "matched_total": "4",
            "side": "BUY",
            "request": {"amount": "100", "price": "0.50"},
        }
        regime = MagicMock()

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
            balance_regime=regime,
        )
        result = await svc.reconcile()

        assert result.unexpected_cancellations == 1
        # filled notional = 4 tokens * 0.50 = 2.0 ; refund = 100 - 2 = 98
        regime.record_partial_fill.assert_called_once()
        kwargs = regime.record_partial_fill.call_args.kwargs
        assert Decimal(str(kwargs["recorded_usdc"])) == Decimal("100")
        assert Decimal(str(kwargs["actual_usdc"])) == Decimal("2.00")
        assert kwargs["order_id"] == "o-cap"
        regime.record_cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_after_partial_without_record_partial_fill_refunds_only_unfilled(self):
        """PR #105 review (Greptile P1): when the balance regime lacks
        ``record_partial_fill``, the fallback used to call
        ``record_cancel(requested)`` which refunds the full notional —
        including the genuinely-spent filled portion — silently over-
        crediting the daily cap. Must refund only the unfilled remainder."""
        now = time.time()
        venue_orders = [
            {
                "id": "o-no-partial-method",
                "timestamp": now,
                "status": "cancelled",
                "size_matched": "4",
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-no-partial-method",
            "status": "submitted_unconfirmed",
            "matched_total": "4",
            "side": "BUY",
            "request": {"amount": "100", "price": "0.50"},
        }

        class _LegacyRegime:
            """Older regime exposing only ``record_cancel`` — no partial-fill API."""

            def __init__(self):
                self.record_cancel = MagicMock()

        regime = _LegacyRegime()
        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
            balance_regime=regime,
        )
        await svc.reconcile()

        # filled = 4 * 0.50 = 2.0 ; unfilled = 100 - 2 = 98 ; only the
        # unfilled portion may be refunded back to the daily cap.
        regime.record_cancel.assert_called_once()
        args, kwargs = regime.record_cancel.call_args
        refunded = Decimal(str(args[0])) if args else Decimal(str(kwargs.get("amount", "0")))
        assert refunded == Decimal("98.00")
        assert kwargs.get("order_id") == "o-no-partial-method"

    @pytest.mark.asyncio
    async def test_cancel_after_partial_zero_price_skips_refund(self):
        """PR #105 review (Cubic P1): when the local record has no resolvable
        price, the filled notional cannot be estimated. Refunding anything
        risks under-counting actual spend on the daily cap. Skip the refund
        and rely on manual reconciliation; cap stays conservative."""
        now = time.time()
        venue_orders = [
            {
                "id": "o-no-price",
                "timestamp": now,
                "status": "cancelled",
                "size_matched": "4",
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-no-price",
            "status": "submitted_unconfirmed",
            "matched_total": "4",
            "side": "BUY",
            # No price anywhere → _resolve_request_price returns 0.
            "request": {"amount": "100"},
        }
        regime = MagicMock()

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
            balance_regime=regime,
        )
        await svc.reconcile()

        # Conservative: do nothing rather than over-refund a 0-priced partial.
        regime.record_partial_fill.assert_not_called()
        regime.record_cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancel_after_partial_sell_is_ignored(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-sell",
                "timestamp": now,
                "status": "cancelled",
                "size_matched": "4",
            }
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        repo.get_by_order_id.return_value = {
            "order_id": "o-sell",
            "status": "submitted_unconfirmed",
            "matched_total": "4",
            "side": "SELL",
            "request": {"amount": "100", "price": "0.50"},
        }
        regime = MagicMock()

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
            balance_regime=regime,
        )
        await svc.reconcile()

        # SELLs never consumed the daily-spend budget → no rollback.
        regime.record_partial_fill.assert_not_called()
        regime.record_cancel.assert_not_called()


class TestPersistFailureIsSwallowed:
    @pytest.mark.asyncio
    async def test_persist_failure_does_not_break_loop(self):
        now = time.time()
        venue_orders = [
            {
                "id": "o-bad-persist",
                "timestamp": now,
                "status": "live",
                "size_matched": "9",
            }
        ]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-bad-persist",
            "status": "submitted_unconfirmed",
            "matched_total": "1",
        }
        repo.upsert_matched_total.side_effect = RuntimeError("sqlite locked")

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        # The partial fill is still observed even though persistence failed —
        # the next pass will retry the upsert with the same delta.
        assert result.partial_fills_observed == 1
        assert result.errors == []
