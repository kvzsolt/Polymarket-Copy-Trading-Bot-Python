"""A02: complete order identity and explicit sizing units.

Outcome-token-exact order identity and unit-strict amount semantics on the
money path:

* ``VenueOrderState`` carries the normalized outcome token so recovery can
  distinguish YES from NO orders on the same condition;
* ``list_open_orders(market=...)`` filtering is strict — a row whose raw
  payload carries no market identity is REJECTED, never kept;
* ``AmbiguousSubmission`` carries the unit of its persisted ``amount``
  (``quote`` for BUY spend, ``shares`` for SELL size);
* the orders-row reader surfaces the persisted identity columns and the
  submission sanitizer keeps the new unit fields.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from services.execution.ambiguous_recovery import (
    AmbiguousSubmission,
    SubmissionAmountUnit,
    SubmissionSide,
)
from services.execution.submission import sanitize_submission_request
from storage.postgres.orders_repo import _row_to_ambiguous_submission
from tests.test_ccxt_prediction_gateway_contract import TOKEN, FakeExchange

_NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# VenueOrderState outcome-token translation
# ---------------------------------------------------------------------------


async def test_order_state_carries_outcome_token_from_parsed_ccxt_order() -> None:
    """CCXT parsed orders expose ``outcomeId``; it is the raw outcome token.

    Verified against the installed ccxt source: ``parse_prediction_order``
    reads ``asset_id`` from the raw CLOB row and ``safe_outcome`` falls back
    to the raw token id whenever the outcome cache misses, so ``outcomeId``
    always carries token identity.
    """

    exchange = FakeExchange()
    exchange.order = {**exchange.order, "outcomeId": TOKEN}
    gateway = CcxtPredictionGateway(exchange)

    order = await gateway.get_order("order-1")

    assert order.token_id == TOKEN


async def test_order_state_carries_outcome_token_from_raw_clob_row() -> None:
    """Raw ``/data/orders`` rows carry ``asset_id`` — the adapter keeps it."""

    exchange = FakeExchange()
    exchange.open_orders = [
        {
            "id": "raw-order-1",
            "status": "LIVE",
            "side": "BUY",
            "price": "0.42",
            "original_size": "10",
            "size_matched": "0",
            "created_at": 1_750_000_000,
            "market": "market-1",
            "asset_id": "token-raw-1",
        }
    ]
    gateway = CcxtPredictionGateway(exchange)

    orders = await gateway.list_open_orders(market="market-1")

    assert [order.token_id for order in orders] == ["token-raw-1"]


async def test_order_state_token_is_none_when_payload_lacks_identity() -> None:
    """No token identity in the payload means ``None`` — never a guess."""

    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(exchange)

    order = await gateway.get_order("order-1")

    assert order.token_id is None


# ---------------------------------------------------------------------------
# Strict market filter
# ---------------------------------------------------------------------------


async def test_open_order_market_filter_rejects_rows_without_market_identity() -> None:
    """A market-scoped read must never return a row it cannot scope.

    The pre-A02 filter kept rows whose raw payload had no ``market`` field,
    letting unscoped rows bypass the filter entirely. That row set cannot
    prove which condition — let alone which outcome token — the row belongs
    to, so a strict filter rejects it.
    """

    exchange = FakeExchange()
    exchange.open_orders = [
        {**exchange.order, "id": "scoped", "info": {"market": "market-1"}},
        # A row with no market identity anywhere must be REJECTED, even when
        # it passes every other filter.
        {**exchange.order, "id": "scoped", "info": {}},
        {**exchange.order, "id": "other-market", "info": {"market": "other"}},
    ]
    gateway = CcxtPredictionGateway(exchange)

    orders = await gateway.list_open_orders(market="market-1")

    assert [order.order_id for order in orders] == ["scoped"]


async def test_open_order_market_filter_accepts_rows_when_no_filter_given() -> None:
    """Without a market filter, unscoped rows remain visible (unchanged)."""

    exchange = FakeExchange()
    exchange.open_orders = [{**exchange.order, "id": "unscoped", "info": {}}]
    gateway = CcxtPredictionGateway(exchange)

    orders = await gateway.list_open_orders()

    assert [order.order_id for order in orders] == ["unscoped"]


# ---------------------------------------------------------------------------
# AmbiguousSubmission amount unit
# ---------------------------------------------------------------------------


def test_amount_unit_accepts_the_enum_values() -> None:
    submission = AmbiguousSubmission(
        idempotency_key="k",
        user_id="u",
        leader_id=None,
        token_id="token",
        condition_id="condition",
        side=SubmissionSide.BUY,
        amount=Decimal("10"),
        price_limit=Decimal("0.5"),
        submitted_at=_NOW,
        amount_unit=SubmissionAmountUnit.QUOTE,
    )
    assert submission.amount_unit is SubmissionAmountUnit.QUOTE


def test_amount_unit_defaults_to_none_and_rejects_other_types() -> None:
    legacy = AmbiguousSubmission(
        idempotency_key="k",
        user_id="u",
        leader_id=None,
        token_id="token",
        condition_id="condition",
        side=SubmissionSide.SELL,
        amount=Decimal("10"),
        price_limit=Decimal("0.5"),
        submitted_at=_NOW,
    )
    assert legacy.amount_unit is None

    with pytest.raises(TypeError):
        AmbiguousSubmission(
            idempotency_key="k",
            user_id="u",
            leader_id=None,
            token_id="token",
            condition_id="condition",
            side=SubmissionSide.SELL,
            amount=Decimal("10"),
            price_limit=Decimal("0.5"),
            submitted_at=_NOW,
            amount_unit="shares",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# Persisted row → submission identity
# ---------------------------------------------------------------------------


def _row(request: dict, *, token_column: str | None = None, unit_column: str | None = None):
    return {
        "idempotency_key": "copy:leader:event",
        "user_id": "user-1",
        "leader_id": "leader-1",
        "order_id": None,
        "status": "submitted_unconfirmed",
        "request_json": json.dumps(request),
        "response_json": json.dumps({"pending_since": 1_760_160_400.0}),
        "created_at": _NOW - timedelta(seconds=600),
        "updated_at": _NOW,
        "token_id": token_column,
        "amount_unit": unit_column,
    }


def test_row_to_submission_reads_identity_columns_for_new_rows() -> None:
    """New rows persist token/unit identity in dedicated columns."""

    request = {
        "token_id": "token-request",
        "condition_id": "condition",
        "side": "BUY",
        "amount": "12.5",
        "price_limit": "0.625",
        "amount_unit": "quote",
    }
    submission = _row_to_ambiguous_submission(
        _row(request, token_column="token-column", unit_column="shares")
    )

    # The dedicated columns are authoritative for rows that carry them.
    assert submission.token_id == "token-column"
    assert submission.amount_unit is SubmissionAmountUnit.SHARES


def test_row_to_submission_falls_back_to_request_json_for_legacy_rows() -> None:
    """Legacy rows have no identity columns; request payload remains readable."""

    request = {
        "token_id": "token-request",
        "condition_id": "condition",
        "side": "SELL",
        "amount": "5",
        "amount_unit": "shares",
    }
    submission = _row_to_ambiguous_submission(_row(request))

    assert submission.token_id == "token-request"
    assert submission.amount_unit is SubmissionAmountUnit.SHARES


def test_row_without_unit_identity_stays_none_and_never_fabricates() -> None:
    request = {
        "token_id": "token-request",
        "condition_id": "condition",
        "side": "BUY",
        "amount": "12.5",
    }
    submission = _row_to_ambiguous_submission(_row(request))

    assert submission.amount_unit is None
    assert submission.token_id == "token-request"


def test_row_with_unrecognized_unit_value_fails_closed_to_none() -> None:
    request = {
        "token_id": "token-request",
        "condition_id": "condition",
        "side": "BUY",
        "amount": "12.5",
        "amount_unit": "usdc",
    }
    submission = _row_to_ambiguous_submission(_row(request))

    assert submission.amount_unit is None


# ---------------------------------------------------------------------------
# Sanitizer keeps the new request fields
# ---------------------------------------------------------------------------


def test_sanitize_submission_request_keeps_unit_and_kind_fields() -> None:
    request = {
        "user_id": "u1",
        "token_id": "token",
        "condition_id": "condition",
        "side": "BUY",
        "amount": "10",
        "amount_unit": "quote",
        "order_kind": "market",
        "secret": "must-be-removed",
    }

    sanitized = sanitize_submission_request(request)

    assert sanitized["amount_unit"] == "quote"
    assert sanitized["order_kind"] == "market"
    assert "secret" not in sanitized
