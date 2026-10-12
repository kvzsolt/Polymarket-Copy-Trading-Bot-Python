"""Read-only, fail-closed evidence contracts for no-order-id recovery.

The production evidence source is the only thing that lets the bounded
`AmbiguousSubmissionRecoveryService` run outside tests.  These contracts pin
its two safety properties:

* every scan is strictly read-only at the venue boundary; and
* any incomplete collection or unreadable account state fails closed and can
  never support a recovered identity or an absence proof.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from integrations.polymarket.ccxt_ambiguous_evidence import CcxtAmbiguousEvidenceSource
from integrations.polymarket.errors import (
    VenueHistoryIncompleteError,
    VenueSchemaError,
    VenueUnavailableError,
)
from integrations.polymarket.models import VenueFill, VenueOrderState
from services.execution.ambiguous_recovery import (
    AccountCheckResult,
    AmbiguousSubmission,
    EvidenceCollectionOutcome,
    RecoveryMatchSource,
    SubmissionAmountUnit,
    SubmissionSide,
)

pytestmark = pytest.mark.asyncio

_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
_TOKEN = "71321045679252212594626385532706912750332728571942532289631379312455583992563"
_CONDITION = "0x" + "ab" * 32
_WALLET = "0x" + "11" * 20

_WRITE_METHODS = frozenset(
    {
        "submit_marketable_order",
        "submit_limit_order",
        "cancel_order",
    }
)


_UNSET = object()


class _FakeGateway:
    """Read-only stand-in that records every interaction."""

    def __init__(
        self,
        *,
        fills: list[VenueFill] | None = None,
        orders: list[VenueOrderState] | None = None,
        fills_error: BaseException | None = None,
        orders_error: BaseException | None = None,
        delay_seconds: float = 0.0,
        fills_value: Any = _UNSET,
        orders_value: Any = _UNSET,
    ) -> None:
        self._fills = list(fills or [])
        self._orders = list(orders or [])
        self._fills_error = fills_error
        self._orders_error = orders_error
        self._delay = delay_seconds
        self._fills_value = fills_value
        self._orders_value = orders_value
        self.calls: list[str] = []

    async def get_fills(self, market: str | None = None, *, trade_id=None, order_id=None):
        self.calls.append("get_fills")
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._fills_error is not None:
            raise self._fills_error
        if self._fills_value is not _UNSET:
            return self._fills_value
        return list(self._fills)

    async def list_open_orders(self, *, order_id=None, market=None):
        self.calls.append("list_open_orders")
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._orders_error is not None:
            raise self._orders_error
        if self._orders_value is not _UNSET:
            return self._orders_value
        return list(self._orders)

    async def submit_marketable_order(self, request):  # pragma: no cover - guard
        self.calls.append("submit_marketable_order")
        raise AssertionError("evidence scan must never submit")

    async def submit_limit_order(self, request):  # pragma: no cover - guard
        self.calls.append("submit_limit_order")
        raise AssertionError("evidence scan must never submit")

    async def cancel_order(self, order_id):  # pragma: no cover - guard
        self.calls.append("cancel_order")
        raise AssertionError("evidence scan must never cancel")


class _SharesReader:
    def __init__(self, values: dict[str, Decimal | None] | None = None, *, error=None) -> None:
        self._values = values or {}
        self._error = error
        self.calls: list[tuple[str, str]] = []

    async def shares(self, wallet: str, token_id: str) -> Decimal | None:
        self.calls.append((wallet, token_id))
        if self._error is not None:
            raise self._error
        return self._values.get(token_id, Decimal("0"))


def _source(
    *,
    gateway: _FakeGateway,
    venue_shares: Decimal | None = Decimal("5"),
    local_shares: Decimal | None = Decimal("5"),
    balance: Decimal | None = Decimal("100"),
    reserved: Decimal | None = Decimal("0"),
    wallet: str | None = _WALLET,
    shares_error: BaseException | None = None,
    read_timeout_seconds: float = 5.0,
    match_window: timedelta = timedelta(minutes=15),
) -> tuple[CcxtAmbiguousEvidenceSource, dict[str, list]]:
    """Build the production source with fully in-memory collaborators."""

    calls: dict[str, list] = {"local": [], "balance": [], "reserved": []}
    reader = _SharesReader(
        {_TOKEN: venue_shares} if venue_shares is not None else {},
        error=shares_error,
    )

    async def _local(user_id: str, condition_id: str, token_id: str) -> Decimal | None:
        calls["local"].append((user_id, condition_id, token_id))
        return local_shares

    async def _balance(address: str) -> Decimal | None:
        calls["balance"].append(address)
        return balance

    async def _reserved(user_id: str) -> Decimal | None:
        calls["reserved"].append(user_id)
        return reserved

    source = CcxtAmbiguousEvidenceSource(
        private_history=gateway,
        shares_reader=reader,
        balance_reader=_balance,
        local_shares_reader=_local,
        reserved_quote_reader=_reserved,
        wallet_for_user=lambda user_id: wallet,
        read_timeout_seconds=read_timeout_seconds,
        match_window=match_window,
    )
    return source, calls


_AUTO_UNIT = object()


def _submission(
    *,
    side: SubmissionSide = SubmissionSide.BUY,
    amount: Decimal | None = Decimal("10"),
    price_limit: Decimal | None = Decimal("0.5"),
    token_id: str | None = _TOKEN,
    condition_id: str | None = _CONDITION,
    age_seconds: int = 300,
    amount_unit: Any = _AUTO_UNIT,
) -> AmbiguousSubmission:
    # The recorded unit mirrors the submit semantics: BUY amount is quote
    # spend, SELL amount is outcome shares.  Pass ``amount_unit=None``
    # explicitly to model a legacy row or a corrupt record.
    unit = (
        (
            SubmissionAmountUnit.QUOTE
            if side is SubmissionSide.BUY
            else SubmissionAmountUnit.SHARES
        )
        if amount_unit is _AUTO_UNIT
        else amount_unit
    )
    return AmbiguousSubmission(
        idempotency_key="copy:leader:event",
        user_id="user-1",
        leader_id="leader-1",
        token_id=token_id,
        condition_id=condition_id,
        side=side,
        amount=amount,
        price_limit=price_limit,
        submitted_at=_NOW - timedelta(seconds=age_seconds),
        amount_unit=unit,
    )


def _fill(
    *,
    order_id: str = "venue-order-1",
    token_id: str = _TOKEN,
    side: str = "BUY",
    size: str = "20",
    price: str = "0.5",
    timestamp: int = int(_NOW.timestamp()) - 120,
) -> VenueFill:
    # Default leg: 20 shares @ 0.5 = exactly the 10-quote BUY submission
    # amount the ``_submission`` helper builds.
    return VenueFill(
        fill_id=f"fill-{order_id}",
        order_id=order_id,
        token_id=token_id,
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        fee=Decimal("0"),
        timestamp=timestamp,
        status="CONFIRMED",
        transaction_hash="0x" + "cd" * 32,
        trader_side="TAKER",
        bucket_index=0,
        raw={},
    )


def _order(
    *,
    order_id: str = "venue-order-9",
    side: str = "BUY",
    price: str = "0.5",
    size: str = "10",
    created_at: datetime | None = None,
    token_id: str | None = _TOKEN,
) -> VenueOrderState:
    return VenueOrderState(
        order_id=order_id,
        status="live",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        created_at=created_at or (_NOW - timedelta(seconds=120)),
        token_id=token_id,
        raw={},
    )


# ---------------------------------------------------------------------------
# Collection completeness / fail-closed
# ---------------------------------------------------------------------------


async def test_missing_identity_fails_closed_without_querying_the_venue() -> None:
    gateway = _FakeGateway(fills=[_fill()])
    source, calls = _source(gateway=gateway)

    scan = await source.scan(_submission(token_id=None))

    assert scan.match is None
    assert scan.private_orders_complete is False
    assert scan.private_fills_complete is False
    assert scan.position_check is AccountCheckResult.INCOMPLETE
    assert scan.balance_check is AccountCheckResult.INCOMPLETE
    assert scan.qualifies_as_complete_absence is False
    assert gateway.calls == []
    assert calls["local"] == []
    assert calls["balance"] == []


async def test_fill_history_error_is_incomplete_and_never_matches() -> None:
    gateway = _FakeGateway(fills=[_fill()], fills_error=RuntimeError("transport down"))
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_complete is False
    assert scan.private_orders_complete is True
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_open_order_error_is_incomplete_but_fills_still_recoverable() -> None:
    gateway = _FakeGateway(fills=[_fill()], orders_error=RuntimeError("transport down"))
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_orders_complete is False
    assert scan.private_fills_complete is True
    assert scan.match is not None
    assert scan.match.source is RecoveryMatchSource.PRIVATE_FILLS


async def test_read_timeout_fails_closed_on_both_collections() -> None:
    gateway = _FakeGateway(fills=[_fill()], delay_seconds=0.25)
    source, _ = _source(gateway=gateway, read_timeout_seconds=0.01)

    scan = await source.scan(_submission())

    assert scan.private_fills_complete is False
    assert scan.private_orders_complete is False
    assert scan.match is None


# ---------------------------------------------------------------------------
# Recovered identity matching
# ---------------------------------------------------------------------------


async def test_unique_complete_fill_recovers_venue_order_id() -> None:
    gateway = _FakeGateway(fills=[_fill(order_id="venue-order-7")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"
    assert scan.match.source is RecoveryMatchSource.PRIVATE_FILLS


async def test_partial_fills_of_one_order_id_aggregate_to_a_match() -> None:
    # BUY amount is quote spend: 12 shares @ 0.5 (6 quote) + 8 shares @ 0.5
    # (4 quote) aggregate to exactly the 10-quote submission.
    gateway = _FakeGateway(
        fills=[
            _fill(order_id="venue-order-7", size="12", price="0.5"),
            _fill(order_id="venue-order-7", size="8", price="0.5"),
        ]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"


async def test_multiple_distinct_fill_candidates_are_ambiguous_and_never_match() -> None:
    gateway = _FakeGateway(
        fills=[
            _fill(order_id="venue-order-7"),
            _fill(order_id="venue-order-8"),
        ]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is None
    assert scan.private_fills_complete is True


async def test_fill_for_another_token_side_price_or_window_never_matches() -> None:
    other_token = _FakeGateway(fills=[_fill(token_id="999999")])
    other_side = _FakeGateway(fills=[_fill(side="SELL")])
    wrong_price = _FakeGateway(fills=[_fill(price="0.75")])
    stale = _FakeGateway(fills=[_fill(timestamp=int(_NOW.timestamp()) - 7200)])

    for gateway in (other_token, other_side, wrong_price, stale):
        source, _ = _source(gateway=gateway)
        scan = await source.scan(_submission())
        assert scan.match is None, gateway.calls

    # A SELL submission accepts a fill at or above its floor; its amount is
    # shares, so 10 shares @ 0.5 aggregate to the 10-share submission.
    sell_gateway = _FakeGateway(fills=[_fill(side="SELL", price="0.5", size="10")])
    sell_source, _ = _source(gateway=sell_gateway)
    sell_scan = await sell_source.scan(
        _submission(side=SubmissionSide.SELL, amount=Decimal("10"), price_limit=Decimal("0.5"))
    )
    assert sell_scan.match is not None


async def test_resting_open_order_is_recovered_from_complete_order_history() -> None:
    gateway = _FakeGateway(orders=[_order(order_id="venue-order-9")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-9"
    assert scan.match.source is RecoveryMatchSource.PRIVATE_ORDERS


async def test_unresolved_identity_never_matches() -> None:
    gateway = _FakeGateway(fills=[_fill(order_id=None)])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is None


# ---------------------------------------------------------------------------
# Outcome-token-exact order matching (A02)
# ---------------------------------------------------------------------------


async def test_yes_and_no_orders_never_cross_match() -> None:
    """Two outcomes of one condition share price/size/side but not a token.

    A submission for the YES token must never attach a NO order id, and a
    venue whose only candidate is the opposite outcome must yield no match
    at all.
    """

    no_token = "9" * len(_TOKEN)
    both = _FakeGateway(
        orders=[
            _order(order_id="venue-yes", token_id=_TOKEN),
            _order(order_id="venue-no", token_id=no_token),
        ]
    )
    only_no = _FakeGateway(orders=[_order(order_id="venue-no", token_id=no_token)])

    both_source, _ = _source(gateway=both)
    both_scan = await both_source.scan(_submission())
    assert both_scan.match is not None
    assert both_scan.match.venue_order_id == "venue-yes"
    assert both_scan.match.source is RecoveryMatchSource.PRIVATE_ORDERS

    no_source, _ = _source(gateway=only_no)
    no_scan = await no_source.scan(_submission())
    assert no_scan.match is None


async def test_order_without_token_identity_never_matches() -> None:
    """A resting order missing its token cannot prove outcome identity."""

    gateway = _FakeGateway(orders=[_order(order_id="venue-anon", token_id=None)])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is None


async def test_fill_for_the_opposite_outcome_never_matches_even_with_equal_numbers():
    no_token = "9" * len(_TOKEN)
    gateway = _FakeGateway(fills=[_fill(order_id="venue-no", token_id=no_token)])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is None


async def test_conflicting_fill_and_order_evidence_blocks_attachment() -> None:
    """Two complete collections disagreeing about the identity attach nothing."""

    gateway = _FakeGateway(
        fills=[_fill(order_id="venue-fills")],
        orders=[_order(order_id="venue-orders")],
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.match is None
    assert scan.private_fills_complete is True
    assert scan.private_orders_complete is True


# ---------------------------------------------------------------------------
# Unit-strict fill matching (A02)
# ---------------------------------------------------------------------------


async def test_sell_amount_never_matches_a_numerically_equal_quote_aggregate() -> None:
    """A 1-share SELL must not match a 2-share fill whose quote equals 1.

    Pre-A02 matching accepted ``total_quote == amount`` for a SELL, so BUY
    spend semantics and SELL share semantics were interchangeable.  The SELL
    amount is shares: only the share aggregate may match.
    """

    gateway = _FakeGateway(fills=[_fill(side="SELL", size="2", price="0.5")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(
        _submission(
            side=SubmissionSide.SELL,
            amount=Decimal("1"),
            price_limit=Decimal("0.5"),
        )
    )

    assert scan.match is None


async def test_buy_amount_never_matches_a_numerically_equal_share_aggregate() -> None:
    """A 2-quote BUY must not match a 2-share fill whose quote is 0.5."""

    gateway = _FakeGateway(fills=[_fill(size="2", price="0.25")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission(amount=Decimal("2")))

    assert scan.match is None


async def test_buy_partial_fills_accumulate_by_quote_spend() -> None:
    """BUY legs aggregate in quote: 5@0.8 + 7.5@0.8 = 10 quote spent."""

    gateway = _FakeGateway(
        fills=[
            _fill(order_id="venue-order-7", size="5", price="0.8"),
            _fill(order_id="venue-order-7", size="7.5", price="0.8"),
        ]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission(amount=Decimal("10"), price_limit=Decimal("0.8")))

    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"


async def test_sell_partial_fills_accumulate_by_shares() -> None:
    """SELL legs aggregate in shares: 2 + 3 = 5 shares sold."""

    gateway = _FakeGateway(
        fills=[
            _fill(order_id="venue-order-7", side="SELL", size="2", price="0.4"),
            _fill(order_id="venue-order-7", side="SELL", size="3", price="0.6"),
        ]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(
        _submission(
            side=SubmissionSide.SELL,
            amount=Decimal("5"),
            price_limit=Decimal("0.4"),
        )
    )

    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"


# ---------------------------------------------------------------------------
# Legacy rows without unit identity fail closed (A02)
# ---------------------------------------------------------------------------


async def test_submission_without_unit_identity_fails_closed_without_private_reads() -> None:
    """A row that persisted an amount but no unit can never be unit-verified.

    Legacy rows cannot have identity fabricated for them: the scan is a
    complete fail-closed no-op (no private read, no match, no absence
    progress) so the row stays duplicate-blocking for the operator.
    """

    gateway = _FakeGateway(fills=[_fill()], orders=[_order()])
    source, calls = _source(gateway=gateway)

    scan = await source.scan(_submission(amount_unit=None))

    assert scan.match is None
    assert scan.private_orders_complete is False
    assert scan.private_fills_complete is False
    assert scan.position_check is AccountCheckResult.INCOMPLETE
    assert scan.balance_check is AccountCheckResult.INCOMPLETE
    assert scan.qualifies_as_complete_absence is False
    assert gateway.calls == []
    assert calls["local"] == []


async def test_unit_side_mismatch_fails_closed() -> None:
    """A BUY recorded in shares (or a SELL in quote) is corrupt: never match."""

    gateway = _FakeGateway(fills=[_fill()])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission(amount_unit=SubmissionAmountUnit.SHARES))

    assert scan.match is None
    assert scan.private_fills_complete is False
    assert scan.qualifies_as_complete_absence is False


async def test_missing_amount_still_scans_without_unit_requirements() -> None:
    """A row without any amount keeps its pre-A02 scan behaviour."""

    gateway = _FakeGateway(fills=[_fill(order_id="venue-order-7", size="7", price="0.5")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission(amount=None, amount_unit=None))

    assert scan.private_fills_complete is True
    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"


# ---------------------------------------------------------------------------
# Account checks
# ---------------------------------------------------------------------------


async def test_position_check_compares_venue_against_persisted_local_shares() -> None:
    matching, _ = _source(
        gateway=_FakeGateway(), venue_shares=Decimal("5"), local_shares=Decimal("5")
    )
    executed, _ = _source(
        gateway=_FakeGateway(), venue_shares=Decimal("15"), local_shares=Decimal("5")
    )
    opposite, _ = _source(
        gateway=_FakeGateway(), venue_shares=Decimal("1"), local_shares=Decimal("5")
    )

    assert (await matching.scan(_submission())).position_check is (
        AccountCheckResult.CONSISTENT_WITH_ABSENCE
    )
    assert (await executed.scan(_submission())).position_check is (
        AccountCheckResult.EVIDENCE_OF_EXECUTION
    )
    assert (await opposite.scan(_submission())).position_check is AccountCheckResult.INCOMPLETE


async def test_sell_position_drop_is_evidence_of_execution() -> None:
    source, _ = _source(
        gateway=_FakeGateway(), venue_shares=Decimal("2"), local_shares=Decimal("5")
    )

    scan = await source.scan(_submission(side=SubmissionSide.SELL))

    assert scan.position_check is AccountCheckResult.EVIDENCE_OF_EXECUTION


async def test_unknown_side_or_unreadable_account_state_is_incomplete() -> None:
    unknown, _ = _source(gateway=_FakeGateway())
    unreadable, _ = _source(gateway=_FakeGateway(), shares_error=RuntimeError("rpc down"))
    no_wallet, _ = _source(gateway=_FakeGateway(), wallet=None)

    assert (await unknown.scan(_submission(side=SubmissionSide.UNKNOWN))).position_check is (
        AccountCheckResult.INCOMPLETE
    )
    assert (await unreadable.scan(_submission())).position_check is AccountCheckResult.INCOMPLETE
    assert (await no_wallet.scan(_submission())).position_check is AccountCheckResult.INCOMPLETE


async def test_balance_check_requires_a_complete_read_and_a_covered_floor() -> None:
    covered, _ = _source(gateway=_FakeGateway(), balance=Decimal("100"), reserved=Decimal("40"))
    uncovered, _ = _source(gateway=_FakeGateway(), balance=Decimal("10"), reserved=Decimal("40"))
    unknown_balance, _ = _source(gateway=_FakeGateway(), balance=None)
    unknown_floor, _ = _source(gateway=_FakeGateway(), reserved=None)

    assert (await covered.scan(_submission())).balance_check is (
        AccountCheckResult.CONSISTENT_WITH_ABSENCE
    )
    assert (await uncovered.scan(_submission())).balance_check is AccountCheckResult.INCOMPLETE
    assert (await unknown_balance.scan(_submission())).balance_check is (
        AccountCheckResult.INCOMPLETE
    )
    assert (await unknown_floor.scan(_submission())).balance_check is AccountCheckResult.INCOMPLETE


async def test_complete_absence_requires_every_check_to_hold() -> None:
    complete, _ = _source(gateway=_FakeGateway())

    scan = await complete.scan(_submission())

    assert scan.qualifies_as_complete_absence is True


# ---------------------------------------------------------------------------
# Read-only guarantee
# ---------------------------------------------------------------------------


async def test_scan_only_ever_calls_read_methods() -> None:
    gateway = _FakeGateway(fills=[_fill()], orders=[_order()])
    source, _ = _source(gateway=gateway)

    await source.scan(_submission())

    assert gateway.calls == ["get_fills", "list_open_orders"]
    assert not (_WRITE_METHODS & set(gateway.calls))


# ---------------------------------------------------------------------------
# Account ownership (audit A01)
# ---------------------------------------------------------------------------


async def test_scan_for_an_unserved_account_performs_no_private_reads() -> None:
    """A submission whose account has no served wallet must never be scanned.

    The gateway is authenticated for ONE wallet.  Enumerating private history
    for a submission owned by a different account would read the wrong
    account's fills/orders, and the token/side/time/price/size match keying
    could then attach a foreign venue order id.  Ownership is therefore
    verified before any private read.
    """
    gateway = _FakeGateway(fills=[_fill()], orders=[_order()])
    source, _ = _source(gateway=gateway, wallet=None)

    scan = await source.scan(_submission())

    assert gateway.calls == []
    assert scan.private_orders_complete is False
    assert scan.private_fills_complete is False
    assert scan.position_check is AccountCheckResult.INCOMPLETE
    assert scan.balance_check is AccountCheckResult.INCOMPLETE
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_wallet_for_user_exposes_the_served_wallet_per_account() -> None:
    gateway = _FakeGateway()
    source, _ = _source(gateway=gateway, wallet=_WALLET)

    # The injected resolver decides which accounts are served; the bootstrap
    # wires exactly one wallet for exactly one user.
    assert source.wallet_for_user("user-1") == _WALLET
    unserved, _ = _source(gateway=gateway, wallet=None)
    assert unserved.wallet_for_user("user-1") is None


async def test_private_reads_still_run_for_the_served_account() -> None:
    gateway = _FakeGateway(fills=[_fill()], orders=[_order()])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert gateway.calls == ["get_fills", "list_open_orders"]
    assert scan.private_fills_complete is True


# ---------------------------------------------------------------------------
# Collection outcome mapping (A03)
#
# A collection that raised, returned an unreadable payload, or stopped before
# proving pagination exhausted is NOT a complete empty collection.  The scan
# must carry which of the five outcomes each private read produced, and only
# ``complete_empty`` can ever support an absence proof.
# ---------------------------------------------------------------------------


async def test_schema_invalid_history_maps_to_invalid_outcome() -> None:
    gateway = _FakeGateway(fills_error=VenueSchemaError("unrecognized page"))
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.INVALID
    assert scan.private_fills_complete is False
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_pagination_cutoff_maps_to_partial_outcome() -> None:
    gateway = _FakeGateway(
        orders_error=VenueHistoryIncompleteError("exceeded privateHistoryMaxPages")
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_orders_outcome is EvidenceCollectionOutcome.PARTIAL
    assert scan.private_orders_complete is False
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_transport_failure_maps_to_unavailable_outcome() -> None:
    unavailable = _FakeGateway(fills_error=VenueUnavailableError("CCXT NetworkError"))
    unknown = _FakeGateway(orders_error=RuntimeError("transport down"))
    unavailable_source, _ = _source(gateway=unavailable)
    unknown_source, _ = _source(gateway=unknown)

    unavailable_scan = await unavailable_source.scan(_submission())
    unknown_scan = await unknown_source.scan(_submission())

    assert unavailable_scan.private_fills_outcome is EvidenceCollectionOutcome.UNAVAILABLE
    assert unknown_scan.private_orders_outcome is EvidenceCollectionOutcome.UNAVAILABLE
    assert unavailable_scan.qualifies_as_complete_absence is False
    assert unknown_scan.qualifies_as_complete_absence is False


async def test_none_collection_is_invalid_not_complete_empty() -> None:
    """A ``None`` read is an unreadable payload, never an empty collection."""

    gateway = _FakeGateway(fills_value=None)
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.INVALID
    assert scan.private_fills_complete is False
    assert scan.qualifies_as_complete_absence is False


async def test_cancellation_is_never_swallowed_into_a_scan_result() -> None:
    """Task cancellation must propagate; it can never become a scan outcome."""

    gateway = _FakeGateway(fills_error=asyncio.CancelledError())
    source, _ = _source(gateway=gateway)

    with pytest.raises(asyncio.CancelledError):
        await source.scan(_submission())


async def test_genuinely_empty_collections_map_to_complete_empty_and_advance_proof() -> None:
    gateway = _FakeGateway()
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_fills_complete is True
    assert scan.private_orders_complete is True
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is True


async def test_unrelated_fill_history_with_zero_candidates_qualifies_as_absence() -> None:
    """Absence is proven over the submission's candidate set, not raw emptiness.

    The market-scoped collection is the account's ENTIRE fill history on the
    condition (append-only, no ``since`` filter), so a routine unrelated fill
    would otherwise block absence forever.  A complete read whose rows are all
    non-candidates for THIS submission (token-exact matcher) is authoritative
    absence evidence.
    """

    gateway = _FakeGateway(
        fills=[_fill(order_id="venue-other", token_id="9" * len(_TOKEN))]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_NONEMPTY
    assert scan.private_fills_candidates_empty is True
    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_orders_candidates_empty is True
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is True


async def test_unrelated_resting_order_with_zero_candidates_qualifies_as_absence() -> None:
    """Mirror of the fill case: unrelated resting orders must not block proof."""

    gateway = _FakeGateway(
        orders=[_order(order_id="venue-other", token_id="9" * len(_TOKEN), side="SELL")]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_NONEMPTY
    assert scan.private_orders_candidates_empty is True
    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_fills_candidates_empty is True
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is True


async def test_candidate_fill_activity_never_supports_an_absence_proof() -> None:
    """A row that IS a candidate blocks absence even when it cannot match.

    One candidate fill whose aggregate cannot reproduce the submission amount
    is unresolved venue activity resembling this submission: absence stays
    unproven and the row stays duplicate-blocking.
    """

    gateway = _FakeGateway(fills=[_fill(order_id="venue-order-7", size="1", price="0.5")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_candidates_empty is False
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_multiple_candidate_orders_never_support_an_absence_proof() -> None:
    """Two indistinguishable candidate orders are ambiguity, never absence."""

    gateway = _FakeGateway(
        orders=[_order(order_id="venue-a"), _order(order_id="venue-b")]
    )
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission(amount=None, amount_unit=None))

    assert scan.private_orders_candidates_empty is False
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_failed_reads_with_zero_candidates_never_qualify_as_absence() -> None:
    """invalid/partial/unavailable never qualify, whatever was read."""

    for error in (
        VenueSchemaError("unrecognized page"),
        VenueHistoryIncompleteError("exceeded privateHistoryMaxPages"),
        VenueUnavailableError("CCXT NetworkError"),
    ):
        gateway = _FakeGateway(fills_error=error)
        source, _ = _source(gateway=gateway)

        scan = await source.scan(_submission())

        assert scan.private_fills_candidates_empty is None, type(error).__name__
        assert scan.match is None, type(error).__name__
        assert scan.qualifies_as_complete_absence is False, type(error).__name__


async def test_nonempty_complete_history_still_recovers_identity() -> None:
    """``complete_nonempty`` supports matching; only absence needs emptiness."""

    gateway = _FakeGateway(fills=[_fill(order_id="venue-order-7")])
    source, _ = _source(gateway=gateway)

    scan = await source.scan(_submission())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_NONEMPTY
    assert scan.private_fills_candidates_empty is False
    assert scan.match is not None
    assert scan.match.venue_order_id == "venue-order-7"


# ---------------------------------------------------------------------------
# End to end with the production gateway (A03)
# ---------------------------------------------------------------------------


class _RawHistoryExchange:
    """Raw CLOB envelopes walked by the production CCXT gateway."""

    def __init__(
        self,
        *,
        order_pages: dict[str | None, object] | None = None,
        trade_pages: dict[str | None, object] | None = None,
        options: dict[str, object] | None = None,
    ) -> None:
        self.options = {"chainId": 137, "signatureType": 3, **(options or {})}
        self._order_pages = order_pages or {None: {"data": [], "next_cursor": ""}}
        self._trade_pages = trade_pages or {None: {"data": [], "next_cursor": ""}}

    async def clob_private_get_data_orders(self, params):
        return self._order_pages[dict(params).get("next_cursor")]

    async def clob_private_get_data_trades(self, params):
        return self._trade_pages[dict(params).get("next_cursor")]

    async def fetch_open_orders(self, *_args):  # pragma: no cover - raw path is used
        raise AssertionError("raw endpoint must be preferred")

    async def fetch_my_trades(self, *_args):  # pragma: no cover - raw path is used
        raise AssertionError("raw endpoint must be used")


async def _gateway_scan(exchange: _RawHistoryExchange) -> Any:
    from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway

    source, _ = _source(gateway=CcxtPredictionGateway(exchange))  # type: ignore[arg-type]
    return await source.scan(_submission())


async def test_malformed_http_200_history_never_becomes_an_absence_proof() -> None:
    """The A03 headline defect, end to end.

    Pre-A03, a schema-drifted 200 fills payload was silently read as an empty
    page, both collections looked "complete", and the scan qualified as a
    complete absence — potentially confirming a submission as absent when the
    venue had actually answered garbage.
    """

    scan = await _gateway_scan(
        _RawHistoryExchange(
            order_pages={None: {"data": [], "next_cursor": ""}},
            trade_pages={None: {"error": "internal error", "next_cursor": ""}},
        )
    )

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.INVALID
    assert scan.private_fills_complete is False
    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_pagination_cutoff_never_advances_the_absence_proof() -> None:
    scan = await _gateway_scan(
        _RawHistoryExchange(
            options={"privateHistoryMaxPages": 1},
            order_pages={None: {"data": [], "next_cursor": "MTAw"}},
            trade_pages={None: {"data": [], "next_cursor": ""}},
        )
    )

    assert scan.private_orders_outcome is EvidenceCollectionOutcome.PARTIAL
    assert scan.private_orders_complete is False
    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is False


async def test_genuine_empty_terminal_pages_advance_the_absence_proof() -> None:
    scan = await _gateway_scan(_RawHistoryExchange())

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is True


async def test_unrelated_market_history_still_advances_the_absence_proof() -> None:
    """End to end: unrelated venue activity on the condition must not block.

    The fills collection carries one confirmed fill for the SAME condition but
    a DIFFERENT outcome token, so the token-exact T7 matcher finds zero
    candidates and a complete read still qualifies as absence for the
    submission's token.
    """

    unrelated_fill = {
        "id": "trade-unrelated",
        "market": _CONDITION,
        "asset_id": "9" * len(_TOKEN),
        "side": "BUY",
        "size": "3",
        "price": "0.30",
        "status": "CONFIRMED",
        "match_time": str(int(_NOW.timestamp()) - 60),
        "trader_side": "TAKER",
        "maker_orders": [],
    }
    scan = await _gateway_scan(
        _RawHistoryExchange(
            order_pages={None: {"data": [], "next_cursor": ""}},
            trade_pages={None: {"data": [unrelated_fill], "next_cursor": ""}},
        )
    )

    assert scan.private_fills_outcome is EvidenceCollectionOutcome.COMPLETE_NONEMPTY
    assert scan.private_fills_candidates_empty is True
    assert scan.private_orders_outcome is EvidenceCollectionOutcome.COMPLETE_EMPTY
    assert scan.private_orders_candidates_empty is True
    assert scan.match is None
    assert scan.qualifies_as_complete_absence is True
