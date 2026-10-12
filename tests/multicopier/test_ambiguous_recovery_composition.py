"""§9 bullets 5, 6, and 14 — ambiguous-submission recovery at the composed
two-copier level.

Every account runtime is composed through the REAL ``build_user_state``, so
the ``AmbiguousSubmissionRecoveryService`` under test is the one production
wires: bound to that account's OWN venue gateway (``CcxtAmbiguousEvidenceSource``
with ``private_history=<the account gateway>``), that account's own wallet
map, and an orders repository whose mutations are account-scoped. The
persistence port is an in-memory scoped fake installed on the REAL
``OrdersRepo`` instance the composition produced — identical semantics to the
SQL compare-and-set proven in
``tests/test_ambiguous_recovery_account_isolation_postgres.py``.

Proves:

* A and B hold IDENTICAL ambiguous submission parameters; each recovers only
  from its own private history (its own gateway is the only evidence source
  scanned) and attaches only its own venue order id.
* A YES and a NO submission at equal price/size never cross-match through the
  composed service, and a BUY quote amount never matches a numerically equal
  SELL share aggregate (unit-strict matching — the composition twin of
  ``tests/test_ccxt_ambiguous_evidence.py``).
* Each account's own absence-proof counters advance only from its own
  qualifying scans (extends the T6 service tests to the production wiring).
* The reservation-expiry reconciler callback wired by
  ``build_trading_context`` closes over its OWN account context: an expired
  no-ID submission or a failed persistence lookup through either account's
  callback never releases unresolved reservations without account-correct
  proof (composition twin of the T9 tests
  ``tests/test_reservation_expiry_no_id_release.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.integrations.polymarket.models import VenueFill
from pmcopy.services.execution.ambiguous_recovery import (
    AmbiguousSubmission,
    RecoveryDisposition,
    SubmissionAmountUnit,
    SubmissionSide,
)
from pmcopy.services.execution.reservation_expiry import ReservationExpiryDecision

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    FUNDER_BETA,
    LEADER_1,
    TOKEN,
    USER_ALPHA,
    USER_BETA,
    build_account_state,
    follow,
    make_config,
    runtime_user,
)

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)

# The complementary outcome token of the same market (synthetic, distinct).
TOKEN_NO = str(int(TOKEN) + 1)


class ScriptedRecoveryGateway:
    """Read-only private-history double with per-account scripted evidence."""

    def __init__(self, *, fills=None, orders=None, fills_error=None):
        self.fills = list(fills or [])
        self.orders = list(orders or [])
        self.fills_error = fills_error
        self.calls: list[str] = []

    async def get_fills(self, market=None, *, trade_id=None, order_id=None):
        self.calls.append("get_fills")
        if self.fills_error is not None:
            raise self.fills_error
        return list(self.fills)

    async def list_open_orders(self, *, order_id=None, market=None):
        self.calls.append("list_open_orders")
        return list(self.orders)

    async def submit_marketable_order(self, request):  # pragma: no cover
        raise AssertionError("recovery evidence scan must never submit")

    async def cancel_order(self, order_id):  # pragma: no cover
        raise AssertionError("recovery evidence scan must never cancel")


@dataclass
class _Row:
    submission: AmbiguousSubmission
    venue_order_id: str | None = None
    absent: bool = False
    consecutive: int = 0
    scans: list[str] = field(default_factory=list)


class InMemoryScopedOrdersRepo:
    """Account-scoped no-ID recovery persistence (installed on the real repo)."""

    def __init__(self) -> None:
        self.rows: dict[str, _Row] = {}
        self.list_calls: list[str] = []

    def add(self, submission: AmbiguousSubmission) -> None:
        self.rows[submission.idempotency_key] = _Row(submission=submission)

    def install(self, repo) -> None:
        repo.list_no_id_ambiguous_submissions = self.list_no_id_ambiguous_submissions
        repo.record_ambiguous_scan_evidence = self.record_ambiguous_scan_evidence
        repo.attach_recovered_venue_order_id = self.attach_recovered_venue_order_id
        repo.mark_ambiguous_submission_confirmed_absent = (
            self.mark_ambiguous_submission_confirmed_absent
        )

    async def list_no_id_ambiguous_submissions(self, *, user_id: str, limit: int):
        self.list_calls.append(user_id)
        owned = [
            row
            for row in self.rows.values()
            if row.submission.user_id == user_id and row.venue_order_id is None and not row.absent
        ]
        owned.sort(key=lambda row: row.submission.submitted_at)
        return tuple(row.submission for row in owned[:limit])

    async def record_ambiguous_scan_evidence(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        scan,
        observed_at: datetime,
        required_scan_count: int,
        evidence_history_limit: int,
    ):
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return None
        if row.venue_order_id is not None or row.absent:
            return None
        qualifies = scan.qualifies_as_complete_absence
        row.consecutive = min(row.consecutive + 1, required_scan_count) if qualifies else 0
        row.scans.append(user_id)
        return SimpleNamespace(consecutive_complete_absence_scans=row.consecutive)

    async def attach_recovered_venue_order_id(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        venue_order_id: str,
        source,
        recovered_at: datetime,
    ) -> bool:
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return False
        if row.venue_order_id is not None or row.absent:
            return False
        row.venue_order_id = venue_order_id
        return True

    async def mark_ambiguous_submission_confirmed_absent(
        self,
        *,
        user_id: str,
        idempotency_key: str,
        required_scan_count: int,
        confirmed_at: datetime,
    ) -> bool:
        row = self.rows.get(idempotency_key)
        if row is None or row.submission.user_id != user_id:
            return False
        if row.consecutive < required_scan_count:
            return False
        row.absent = True
        return True


def _submission(
    *,
    user_id: str,
    token_id: str = TOKEN,
    side: SubmissionSide = SubmissionSide.BUY,
    amount: Decimal = Decimal("12.50"),
    amount_unit: SubmissionAmountUnit | None = SubmissionAmountUnit.QUOTE,
    key_suffix: str = "",
) -> AmbiguousSubmission:
    return AmbiguousSubmission(
        idempotency_key=f"copy:{LEADER_1}:event:{user_id}{key_suffix}",
        user_id=user_id,
        leader_id=LEADER_1,
        token_id=token_id,
        condition_id=CONDITION,
        side=side,
        amount=amount,
        price_limit=Decimal("0.625"),
        submitted_at=NOW - timedelta(seconds=600),
        amount_unit=amount_unit,
    )


def _match_fill(
    *,
    order_id: str,
    token_id: str = TOKEN,
    side: str = "BUY",
    size: str = "20",
    price: str = "0.625",
) -> VenueFill:
    """One fill leg whose QUOTE aggregate equals the 12.50 BUY spend exactly
    (20 shares x 0.625 = 12.50 quote)."""
    return VenueFill(
        fill_id=f"fill-{order_id}",
        order_id=order_id,
        token_id=token_id,
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        fee=Decimal("0"),
        timestamp=int((NOW - timedelta(seconds=590)).timestamp()),
        status="MATCHED",
        trader_side="TAKER",
    )


def _compose_account(user_id: str, funder: str, gateway):
    config = make_config()
    user = runtime_user(user_id, funder, {LEADER_1: follow(user_id, LEADER_1)})
    state = build_account_state(user, config, gateway=gateway)
    assert state.ambiguous_recovery is not None
    return state


def _hermetic_evidence_readers(state, *, venue_shares=Decimal("0")) -> None:
    """Keep the evidence readers off the network and the clocks deterministic.

    Patches the reader collaborators the production composition installed
    (``_build_ambiguous_recovery_service``) with in-memory equivalents, and
    pins BOTH the service clock (grace period) and the evidence-source clock
    (match window) to the fixed ``NOW + 700s`` instant so recovery is
    deterministic regardless of wall-clock time.
    """
    source = state.ambiguous_recovery._evidence_source
    source._shares_reader = SimpleNamespace(shares=AsyncMock(return_value=venue_shares))
    source._clock = SimpleNamespace(now=lambda: NOW + timedelta(seconds=700))
    state.ambiguous_recovery._clock = SimpleNamespace(now=lambda: NOW + timedelta(seconds=700))

    async def _balance(_address):
        return Decimal("100")

    async def _local_shares(_user_id, _condition_id, _token_id):
        return Decimal("0")

    async def _reserved_quote(_user_id):
        return Decimal("0")

    source._balance_reader = _balance
    source._local_shares_reader = _local_shares
    source._reserved_quote_reader = _reserved_quote


# ---------------------------------------------------------------------------
# §9 bullet 5 — identical ambiguous submissions recover per-account only
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_identical_submissions_recover_only_from_their_own_private_history():
    gateway_a = ScriptedRecoveryGateway(fills=[_match_fill(order_id="venue-order-a")])
    gateway_b = ScriptedRecoveryGateway(fills=[_match_fill(order_id="venue-order-b")])
    state_a = _compose_account(USER_ALPHA, FUNDER_ALPHA, gateway_a)
    state_b = _compose_account(USER_BETA, FUNDER_BETA, gateway_b)
    _hermetic_evidence_readers(state_a)
    _hermetic_evidence_readers(state_b)

    repo = InMemoryScopedOrdersRepo()
    repo.add(_submission(user_id=USER_ALPHA))
    repo.add(_submission(user_id=USER_BETA))
    repo.install(state_a.orders_repo)
    repo.install(state_b.orders_repo)

    results_a = await state_a.ambiguous_recovery.recover_once()
    results_b = await state_b.ambiguous_recovery.recover_once()

    assert [r.disposition for r in results_a] == [RecoveryDisposition.RECOVERED_ORDER_ID]
    assert [r.disposition for r in results_b] == [RecoveryDisposition.RECOVERED_ORDER_ID]
    assert results_a[0].venue_order_id == "venue-order-a"
    assert results_b[0].venue_order_id == "venue-order-b"

    # Each scan read ONLY the account's own gateway.
    assert gateway_a.calls and gateway_b.calls
    row_a = repo.rows[_submission(user_id=USER_ALPHA).idempotency_key]
    row_b = repo.rows[_submission(user_id=USER_BETA).idempotency_key]
    assert row_a.venue_order_id == "venue-order-a"
    assert row_b.venue_order_id == "venue-order-b"
    # Batch listing was scoped per account before evidence collection.
    assert sorted(repo.list_calls) == [USER_ALPHA, USER_BETA]


@pytest.mark.asyncio
async def test_one_accounts_gateway_outage_never_resolves_or_resets_the_other():
    """A's private-history failure stays A's: B still recovers normally."""
    gateway_a = ScriptedRecoveryGateway(fills_error=RuntimeError("auth failed for alpha history"))
    gateway_b = ScriptedRecoveryGateway(fills=[_match_fill(order_id="venue-order-b")])
    state_a = _compose_account(USER_ALPHA, FUNDER_ALPHA, gateway_a)
    state_b = _compose_account(USER_BETA, FUNDER_BETA, gateway_b)
    _hermetic_evidence_readers(state_a)
    _hermetic_evidence_readers(state_b)

    repo = InMemoryScopedOrdersRepo()
    repo.add(_submission(user_id=USER_ALPHA))
    repo.add(_submission(user_id=USER_BETA))
    repo.install(state_a.orders_repo)
    repo.install(state_b.orders_repo)

    results_a = await state_a.ambiguous_recovery.recover_once()
    results_b = await state_b.ambiguous_recovery.recover_once()

    assert results_a[0].disposition is RecoveryDisposition.INCONCLUSIVE
    assert repo.rows[_submission(user_id=USER_ALPHA).idempotency_key].venue_order_id is None
    assert results_b[0].disposition is RecoveryDisposition.RECOVERED_ORDER_ID
    assert repo.rows[_submission(user_id=USER_BETA).idempotency_key].venue_order_id == (
        "venue-order-b"
    )


# ---------------------------------------------------------------------------
# §9 bullet 6 — outcome-token and amount-unit identity at the composed service
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yes_and_no_submissions_never_cross_match_through_the_composed_service():
    """A's YES submission vs a numerically identical NO fill: never a match."""
    gateway = ScriptedRecoveryGateway(
        # Numerically identical size/price/time — but the OTHER outcome token.
        fills=[_match_fill(order_id="venue-order-no", token_id=TOKEN_NO)]
    )
    state = _compose_account(USER_ALPHA, FUNDER_ALPHA, gateway)
    _hermetic_evidence_readers(state)
    repo = InMemoryScopedOrdersRepo()
    yes_submission = _submission(user_id=USER_ALPHA, token_id=TOKEN)
    repo.add(yes_submission)
    repo.install(state.orders_repo)

    results = await state.ambiguous_recovery.recover_once()

    # The scan collected evidence successfully but recovered NO identity for
    # the YES submission (the NO fill is not a candidate): the row stays
    # unresolved and keeps blocking duplicates.
    assert results[0].disposition is not RecoveryDisposition.RECOVERED_ORDER_ID
    assert results[0].venue_order_id is None
    row = repo.rows[yes_submission.idempotency_key]
    assert row.venue_order_id is None
    assert row.absent is False


@pytest.mark.asyncio
async def test_buy_quote_amount_never_matches_a_numerically_equal_share_aggregate():
    """A BUY 12.50-quote submission vs a SELL 12.50-share fill: never a match."""
    # Same token, side, window, and price — the fill passes every candidate
    # gate, but its SHARE total (12.50) is what a unit-confused matcher would
    # compare against the BUY quote amount. The quote aggregate is
    # 12.50 x 0.625 = 7.8125 != 12.50, so a unit-strict matcher must refuse.
    gateway = ScriptedRecoveryGateway(
        fills=[
            _match_fill(
                order_id="venue-order-share-decoy",
                side="BUY",
                size="12.50",
                price="0.625",
            )
        ]
    )
    state = _compose_account(USER_ALPHA, FUNDER_ALPHA, gateway)
    _hermetic_evidence_readers(state)
    repo = InMemoryScopedOrdersRepo()
    buy_submission = _submission(
        user_id=USER_ALPHA,
        side=SubmissionSide.BUY,
        amount=Decimal("12.50"),
        amount_unit=SubmissionAmountUnit.QUOTE,
    )
    repo.add(buy_submission)
    repo.install(state.orders_repo)

    results = await state.ambiguous_recovery.recover_once()

    assert results[0].disposition is RecoveryDisposition.INCONCLUSIVE
    assert repo.rows[buy_submission.idempotency_key].venue_order_id is None


# ---------------------------------------------------------------------------
# §9 bullet 5 (proof counters) + bullet 14 (expiry callback account scope)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_absence_proof_counters_advance_only_from_own_qualifying_scans():
    gateway_a = ScriptedRecoveryGateway()  # complete empty private history
    gateway_b = ScriptedRecoveryGateway()
    state_a = _compose_account(USER_ALPHA, FUNDER_ALPHA, gateway_a)
    state_b = _compose_account(USER_BETA, FUNDER_BETA, gateway_b)
    _hermetic_evidence_readers(state_a)
    _hermetic_evidence_readers(state_b)

    repo = InMemoryScopedOrdersRepo()
    sub_a = _submission(user_id=USER_ALPHA)
    sub_b = _submission(user_id=USER_BETA)
    repo.add(sub_a)
    repo.add(sub_b)
    repo.install(state_a.orders_repo)
    repo.install(state_b.orders_repo)

    for _ in range(2):
        await state_a.ambiguous_recovery.recover_once()
    await state_b.ambiguous_recovery.recover_once()

    row_a = repo.rows[sub_a.idempotency_key]
    row_b = repo.rows[sub_b.idempotency_key]
    assert row_a.consecutive == 2
    assert row_b.consecutive == 1
    assert row_a.scans == [USER_ALPHA, USER_ALPHA]
    assert row_b.scans == [USER_BETA]


@pytest.mark.asyncio
async def test_reservation_expiry_callback_is_account_scoped_and_fail_closed(
    monkeypatch,
):
    """Bullet 14 through the production-wired expiry callback.

    ``build_trading_context`` binds ``reservation_service.reconciler_callback``
    to a closure over ITS OWN context; an unresolved no-ID submission through
    A's callback stays reserved, a persistence-lookup failure stays reserved,
    and each callback only ever consults its own account's repositories.
    """
    from pmcopy.orchestrators.runtime_bootstrap import build_trading_context

    async def _no_reconcile(context, **_kwargs):
        return 0

    monkeypatch.setattr(
        "pmcopy.orchestrators.trade_pipeline.reconcile_submitted_unconfirmed", _no_reconcile
    )
    monkeypatch.setattr(
        "pmcopy.services.risk.risk_monitor.reconcile_unconfirmed_risk_orders", _no_reconcile
    )
    monkeypatch.setattr(
        "pmcopy.orchestrators.runtime_bootstrap.rehydrate_position_manager",
        AsyncMock(return_value=None),
    )

    config = make_config()
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    beta = runtime_user(USER_BETA, FUNDER_BETA, {LEADER_1: follow(USER_BETA, LEADER_1)})
    state_a = build_account_state(alpha, config)
    state_b = build_account_state(beta, config)
    ctx_a = await build_trading_context(state_a, alpha.follows[LEADER_1], config)
    ctx_b = await build_trading_context(state_b, beta.follows[LEADER_1], config)

    callback_a = ctx_a.extra["reservation_service"].reconciler_callback
    callback_b = ctx_b.extra["reservation_service"].reconciler_callback
    assert callable(callback_a) and callable(callback_b)
    assert callback_a is not callback_b

    looked_up: list[tuple[str, str]] = []

    async def _a_row(reservation_id):
        looked_up.append((USER_ALPHA, reservation_id))
        # Unresolved no-ID submission: never persisted a venue identity.
        return {"status": "submitted_unconfirmed", "order_id": None, "user_id": USER_ALPHA}

    async def _b_row(reservation_id):
        looked_up.append((USER_BETA, reservation_id))
        raise RuntimeError("persistence outage")

    ctx_a.orders_repo.get_by_reservation_id = _a_row
    ctx_b.orders_repo.get_by_reservation_id = _b_row

    active_a = SimpleNamespace(
        id="res-alpha",
        status="active",
        order_id=None,
        venue_order_id=None,
        side="BUY",
        condition_id=CONDITION,
        token_id=TOKEN,
        user_id=USER_ALPHA,
    )
    active_b = SimpleNamespace(id="res-beta", status="active", order_id=None, side="BUY")

    async def _active(reservation_id):
        return active_a if reservation_id == "res-alpha" else active_b

    ctx_a.extra["reservations_repo"].get_by_id = _active
    ctx_b.extra["reservations_repo"].get_by_id = _active

    decision_a = await callback_a("res-alpha")
    decision_b = await callback_b("res-beta")

    # Unresolved no-ID submission and failed lookup NEVER release capacity.
    assert decision_a is ReservationExpiryDecision.KEEP_PENDING_RECOVERY
    assert decision_b is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE
    # Each callback consulted only its own account's orders repository.
    assert looked_up == [(USER_ALPHA, "res-alpha"), (USER_BETA, "res-beta")]
