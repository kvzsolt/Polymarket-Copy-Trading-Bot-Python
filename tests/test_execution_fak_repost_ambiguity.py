"""C5/C5b — FAK repost leg must persist ambiguity; avg-fill must not substitute.

C5: in the FAK repost loop the submission row was written AFTER the POST
inside the same try; an exception after the wire logged and ``break``ed with
no persisted row — violating the persist-ambiguity rule and leaving an
unreconcilable live venue order. The first attempt handles ambiguity
correctly (saves ``submitted_unconfirmed`` before break); the repost leg must
do the same.

C5b: ``avg_fill_price = _response_avg_price(response) or worst_price``
substituted the limit price for an unknown average fill — the exact CLAUDE.md
violation ("Report unknown average fill price as zero/unknown. Never
substitute the order limit price and call it an average."). Unknown averages
must remain unknown (zero), not be fabricated from the slippage ceiling.
"""

from __future__ import annotations

import time
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from integrations.polymarket.models import VenueSubmitResult
from interfaces.clob_gateway import ClobGateway
from services.executionService import ExecutionRequest, ExecutionService
from services.market_data_service import MarketDataBundle, MarketDataPolicy, MarketDataSource


def _make_bundle(*, bid="0.49", ask="0.51", min_order_size="0") -> MarketDataBundle:
    now = time.monotonic()
    b = Decimal(bid) if bid else None
    a = Decimal(ask) if ask else None
    return MarketDataBundle(
        token_id="0xabc",
        quote=QuoteSnapshot(
            token_id="0xabc",
            best_bid=b,
            best_ask=a,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id="0xabc",
            bids={b: Decimal("100")} if b else {},
            asks={a: Decimal("100")} if a else {},
            best_bid=b,
            best_ask=a,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id="0xabc",
            tick_size=Decimal("0.01"),
            min_order_size=Decimal(min_order_size),
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )


def _mock_mds(*, bid="0.49", ask="0.51", min_order_size="0") -> AsyncMock:
    mds = AsyncMock()
    mds._policy = MarketDataPolicy(rest_snapshot_max_spread_bps=300)
    mds.require_fresh_data = AsyncMock(
        return_value=_make_bundle(bid=bid, ask=ask, min_order_size=min_order_size)
    )
    return mds


def _make_config():
    return SimpleNamespace(
        execution=SimpleNamespace(
            execution_require_fresh_book=False,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=5000,
            max_book_spread_ticks=0,
            min_share_price=0.0,
            max_share_price=0.0,
            slippage_tolerance=2.0,
            max_retries=3,
            retry_backoff_base=1.0,
            pending_submission_ttl_seconds=120,
            non_terminal_failure_max_attempts=3,
            transaction_fee_bps=20,
            gas_cost_usdc=0.03,
            risk_buffer_pct=0.10,
            paper_trade_only=False,
            enable_copy_decision_gate=False,
            enable_near_resolution_gate=False,
            near_resolution_gate_price=0.95,
            near_resolution_max_age_seconds=15,
            meta_cache_ttl_good=300,
            meta_cache_ttl_thin=30,
            copy_fak_max_attempts=3,
            exit_fak_max_attempts=8,
            kill_switch=False,
        ),
        global_config=SimpleNamespace(kill_switch=False),
        raw={"EXECUTION_SUBMIT_CONCURRENCY": 2},
    )


def _order_repo() -> MagicMock:
    repo = MagicMock()
    repo.get_by_idempotency_key = AsyncMock(return_value=None)
    repo.is_submission_locked = AsyncMock(return_value=False)
    repo.try_acquire_submission_lock = AsyncMock(return_value=True)
    repo.save_submission = AsyncMock()
    repo.update_submission_status = AsyncMock()
    repo.release_submission_lock = AsyncMock()
    return repo


def _req(*, side="BUY", amount="10", order_type="FAK", price_limit="0.55") -> ExecutionRequest:
    return ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id="0xabc",
        side=side,
        amount=Decimal(amount),
        max_slippage_bps=200,
        reason="copy_trade",
        order_type=order_type,
        price_limit=Decimal(price_limit),
        signal_id="sig-1",
    )


@pytest.fixture(autouse=True)
def _reset_circuit():
    import services.executionService as _es

    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


def _build_svc(*, gateway, mds=None) -> ExecutionService:
    clob = AsyncMock()
    clob.get_price = AsyncMock(return_value={"price": "0.50"})
    clob.client = SimpleNamespace(
        get_order=lambda _oid: {"status": "filled", "size_matched": "0", "avg_price": None},
    )
    return ExecutionService(
        clob,
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal("0"))),
        _order_repo(),
        config=_make_config(),
        clob_gateway=gateway,
        market_data_service=mds or _mock_mds(),
    )


@pytest.mark.asyncio
async def test_c5_repost_leg_persists_ambiguity_when_post_raises():
    """C5: a repost-leg POST that raises after hitting the wire must persist a
    ``submitted_unconfirmed`` row so the reconciler owns the in-flight order,
    not silently drop it via ``break``."""
    call = {"n": 0}

    async def submit(req):
        call["n"] += 1
        if call["n"] == 1:
            # First attempt: partial fill (3 of 10 USDC), success -> enters
            # the repost loop to fill the remaining 7.
            return VenueSubmitResult(
                success=True,
                order_id="ord-1",
                status="matched",
                raw={"size_matched": "3", "avg_price": "0.51"},
            )
        # Repost leg: raise after the wire (simulate timeout/connection drop).
        raise RuntimeError("connection reset after POST")

    gw = MagicMock(spec=ClobGateway)
    gw.submit_marketable_order = AsyncMock(side_effect=submit)
    gw.get_order = AsyncMock(return_value=None)
    svc = _build_svc(gateway=gw)

    await svc.execute(_req(amount="10"))

    # The repost leg must have persisted an ambiguity row (status contains
    # submitted_unconfirmed) even though the POST raised. The repost-leg
    # idempotency key carries the ``:fak:`` marker, distinguishing it from
    # the first-attempt success save.
    saved = svc.order_repo.save_submission.call_args_list
    repost_ambiguity = [
        c.args[0] if c.args else c.kwargs.get("record")
        for c in saved
        if c.args
        and "submitted_unconfirmed" in str(getattr(c.args[0], "status", ""))
        and ":fak:" in str(getattr(c.args[0], "idempotency_key", ""))
    ]
    assert repost_ambiguity, (
        "repost leg must persist submitted_unconfirmed on POST exception, "
        f"got records: {[(c.args[0].idempotency_key, c.args[0].status) for c in saved if c.args]}"
    )


@pytest.mark.asyncio
async def test_c5b_unknown_avg_fill_price_not_substituted_with_worst_price():
    """C5b: when the venue response omits an average fill price, the reported
    avg must remain unknown (None/0), NOT fabricated from the slippage ceiling
    ``worst_price``. Substituting the limit price biases cost basis and PnL."""
    gw = MagicMock(spec=ClobGateway)
    # First attempt fills fully but with NO avg_price in the raw response.
    gw.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-1",
            status="matched",
            raw={"size_matched": "10"},  # filled 10 shares, no avg_price
        )
    )
    gw.get_order = AsyncMock(return_value=None)
    svc = _build_svc(gateway=gw)

    result = await svc.execute(_req(amount="10"))

    assert result.accepted is True
    # Unknown average fill price must NOT be fabricated as the worst_price
    # (slippage ceiling ~0.52). It must remain None or zero/unknown.
    assert result.avg_price is None or result.avg_price == Decimal("0"), (
        f"unknown avg fill must stay unknown, got {result.avg_price}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "price_limit", "initial_bundle", "repost_bundle"),
    [
        ("BUY", "0.55", _make_bundle(ask="0.51"), _make_bundle(ask="0.60")),
        ("SELL", "0.45", _make_bundle(bid="0.49"), _make_bundle(bid="0.40")),
    ],
)
async def test_fak_repost_preserves_original_price_protection(
    side: str,
    price_limit: str,
    initial_bundle: MarketDataBundle,
    repost_bundle: MarketDataBundle,
) -> None:
    """Never let a later quote weaken the source-derived cap or floor."""

    mds = _mock_mds()
    mds.require_fresh_data.side_effect = [initial_bundle, repost_bundle]
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        side_effect=[
            VenueSubmitResult(
                success=True,
                order_id="ord-1",
                status="matched",
                raw={"size_matched": "3", "avg_price": "0.49"},
            ),
            VenueSubmitResult(
                success=False,
                status="canceled",
                error="order_not_fillable",
                non_retryable=True,
            ),
        ]
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway, mds=mds)

    await service.execute(_req(side=side, amount="10", price_limit=price_limit))

    repost = gateway.submit_marketable_order.await_args_list[1].args[0]
    assert repost.price_limit == Decimal(price_limit)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "quote", "price_limit", "expected"),
    [
        ("BUY", _make_bundle(ask="0.60"), "0.555", "0.55"),
        ("SELL", _make_bundle(bid="0.40"), "0.455", "0.46"),
    ],
)
async def test_initial_fak_price_limit_rounds_toward_protection(
    side: str,
    quote: MarketDataBundle,
    price_limit: str,
    expected: str,
) -> None:
    """Quantization must not cross a non-tick-aligned cap or floor."""

    mds = _mock_mds()
    mds.require_fresh_data.return_value = quote
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            status="canceled",
            error="order_not_fillable",
            non_retryable=True,
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway, mds=mds)

    await service.execute(_req(side=side, amount="10", price_limit=price_limit))

    submitted = gateway.submit_marketable_order.await_args.args[0]
    assert submitted.price_limit == Decimal(expected)
