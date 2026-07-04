"""2026-07-04 exit-storm incident — Defect A regression suite.

A venue "not enough balance / allowance" rejection must classify as
``insufficient_balance`` (and raise ``VenueInsufficientBalanceError``) on BOTH
submission paths, so the risk monitor's drift detector (``_is_drift_error``)
sees a phantom-position signal instead of an opaque ``VenueInvalidOrderError``
whose venue text was stripped by double redaction.

The classification split must NOT widen: min-size / tick-size / generic
validation rejections stay ``invalid_order`` (still terminal).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from integrations.polymarket.clob_v2_gateway import _classify_error, _raise_classified
from integrations.polymarket.errors import (
    TerminalVenueError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
)

_VENUE_BALANCE_MSG = (
    "not enough balance / allowance: the balance is not enough -> "
    "balance: 3214, order amount: 5560000"
)


class PolyApiException(Exception):
    """Test double matching py-clob-client-v2's exception class name."""

    def __init__(self, msg: str, status_code: int | None = None):
        super().__init__(msg)
        self.status_code = status_code


class TestInsufficientBalanceClassification:
    def test_status_400_with_balance_body(self):
        exc = PolyApiException(_VENUE_BALANCE_MSG, status_code=400)
        assert _classify_error(exc) == (False, "insufficient_balance")

    def test_message_only_without_status_code(self):
        # Status-code-less legacy shape must also hit the dedicated branch.
        assert _classify_error(Exception(_VENUE_BALANCE_MSG)) == (
            False,
            "insufficient_balance",
        )

    def test_raise_classified_raises_dedicated_subclass(self):
        with pytest.raises(VenueInsufficientBalanceError) as exc_info:
            _raise_classified(PolyApiException(_VENUE_BALANCE_MSG, status_code=400))
        # Every existing terminal handler must keep catching it.
        assert isinstance(exc_info.value, VenueInvalidOrderError)
        assert isinstance(exc_info.value, TerminalVenueError)
        assert "insufficient_balance" in str(exc_info.value)

    def test_min_size_rejection_stays_invalid_order_with_400(self):
        # The 03:26 incident shape: BUY below venue minimum — must NOT be
        # captured by the balance split (would masquerade as position drift).
        exc = PolyApiException(
            "invalid amount for a marketable BUY order ($0.52), min size: 1",
            status_code=400,
        )
        assert _classify_error(exc) == (False, "invalid_order")

    def test_min_size_rejection_stays_invalid_order_message_only(self):
        assert _classify_error(Exception("size (3) lower than the minimum: 5")) == (
            False,
            "invalid_order",
        )

    def test_tick_size_rejection_stays_invalid_order(self):
        with pytest.raises(VenueInvalidOrderError) as exc_info:
            _raise_classified(Exception("tick size does not match"))
        assert not isinstance(exc_info.value, VenueInsufficientBalanceError)


class TestExecutionServiceSeam:
    """The gateway subclass must surface as the canonical drift-detectable
    error string on ExecutionResult — mirroring the SDK-wrapper dict path
    (``insufficient_balance | {raw}``) — and burn no retries."""

    @pytest.mark.asyncio
    async def test_rejection_shape_and_single_submit(self):
        import time as _time

        from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
        from interfaces.clob_gateway import ClobGateway
        from services.executionService import ExecutionRequest, ExecutionService
        from services.market_data_service import MarketDataBundle, MarketDataSource
        from services.risk.risk_monitor import _is_drift_error

        ec = SimpleNamespace(
            min_share_price=0.0,
            max_share_price=0.0,
            pending_submission_ttl_seconds=120,
            enable_executable_order_guard=True,
            circuit_failure_threshold=3,
            circuit_cooldown_seconds=60,
            execution_require_fresh_book=False,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=5000,
            max_book_spread_ticks=0,
            slippage_tolerance=2.0,
            max_retries=3,
            retry_backoff_base=1.0,
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
        )
        config = SimpleNamespace(execution=ec, raw={"EXECUTION_SUBMIT_CONCURRENCY": 2})

        now = _time.monotonic()
        bid, ask = Decimal("0.49"), Decimal("0.51")
        bundle = MarketDataBundle(
            token_id="t1",
            quote=QuoteSnapshot(
                token_id="t1",
                best_bid=bid,
                best_ask=ask,
                last_trade_price=None,
                source="wss",
                monotonic_ts=now,
            ),
            book=BookSnapshot(
                token_id="t1",
                bids={bid: Decimal("100")},
                asks={ask: Decimal("100")},
                best_bid=bid,
                best_ask=ask,
                source="wss",
                monotonic_ts=now,
            ),
            tick=TickSnapshot(
                token_id="t1",
                tick_size=Decimal("0.01"),
                min_order_size=Decimal("1"),
                neg_risk=False,
                source="wss",
                monotonic_ts=now,
            ),
            source=MarketDataSource.WSS,
        )
        mds = AsyncMock()
        mds.require_fresh_data = AsyncMock(return_value=bundle)

        class _FeeSvc:
            async def get_fee_bps(self, _token_id: str) -> int:
                return 0

        stub_clob = SimpleNamespace(
            get_price=AsyncMock(return_value={"price": "0.50"}),
            client=SimpleNamespace(get_order=lambda _oid: {"status": "unknown"}),
        )

        repo = MagicMock()
        repo.get_by_idempotency_key = AsyncMock(return_value=None)
        repo.try_acquire_submission_lock = AsyncMock(return_value=True)
        repo.is_submission_locked = AsyncMock(return_value=False)
        repo.save_submission = AsyncMock()
        repo.update_submission_status = AsyncMock()
        repo.release_submission_lock = AsyncMock()

        gateway = MagicMock(spec=ClobGateway)
        gateway.submit_marketable_order = AsyncMock(
            side_effect=VenueInsufficientBalanceError(
                "insufficient_balance: insufficient_funds"
            )
        )
        gateway.get_order = AsyncMock()

        svc = ExecutionService(
            stub_clob,
            _FeeSvc(),
            repo,
            config=config,
            clob_gateway=gateway,
            market_data_service=mds,
        )

        result = await svc.execute(
            ExecutionRequest(
                user_id="u1",
                leader_id="l1",
                token_id="t1",
                side="SELL",
                amount=Decimal("5"),
                max_slippage_bps=100,
                reason="risk_exit",
                signal_id="sig-bal-1",
            )
        )

        assert result.accepted is False
        assert result.status == "rejected"
        assert str(result.error or "").startswith("insufficient_balance | ")
        # The whole point: the risk monitor must see a drift signal.
        assert _is_drift_error(str(result.error)) is True
        # Terminal — exactly one POST, no retry burn.
        assert gateway.submit_marketable_order.await_count == 1
