"""Category 4: Execution idempotency extended.

Covers:
- Duplicate signal suppression (same signal_id -> second call ignored)
- Stale lock expiry (lock from old signal_id that timed out -> new signal allowed)
- Ambiguous POST handling: timeout vs connection error flagged differently
- Concurrent submission blocking (two signals race -> only one executes)
- Order repository lock/unlock cycle
"""

from __future__ import annotations

import contextlib


import asyncio
import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.executionService import (
    ExecutionRequest,
    ExecutionService,
    _is_pending_submission_expired,
    _is_retry_blocking_existing,
    _sanitize_submission_request,
    _sanitize_submission_response,
    _truncate_text,
)
from storage.postgres.orders_repo import OrdersRepo
from utils.createClobClient import MarketMeta
from tests.conftest import make_clob_gateway
from integrations.polymarket.models import VenueSubmitResult
from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from services.market_data_service import MarketDataBundle, MarketDataSource


# ---------------------------------------------------------------------------
# Autouse fixture: reset the process-wide circuit breaker
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_circuit():
    """Reset the circuit breaker around every test."""
    import services.executionService as _es

    _es._CLOB_CIRCUIT.reset()
    yield
    _es._CLOB_CIRCUIT.reset()


def _make_exec_config(pending_ttl: int = 120) -> SimpleNamespace:
    """Minimal AppConfig-shaped object for ExecutionService tests."""
    return SimpleNamespace(
        execution=SimpleNamespace(
            min_share_price=0,
            max_share_price=0,
            pending_submission_ttl_seconds=pending_ttl,
            circuit_failure_threshold=5,
            circuit_cooldown_seconds=60,
            execution_require_fresh_book=False,
            book_spread_mode="absolute_bps",
            max_book_spread_bps=500,
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
        ),
        policy_version="test",
        raw={},
    )


def _make_mds(
    token_id: str = "t1",
    *,
    best_bid: Decimal = Decimal("0.49"),
    best_ask: Decimal = Decimal("0.51"),
    tick_size: Decimal = Decimal("0.01"),
    min_order_size: Decimal = Decimal("1"),
):
    """Return an AsyncMock MarketDataService yielding a valid bundle."""
    now = time.monotonic()
    bundle = MarketDataBundle(
        token_id=token_id,
        quote=QuoteSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=None,
            source="wss",
            monotonic_ts=now,
        ),
        book=BookSnapshot(
            token_id=token_id,
            bids={best_bid: Decimal("100")},
            asks={best_ask: Decimal("100")},
            best_bid=best_bid,
            best_ask=best_ask,
            source="wss",
            monotonic_ts=now,
        ),
        tick=TickSnapshot(
            token_id=token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=False,
            source="wss",
            monotonic_ts=now,
        ),
        source=MarketDataSource.WSS,
    )
    mds = AsyncMock()
    mds.require_fresh_data = AsyncMock(return_value=bundle)
    return mds


class _FeeSvc:
    async def get_fee_bps(self, _token_id: str) -> int:
        return 0


class _MetaSvc:
    async def get(self, token_id: str, refresh: bool = False):
        return MarketMeta(
            token_id=token_id,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0"),
            neg_risk=False,
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )


def _make_gateway() -> MagicMock:
    """Expose the canonical typed venue boundary; private SDK hooks are absent."""
    return make_clob_gateway(VenueSubmitResult(success=True, order_id="ord-1", status="submitted"))


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _make_req(**overrides) -> ExecutionRequest:
    defaults = dict(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-1",
    )
    defaults.update(overrides)
    return ExecutionRequest(**defaults)


# ---------------------------------------------------------------------------
# Duplicate signal suppression
# ---------------------------------------------------------------------------


class TestDuplicateSignalSuppression:
    @pytest.mark.asyncio
    async def test_same_signal_id_second_call_ignored(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-dup")

            result1 = await svc.execute(req)
            assert result1.accepted is True
            assert result1.order_id == "ord-1"

            result2 = await svc.execute(req)
            assert result2.status == "duplicate_ignored"
            assert result2.accepted is True

    @pytest.mark.asyncio
    async def test_different_signal_id_allowed(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )

            req1 = _make_req(signal_id="sig-A")
            req2 = _make_req(signal_id="sig-B")
            result1 = await svc.execute(req1)
            result2 = await svc.execute(req2)
            assert result1.accepted is True
            assert result2.accepted is True
            assert clob.submit_marketable_order.await_count == 2

    @pytest.mark.asyncio
    async def test_same_idempotency_key_blocks(self, tmp_path):
        """Two identical requests -> second blocked by effective-idempotency."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )

            req = _make_req(signal_id="same-key")
            await svc.execute(req)
            result2 = await svc.execute(req)
            assert result2.status == "duplicate_ignored"


# ---------------------------------------------------------------------------
# Stale lock expiry
# ---------------------------------------------------------------------------


class TestStaleLockExpiry:
    def test_expired_pending_allows_retry(self):
        """A pending submission older than TTL should be considered expired."""
        now = time.time()
        existing = {
            "status": "pending",
            "response": {
                "status": "pending",
                "pending_since": now - 200,  # 200 seconds ago
                "pending_ttl_seconds": 120,
                "pending_expires_at": now - 80,
            },
        }
        assert _is_pending_submission_expired(existing, now_ts=now) is True

    def test_not_expired_pending_blocks_retry(self):
        """A recent pending submission should block retry."""
        now = time.time()
        existing = {
            "status": "pending",
            "response": {
                "status": "pending",
                "pending_since": now - 10,
                "pending_ttl_seconds": 120,
                "pending_expires_at": now + 110,
            },
        }
        assert _is_pending_submission_expired(existing, now_ts=now) is False

    def test_terminal_status_not_expired(self):
        """A submitted order should never be considered expired."""
        existing = {
            "status": "submitted",
            "order_id": "ord-1",
        }
        assert _is_pending_submission_expired(existing) is False

    def test_no_existing_not_expired(self):
        assert _is_pending_submission_expired(None) is False

    def test_empty_existing_not_expired(self):
        assert _is_pending_submission_expired({}) is False

    def test_pending_with_no_timestamp_is_expired(self):
        """A blocking lock with no usable timestamp should expire (fail-open)."""
        existing = {"status": "pending"}
        assert _is_pending_submission_expired(existing) is True

    @pytest.mark.asyncio
    async def test_expired_submission_allows_new_execution(self, tmp_path):
        """When an existing PENDING submission is expired, a new one should proceed.

        This tests the stale-lock expiry path: a previous submission that got stuck
        in 'pending' state (no order_id) should not block future attempts once expired.
        """
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-stale")

            # Pin the effective-idempotency key so we can seed the stale row
            # under the exact key the service will compute on execute().
            key = "test-stale-idem-key"
            now = time.time()
            with patch(
                "services.executionService._effective_idempotency_key",
                return_value=key,
            ):
                await svc._save_submission(
                    idempotency_key=key,
                    request={
                        "user_id": req.user_id,
                        "leader_id": req.leader_id,
                        "token_id": req.token_id,
                        "side": "BUY",
                        "amount": str(req.amount),
                        "reason": req.reason,
                        "signal_id": req.signal_id,
                    },
                    response={
                        "success": False,
                        "status": "pending",
                        "pending_since": now - 200,
                        "pending_ttl_seconds": 120,
                        "pending_expires_at": now - 80,
                    },
                )

                # Verify the stale submission exists
                existing = await repo.get_by_idempotency_key(key)
                assert existing is not None
                assert existing.get("status") == "pending"

                # Now execute — should detect the expired pending and proceed
                result = await svc.execute(req)
            # Should NOT be duplicate_ignored — the stale lock should be expired
            assert result.status != "duplicate_ignored"
            # Should have attempted a real submission (accepted)
            assert result.accepted is True


# ---------------------------------------------------------------------------
# Ambiguous POST handling
# ---------------------------------------------------------------------------


class TestAmbiguousPostHandling:
    @pytest.mark.asyncio
    async def test_ambiguous_post_failure_returns_unconfirmed(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            clob.submit_marketable_order = AsyncMock(
                return_value=VenueSubmitResult(
                    success=False,
                    ambiguous_post_failure=True,
                    error="timeout after POST",
                )
            )
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-ambig")
            result = await svc.execute(req)
            assert result.status == "submitted_unconfirmed"
            assert result.accepted is True

    @pytest.mark.asyncio
    async def test_post_exception_after_create_flagged_ambiguous(self, tmp_path):
        """An unclassified transport failure retains reconciliation ownership."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            clob.submit_marketable_order = AsyncMock(side_effect=ConnectionError("connection lost"))
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-exc")
            result = await svc.execute(req)
            assert result.status == "submitted_unconfirmed"

    @pytest.mark.asyncio
    async def test_create_exception_unknown_is_ambiguous(self, tmp_path):
        """Issue #100: an unknown gateway exception type (here ValueError with
        a message the substring classifier doesn't recognize) must be
        treated as **ambiguous**, not terminal — the call may have already
        touched the wire before raising. Releasing the duplicate-lock on
        terminal classification was the old unsafe behavior; the safe
        outcome is ``submitted_unconfirmed`` so reconciliation owns the
        resolution."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            clob.submit_marketable_order = AsyncMock(side_effect=ValueError("bad params"))
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-create-fail")
            result = await svc.execute(req)
            assert result.status == "submitted_unconfirmed"


# ---------------------------------------------------------------------------
# Concurrent submission blocking
# ---------------------------------------------------------------------------


class TestConcurrentSubmissionBlocking:
    @pytest.mark.asyncio
    async def test_two_races_only_one_executes(self, tmp_path):
        """Two concurrent requests with same signal -> only one order posted."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)

            started = asyncio.Event()
            proceed = asyncio.Event()

            async def submit(request):
                started.set()
                await proceed.wait()
                return VenueSubmitResult(success=True, order_id="ord-race", status="submitted")

            clob = _make_gateway()
            clob.submit_marketable_order = AsyncMock(side_effect=submit)
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                clob_gateway=clob,
                config=_make_exec_config(),
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-race")

            # Launch first task that will block during the gateway submit
            task1 = asyncio.create_task(svc.execute(req))
            await started.wait()

            # Second request should be blocked (duplicate_ignored or inflight_lock)
            result2 = await svc.execute(req)
            assert result2.status in {"duplicate_ignored", "inflight_lock"}

            # Release the first task
            proceed.set()
            result1 = await task1
            assert result1.accepted is True

            # Only one gateway submission occurred
            assert clob.submit_marketable_order.await_count == 1


# ---------------------------------------------------------------------------
# Order repository lock/unlock cycle
# ---------------------------------------------------------------------------


class TestOrderRepoLockCycle:
    @pytest.mark.asyncio
    async def test_acquire_and_check_locked(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            key = "lock-key-1"
            payload = {"user_id": "u1", "token_id": "t1"}

            assert await repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
            assert await repo.is_submission_locked(key) is True

    @pytest.mark.asyncio
    async def test_second_acquire_fails(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            key = "lock-key-2"
            payload = {"user_id": "u1"}

            assert await repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
            assert await repo.try_acquire_submission_lock(key, "u1", "l1", payload) is False

    @pytest.mark.asyncio
    async def test_different_keys_both_lock(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            payload = {"user_id": "u1"}

            assert await repo.try_acquire_submission_lock("key-A", "u1", "l1", payload) is True
            assert await repo.try_acquire_submission_lock("key-B", "u1", "l1", payload) is True

    @pytest.mark.asyncio
    async def test_get_by_idempotency_key_returns_record(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            key = "lookup-key"
            payload = {"user_id": "u1", "side": "BUY"}

            await repo.try_acquire_submission_lock(key, "u1", "l1", payload)
            record = await repo.get_by_idempotency_key(key)
            assert record is not None
            assert record["idempotency_key"] == key
            assert record["status"] == "pending_lock"

    @pytest.mark.asyncio
    async def test_nonexistent_key_returns_none(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.get_by_idempotency_key("nonexistent") is None

    @pytest.mark.asyncio
    async def test_is_submission_locked_false_for_nonexistent(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            assert await repo.is_submission_locked("nonexistent") is False

    @pytest.mark.asyncio
    async def test_save_and_retrieve_submission(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)

            from storage.postgres.orders_repo import OrderSubmissionRecord

            await repo.save_submission(
                OrderSubmissionRecord(
                    idempotency_key="save-key",
                    user_id="u1",
                    leader_id=None,
                    order_id="ord-1",
                    status="submitted",
                    request_payload={"user_id": "u1", "side": "BUY", "amount": "10"},
                    response_payload={
                        "success": True,
                        "orderID": "ord-1",
                        "status": "submitted",
                    },
                )
            )
            record = await repo.get_by_idempotency_key("save-key")
            assert record is not None
            assert record["order_id"] == "ord-1"

    @pytest.mark.asyncio
    async def test_processed_trade_lifecycle(self, tmp_path):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)

            key = "trade-1"
            assert await repo.has_processed_trade(key) is False

            await repo.mark_processed_trade(
                trade_key=key,
                user_id="u1",
                leader_id="l1",
                tx_hash="0xABC",
                side="BUY",
                asset="asset-1",
                condition_id="cond-1",
                status="executed",
            )
            assert await repo.has_processed_trade(key) is True

            trade = await repo.get_processed_trade(key)
            assert trade is not None
            assert trade["status"] == "executed"
            assert trade["user_id"] == "u1"


# ---------------------------------------------------------------------------
# Submission sanitization
# ---------------------------------------------------------------------------


class TestSanitization:
    def test_truncate_text(self):
        result = _truncate_text("short", max_chars=10)
        assert result == "short"

    def test_truncate_text_long(self):
        result = _truncate_text("a" * 200, max_chars=100)
        assert len(result) < 200
        assert result.endswith("...[truncated]")

    def test_sanitize_request_filters_fields(self):
        request = {
            "user_id": "u1",
            "side": "BUY",
            "secret": "should_be_removed",
        }
        result = _sanitize_submission_request(request)
        assert "user_id" in result
        assert "secret" not in result

    def test_sanitize_response_success(self):
        response = {
            "success": True,
            "orderID": "ord-1",
            "status": "submitted",
            "effective_price": Decimal("0.4125"),
        }
        result = _sanitize_submission_response(response)
        assert result["success"] is True
        assert result["effective_price"] == "0.4125"

    def test_sanitize_response_non_dict(self):
        result = _sanitize_submission_response("error string")
        assert result["success"] is False

    def test_sanitize_response_none_values(self):
        result = _sanitize_submission_response(None)
        assert result["success"] is False


# ---------------------------------------------------------------------------
# _is_retry_blocking_existing
# ---------------------------------------------------------------------------


class TestRetryBlocking:
    def test_none_existing_not_blocking(self):
        assert _is_retry_blocking_existing(None) is False

    def test_empty_existing_not_blocking(self):
        assert _is_retry_blocking_existing({}) is False

    def test_order_id_present_is_blocking(self):
        assert _is_retry_blocking_existing({"order_id": "ord-1"}) is True

    def test_pending_status_is_blocking(self):
        now = time.time()
        existing = {
            "status": "pending",
            "response": {
                "status": "pending",
                "pending_since": now,
                "pending_expires_at": now + 120,
            },
        }
        assert _is_retry_blocking_existing(existing, now_ts=now) is True

    def test_failed_status_not_blocking(self):
        assert _is_retry_blocking_existing({"status": "failed"}) is False

    def test_expired_pending_not_blocking(self):
        now = time.time()
        existing = {
            "status": "pending",
            "response": {
                "status": "pending",
                "pending_since": now - 200,
                "pending_expires_at": now - 80,
            },
        }
        assert _is_retry_blocking_existing(existing, now_ts=now) is False


# ---------------------------------------------------------------------------
# Issue #94 + PR #105 review — when signal_id is absent, intent_id MUST be
# derived deterministically from the request shape. A per-call uuid4()
# (the previous fix) broke retry safety: a network retry generated a fresh
# key, missed the dedup row, and could POST a duplicate signed order.
# The deterministic-hash form keeps retries safe; callers that need to
# distinguish genuinely-different intents must supply their own signal_id.
# ---------------------------------------------------------------------------


class TestBlankSignalIdFallback:
    @pytest.mark.asyncio
    async def test_retry_with_blank_signal_id_collapses_to_one_submission(self, tmp_path):
        """A retry of the same request shape (signal_id absent) must collide on
        the idempotency table — i.e. the second attempt is duplicate-ignored.
        Otherwise a network-level retry could submit a duplicate signed order
        because the venue saw the first POST but the caller did not."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )

            req1 = _make_req(signal_id=None)
            req2 = _make_req(signal_id=None)

            result1 = await svc.execute(req1)
            result2 = await svc.execute(req2)

            assert result1.accepted is True
            assert result2.status == "duplicate_ignored"
            assert clob.submit_marketable_order.await_count == 1

    @pytest.mark.asyncio
    async def test_empty_string_signal_id_also_retry_safe(self, tmp_path):
        """``signal_id == ""`` and ``signal_id is None`` must both route through
        the deterministic-hash branch — neither generates a fresh per-call id."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )

            req1 = _make_req(signal_id="")
            req2 = _make_req(signal_id="")
            await svc.execute(req1)
            await svc.execute(req2)
            # Retry-safe: second call collides on dedup, venue saw only one POST.
            assert clob.submit_marketable_order.await_count == 1

    @pytest.mark.asyncio
    async def test_distinct_intents_with_blank_signal_id_must_set_signal_id(self, tmp_path):
        """When a caller genuinely needs to submit two distinct trade intents
        with the same shape in the same window, it must disambiguate via
        ``signal_id``. With distinct signal_ids both reach the venue."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )

            req1 = _make_req(signal_id="sig-A")
            req2 = _make_req(signal_id="sig-B")
            await svc.execute(req1)
            await svc.execute(req2)
            assert clob.submit_marketable_order.await_count == 2

    @pytest.mark.asyncio
    async def test_explicit_signal_id_still_collapses_duplicates(self, tmp_path):
        """The deterministic branch must NOT degrade intentional dedupe: when
        an explicit signal_id is provided, two identical-key requests still
        collapse (existing duplicate-ignored behavior unchanged)."""
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )

            req = _make_req(signal_id="sig-explicit")
            result1 = await svc.execute(req)
            result2 = await svc.execute(req)
            assert result1.accepted is True
            assert result2.status == "duplicate_ignored"
            assert clob.submit_marketable_order.await_count == 1


# ---------------------------------------------------------------------------
# Issue #95 — duplicate-order protection must be atomic. A repo without
# ``try_acquire_submission_lock`` is unsafe for live trading: two concurrent
# tasks could both observe no blocking row and both submit signed orders.
# The fix removes the non-atomic fallback and fails closed.
# ---------------------------------------------------------------------------


class _NoCASRepo:
    """OrderRepository-shaped fake WITHOUT ``try_acquire_submission_lock``.

    Mimics a misconfigured runtime where an older repo without atomic
    submission locking is wired in. ExecutionService must refuse to call
    the gateway in this case.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_by_idempotency_key(self, key: str):
        self.calls.append(f"get_by_idempotency_key:{key}")
        return None

    # Deliberately NO try_acquire_submission_lock / is_submission_locked.


class TestAtomicLockFailClosed:
    @pytest.mark.asyncio
    async def test_repo_without_atomic_lock_fails_closed_before_gateway(self, tmp_path):
        """No try_acquire_submission_lock → reject with atomic_lock_unavailable;
        the gateway must never be called."""
        clob = _make_gateway()
        repo = _NoCASRepo()
        svc = ExecutionService(
            None,
            _FeeSvc(),
            repo,
            config=_make_exec_config(),
            clob_gateway=clob,
            market_data_service=_make_mds(),
        )
        req = _make_req(signal_id="sig-no-cas")

        result = await svc.execute(req)

        assert result.accepted is False
        assert result.status == "atomic_lock_unavailable"
        assert "atomic" in (result.error or "")
        # Critical invariant: no signed order ever reached the venue.
        assert clob.submit_marketable_order.await_count == 0


class TestAtomicLockConcurrencyStress:
    @pytest.mark.asyncio
    async def test_n_concurrent_identical_intents_yield_exactly_one_submission(self, tmp_path):
        """The real Postgres ``OrdersRepo`` must serialize duplicate-identity
        submissions atomically: ``N`` simultaneous execute() calls with the
        same effective idempotency key produce exactly one venue submission.

        The prior PG-migration TOCTOU concern (multiple winners passing the
        existing-row check then all INSERTing ``pending_lock``) is closed by
        P1-g: ``try_acquire_submission_lock`` is ``INSERT ... ON CONFLICT DO
        UPDATE ... WHERE`` where the WHERE only matches an already-stale,
        never-submitted lock. Concurrent FRESH acquirers therefore fail the
        WHERE (age ≈ 0 < takeover TTL), so exactly one task wins the INSERT and
        the losers get ``inflight_lock`` / ``duplicate_ignored`` — no second
        signed order. Proven here and in the release-gate twin
        ``test_live_execution_invariants::test_concurrent_identical_intents_durable_state``.
        """
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-race")

            N = 25  # keep under the asyncio default concurrency without burning the suite
            results = await asyncio.gather(*[svc.execute(req) for _ in range(N)])

            # THE safety invariant: exactly one signed order ever reached the
            # venue across all N concurrent identical intents.
            assert clob.submit_marketable_order.await_count == 1

            # Every other task must have collapsed to a NON-submitting status —
            # never a second signed order. Two correct loser outcomes exist and
            # the split between them is timing-dependent at high concurrency:
            #   * ``duplicate_ignored`` — the loser saw the winner's persisted
            #     order row (winner already past save_submission).
            #   * ``inflight_lock`` — the loser raced in while the winner held
            #     the submission lock but had not yet persisted an order_id.
            # Both are safe (no venue POST). The earlier assertion wrongly
            # treated only ``duplicate_ignored`` as acceptable and so flagged the
            # ``inflight_lock`` losers; exactly-one-POST already proves the
            # invariant. Assert exactly one *submitting* winner and that it
            # carries the venue order_id.
            NON_SUBMITTING = {"duplicate_ignored", "inflight_lock"}
            submitting = [r for r in results if r.status not in NON_SUBMITTING]
            assert len(submitting) == 1, (
                f"exactly one task may submit; got statuses {sorted(r.status for r in submitting)}"
            )
            assert submitting[0].order_id == "ord-1"
            assert len([r for r in results if r.status in NON_SUBMITTING]) == N - 1

    @pytest.mark.asyncio
    async def test_concurrent_retries_after_expiry_yield_exactly_one_submission(self, tmp_path):
        """Post-expiry retry stays single-winner under concurrency.

        Once a prior pending lock expires, the strictly fail-closed loser path
        (P1-n) relies on ``try_acquire_submission_lock`` taking the dead lock
        over at the pending TTL. That takeover must itself serialise: N
        concurrent execute() calls that all observe the same expired lock
        produce exactly ONE venue POST. The winner's takeover resets the row to
        a fresh ``pending_lock`` (age < TTL) so every other retrier fails the
        takeover WHERE and fails closed — never a second signed order.
        """
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            clob = _make_gateway()
            svc = ExecutionService(
                None,
                _FeeSvc(),
                repo,
                config=_make_exec_config(),
                clob_gateway=clob,
                market_data_service=_make_mds(),
            )
            req = _make_req(signal_id="sig-expiry-race")
            key = "test-expiry-race-key"
            now = time.time()
            with patch(
                "services.executionService._effective_idempotency_key",
                return_value=key,
            ):
                # Seed an expired pending lock: no order_id, pending TTL elapsed.
                await svc._save_submission(
                    idempotency_key=key,
                    request={
                        "user_id": req.user_id,
                        "leader_id": req.leader_id,
                        "token_id": req.token_id,
                        "side": "BUY",
                        "amount": str(req.amount),
                        "reason": req.reason,
                        "signal_id": req.signal_id,
                    },
                    response={
                        "success": False,
                        "status": "pending",
                        "pending_since": now - 600,
                        "pending_ttl_seconds": 120,
                        "pending_expires_at": now - 480,
                    },
                )

                N = 25
                results = await asyncio.gather(*[svc.execute(req) for _ in range(N)])

            # The invariant: exactly one venue POST despite N concurrent
            # post-expiry retries against the same dead lock.
            assert clob.submit_marketable_order.await_count == 1
            NON_SUBMITTING = {"duplicate_ignored", "inflight_lock"}
            submitting = [r for r in results if r.status not in NON_SUBMITTING]
            assert len(submitting) == 1, (
                "exactly one task may submit after expiry; got "
                f"{sorted(r.status for r in submitting)}"
            )
