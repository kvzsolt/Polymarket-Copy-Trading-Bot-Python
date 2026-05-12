"""Category 4: Execution idempotency extended.

Covers:
- Duplicate signal suppression (same signal_id -> second call ignored)
- Stale lock expiry (lock from old signal_id that timed out -> new signal allowed)
- Ambiguous POST handling: timeout vs connection error flagged differently
- Concurrent submission blocking (two signals race -> only one executes)
- Order repository lock/unlock cycle
"""

from __future__ import annotations

import asyncio
import sys
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.executionService import (
    ExecutionRequest,
    ExecutionResult,
    ExecutionService,
    _idempotency_key,
    _is_pending_submission_expired,
    _is_retry_blocking_existing,
    _build_pending_response,
    _sanitize_submission_request,
    _sanitize_submission_response,
    _truncate_text,
)
from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from utils.createClobClient import MarketMeta
from cache.market_state_store import BookSnapshot, QuoteSnapshot, TickSnapshot
from services.market_data_service import MarketDataBundle, MarketDataSource


# ---------------------------------------------------------------------------
# Autouse fixture: reset circuit breaker + stabilize ENV spread-gate
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_circuit():
    """Reset circuit breaker and stabilize ENV spread-gate for all tests."""
    import services.executionService as _es
    _es._CLOB_CIRCUIT.reset()
    with patch.dict("config.env.ENV", {"MAX_BOOK_SPREAD_BPS": "500"}, clear=False):
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


class _SimpleClob:
    def __init__(self):
        self.get_price = AsyncMock(return_value={"price": "0.50"})
        self.post_signed_order = AsyncMock(
            return_value={"success": True, "orderID": "ord-1", "status": "submitted"}
        )
        self.client = SimpleNamespace(
            get_order=lambda _oid: {
                "status": "filled",
                "size_matched": "5",
                "avg_price": "0.50",
            }
        )

    async def create_market_order(self, **kwargs):
        return {"signed": True}


def _prepare_db(path: Path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


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
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-dup")

        result1 = await svc.execute(req)
        assert result1.accepted is True
        assert result1.order_id == "ord-1"

        result2 = await svc.execute(req)
        assert result2.status == "duplicate_ignored"
        assert result2.accepted is True

    @pytest.mark.asyncio
    async def test_different_signal_id_allowed(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())

        req1 = _make_req(signal_id="sig-A")
        req2 = _make_req(signal_id="sig-B")
        result1 = await svc.execute(req1)
        result2 = await svc.execute(req2)
        assert result1.accepted is True
        assert result2.accepted is True
        assert clob.post_signed_order.await_count == 2

    @pytest.mark.asyncio
    async def test_same_idempotency_key_blocks(self, tmp_path):
        """Two requests producing the same idempotency key -> second blocked."""
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())

        req = _make_req(signal_id="same-key")
        key1 = _idempotency_key(req)
        key2 = _idempotency_key(req)
        assert key1 == key2

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
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-stale")

        # Simulate a stuck pending submission directly in the repo
        key = _idempotency_key(req)
        now = time.time()
        svc._save_submission(
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
        existing = repo.get_by_idempotency_key(key)
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
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        clob.post_signed_order = AsyncMock(
            return_value={
                "success": False,
                "ambiguous_post_failure": True,
                "error": "timeout after POST",
            }
        )
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-ambig")
        result = await svc.execute(req)
        assert result.status == "submitted_unconfirmed"
        assert result.accepted is True

    @pytest.mark.asyncio
    async def test_post_exception_after_create_flagged_ambiguous(self, tmp_path):
        """If create_market_order succeeds but post_signed_order throws, it's ambiguous."""
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        clob.post_signed_order = AsyncMock(side_effect=ConnectionError("connection lost"))
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-exc")
        result = await svc.execute(req)
        assert result.status == "submitted_unconfirmed"

    @pytest.mark.asyncio
    async def test_create_exception_is_normal_failure(self, tmp_path):
        """If create_market_order throws, it's NOT ambiguous — normal failure."""
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        clob = _SimpleClob()
        clob.create_market_order = AsyncMock(side_effect=ValueError("bad params"))
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-create-fail")
        result = await svc.execute(req)
        assert result.accepted is False
        assert result.status == "failed"


# ---------------------------------------------------------------------------
# Concurrent submission blocking
# ---------------------------------------------------------------------------


class TestConcurrentSubmissionBlocking:
    @pytest.mark.asyncio
    async def test_two_races_only_one_executes(self, tmp_path):
        """Two concurrent requests with same signal -> only one order posted."""
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))

        class _BlockingClob:
            def __init__(self):
                self.get_price = AsyncMock(return_value={"price": "0.50"})
                self.post_signed_order = AsyncMock(
                    return_value={"success": True, "orderID": "ord-race", "status": "submitted"}
                )
                self.client = SimpleNamespace(
                    get_order=lambda _oid: {"status": "filled", "size_matched": "5", "avg_price": "0.50"}
                )
                self.started = asyncio.Event()
                self.proceed = asyncio.Event()

            async def create_market_order(self, **kwargs):
                self.started.set()
                await self.proceed.wait()
                return {"signed": True}

        clob = _BlockingClob()
        svc = ExecutionService(clob, _MetaSvc(), _FeeSvc(), repo, config=_make_exec_config(), market_data_service=_make_mds())
        req = _make_req(signal_id="sig-race")

        # Launch first task that will block during create_market_order
        task1 = asyncio.create_task(svc.execute(req))
        await clob.started.wait()

        # Second request should be blocked (duplicate_ignored or inflight_lock)
        result2 = await svc.execute(req)
        assert result2.status in {"duplicate_ignored", "inflight_lock"}

        # Release the first task
        clob.proceed.set()
        result1 = await task1
        assert result1.accepted is True

        # Only one post_signed_order call happened
        assert clob.post_signed_order.await_count == 1


# ---------------------------------------------------------------------------
# Order repository lock/unlock cycle
# ---------------------------------------------------------------------------


class TestOrderRepoLockCycle:
    def test_acquire_and_check_locked(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        key = "lock-key-1"
        payload = {"user_id": "u1", "token_id": "t1"}

        assert repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
        assert repo.is_submission_locked(key) is True

    def test_second_acquire_fails(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        key = "lock-key-2"
        payload = {"user_id": "u1"}

        assert repo.try_acquire_submission_lock(key, "u1", "l1", payload) is True
        assert repo.try_acquire_submission_lock(key, "u1", "l1", payload) is False

    def test_different_keys_both_lock(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        payload = {"user_id": "u1"}

        assert repo.try_acquire_submission_lock("key-A", "u1", "l1", payload) is True
        assert repo.try_acquire_submission_lock("key-B", "u1", "l1", payload) is True

    def test_get_by_idempotency_key_returns_record(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        key = "lookup-key"
        payload = {"user_id": "u1", "side": "BUY"}

        repo.try_acquire_submission_lock(key, "u1", "l1", payload)
        record = repo.get_by_idempotency_key(key)
        assert record is not None
        assert record["idempotency_key"] == key
        assert record["status"] == "pending_lock"

    def test_nonexistent_key_returns_none(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        assert repo.get_by_idempotency_key("nonexistent") is None

    def test_is_submission_locked_false_for_nonexistent(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))
        assert repo.is_submission_locked("nonexistent") is False

    def test_save_and_retrieve_submission(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))

        repo.save_submission(
            idempotency_key="save-key",
            request={"user_id": "u1", "side": "BUY", "amount": "10"},
            response={"success": True, "orderID": "ord-1", "status": "submitted"},
        )
        record = repo.get_by_idempotency_key("save-key")
        assert record is not None
        assert record["order_id"] == "ord-1"

    def test_processed_trade_lifecycle(self, tmp_path):
        db_path = tmp_path / "state.db"
        _prepare_db(db_path)
        repo = OrderRepository(str(db_path))

        key = "trade-1"
        assert repo.has_processed_trade(key) is False

        repo.mark_processed_trade(
            trade_key=key,
            user_id="u1",
            leader_id="l1",
            tx_hash="0xABC",
            side="BUY",
            asset="asset-1",
            condition_id="cond-1",
            status="executed",
        )
        assert repo.has_processed_trade(key) is True

        trade = repo.get_processed_trade(key)
        assert trade is not None
        assert trade["status"] == "executed"
        assert trade["user_id"] == "u1"


# ---------------------------------------------------------------------------
# Idempotency key generation
# ---------------------------------------------------------------------------


class TestIdempotencyKey:
    def test_same_request_same_key(self):
        req = _make_req()
        assert _idempotency_key(req) == _idempotency_key(req)

    def test_different_signal_different_key(self):
        req1 = _make_req(signal_id="sig-A")
        req2 = _make_req(signal_id="sig-B")
        assert _idempotency_key(req1) != _idempotency_key(req2)

    def test_different_amount_different_key(self):
        req1 = _make_req(amount=Decimal("5"))
        req2 = _make_req(amount=Decimal("10"))
        assert _idempotency_key(req1) != _idempotency_key(req2)


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
        response = {"success": True, "orderID": "ord-1", "status": "submitted"}
        result = _sanitize_submission_response(response)
        assert result["success"] is True

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
