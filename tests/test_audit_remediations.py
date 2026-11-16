"""Regression suite for project-wide concurrency, money-path, and security audit fixes."""

from __future__ import annotations

import asyncio
from decimal import Decimal
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from eth_abi import encode as abi_encode

from pmcopy.clients.alchemy_multiplex_ws import (
    AlchemyMultiplexWebSocket,
    SlotSubscription,
)
from pmcopy.clients.network.provider_transport import (
    ProviderEndpoints,
    ProviderHttpTransport,
)
from pmcopy.clients.network.rpc_fallback import _mask_url
from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from pmcopy.domain.detection import DetectionMode
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.domain.tracked_users import TrackedTraderAddresses, TrackedUserDirectory
from pmcopy.orchestrators.alchemy_detection_runtime import AlchemyDetectionRuntime
from pmcopy.services.onchain_detection.alchemy_ws_meter import AlchemyWsMeter
from pmcopy.services.onchain_detection.mined_fill_deduplicator import (
    LogEventLruDeduplicator,
)
from pmcopy.services.onchain_detection.order_filled_detector import (
    ORDER_FILLED_V2_TOPIC0,
    build_signals_from_log_for_directory,
    decode_order_filled_log,
)
from pmcopy.services.onchain_detection.pending_tx_detector import (
    build_signals_from_pending_tx_for_directory,
)
from pmcopy.services.onchain_detection.trade_detection_models import (
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
)


_LEADER_EOA = "0x" + "11" * 20
_LEADER_PROXY = "0x" + "22" * 20
_OTHER_MAKER = "0x" + "33" * 20
_EXCHANGE = POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower()


def _make_v2_log(
    *,
    maker: str,
    taker: str,
    maker_side: int,
    token_id: int,
    maker_amount: int,
    taker_amount: int,
    tx_hash: str = "0x" + "ab" * 32,
    log_index: int = 0,
    removed: bool = False,
) -> dict[str, Any]:
    data = b"".join(
        val.to_bytes(32, "big")
        for val in (maker_side, token_id, maker_amount, taker_amount, 0, 0, 0)
    )
    return {
        "address": _EXCHANGE,
        "topics": [
            "0x" + ORDER_FILLED_V2_TOPIC0.hex(),
            "0x" + "cc" * 32,
            "0x" + ("0" * 24) + maker[2:].lower(),
            "0x" + ("0" * 24) + taker[2:].lower(),
        ],
        "data": "0x" + data.hex(),
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
        "blockNumber": hex(500),
        "removed": removed,
    }


class _ControlledWs:
    """Deterministic full-duplex WebSocket stub for concurrency/backpressure tests."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.incoming: asyncio.Queue[str | None] = asyncio.Queue()
        self.closed = False
        self._sub_seq = 0

    async def __aenter__(self) -> "_ControlledWs":
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        self.closed = True
        return False

    async def send(self, payload: str) -> None:
        req = json.loads(payload)
        self.sent.append(req)
        req_id = req["id"]
        method = req["method"]
        if method == "eth_subscribe":
            self._sub_seq += 1
            reply = {"jsonrpc": "2.0", "id": req_id, "result": f"sub-{self._sub_seq}"}
        elif method == "eth_unsubscribe":
            reply = {"jsonrpc": "2.0", "id": req_id, "result": True}
        else:
            reply = {"jsonrpc": "2.0", "id": req_id, "result": None}
        await self.incoming.put(json.dumps(reply))

    def __aiter__(self) -> "_ControlledWs":
        return self

    async def __anext__(self) -> str:
        item = await self.incoming.get()
        if item is None:
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        self.closed = True
        await self.incoming.put(None)


@pytest.mark.asyncio
async def test_phase2_ws_full_queue_does_not_deadlock_breaker_unsubscribe() -> None:
    """CRITICAL-1: Saturation of _notification_queue must never block JSON-RPC acks."""
    sock = _ControlledWs()
    mode = DetectionMode.ON_CHAIN_PENDING
    slow_gate = asyncio.Event()
    ws_holder: dict[str, AlchemyMultiplexWebSocket] = {}

    async def _on_exceeded(_bytes: int) -> None:
        nonlocal mode
        mode = DetectionMode.ON_CHAIN_LOGS_ONLY
        await ws_holder["ws"].unsubscribe_slot("pending_exchange")

    meter = AlchemyWsMeter(
        max_hourly_ws_mb=Decimal("1.5"),
        mode_getter=lambda: mode,
        active_subscriptions_getter=lambda: ws_holder["ws"].active_subscription_count,
        on_hourly_limit_exceeded=_on_exceeded,
    )
    ws = AlchemyMultiplexWebSocket(
        ws_url="wss://polygon-mainnet.g.alchemy.com/v2/testkey1234567890",
        meter=meter,
        rpc_timeout_seconds=1.0,
        connect_factory=lambda _url: sock,
    )
    ws_holder["ws"] = ws

    async def _slow_callback(_msg: dict[str, Any]) -> None:
        await slow_gate.wait()

    try:
        await ws.start(
            {
                "pending_exchange": SlotSubscription(
                    params=[
                        "alchemy_pendingTransactions",
                        {"toAddress": [_EXCHANGE], "hashesOnly": False},
                    ],
                    on_message=_slow_callback,
                )
            }
        )
        await ws.wait_until_subscribed()
        # Saturate the notification queue with > 2,100 frames while _slow_callback blocks dispatch_loop.
        notif = json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "eth_subscription",
                "params": {"subscription": "sub-1", "result": {"hash": "0x1", "pad": "x" * 760}},
            }
        )
        # First frame enters _slow_callback and holds _dispatch_loop busy.
        await sock.incoming.put(notif)
        await asyncio.sleep(0.02)
        for _ in range(2060):
            await sock.incoming.put(notif)

        # Breaker must complete eth_unsubscribe within < 0.8s even while _slow_callback is still blocked!
        await asyncio.wait_for(
            self_wait_until(lambda: "pending_exchange" not in ws.active_slots),
            timeout=0.8,
        )
        assert meter.breaker_tripped is True
        assert mode is DetectionMode.ON_CHAIN_LOGS_ONLY
    finally:
        slow_gate.set()
        await ws.stop()


async def self_wait_until(predicate: Any, step: float = 0.01) -> None:
    for _ in range(500):
        if predicate():
            return
        await asyncio.sleep(step)
    raise AssertionError("predicate did not become true within bounded wait")


@pytest.mark.asyncio
async def test_phase2_logs_resubscribe_subscribes_new_before_unsubscribing_old() -> None:
    """Zero-gap atomic cutover: eth_subscribe(new) precedes eth_unsubscribe(old) on slot update."""
    sock = _ControlledWs()
    ws = AlchemyMultiplexWebSocket(
        ws_url="wss://polygon-mainnet.g.alchemy.com/v2/testkey1234567890",
        debounce_seconds=0.0,
        connect_factory=lambda _url: sock,
    )

    async def _noop(_msg: dict[str, Any]) -> None:
        return None

    try:
        await ws.start(
            {
                "logs_maker": SlotSubscription(
                    params=[
                        "logs",
                        {
                            "address": [_EXCHANGE],
                            "topics": [["0x01"], None, ["0x" + "0" * 24 + "11" * 20]],
                        },
                    ],
                    on_message=_noop,
                )
            }
        )
        await ws.wait_until_subscribed()
        sock.sent.clear()

        await ws.reconcile_now(
            {
                "logs_maker": SlotSubscription(
                    params=[
                        "logs",
                        {
                            "address": [_EXCHANGE],
                            "topics": [["0x01"], None, ["0x" + "0" * 24 + "22" * 20]],
                        },
                    ],
                    on_message=_noop,
                )
            }
        )
        methods = [m["method"] for m in sock.sent]
        assert methods == ["eth_subscribe", "eth_unsubscribe"]
    finally:
        await ws.stop()


def test_phase2_counterparty_taker_logs_suppressed_preventing_2x_double_copy() -> None:
    """CRITICAL-2: Counterparty maker logs (topics[3]=leader) must not double-emit alongside taker summary."""
    leader_a = "0x" + "aa" * 20
    leader_b = "0x" + "bb" * 20
    directory = TrackedUserDirectory.from_wallets([leader_a, leader_b])

    # Leader B buys 100 shares as TAKER from Leader A (50 shares) and untracked _OTHER_MAKER (50 shares).
    log_maker_a = decode_order_filled_log(
        _make_v2_log(
            maker=leader_a,
            taker=leader_b,
            maker_side=MAKER_SIDE_SELL,
            token_id=777,
            maker_amount=50_000_000,
            taker_amount=25_000_000,
            log_index=0,
        )
    )
    log_maker_other = decode_order_filled_log(
        _make_v2_log(
            maker=_OTHER_MAKER,
            taker=leader_b,
            maker_side=MAKER_SIDE_BUY,  # Mint match on complement token 778!
            token_id=778,
            maker_amount=25_000_000,
            taker_amount=50_000_000,
            log_index=1,
        )
    )
    log_taker_summary_b = decode_order_filled_log(
        _make_v2_log(
            maker=leader_b,
            taker=_EXCHANGE,
            maker_side=MAKER_SIDE_BUY,
            token_id=777,
            maker_amount=50_000_000,
            taker_amount=100_000_000,
            log_index=2,
        )
    )
    assert log_maker_a and log_maker_other and log_taker_summary_b

    sigs_0 = build_signals_from_log_for_directory(log_maker_a, directory, event_ts=None)
    sigs_1 = build_signals_from_log_for_directory(log_maker_other, directory, event_ts=None)
    sigs_2 = build_signals_from_log_for_directory(log_taker_summary_b, directory, event_ts=None)

    # Log 0 emits ONLY Leader A (SELL 50 shares of 777), NOT Leader B!
    assert [(s.leader_wallet, s.side, s.token_id, s.size) for s in sigs_0] == [
        (leader_a, "SELL", "777", Decimal("50"))
    ]
    # Log 1 (untracked maker Mint match) emits NOTHING (prevents bogus SELL 778 for Leader B)!
    assert sigs_1 == ()
    # Log 2 (canonical taker summary) emits Leader B's exact 100-share BUY of 777 once!
    assert [(s.leader_wallet, s.side, s.token_id, s.size) for s in sigs_2] == [
        (leader_b, "BUY", "777", Decimal("100"))
    ]


def test_phase2_removed_reorg_log_does_not_poison_lru() -> None:
    """MEDIUM-10: removed=True reorg logs must be rejected and not poison canonical logIndex."""
    dedup = LogEventLruDeduplicator(max_entries=16)
    reorg_log = _make_v2_log(
        maker=_LEADER_EOA,
        taker=_EXCHANGE,
        maker_side=MAKER_SIDE_BUY,
        token_id=1,
        maker_amount=1_000_000,
        taker_amount=2_000_000,
        log_index=3,
        removed=True,
    )
    canonical_log = _make_v2_log(
        maker=_LEADER_EOA,
        taker=_EXCHANGE,
        maker_side=MAKER_SIDE_BUY,
        token_id=1,
        maker_amount=1_000_000,
        taker_amount=2_000_000,
        log_index=3,
        removed=False,
    )
    assert dedup.is_duplicate_raw_log(reorg_log) is True
    assert dedup.is_duplicate_raw_log(canonical_log) is False
    assert dedup.is_duplicate_raw_log(canonical_log) is True


def test_phase2_pending_taker_price_improvement_uses_matched_maker_fills() -> None:
    """HIGH-4: Marketable taker BUY with $0.99 limit cap filled at $0.50 computes 100 shares @ $0.50."""
    directory = TrackedUserDirectory.from_entries(
        [
            TrackedTraderAddresses(
                leader_wallet=_LEADER_EOA,
                eoa_address=_LEADER_EOA,
                proxy_address=_LEADER_PROXY,
            )
        ]
    )
    order_tuple_type = (
        "(uint256,address,address,uint256,uint256,uint256,"
        "uint8,uint8,uint256,bytes32,bytes32,bytes)"
    )
    taker_order = (
        1,
        _LEADER_PROXY,
        _LEADER_EOA,
        999,
        99_000_000,  # worst-case $0.99 cap for 100 shares
        100_000_000,
        MAKER_SIDE_BUY,
        2,
        1700000000,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x11" * 65,
    )
    maker_order = (
        2,
        _OTHER_MAKER,
        _OTHER_MAKER,
        999,
        100_000_000,  # selling 100 shares
        50_000_000,  # for $50 USDC ($0.50/share)
        MAKER_SIDE_SELL,
        0,
        1700000000,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x22" * 65,
    )
    from pmcopy.services.onchain_detection.calldata_decoder import (
        MATCH_ORDERS_SELECTOR,
        SAFE_EXEC_TRANSACTION_SELECTOR,
    )

    encoded_args = abi_encode(
        [
            "bytes32",
            order_tuple_type,
            f"{order_tuple_type}[]",
            "uint256",
            "uint256[]",
            "uint256",
            "uint256[]",
        ],
        [b"\x00" * 32, taker_order, [maker_order], 50_000_000, [100_000_000], 0, [0]],
    )
    inner_calldata = MATCH_ORDERS_SELECTOR + encoded_args
    tx = {
        "hash": "0x" + "dd" * 32,
        "to": _EXCHANGE,
        "input": "0x" + inner_calldata.hex(),
    }
    signals = build_signals_from_pending_tx_for_directory(tx, directory, recv_ts_ms=123456)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.leader_wallet == _LEADER_EOA
    assert sig.token_holder_wallet == _LEADER_PROXY
    assert sig.side == "BUY"
    assert sig.size == Decimal("100")
    assert sig.usdc_size == Decimal("50")
    assert sig.price == Decimal("0.5")

    # Safe execTransaction wrapping the exact same matchOrders call:
    safe_args = abi_encode(
        [
            "address",
            "uint256",
            "bytes",
            "uint8",
            "uint256",
            "uint256",
            "uint256",
            "address",
            "address",
            "bytes",
        ],
        [
            _EXCHANGE,
            0,
            inner_calldata,
            0,
            0,
            0,
            0,
            "0x" + "00" * 20,
            "0x" + "00" * 20,
            b"\x33" * 65,
        ],
    )
    safe_tx = {
        "hash": "0x" + "ee" * 32,
        "to": _LEADER_PROXY,
        "input": "0x" + (SAFE_EXEC_TRANSACTION_SELECTOR + safe_args).hex(),
    }
    safe_sigs = build_signals_from_pending_tx_for_directory(safe_tx, directory, recv_ts_ms=123456)
    assert len(safe_sigs) == 1
    assert safe_sigs[0].size == Decimal("100")
    assert safe_sigs[0].price == Decimal("0.5")


@pytest.mark.asyncio
async def test_phase2_mined_log_preserves_transport_receipt_and_does_not_fabricate_block_ts() -> (
    None
):
    """MEDIUM-10: Mined logs retain TransportReceipt and leave event_ts=None without block reader."""
    directory = TrackedUserDirectory.from_entries(
        [
            TrackedTraderAddresses(
                leader_wallet=_LEADER_EOA,
                eoa_address=_LEADER_EOA,
                proxy_address=_LEADER_PROXY,
            )
        ]
    )
    emitted: list[Any] = []
    runtime = AlchemyDetectionRuntime(
        ws_url="wss://polygon-mainnet.g.alchemy.com/v2/testkey1234567890",
        mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
        active_exchange_addresses=(_EXCHANGE, POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower()),
        include_legacy_v1_exchanges=False,
        track_direct_user_txs=False,
        max_hourly_ws_mb=Decimal("250"),
        subscription_debounce_seconds=2.0,
        mempool_reservation_ttl_seconds=15.0,
        initial_directory=directory,
        directory_builder=lambda _w: directory,
        sink=AsyncMock(side_effect=lambda s: emitted.append(s)),
        condition_id_resolver=MagicMock(resolve=AsyncMock(return_value="cond-1")),
    )
    receipt = TransportReceipt(wall_timestamp_ms=1700000000000, monotonic_ns=999, boot_id="boot-1")
    raw_log = _make_v2_log(
        maker=_LEADER_PROXY,
        taker=_EXCHANGE,
        maker_side=MAKER_SIDE_SELL,
        token_id=42,
        maker_amount=10_000_000,
        taker_amount=6_000_000,
    )
    raw_log["_source_receipt"] = receipt
    await runtime._handle_log_notification(raw_log)
    assert len(emitted) == 1
    assert emitted[0].source_receipt == receipt
    assert emitted[0].event_ts is None
    assert emitted[0].token_holder_wallet == _LEADER_PROXY


@pytest.mark.asyncio
async def test_phase2_rpc_mask_url_and_reusable_injected_provider_transport() -> None:
    """CRITICAL-3 & HIGH-8: _mask_url redacts /v2/ keys and ProviderHttpTransport reuses injected transport."""
    secret = "SuperSecretAlchemyKey999"
    masked = _mask_url(f"https://polygon-mainnet.g.alchemy.com/v2/{secret}")
    assert secret not in masked
    assert masked == "https://polygon-mainnet.g.alchemy.com/v2/***"

    mock_transport = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={"jsonrpc": "2.0", "id": json.loads(req.content)["id"], "result": "0x89"},
        )
    )
    client = ProviderHttpTransport(
        ProviderEndpoints(alchemy="https://polygon-mainnet.g.alchemy.com/v2/test-key"),
        http_transport=mock_transport,
    )
    assert await client.request("alchemy", "eth_chainId", []) == "0x89"
    assert await client.request("alchemy", "eth_chainId", []) == "0x89"


# ---------------------------------------------------------------------------
# Phase 3: Money-Path, Execution, Risk & Reconciliation Concurrency Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase3_reservation_expiry_skips_locked_scope_preventing_deadlock() -> None:
    """C-01: _reconcile_expired_reservation must not self-deadlock on an already-locked scope."""
    from pmcopy.orchestrators.runtime_bootstrap import _bind_reservation_runtime
    from pmcopy.services.execution.reservation_expiry import ReservationExpiryDecision
    from pmcopy.services.positionActionCoordinator import PositionKey
    from pmcopy.services.positionCoordinator import PositionCoordinator

    coord_locks = PositionCoordinator()
    scope = PositionKey(user_id="u1", condition_id="cond-1", token_id="tok-1")
    reservation_service = MagicMock()
    reservation_obj = MagicMock(
        id="res-1",
        status="active",
        user_id="u1",
        condition_id="cond-1",
        token_id="tok-1",
        side="BUY",
        order_id="ord-1",
        venue_order_id="ord-1",
    )
    reservations_repo = MagicMock(get_by_id=AsyncMock(return_value=reservation_obj))
    reservation_service.reservations_repo = reservations_repo
    orders_repo = MagicMock(
        get_unconfirmed_buys=AsyncMock(
            return_value=[
                {
                    "order_id": "ord-1",
                    "user_id": "u1",
                    "condition_id": "cond-1",
                    "asset": "tok-1",
                    "side": "BUY",
                }
            ]
        ),
        get_unconfirmed_sells=AsyncMock(return_value=[]),
        get_unconfirmed_order_submissions=AsyncMock(return_value=[]),
        get_by_reservation_id=AsyncMock(
            return_value={
                "order_id": "ord-1",
                "user_id": "u1",
                "condition_id": "cond-1",
                "asset": "tok-1",
                "side": "BUY",
            }
        ),
        get_by_order_id=AsyncMock(return_value=None),
    )
    context = MagicMock()
    context.user_id = "u1"
    context.extra = {}
    context.position_coordinator = coord_locks
    context.orders_repo = orders_repo
    context.execution_service = MagicMock()

    exec_coord = MagicMock(
        reservation_service=reservation_service,
        reservations_repo=reservations_repo,
        leader_sell_target_service=None,
    )
    _bind_reservation_runtime(context, exec_coord)
    callback = reservation_service.reconciler_callback
    assert callable(callback)

    # Simulate trade_pipeline / risk_monitor already holding `scope` when inline sweep fires:
    async with coord_locks.acquire(scope):
        decision = await asyncio.wait_for(callback("res-1"), timeout=0.35)
    assert decision is ReservationExpiryDecision.KEEP_PENDING_RECOVERY


@pytest.mark.asyncio
async def test_phase3_cancelled_submit_persists_submitted_unconfirmed_via_shield(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """C-02: CancelledError during in-flight _submit_gateway must shield-persist submitted_unconfirmed."""
    from pmcopy.config.app_config import load_config
    from pmcopy.domain.execution import ExecutionOrderType
    from pmcopy.services.executionService import ExecutionRequest, ExecutionService

    saved_statuses: list[str] = []

    class _Repo:
        async def get_by_idempotency_key(self, _key: str) -> None:
            return None

        async def try_acquire_submission_lock(self, *_a: Any, **_kw: Any) -> bool:
            return True

        async def is_submission_locked(self, _key: str) -> bool:
            return True

        async def save_submission(self, record: Any) -> None:
            await asyncio.sleep(0)
            saved_statuses.append(str(record.status))

    in_post = asyncio.Event()

    class _Gateway:
        async def submit_marketable_order(self, _req: Any) -> Any:
            in_post.set()
            await asyncio.sleep(10.0)

    monkeypatch.setenv(
        "PMCOPY__ALCHEMY_WSS_URL",
        "wss://polygon-mainnet.g.alchemy.com/v2/testkey1234567890",
    )
    monkeypatch.setenv("PMCOPY__DATABASE__DSN", "postgresql://u:p@localhost:5432/pmcopy")
    monkeypatch.setenv("PMCOPY__WALLET__FUNDER_ADDRESS", "0x" + "aa" * 20)
    monkeypatch.setenv("PMCOPY__EXECUTION__TRADING_MODE", "paper")
    cfg = load_config(secrets_path=tmp_path / "missing.env")
    svc = ExecutionService(None, order_repo=_Repo(), config=cfg, clob_gateway=_Gateway())
    req = ExecutionRequest(
        user_id="u1",
        leader_id="0x" + "11" * 20,
        token_id="123456",
        side="BUY",
        amount=Decimal("10"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="0x" + "ab" * 32,
        condition_id="0x" + "cd" * 32,
        order_type=ExecutionOrderType.FAK,
        price_limit=Decimal("0.50"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("1"),
        neg_risk=False,
    )
    task = asyncio.create_task(svc.execute(req))
    await asyncio.wait_for(in_post.wait(), timeout=1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert saved_statuses[-1] == "submitted_unconfirmed"


@pytest.mark.asyncio
async def test_phase3_coordinator_releases_reservation_on_geoblocked_and_metadata_unavailable() -> (
    None
):
    """C-2: Pre-POST gate rejections must release account and task reservations immediately."""
    from pmcopy.services.execution.coordinator import ExecutionCoordinator, ExecutionIntent
    from pmcopy.services.executionService import ExecutionResult

    for status_code in (
        "geoblocked",
        "market_metadata_unavailable",
        "task_protection_unavailable",
        "task_counter_outcome_unavailable",
    ):
        released: list[tuple[str, str]] = []
        reservation_service = MagicMock(
            reserve_for_intent=AsyncMock(return_value=MagicMock(id="res-99")),
            release_terminal=AsyncMock(
                side_effect=lambda rid, *, reason="", _rel=released: _rel.append((rid, reason))
            ),
        )
        exec_service = MagicMock(
            execute=AsyncMock(
                return_value=ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status=status_code,
                    requested_amount=Decimal("10"),
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=status_code,
                )
            )
        )
        coord = ExecutionCoordinator(
            reservation_service=reservation_service,
            positions_repo=MagicMock(get_position=AsyncMock(return_value=None)),
            execution_service=exec_service,
        )
        res = await coord.submit_copy_trade(
            ExecutionIntent(
                user_id="u1",
                condition_id="cond-1",
                token_id="tok-1",
                side="BUY",
                size=Decimal("20"),
                price=Decimal("0.50"),
                notional_usdc=Decimal("10"),
                idempotency_key="idem-1",
                source="copy_trade",
            )
        )
        assert res.accepted is False
        assert released == [("res-99", status_code)]


def test_phase3_terminal_wss_evidence_with_partial_fill_not_dropped_as_zero_cancel() -> None:
    """C-03: _terminal_user_order_evidence must NOT collapse partial fills into no-price cancelled."""
    from pmcopy.services.execution.reconciliation_service import ReconciliationService

    partial_record = {
        "response": {
            "user_wss_order_evidence": {
                "order_id": "ord-1",
                "kind": "cancelled",
                "terminal": True,
                "size_matched": "12.5",
                "original_size": "25",
            }
        }
    }
    zero_record = {
        "response": {
            "user_wss_order_evidence": {
                "order_id": "ord-2",
                "kind": "cancelled",
                "terminal": True,
                "size_matched": "0",
                "original_size": "25",
            }
        }
    }
    assert ReconciliationService._terminal_user_order_evidence(partial_record) is None
    zero_ev = ReconciliationService._terminal_user_order_evidence(zero_record)
    assert zero_ev is not None and zero_ev["status"] == "cancelled"


@pytest.mark.asyncio
async def test_phase3_reconcile_terminal_no_fill_updates_submission_status_and_idempotent_balance_cancel() -> (
    None
):
    """C-04: Terminal no-fill must update orders submission status AND BalanceRegime.record_cancel must be idempotent."""
    from pmcopy.services.balance_regime import BalanceRegime
    from pmcopy.services.execution.reconciliation_service import ReconciliationService

    regime = BalanceRegime()
    regime.record_buy(Decimal("50"), order_id="buy-other")
    # Cancelling an unrecorded order_id twice must subtract at most once (or only recorded amount)!
    regime.record_cancel(Decimal("10"), order_id="untracked-ord-1")
    after_first = regime.daily_spent_usdc
    regime.record_cancel(Decimal("10"), order_id="untracked-ord-1")
    assert regime.daily_spent_usdc == after_first

    orders_repo = MagicMock(
        get_by_order_id=AsyncMock(return_value={"request": {"amount": "10"}, "response": {}}),
        delete_processed_trade=AsyncMock(),
        update_submission_status=AsyncMock(),
    )
    svc = ReconciliationService(
        orders_repo=orders_repo,
        positions_repo=MagicMock(),
        fills_repo=MagicMock(),
        clob_gateway=MagicMock(),
    )
    applied = await svc.reconcile_trade_record(
        {
            "order_id": "ord-nofill",
            "trade_key": "tk-1",
            "user_id": "u1",
            "condition_id": "cond-1",
            "asset": "tok-1",
        },
        "BUY",
        confirm_fill=AsyncMock(return_value={"status": "cancelled", "size_matched": "0"}),
        pnl_tracker=None,
        get_position=AsyncMock(return_value=None),
        logger=None,
        balance_regime=regime,
    )
    assert applied is True
    orders_repo.update_submission_status.assert_awaited_once()


@pytest.mark.asyncio
async def test_phase3_ccxt_maker_legs_filters_counterparty_makers_and_paper_fill_is_confirmed() -> (
    None
):
    """C-06 & C-07: _trade_legs filters other makers' legs; PaperOrderSimulator emits CONFIRMED fill."""
    from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
    from pmcopy.integrations.polymarket.models import MarketableOrderRequest
    from pmcopy.integrations.polymarket.paper_execution import PaperOrderSimulator

    own_wallet = "0x" + "aa" * 20
    other_wallet = "0x" + "bb" * 20
    trade_payload = {
        "info": {
            "trader_side": "MAKER",
            "maker_orders": [
                {
                    "order_id": "own-maker-1",
                    "maker_address": own_wallet,
                    "asset_id": "101",
                    "side": "BUY",
                    "matched_amount": "15",
                    "price": "0.40",
                },
                {
                    "order_id": "other-maker-2",
                    "maker_address": other_wallet,
                    "asset_id": "101",
                    "side": "BUY",
                    "matched_amount": "85",
                    "price": "0.40",
                },
            ],
        }
    }
    legs = list(CcxtPredictionGateway._trade_legs(trade_payload, wallet_address=own_wallet))
    assert [leg["order_id"] for leg in legs] == ["own-maker-1"]

    sim = PaperOrderSimulator()
    res = await sim.submit_marketable_order(
        MarketableOrderRequest(
            token_id="101",
            side="BUY",
            size=Decimal("10"),
            price_limit=Decimal("0.50"),
            condition_id="cond-1",
            tick_size=Decimal("0.01"),
            neg_risk=False,
        )
    )
    assert res.success is True
    fills = await sim.get_fills(order_id=res.order_id)
    assert len(fills) == 1
    assert fills[0].status == "CONFIRMED"


@pytest.mark.asyncio
async def test_phase3_cross_source_claim_serializes_concurrent_same_tx_siblings() -> None:
    """C-05: Concurrent sibling fills of the same tx must not return winner=unknown on races."""
    from pmcopy.orchestrators.onchain_signal_dispatch import CrossSourceTradeClaimPolicy

    in_flight_keys: set[str] = set()
    committed_keys: dict[str, str] = {}

    class _SlowClaimRepo:
        async def try_claim_processed_trade(
            self, *, trade_key: str, status: str, **_kw: Any
        ) -> bool:
            if trade_key in in_flight_keys or trade_key in committed_keys:
                return False
            in_flight_keys.add(trade_key)
            await asyncio.sleep(0.02)
            committed_keys[trade_key] = status
            in_flight_keys.discard(trade_key)
            return True

        async def get_processed_trade(self, trade_key: str) -> dict[str, Any] | None:
            status = committed_keys.get(trade_key)
            return {"status": status} if status else None

        async def release_cross_source_claim(self, trade_key: str) -> bool:
            return committed_keys.pop(trade_key, None) is not None

    policy = CrossSourceTradeClaimPolicy()
    ctx = MagicMock(user_id="u1", orders_repo=_SlowClaimRepo())
    raw0 = {
        "tx_hash": "0x" + "99" * 32,
        "condition_id": "cond-1",
        "token_id": "111",
        "side": "BUY",
        "log_index": 0,
    }
    raw1 = {
        "tx_hash": "0x" + "99" * 32,
        "condition_id": "cond-1",
        "token_id": "111",
        "side": "BUY",
        "log_index": 1,
    }
    d0, d1 = await asyncio.gather(
        policy.try_claim(ctx, _LEADER_EOA, raw0, source=DetectionMode.ON_CHAIN_LOGS_ONLY),
        policy.try_claim(ctx, _LEADER_EOA, raw1, source=DetectionMode.ON_CHAIN_LOGS_ONLY),
    )
    assert d0.dispatch is True
    assert d1.dispatch is True


# ---------------------------------------------------------------------------
# Phase 4: Supervisor, Copy Tasks v1, Storage & CLI Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_phase4_copy_task_buy_on_sells_constructs_sizing_and_enforces_freshness() -> None:
    """H-5: buy_on_sells=True on a leader SELL must build BUY sizing and check signal freshness."""
    from datetime import datetime, timedelta, timezone

    from pmcopy.domain.detection import DetectionFinality, LeaderTradeEvent, TradeSide
    from pmcopy.services.copy_tasks.domain import CopyTask, MonitoringMode, ReactionPolicy
    from pmcopy.services.copy_tasks.event_routing import (
        CopyTaskEventProcessor,
        CopyTaskSourceEvent,
        FollowerTaskSnapshot,
    )

    now = datetime(2026, 11, 16, 12, 0, 0, tzinfo=timezone.utc)
    stale_time = now - timedelta(seconds=120)
    stale_sell_trade = LeaderTradeEvent(
        mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
        finality=DetectionFinality.CONFIRMED,
        leader_wallet=_LEADER_EOA,
        condition_id="0x" + "ab" * 32,
        token_id="999",
        side=TradeSide.SELL,
        shares=Decimal("50"),
        price=Decimal("0.40"),
        notional=Decimal("20"),
        transaction_hash="0x" + "cd" * 32,
        detected_at=now,
        venue_timestamp=stale_time,
    )
    task = MagicMock(spec=CopyTask)
    task.task_id = "task-1"
    task.follower_account = "u1"
    task.leader_wallet = _LEADER_EOA
    task.enabled = True
    task.monitoring_mode = MonitoringMode.NORMAL
    task.reaction = ReactionPolicy(follow_buys=True, copy_sells=False, buy_on_sells=True)

    snapshot = FollowerTaskSnapshot(
        follower_pusd_balance=Decimal("1000"),
        follower_reserved_notional=Decimal("0"),
        follower_market_exposure=Decimal("0"),
        current_open_positions=0,
        is_existing_market=False,
        execution_token_id="999",
        execution_reference_price=Decimal("0.40"),
        sell_shares=None,  # Follower holds NO shares yet because this is a counter-entry BUY!
    )
    captured_sizing: list[Any] = []

    async def _coord_process(**kwargs: Any) -> Any:
        captured_sizing.append(kwargs.get("sizing_request"))
        return MagicMock(allowed=True, reason="ok")

    processor = CopyTaskEventProcessor(
        task_reader=MagicMock(list_active=AsyncMock(return_value=[task])),
        task_budgets=MagicMock(
            snapshot=AsyncMock(
                return_value=MagicMock(
                    task_total=Decimal("0"),
                    task_daily=Decimal("0"),
                    task_market=Decimal("0"),
                    task_has_entry=False,
                    global_market_has_entry=False,
                    task_has_sports_type_entry=False,
                )
            )
        ),
        coordinator=MagicMock(process=AsyncMock(side_effect=_coord_process)),
        snapshot_provider=MagicMock(snapshot_for=AsyncMock(return_value=snapshot)),
        execution=MagicMock(),
        clock=MagicMock(now=MagicMock(return_value=now)),
        max_signal_age_seconds=30.0,
    )

    # 1) Stale leader SELL converted to follower BUY must be rejected as signal_too_old:
    stale_res = await processor.process(
        _LEADER_EOA,
        CopyTaskSourceEvent(trade=stale_sell_trade, outcome_label="YES"),
    )
    assert stale_res[0].allowed is False
    assert stale_res[0].reason == "signal_too_old"

    # 2) Fresh leader SELL converted to follower BUY must NOT fail with follower_sell_shares_unavailable
    #    and MUST pass a populated CopySizingRequest to coordinator.process:
    fresh_sell_trade = LeaderTradeEvent(
        mode=DetectionMode.ON_CHAIN_LOGS_ONLY,
        finality=DetectionFinality.CONFIRMED,
        leader_wallet=_LEADER_EOA,
        condition_id="0x" + "ab" * 32,
        token_id="999",
        side=TradeSide.SELL,
        shares=Decimal("50"),
        price=Decimal("0.40"),
        notional=Decimal("20"),
        transaction_hash="0x" + "ef" * 32,
        detected_at=now,
        venue_timestamp=now - timedelta(seconds=2),
    )
    fresh_res = await processor.process(
        _LEADER_EOA,
        CopyTaskSourceEvent(trade=fresh_sell_trade, outcome_label="YES"),
    )
    assert fresh_res[0].allowed is True
    assert len(captured_sizing) == 1
    assert captured_sizing[0] is not None


@pytest.mark.asyncio
async def test_phase4_leader_worker_supervisor_retires_idle_leader_freeing_slot() -> None:
    """M-4: LeaderWorkerSupervisor.retire_leader drains and removes a retired leader slot."""
    from pmcopy.services.copy_tasks.worker import LeaderWorkerSupervisor

    handled: list[tuple[str, str]] = []

    async def _handler(leader: str, item: str) -> None:
        handled.append((leader, item))

    sup: LeaderWorkerSupervisor[str] = LeaderWorkerSupervisor(
        handler=_handler,
        queue_size=4,
        max_concurrent_leaders=1,
        max_leaders=1,
    )
    await sup.start()
    try:
        await sup.submit("0xleader1", "ev1")
        await asyncio.sleep(0.02)
        assert sup.worker_count == 1
        retired = await sup.retire_leader("0xleader1")
        assert retired is True
        assert sup.worker_count == 0
        # Slot is now free for a newly promoted leader even with max_leaders=1!
        await sup.submit("0xleader2", "ev2")
        await asyncio.sleep(0.02)
        assert handled == [("0xleader1", "ev1"), ("0xleader2", "ev2")]
    finally:
        await sup.close()


@pytest.mark.asyncio
async def test_phase4_reservations_cli_releases_terminal_evidence_rows() -> None:
    """CLI parity: _demand_submission_evidence allows release on RELEASE_TERMINAL_EVIDENCE."""
    from pmcopy.bot_cli.commands.reservations import _demand_submission_evidence

    row = {"id": "res-term-1", "classification": "safe_to_release", "action": "release"}
    orders_repo = MagicMock(
        get_by_reservation_id=AsyncMock(
            return_value={"idempotency_key": "k1", "status": "confirmed_absent", "order_id": None}
        )
    )
    out = await _demand_submission_evidence(row, orders_repo)
    assert out["action"] == "release"
    assert out["classification"] == "safe_to_release"


def test_phase4_multi_user_config_rejects_case_colliding_copier_ids() -> None:
    """Case-insensitive copier ID collision check prevents silent roster overwrites."""
    from pydantic import SecretStr

    from pmcopy.config.app_config import SignatureType, WalletConfig
    from pmcopy.services.multiUserConfig import RuntimeUser, _reject_duplicate_users

    u1 = RuntimeUser(
        user_id="Alpha",
        wallet=WalletConfig(
            private_key=SecretStr(""),
            chain_id=137,
            signature_type=SignatureType.DEPOSIT_WALLET,
            funder_address="0x" + "11" * 20,
        ),
    )
    u2 = RuntimeUser(
        user_id="alpha",
        wallet=WalletConfig(
            private_key=SecretStr(""),
            chain_id=137,
            signature_type=SignatureType.DEPOSIT_WALLET,
            funder_address="0x" + "22" * 20,
        ),
    )
    with pytest.raises(ValueError, match="duplicate"):
        _reject_duplicate_users([u1, u2])


def test_phase4_account_lease_renew_sql_requires_unexpired_lease() -> None:
    """Expired leases must not be silently resurrected via _RENEW_SQL without takeover audit."""
    from pmcopy.storage.postgres.account_lease_repo import AccountLeaseRepo

    assert "expires_at > now()" in AccountLeaseRepo._RENEW_SQL


@pytest.mark.asyncio
async def test_phase4_resolve_cli_supports_no_order_id_confirmed_absent(tmp_path: Any) -> None:
    """resolve --idempotency-key --apply --status confirmed_absent works on no-ID rows."""
    import argparse

    from pmcopy.bot_cli.commands.resolve import ResolveCommand

    emitted: list[str] = []
    orders_repo = MagicMock(
        get_by_idempotency_key=AsyncMock(
            return_value={
                "idempotency_key": "idem-noid-1",
                "order_id": None,
                "user_id": "u1",
                "status": "submitted_unconfirmed",
                "request": {"side": "BUY", "token_id": "101", "condition_id": "cond-1"},
                "response": {},
            }
        ),
        update_submission_status_by_idempotency_key=AsyncMock(),
        delete_processed_trade=AsyncMock(),
    )
    reservations_repo = MagicMock(get_active_by_order_id=AsyncMock(return_value=None))
    cmd = ResolveCommand(
        orders_repo,
        reservations_repo,
        backup_dir=str(tmp_path),
        emit=emitted.append,
    )
    rc = await cmd.run(
        argparse.Namespace(
            order_id=None,
            idempotency_key="idem-noid-1",
            user_id="u1",
            apply=True,
            status="confirmed_absent",
            size_matched=None,
            avg_price=None,
            note="Verified zero orders/fills on Polymarket CLOB",
        )
    )
    assert rc == 0
    payload = json.loads(emitted[-1])
    assert payload["applied"] is True
    assert payload["status"] == "manually_confirmed_absent"
