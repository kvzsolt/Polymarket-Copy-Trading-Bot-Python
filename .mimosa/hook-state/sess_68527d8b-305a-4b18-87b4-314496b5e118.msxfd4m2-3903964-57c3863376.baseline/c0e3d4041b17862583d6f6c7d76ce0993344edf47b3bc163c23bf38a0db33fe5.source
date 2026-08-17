"""Tests for ``orchestrators/onchain_detection_runtime.py`` (PR2)."""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal

import pytest

from config.addresses import POLYMARKET_CTF_EXCHANGE_V2_ADDRESS
from orchestrators.onchain_detection_runtime import OnchainDetectionRuntime
from services.onchain_detection.order_filled_detector import ORDER_FILLED_TOPIC0
from services.onchain_detection.pending_tx_detector import PendingTxConfig
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)

_WATCHED = "0x" + "ab" * 20
_OTHER = "0x" + "cd" * 20
_TX = "0x" + "12" * 32


class _NoopWebSocket:
    """Lifecycle-compatible socket double; unit tests inject events directly."""

    def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def _noop_websocket_factory(**_kwargs) -> _NoopWebSocket:
    return _NoopWebSocket()


def _pad32(hexstr: str) -> str:
    if hexstr.startswith("0x") or hexstr.startswith("0X"):
        hexstr = hexstr[2:]
    return "0x" + hexstr.lower().rjust(64, "0")


def _word(n: int) -> bytes:
    return n.to_bytes(32, "big")


def _build_log(
    *,
    maker: str = _WATCHED,
    taker: str = _OTHER,
    maker_side: int = 0,
    log_index: int = 0,
    tx_hash: str = _TX,
) -> dict:
    data = (
        _word(maker_side)
        + _word(12345)
        + _word(2_000_000)  # makerAmountFilled
        + _word(1_000_000)  # takerAmountFilled
        + _word(0)  # fee
        + _word(0)  # builder
        + _word(0)  # metadata
    )
    return {
        "address": POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        "topics": [
            "0x" + ORDER_FILLED_TOPIC0.hex(),
            _pad32("aa"),
            _pad32(maker),
            _pad32(taker),
        ],
        "data": "0x" + data.hex(),
        "transactionHash": tx_hash,
        "logIndex": hex(log_index),
        "blockNumber": hex(123),
    }


def _make_pending_signal(
    tx_hash: str = _TX,
    leader: str = _WATCHED,
    event_ts: int | None = None,
) -> OnchainTradeSignal:
    if event_ts is None:
        event_ts = int(time.time() * 1000)
    return OnchainTradeSignal(
        leader_wallet=leader,
        token_id="12345",
        side="BUY",
        size=Decimal("1"),
        price=Decimal("2"),
        usdc_size=Decimal("2"),
        tx_hash=tx_hash,
        confidence=DetectionConfidence.PENDING_PROBABLE,
        maker_side=0,
        watched_role="taker",
        event_ts=event_ts,
        log_index=None,
        condition_id="cond-x",
        provisional=True,
        exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        order_hash=None,
    )


def _make_mined_signal(
    tx_hash: str = _TX,
    leader: str = _WATCHED,
    log_index: int = 0,
) -> OnchainTradeSignal:
    return OnchainTradeSignal(
        leader_wallet=leader,
        token_id="12345",
        side="BUY",
        size=Decimal("1"),
        price=Decimal("2"),
        usdc_size=Decimal("2"),
        tx_hash=tx_hash,
        confidence=DetectionConfidence.MINED_AUTHORITATIVE,
        maker_side=0,
        watched_role="maker",
        event_ts=2000,
        log_index=log_index,
        condition_id="cond-x",
        provisional=False,
        exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        order_hash=None,
    )


def _make_runtime(
    dispatched: list[OnchainTradeSignal],
    *,
    enable_pending: bool = True,
    pending_config: PendingTxConfig | None = None,
) -> OnchainDetectionRuntime:
    async def sink(signal: OnchainTradeSignal) -> None:
        # Yield control briefly so SonarCloud S7503 recognises this as a
        # genuine async function (otherwise the sync append-only body
        # trips the rule).
        await asyncio.sleep(0)
        dispatched.append(signal)

    return OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://test.invalid/ws",
        signal_sink=sink,
        enable_pending=enable_pending,
        enable_mined=True,
        pending_config=pending_config or PendingTxConfig(max_signal_age_ms=10_000),
        condition_id_resolver=None,
        websocket_factory=_noop_websocket_factory,
    )


@pytest.mark.asyncio
async def test_mined_signal_dispatched_when_watched() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=3))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
        sig = dispatched[0]
        assert sig.confidence == DetectionConfidence.MINED_AUTHORITATIVE
        assert sig.log_index == 3
        assert sig.provisional is False
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_duplicate_mined_log_not_dispatched_twice() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=1))
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=1))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_same_tx_multiple_logs_each_dispatched() -> None:
    """Greptile P1 #2 re-review: one tx with N mined ``OrderFilled`` logs
    (different ``logIndex``) produces N dispatches — one per distinct
    fill. The previous tx_hash-only idempotency gate collapsed all logs
    for a tx to a single dispatch, silently dropping distinct fills.

    The ``MinedFillDeduplicator``'s ``(tx_hash, log_index)`` _mined_seen
    set still guards against re-enqueuing the SAME log on a noisy
    WebSocket reconnect; the dispatcher's per-fill ``(tx_hash,
    log_index)`` gate is what ensures each distinct fill reaches the
    sink exactly once.
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0))
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=1))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 2, (
            "one dispatch per (tx_hash, log_index) — distinct fills must not be collapsed"
        )
        log_indices = {sig.log_index for sig in dispatched}
        assert log_indices == {0, 1}
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_supersedes_pending_no_double_dispatch() -> None:
    """A pending signal for tx X is superseded by the mined signal for tx X
    under the *synchronous* ordering (existing test for the pre-race-closure
    path). The shared-tx_hash idempotency gate additionally closes the
    async interleaving race — see ``test_pending_then_mined_same_tx_dispatches_once``.
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        pending = _make_pending_signal()
        await rt._dedupe.record_pending(pending.tx_hash, pending.leader_wallet)
        await rt._enqueue(pending)
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0, tx_hash=pending.tx_hash))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
        assert dispatched[0].confidence == DetectionConfidence.MINED_AUTHORITATIVE
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_pending_then_mined_same_tx_both_dispatched() -> None:
    """Greptile P1 #2 re-review: a pending signal followed by a mined
    ``OrderFilled`` log *for the same tx_hash* dispatches BOTH signals.
    The pending is provisional (estimated from calldata); the mined is
    authoritative (from the actual ``OrderFilled`` log). The dispatcher's
    job is per-fill uniqueness (keyed on ``(tx_hash, log_index)`` for
    mined, ``tx_hash`` for pending) — cross-signal dedup is the copy
    pipeline's responsibility via ``build_trade_key`` (keyed on
    ``leader|tx_hash|side|asset``, which matches for the same fill
    regardless of provisional vs authoritative).

    The previous tx_hash-only gate collapsed these to one dispatch,
    which also collapsed distinct multi-fill mined logs — the bug this
    re-review fixes. The ``supersede_pending`` mechanism still drops
    not-yet-dispatched pending signals when the mined arrives first
    (see ``test_mined_then_pending_same_tx_dispatches_once``).
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        tx = "0x" + "ff" * 32
        # Pre-record the pending so _on_pending_tx's record_pending check passes
        assert await rt._dedupe.record_pending(tx, _WATCHED) is True
        await rt._enqueue(_make_pending_signal(tx_hash=tx))
        # Let the dispatcher pull the pending and invoke the sink.
        await asyncio.sleep(0.02)
        assert len(dispatched) == 1, "pending dispatched"
        assert dispatched[0].confidence == DetectionConfidence.PENDING_PROBABLE
        # Mined for the same tx arrives — dispatches independently
        # (different idempotency key: (tx_hash, 0) vs tx_hash). The copy
        # pipeline's build_trade_key catches the duplicate fill.
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0, tx_hash=tx))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 2, (
            "mined dispatches alongside the provisional pending — "
            "copy pipeline dedupes via build_trade_key"
        )
        assert dispatched[1].confidence == DetectionConfidence.MINED_AUTHORITATIVE
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_then_pending_same_tx_dispatches_once() -> None:
    """Symmetric Greptile P1 #2 case: if mined arrives first (the leader's
    fill is processed through the authoritative path) and a pending
    signal for the same tx arrives later, the pending must be dropped.
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        tx = "0x" + "ee" * 32
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0, tx_hash=tx))
        await asyncio.sleep(0.02)
        assert len(dispatched) == 1
        assert dispatched[0].confidence == DetectionConfidence.MINED_AUTHORITATIVE
        # Pending for same tx arrives late — must be dropped.
        assert await rt._dedupe.record_pending(tx, _WATCHED) is True
        await rt._enqueue(_make_pending_signal(tx_hash=tx))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1, (
            "pending for already-dispatched tx dropped — _enable_pending guard plus _claim_or_drop"
        )
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_dispatcher_idempotency_cap_evicts_oldest() -> None:
    """FIFO eviction at the dispatched_cap keeps the idempotency set
    bounded in long-running processes.

    Greptile P1 re-review ("Mined keys never expire"): a mined signal adds
    BOTH its log key ``(tx_hash, log_index)`` AND its leader key
    ``(tx_hash, leader)`` to the single ``_dispatched_keys`` set + FIFO.
    Both must be evicted by the cap — no unbounded auxiliary set.
    """
    dispatched: list[OnchainTradeSignal] = []

    async def sink(signal: OnchainTradeSignal) -> None:
        dispatched.append(signal)

    rt = OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://test.invalid/ws",
        signal_sink=sink,
        enable_pending=False,
        enable_mined=False,
        dispatched_cap=4,  # small cap: 2 mined signals = 4 FIFO entries
    )
    # Each mined signal adds 2 FIFO entries (log key + leader key).
    # With cap=4, 2 mined signals fill the FIFO; the 3rd evicts the 1st.
    for i in range(5):
        sig = _make_mined_signal(tx_hash=f"0x{i:064x}", log_index=i)
        first_ok = await rt._claim_or_drop(sig)
        second_ok = await rt._claim_or_drop(sig)
        assert first_ok is True, f"first arrival for {sig.tx_hash} should dispatch"
        assert second_ok is False, f"second arrival for {sig.tx_hash} should drop"
    # The set is bounded — no unbounded growth of mined-leader keys.
    snapshot = rt.dispatched_tx_hashes_snapshot()
    assert len(snapshot) <= 4, (
        f"set should be FIFO-evicted to dispatched_cap=4, got {len(snapshot)} "
        "(mined-leader keys must also be evicted, not just log keys)"
    )


@pytest.mark.asyncio
async def test_duplicate_pending_no_double_dispatch() -> None:
    """Two pending signals for the same (tx, leader) -> only the first is dispatched."""
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        pending = _make_pending_signal()
        is_new_1 = await rt._dedupe.record_pending(pending.tx_hash, pending.leader_wallet)
        await rt._enqueue(pending)
        is_new_2 = await rt._dedupe.record_pending(pending.tx_hash, pending.leader_wallet)
        assert is_new_1 is True
        assert is_new_2 is False
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_same_tx_multiple_leaders_each_dispatched() -> None:
    """Greptile P1 re-review: one pending ``matchOrders`` tx containing
    multiple watched-wallet matches (e.g. taker=walletA, maker=walletB)
    produces one dispatch *per leader*, not one per tx. The previous
    tx_hash-only pending key collapsed all matches to a single dispatch,
    silently dropping distinct leader fills.
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        tx = "0x" + "ab" * 32
        leader_a = "0x" + "11" * 20
        leader_b = "0x" + "22" * 20
        pending_a = _make_pending_signal(tx_hash=tx, leader=leader_a)
        pending_b = _make_pending_signal(tx_hash=tx, leader=leader_b)
        assert await rt._dedupe.record_pending(tx, leader_a) is True
        assert await rt._dedupe.record_pending(tx, leader_b) is True
        await rt._enqueue(pending_a)
        await rt._enqueue(pending_b)
        await asyncio.sleep(0.05)
        assert len(dispatched) == 2, (
            "one dispatch per (tx_hash, leader_wallet) — distinct leaders "
            "in the same tx must not be collapsed"
        )
        leaders = {sig.leader_wallet for sig in dispatched}
        assert leaders == {leader_a, leader_b}
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_for_one_leader_does_not_suppress_pending_for_another() -> None:
    """Greptile P1 re-review: a mined fill for leaderA must NOT suppress
    a pending signal for leaderB in the same tx. Per-leader
    ``supersede_pending`` ensures only the matching leader's pending is
    superseded.
    """
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        tx = "0x" + "cd" * 32
        leader_a = "0x" + "11" * 20
        leader_b = "0x" + "22" * 20
        # Record pending for both leaders.
        assert await rt._dedupe.record_pending(tx, leader_a) is True
        assert await rt._dedupe.record_pending(tx, leader_b) is True
        await rt._enqueue(_make_pending_signal(tx_hash=tx, leader=leader_b))
        # Mined for leaderA arrives — should supersede only leaderA's pending.
        await rt._on_mined_log(_build_log(maker=leader_a, log_index=0, tx_hash=tx))
        await asyncio.sleep(0.05)
        # leaderB's pending should still dispatch (not suppressed by leaderA's mined).
        leaders_dispatched = {sig.leader_wallet for sig in dispatched}
        assert leader_b in leaders_dispatched, (
            "leaderB's pending must not be suppressed by leaderA's mined"
        )
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_pending_not_dispatched_when_flag_off() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched, enable_pending=False)
    await rt.start()
    try:
        await rt._enqueue(_make_pending_signal())
        await asyncio.sleep(0.05)
        assert len(dispatched) == 0
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_pending_stale_checked_at_dispatch_time() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(
        dispatched,
        pending_config=PendingTxConfig(max_signal_age_ms=1),
    )
    await rt.start()
    try:
        pending = _make_pending_signal(event_ts=int(time.time() * 1000) - 10_000)
        assert await rt._dedupe.record_pending(pending.tx_hash, pending.leader_wallet)
        await rt._enqueue(pending)
        await asyncio.sleep(0.05)
        assert dispatched == []
        assert rt.dispatched_tx_hashes_snapshot() == set()
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_pending_below_required_confidence_not_dispatched() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(
        dispatched,
        pending_config=PendingTxConfig(
            max_signal_age_ms=10_000,
            confidence_required=DetectionConfidence.MINED_AUTHORITATIVE,
        ),
    )
    await rt.start()
    try:
        pending = _make_pending_signal()
        assert await rt._dedupe.record_pending(pending.tx_hash, pending.leader_wallet)
        await rt._enqueue(pending)
        await asyncio.sleep(0.05)
        assert dispatched == []
        assert rt.dispatched_tx_hashes_snapshot() == set()
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_signal_with_unwatched_wallet_not_dispatched() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_OTHER, taker="0x" + "ee" * 20))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 0
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_update_watched_wallets() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    new_wallet = "0x" + "99" * 20
    rt.update_watched_wallets({new_wallet})
    assert new_wallet in rt._watched
    assert _WATCHED not in rt._watched


@pytest.mark.asyncio
async def test_mined_log_dropped_for_unwatched_exchange_address() -> None:
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        log = _build_log(maker=_WATCHED)
        log["address"] = "0x" + "ff" * 20
        await rt._on_mined_log(log)
        await asyncio.sleep(0.05)
        assert len(dispatched) == 0
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_condition_id_resolver_invoked() -> None:
    dispatched: list[OnchainTradeSignal] = []
    resolved: list[str] = []

    async def resolver(token_id: str) -> str:
        # Yield control briefly so SonarCloud S7503 recognises this as a
        # genuine async function (otherwise the sync append-only body
        # trips the rule).
        await asyncio.sleep(0)
        resolved.append(token_id)
        return "resolved-cond"

    async def sink(signal: OnchainTradeSignal) -> None:
        await asyncio.sleep(0)
        dispatched.append(signal)

    rt = OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://test.invalid/ws",
        signal_sink=sink,
        enable_pending=True,
        enable_mined=True,
        condition_id_resolver=resolver,
        websocket_factory=_noop_websocket_factory,
    )
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0))
        await asyncio.sleep(0.05)
        assert resolved == ["12345"]
        assert len(dispatched) == 1
        assert dispatched[0].condition_id == "resolved-cond"
    finally:
        await rt.stop()
