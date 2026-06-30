"""Tests for ``orchestrators/onchain_detection_runtime.py`` (PR2)."""
from __future__ import annotations

import asyncio
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


def _make_pending_signal(tx_hash: str = _TX, leader: str = _WATCHED) -> OnchainTradeSignal:
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
        event_ts=1000,
        log_index=None,
        condition_id="cond-x",
        provisional=True,
        exchange=POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower(),
        order_hash=None,
    )


def _make_runtime(
    dispatched: list[OnchainTradeSignal],
    *,
    enable_pending: bool = True,
) -> OnchainDetectionRuntime:
    async def sink(signal: OnchainTradeSignal) -> None:
        dispatched.append(signal)

    return OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://test.invalid/ws",
        signal_sink=sink,
        enable_pending=enable_pending,
        enable_mined=True,
        pending_config=PendingTxConfig(max_signal_age_ms=10_000),
        condition_id_resolver=None,
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
async def test_same_tx_multiple_logs_each_dispatched_once() -> None:
    """One tx, two distinct fill logs -> two distinct signals (composite key)."""
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0))
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=1))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 2
        assert {s.log_index for s in dispatched} == {0, 1}
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_mined_supersedes_pending_no_double_dispatch() -> None:
    """A pending signal for tx X is superseded by the mined signal for tx X."""
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        pending = _make_pending_signal()
        await rt._dedupe.record_pending(pending.tx_hash)
        await rt._enqueue(pending)
        await rt._on_mined_log(_build_log(maker=_WATCHED, log_index=0, tx_hash=pending.tx_hash))
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
        assert dispatched[0].confidence == DetectionConfidence.MINED_AUTHORITATIVE
    finally:
        await rt.stop()


@pytest.mark.asyncio
async def test_duplicate_pending_no_double_dispatch() -> None:
    """Two pending signals for the same tx -> only the first is dispatched."""
    dispatched: list[OnchainTradeSignal] = []
    rt = _make_runtime(dispatched)
    await rt.start()
    try:
        pending = _make_pending_signal()
        is_new_1 = await rt._dedupe.record_pending(pending.tx_hash)
        await rt._enqueue(pending)
        is_new_2 = await rt._dedupe.record_pending(pending.tx_hash)
        assert is_new_1 is True
        assert is_new_2 is False
        await asyncio.sleep(0.05)
        assert len(dispatched) == 1
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
        resolved.append(token_id)
        return "resolved-cond"

    async def sink(signal: OnchainTradeSignal) -> None:
        dispatched.append(signal)

    rt = OnchainDetectionRuntime(
        watched_wallets={_WATCHED},
        ws_url="wss://test.invalid/ws",
        signal_sink=sink,
        enable_pending=True,
        enable_mined=True,
        condition_id_resolver=resolver,
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