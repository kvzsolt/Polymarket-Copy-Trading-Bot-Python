"""Unit tests for services.discovery.position_snapshot_service.

These tests stub the data-api fetcher + an in-memory DiscoveryRepo
double so they exercise the snapshot pipeline without a real Postgres
instance. End-to-end PG verification lives in the release-gate suite
(test_postgres_migration.py spins testcontainers and exercises the
DiscoveryRepo directly).
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from domain.discovery_models import PositionSnapshotRecord
from services.discovery.position_snapshot_service import (
    SnapshotResult,
    normalise_position,
    snapshot_followed_leaders,
)


class FakeDiscoveryRepo:
    """In-memory double matching the storage.postgres.discovery_repo
    methods used by the snapshot service."""

    def __init__(self) -> None:
        self.inserted: list[tuple[PositionSnapshotRecord, str]] = []
        self.fail_for_wallet: str | None = None

    async def insert_position_snapshot(
        self, snap: PositionSnapshotRecord, *, wallet_role: str = "leader"
    ) -> int | None:
        if wallet_role not in {"leader", "own"}:
            raise ValueError(wallet_role)
        if (
            self.fail_for_wallet is not None
            and snap.trader_wallet == self.fail_for_wallet
        ):
            raise RuntimeError("simulated persistence failure")
        self.inserted.append((snap, wallet_role))
        return len(self.inserted)


# ---------------------------------------------------------------------------
# normalise_position
# ---------------------------------------------------------------------------


def test_normalise_position_happy_path() -> None:
    raw = {
        "conditionId": "0xcond",
        "tokenID": "0xtoken",
        "size": "10",
        "avgPrice": "0.4",
        "currentValue": "5",
        "curPrice": "0.5",
        "realizedPnl": "0.2",
        "title": "Will X?",
    }
    snap = normalise_position("0xWALLET", raw, snapshot_ts=1700000000)
    assert snap is not None
    assert snap.trader_wallet == "0xwallet"
    assert snap.condition_id == "0xcond"
    assert snap.token_id == "0xtoken"
    assert snap.size == Decimal("10")
    assert snap.avg_price == Decimal("0.4")
    assert snap.current_value == Decimal("5")
    assert snap.current_price == Decimal("0.5")
    assert snap.realized_pnl == Decimal("0.2")
    assert snap.snapshot_ts == 1700000000
    assert json.loads(snap.raw_json)["title"] == "Will X?"


def test_normalise_position_accepts_snake_case_keys() -> None:
    snap = normalise_position(
        "0xw",
        {
            "condition_id": "c",
            "token_id": "t",
            "size": "1",
            "avg_price": "0.5",
            "current_value": "0.5",
            "cur_price": "0.5",
            "realized_pnl": "0",
        },
        snapshot_ts=1,
    )
    assert snap is not None
    assert snap.condition_id == "c" and snap.token_id == "t"


def test_normalise_position_returns_none_without_identity() -> None:
    assert normalise_position("0xw", {}, snapshot_ts=1) is None
    assert (
        normalise_position("0xw", {"conditionId": "c"}, snapshot_ts=1) is None
    )
    assert (
        normalise_position("0xw", {"tokenID": "t"}, snapshot_ts=1) is None
    )


def test_normalise_position_defaults_missing_decimals_to_zero() -> None:
    snap = normalise_position(
        "0xw",
        {"conditionId": "c", "tokenID": "t"},
        snapshot_ts=1,
    )
    assert snap is not None
    assert snap.size == Decimal("0")
    assert snap.avg_price == Decimal("0")
    assert snap.current_value == Decimal("0")
    assert snap.current_price == Decimal("0")
    assert snap.realized_pnl == Decimal("0")


# ---------------------------------------------------------------------------
# snapshot_followed_leaders
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_persists_leader_and_own_with_correct_role() -> None:
    repo = FakeDiscoveryRepo()
    canned = {
        "0xleader1": [
            {"conditionId": "c1", "tokenID": "t1", "size": "10",
             "avgPrice": "0.5", "currentValue": "5", "curPrice": "0.5"},
        ],
        "0xown1": [
            {"conditionId": "c2", "tokenID": "t2", "size": "20",
             "avgPrice": "0.3", "currentValue": "6", "curPrice": "0.3"},
        ],
    }

    async def fetch(url: str):
        for wallet, payload in canned.items():
            if wallet in url:
                return payload
        return []

    result = await snapshot_followed_leaders(
        leader_wallets=["0xleader1"],
        own_wallets=["0xown1"],
        repo=repo,
        fetcher=fetch,
        snapshot_ts=42,
    )
    roles = [r for _, r in repo.inserted]
    assert roles == ["leader", "own"], roles
    assert len(result.leader_snapshots["0xleader1"]) == 1
    assert len(result.own_snapshots) == 1
    assert result.leaders_failed == []
    assert result.own_failed is False


@pytest.mark.asyncio
async def test_snapshot_fail_soft_on_leader_fetch_error() -> None:
    repo = FakeDiscoveryRepo()

    async def fetch(url: str):
        if "0xbad" in url:
            raise RuntimeError("simulated 503")
        return []

    result = await snapshot_followed_leaders(
        leader_wallets=["0xbad"],
        own_wallets=[],
        repo=repo,
        fetcher=fetch,
    )
    assert "0xbad" in result.leaders_failed
    assert result.leader_snapshots["0xbad"] == []
    assert repo.inserted == [], "no partial inserts on fetch failure"


@pytest.mark.asyncio
async def test_snapshot_fail_soft_on_own_fetch_error() -> None:
    repo = FakeDiscoveryRepo()

    async def fetch(url: str):
        raise RuntimeError("simulated 503")

    result = await snapshot_followed_leaders(
        leader_wallets=[],
        own_wallets=["0xownX"],
        repo=repo,
        fetcher=fetch,
    )
    assert result.own_failed is True
    assert result.own_snapshots == []


@pytest.mark.asyncio
async def test_snapshot_handles_non_list_payload() -> None:
    repo = FakeDiscoveryRepo()

    async def fetch(url: str):
        return {"error": "not a list"}

    result = await snapshot_followed_leaders(
        leader_wallets=["0xleader"],
        own_wallets=[],
        repo=repo,
        fetcher=fetch,
    )
    assert "0xleader" in result.leaders_failed
    assert repo.inserted == []


@pytest.mark.asyncio
async def test_snapshot_skips_rows_missing_identity() -> None:
    repo = FakeDiscoveryRepo()

    async def fetch(url: str):
        return [
            {"conditionId": "c", "tokenID": "t", "size": "1",
             "avgPrice": "0.5", "currentValue": "0.5"},
            {"conditionId": "c-only"},  # skipped — missing tokenID
            {"tokenID": "t-only"},  # skipped — missing conditionId
            {},  # skipped — both missing
        ]

    result = await snapshot_followed_leaders(
        leader_wallets=["0xleader"],
        own_wallets=[],
        repo=repo,
        fetcher=fetch,
    )
    assert len(result.leader_snapshots["0xleader"]) == 1
    assert len(repo.inserted) == 1


@pytest.mark.asyncio
async def test_snapshot_persist_failure_does_not_block_in_memory_result() -> None:
    """If the DB write raises, the snapshot stays in the in-memory tree
    so the banner can still render — operator-facing degradation > crash."""
    repo = FakeDiscoveryRepo()
    repo.fail_for_wallet = "0xleader"

    async def fetch(url: str):
        return [
            {"conditionId": "c", "tokenID": "t", "size": "1",
             "avgPrice": "0.5", "currentValue": "0.5"},
        ]

    result = await snapshot_followed_leaders(
        leader_wallets=["0xleader"],
        own_wallets=[],
        repo=repo,
        fetcher=fetch,
    )
    # In-memory snapshot present even though DB persist raised.
    assert len(result.leader_snapshots["0xleader"]) == 1


@pytest.mark.asyncio
async def test_snapshot_lowercases_wallet_consistently() -> None:
    repo = FakeDiscoveryRepo()

    async def fetch(url: str):
        return [
            {"conditionId": "c", "tokenID": "t", "size": "1",
             "avgPrice": "0.5", "currentValue": "0.5"},
        ]

    result = await snapshot_followed_leaders(
        leader_wallets=["0xABC123"],
        own_wallets=["0xDEF456"],
        repo=repo,
        fetcher=fetch,
    )
    # Result keys + inserted rows must all be lowercase.
    assert "0xabc123" in result.leader_snapshots
    inserted_wallets = {snap.trader_wallet for snap, _ in repo.inserted}
    assert inserted_wallets == {"0xabc123", "0xdef456"}, inserted_wallets


def test_snapshot_result_is_immutable() -> None:
    """Catch accidental mutation that would corrupt the banner mid-render."""
    result = SnapshotResult(
        leader_snapshots={}, own_snapshots=[], leaders_failed=[], own_failed=False
    )
    with pytest.raises((AttributeError, TypeError)):
        result.own_failed = True  # type: ignore[misc]
