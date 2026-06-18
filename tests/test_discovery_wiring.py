"""Discovery refresh is DB-only and must not start monitors."""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_leaders_repo(tiers: dict[str, list[str]]):
    repo = MagicMock()
    repo.get_leaders_by_tier = AsyncMock(side_effect=lambda t: list(tiers.get(t, [])))
    return repo


class _FakeRegistry:
    def __init__(self):
        self.refresh_count = 0

    async def refresh(self, _leaders):
        self.refresh_count += 1


def _dynamic_config(*, enabled: bool):
    return SimpleNamespace(
        wallet=SimpleNamespace(funder_address="0x" + "f" * 40),
        sizing=SimpleNamespace(
            mode="proportional",
            percentage=2,
            fixed_amount=3,
            buy_min_amount=6,
            buy_max_amount=15,
            trade_multiplier=1,
        ),
        execution=SimpleNamespace(max_position_size_usdc=0, min_balance_headroom_usdc=0),
        risk=SimpleNamespace(max_open_positions=40),
        discovery=SimpleNamespace(
            discovery_dynamic_monitoring_enabled=enabled,
            max_auto_traders_count=2,
            discovery_min_copyable_grouped_signal_ratio=0.75,
            discovery_min_copyable_notional_ratio=0.60,
            discovery_min_group_notional_usdc=5,
            discovery_min_auto_sports_esports_ratio=0.75,
            discovery_auto_prefer_sports_esports=True,
            discovery_evaluation_ttl_seconds=3600,
            discovery_reject_stale_evaluations=False,
            discovery_require_7d_non_negative=True,
            discovery_min_7d_pnl_usdc=0.0,
            leader_score_version="leader_score_v2",
            auto_leader_require_score_version=True,
        ),
        leader=SimpleNamespace(auto_discover_min_score=0.45),
    )


class _DynamicRegistry(_FakeRegistry):
    def __init__(self, *, enabled: bool):
        super().__init__()
        self.config = _dynamic_config(enabled=enabled)


def _eval_row(wallet: str):
    return SimpleNamespace(
        wallet=wallet,
        status="live_eligible",
        eligible_live=True,
        rejection_reason=None,
        final_score=0.80,
        total_volume_15d=1000.0,
        score_version="leader_score_v2",
        copyable_signal_ratio=0.90,
        copyable_notional_ratio=0.85,
        sports_esports_ratio=0.90,
        total_grouped_signals=10,
        pnl_7d=Decimal("1"),
        updated_at=9_999_999_999.0,
        config_hash="",
    )


@pytest.mark.asyncio
async def test_discovery_loop_does_not_callback_for_tier_leaders():
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    repo = _make_leaders_repo({"live_follow": ["0xAAA"], "paper_follow": ["0xBBB"]})
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xAAA"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert registry.refresh_count > 0
    assert repo.get_leaders_by_tier.call_count == 0
    assert callback.call_count == 0


@pytest.mark.asyncio
async def test_discovery_loop_refresh_exception_resilience():
    from app import _discovery_refresh_loop

    registry = MagicMock()
    call_count = 0

    async def _failing_refresh(_leaders):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("API down")

    registry.refresh = _failing_refresh
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xFFF"],
            refresh_interval=0,
            leaders_repo=_make_leaders_repo({"live_follow": ["0xFFF"]}),
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.08)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert call_count >= 3
    assert callback.call_count == 0


@pytest.mark.asyncio
async def test_discovery_loop_enabled_callback_only_new_wallets():
    from orchestrators.runtime_discovery import discovery_refresh_loop

    registry = _DynamicRegistry(enabled=True)
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(
            return_value=[
                _eval_row("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                _eval_row("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ]
        )
    )
    callback = AsyncMock(return_value=["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"])
    already = {"0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}

    task = asyncio.create_task(
        discovery_refresh_loop(
            registry=registry,
            configured_leaders=[],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=already,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    callback.assert_awaited_once_with(["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"])
    assert "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in already
    assert "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" in already


@pytest.mark.asyncio
async def test_discovery_loop_disabled_dynamic_monitoring_does_not_call_callback():
    from orchestrators.runtime_discovery import discovery_refresh_loop

    registry = _DynamicRegistry(enabled=False)
    repo = SimpleNamespace(list_evaluations=AsyncMock(return_value=[_eval_row("0x" + "b" * 40)]))
    callback = AsyncMock()

    task = asyncio.create_task(
        discovery_refresh_loop(
            registry=registry,
            configured_leaders=[],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    repo.list_evaluations.assert_not_called()
    callback.assert_not_called()


@pytest.mark.asyncio
async def test_discovery_loop_callback_failure_does_not_poison_refresh():
    from orchestrators.runtime_discovery import discovery_refresh_loop

    registry = _DynamicRegistry(enabled=True)
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(return_value=[_eval_row("0x" + "c" * 40)])
    )
    callback = AsyncMock(side_effect=[
        RuntimeError("boom"),
        ["0x" + "c" * 40],
    ])

    task = asyncio.create_task(
        discovery_refresh_loop(
            registry=registry,
            configured_leaders=[],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.08)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert registry.refresh_count >= 2
    assert callback.await_count >= 2


@pytest.mark.asyncio
async def test_discovery_loop_empty_callback_result_retries_wallet():
    from orchestrators.runtime_discovery import discovery_refresh_loop

    wallet = "0x" + "d" * 40
    registry = _DynamicRegistry(enabled=True)
    repo = SimpleNamespace(
        list_evaluations=AsyncMock(return_value=[_eval_row(wallet)])
    )
    callback = AsyncMock(return_value=[])
    already: set[str] = set()

    task = asyncio.create_task(
        discovery_refresh_loop(
            registry=registry,
            configured_leaders=[],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=already,
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert callback.await_count >= 2
    assert already == set()


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
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


@pytest.mark.asyncio
async def test_get_leaders_by_tier():
    from storage.postgres.leaders_repo import LeadersRepo

    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            for wallet, tier in [
                ("0x111", "live_follow"),
                ("0x222", "paper_follow"),
                ("0x333", "watchlist"),
                ("0x444", "live_follow"),
            ]:
                await conn.execute(
                    "INSERT INTO discovered_leaders "
                    "(wallet, active, follow_tier, updated_at) "
                    "VALUES ($1, TRUE, $2, 1000.0)",
                    wallet,
                    tier,
                )

        repo = LeadersRepo(pool)
        live = await repo.get_leaders_by_tier("live_follow")
        paper = await repo.get_leaders_by_tier("paper_follow")
        watchlist = await repo.get_leaders_by_tier("watchlist")
        none = await repo.get_leaders_by_tier("nonexistent")

        assert sorted(live) == ["0x111", "0x444"]
        assert paper == ["0x222"]
        assert watchlist == ["0x333"]
        assert none == []


@pytest.mark.asyncio
async def test_discovery_repo_persists_evaluation_metadata_and_run_history():
    from services.discovery.candidate_scoring import FalconDiscoveryEvaluation
    from services.discovery.falcon_candidate_service import FalconCandidate
    from storage.postgres.discovery_repo import DiscoveryRepo

    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        evaluation = FalconDiscoveryEvaluation(
            wallet="0x" + "d" * 40,
            status="live_eligible",
            final_score=0.8,
            falcon_quality_score=0.7,
            copyability_score=0.9,
            recent_state_score=0.6,
            market_fit_score=0.5,
            h_score=80,
            roi_15d=20,
            win_rate_15d=60,
            sharpe_15d=2,
            pnl_15d=100,
            total_trades_15d=50,
            markets_traded_15d=10,
            trajectory="stable",
            total_grouped_signals=10,
            meaningful_signals=9,
            copyable_signals=8,
            copyable_signal_ratio=0.8,
            copyable_notional_ratio=0.9,
            sports_esports_ratio=0.9,
            fast_crypto_signal_ratio=0.0,
            fast_crypto_notional_ratio=0.0,
            pnl_7d=Decimal("1"),
            pnl_3d=Decimal("1"),
            rejection_reason=None,
            eligible_live=True,
            eligible_shadow=False,
            score_version="leader_score_v2",
            score_breakdown_json={"version": "leader_score_v2"},
            leaderboard_rank=12,
            falcon_tier="gold",
            total_volume_15d=12345.0,
            source_agent_id=584,
            source_sort_by="h_score",
            source_rank=12,
            all_seen_sort_modes=("h_score", "pnl"),
        )

        await repo.upsert_evaluation(evaluation)
        stored = await repo.get_evaluation(evaluation.wallet)

        assert stored is not None
        assert stored.score_version == "leader_score_v2"
        assert stored.score_breakdown_json["version"] == "leader_score_v2"
        assert stored.leaderboard_rank == 12
        assert stored.falcon_tier == "gold"
        assert stored.total_volume_15d == pytest.approx(12345.0)
        assert stored.all_seen_sort_modes == ("h_score", "pnl")

        candidate = FalconCandidate(
            wallet=evaluation.wallet,
            h_score=80,
            roi_15d=20,
            win_rate_15d=60,
            sharpe_15d=2,
            pnl_15d=100,
            total_trades_15d=50,
            markets_traded_15d=10,
            trajectory="stable",
            raw={"source": "test"},
            total_volume_15d=12345.0,
            leaderboard_rank=12,
            tier="gold",
            source_sort_by="h_score",
            source_rank=12,
        )
        await repo.create_discovery_run(
            run_id="run-1",
            started_at=1.0,
            config_hash="hash",
            falcon_sort_modes=["h_score", "pnl"],
        )
        await repo.upsert_run_candidate(
            run_id="run-1",
            candidate=candidate,
            evaluation=evaluation,
        )
        await repo.complete_discovery_run(
            run_id="run-1",
            completed_at=2.0,
            candidate_count=1,
            evaluated_count=1,
            error_count=0,
            status="completed",
        )

        async with pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM leader_discovery_runs WHERE run_id = 'run-1'"
            )
            snapshot = await conn.fetchrow(
                "SELECT * FROM leader_discovery_run_candidates WHERE run_id = 'run-1'"
            )

        assert run["status"] == "completed"
        assert run["candidate_count"] == 1
        assert snapshot["wallet"] == evaluation.wallet
        assert snapshot["final_score"] == pytest.approx(0.8)
