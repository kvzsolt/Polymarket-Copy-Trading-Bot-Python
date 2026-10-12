"""Automatic runtime admission keeps account scopes and exits separate."""

from types import SimpleNamespace
from unittest.mock import AsyncMock


from pmcopy.domain.auto_trader import AutoRoster, AutoTraderScope, Trader, TraderStatus
from pmcopy.orchestrators.auto_trader_runtime import AutoTraderRuntime
from tests.test_auto_trader_service import MemoryRepository, service, A, B, SCOPE


async def test_only_selected_scope_may_buy_and_draining_scope_may_sell():
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(A, status=TraderStatus.ACTIVE),
                Trader(B, status=TraderStatus.DRAINING),
            ),
        )
    )
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)})
    assert await runtime.admit("follower", A, "BUY")
    assert not await runtime.admit("follower", B, "BUY")
    assert await runtime.admit("follower", B, "SELL")
    assert not await runtime.admit("another-follower", A, "BUY")


async def test_database_error_blocks_auto_admission():
    repo = MemoryRepository()
    repo.load = AsyncMock(side_effect=ConnectionError())
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)})
    assert not await runtime.admit("follower", A, "BUY")


async def test_runtime_restores_both_active_and_paper_cohort_without_discovery():
    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE), Trader(B)))
    )
    manager = service(repo)
    runtime = AutoTraderRuntime(services={SCOPE: manager})
    assert await runtime.startup() == [A, B]
    assert await runtime.paper_wallets("follower") == (B,)


async def test_context_tiers_resolve_from_the_correct_automatic_account():
    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE), Trader(B)))
    )
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)})
    view = runtime.for_account("follower")
    assert (await view.get_tier(A)).follow_tier == "live_follow"
    assert (await view.get_tier(B)).follow_tier == "paper_follow"
    assert (await view.get_roster(A)).wallet == A


def test_paper_qualification_defaults_are_configurable_without_another_roster_count():
    from tests.config_helpers import load_test_config

    config = load_test_config(discovery={"auto_follow": {"paper_days": 3, "minimum_episodes": 10}})
    assert config.discovery.auto_follow.paper_days == 3
    assert config.discovery.auto_follow.minimum_episodes == 10
    assert config.discovery.scheduler.max_active_leaders == 3


async def test_falcon_adapter_retains_explicit_rejections_without_sweeping_empty_targets():
    from pmcopy.orchestrators.auto_trader_runtime import FalconEvidenceProvider
    from datetime import UTC, datetime

    source = SimpleNamespace(
        run=AsyncMock(),
        last_cycle_degraded=False,
        last_cycle_id="cycle-1",
        last_evaluations=(
            SimpleNamespace(wallet=A, score=0.7, rejected_reason=None),
            SimpleNamespace(wallet=B, score=0.0, rejected_reason="bad_quality"),
        ),
    )
    provider = FalconEvidenceProvider(
        source=source, clock=lambda: datetime(2026, 9, 16, tzinfo=UTC)
    )
    assert await provider.evaluate(()) == ()
    source.run.assert_not_called()
    rows = await provider.evaluate((A, B))
    assert rows[0].accepted and not rows[1].accepted
    source.run.assert_awaited_once_with(wallets=(A, B))


async def test_explicit_paper_mode_evaluates_active_roster_without_private_admission():
    scope = AutoTraderScope("follower", "paper")
    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE), Trader(B)))
    )
    runtime = AutoTraderRuntime(services={scope: service(repo)})
    assert await runtime.paper_wallets("follower") == (A, B)
    assert not await runtime.admit("follower", A, "BUY")


async def test_provider_block_persists_across_runtime_restart_but_allows_owned_exit():
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            entry_block_reason="falcon_unavailable",
            traders=(Trader(A, status=TraderStatus.ACTIVE),),
        )
    )
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)})
    assert not await runtime.admit("follower", A, "BUY")
    assert await runtime.admit("follower", A, "SELL")


async def test_monitor_set_keeps_owned_exits_and_drops_flat_former_members():
    from tests.test_auto_trader_service import NOW

    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(A, status=TraderStatus.ACTIVE),
                Trader(B, status=TraderStatus.DRAINING, selected_at=NOW),
            ),
        )
    )
    ownership = SimpleNamespace(retained_wallets=AsyncMock(return_value={B}))
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)}, ownership=ownership)
    assert await runtime.monitored_wallets() == [A, B]
    ownership.retained_wallets.return_value = set()
    assert await runtime.monitored_wallets() == [A]
    ownership.retained_wallets.side_effect = ConnectionError()
    assert await runtime.monitored_wallets() == [A, B]


async def test_former_paper_reserve_with_inventory_retains_exit_monitor():
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(A, status=TraderStatus.ACTIVE),
                Trader(B, status=TraderStatus.DEGRADED),
            ),
        )
    )
    ownership = SimpleNamespace(retained_wallets=AsyncMock(return_value={B}))
    runtime = AutoTraderRuntime(services={SCOPE: service(repo)}, ownership=ownership)
    assert await runtime.monitored_wallets() == [A, B]


async def test_monitor_refresh_retires_flat_wallets_and_hot_adds_only_desired_cohort():
    repo = MemoryRepository(
        AutoRoster(initialized=True, traders=(Trader(A, status=TraderStatus.ACTIVE),))
    )
    runtime = AutoTraderRuntime(
        services={SCOPE: service(repo)},
        ownership=SimpleNamespace(retained_wallets=AsyncMock(return_value=set())),
    )
    monitored = {B}
    add = AsyncMock(return_value=[A])
    remove = AsyncMock()
    await runtime.refresh_monitors(
        monitored=monitored, on_new_leaders=add, on_retired_leaders=remove
    )
    add.assert_awaited_once_with([A])
    remove.assert_awaited_once_with([B])
    assert monitored == {A}


async def test_one_provider_scope_failure_does_not_skip_other_followers():
    other_scope = AutoTraderScope("other-follower", "live")
    first = SimpleNamespace(evaluate_known=AsyncMock(side_effect=ConnectionError()))
    second = SimpleNamespace(evaluate_known=AsyncMock())
    runtime = AutoTraderRuntime(services={SCOPE: first, other_scope: second})
    import pytest

    with pytest.raises(ConnectionError):
        await runtime.evaluate_known()
    second.evaluate_known.assert_awaited_once_with(other_scope)
