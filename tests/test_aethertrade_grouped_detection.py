"""Grouped source planning never invents broad feeds or drops monitored wallets."""

from __future__ import annotations

import pytest


def test_empty_roster_has_no_subscriptions() -> None:
    """An empty owner OR-filter must never become an exchange-wide feed."""
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    assert GroupedDetectionPlanner().plan(()) == ()


def test_default_mined_owner_groups_cover_more_than_one_hundred_wallets() -> None:
    """Every owner appears exactly once in bounded native-owner groups."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    policies = tuple(TraderDetectionPolicy(wallet=f"0x{number:040x}") for number in range(1, 252))
    groups = GroupedDetectionPlanner().plan(policies)
    assert [len(group.wallets) for group in groups] == [100, 100, 51]
    assert {wallet for group in groups for wallet in group.wallets} == {p.wallet for p in policies}
    for group in groups:
        params = group.subscribe_params()
        assert group.provider == "alchemy"
        assert params[0] == "logs"
        assert len(params[1]["topics"]) == 3
        assert params[1]["topics"][1] is None
        assert len(params[1]["topics"][2]) == len(group.wallets)
        assert len(params[1]["address"]) == 2


def test_pending_is_shared_and_keeps_independent_mined_coverage() -> None:
    """Missing mempool evidence cannot hide a pending trader's mined action."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    policies = tuple(
        TraderDetectionPolicy(wallet=f"0x{number:040x}", mode="pending") for number in (1, 2)
    )
    groups = GroupedDetectionPlanner(pending_enabled=True).plan(policies)
    assert len(groups) == 2
    pending = next(group for group in groups if group.kind == "pending")
    assert pending.subscribe_params()[1]["hashesOnly"] is True
    assert "fromAddress" not in pending.subscribe_params()[1]
    mined = next(group for group in groups if group.kind == "mined")
    assert set(mined.wallets) == {policy.wallet for policy in policies}
    assert len(GroupedDetectionPlanner().plan(policies)) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"mode": "polling"},
        {"provider": "another"},
        {"mode": "pending", "provider": "infura"},
        {"on_budget_exhausted": "paid"},
        {"wallet": "0x123"},
        {"enabled": "false"},
    ],
)
def test_invalid_policies_fail_closed(changes: dict[str, object]) -> None:
    """Configuration cannot activate REST, extra providers, or paid fallback."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy

    with pytest.raises(ValueError):
        TraderDetectionPolicy(**({"wallet": "0x" + "11" * 20} | changes))


def test_unverified_infura_stream_and_duplicate_wallet_policy_are_rejected() -> None:
    """A wallet gets one owner policy; Infura stream billing needs explicit proof."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    policy = TraderDetectionPolicy(wallet="0x" + "11" * 20, provider="infura")
    with pytest.raises(ValueError, match="billing"):
        GroupedDetectionPlanner().plan((policy,))
    with pytest.raises(ValueError, match="duplicate"):
        GroupedDetectionPlanner(infura_logs_billing_verified=True).plan((policy, policy))


def test_disabled_trader_is_never_subscribed() -> None:
    """Disabled desired policy cannot keep a hidden source feed alive."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    assert (
        GroupedDetectionPlanner().plan(
            (TraderDetectionPolicy(wallet="0x" + "11" * 20, enabled=False),)
        )
        == ()
    )


def test_boolean_verification_cannot_bypass_missing_infura_per_block_metering() -> None:
    """A claimed billing verification cannot implement an absent stream meter."""
    from pmcopy.domain.grouped_detection import TraderDetectionPolicy
    from pmcopy.services.grouped_detection import GroupedDetectionPlanner

    with pytest.raises(ValueError, match="billing"):
        GroupedDetectionPlanner(infura_logs_billing_verified=True).plan(
            (TraderDetectionPolicy(wallet="0x" + "11" * 20, provider="infura"),)
        )
