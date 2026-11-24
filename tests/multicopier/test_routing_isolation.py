"""§9 routing scenarios: whitelists against a global roster, protection
continuity, and per-account pipeline fan-out.

* A follows L1 only and B follows L2 only; neither receives the other's
  trades even when the global roster contains both leaders.
* Empty or disabled follows do not orphan existing position protection /
  reconciliation (risk-only contexts).
* One leader event fans out to exactly that leader's subscribed contexts,
  and one account's pipeline failure never blocks the other account's
  execution (the dispatcher isolates per-context failures).
* A and B following the SAME leader compose independent sizing inputs and
  independent claim/dedup identities — no cross-account deduplication by
  construction (SQL-level proof runs in ``test_multicopier_postgres.py``).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pmcopy.orchestrators.bootstrap.runtime_bootstrap import build_context_maps
from pmcopy.orchestrators.lifecycle.runtime_monitor import LeaderBatchDispatcher
from pmcopy.orchestrators.pipeline.trade_pipeline import build_trade_key
from pmcopy.services.positions.position_action_coordinator import PositionScope

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    FUNDER_BETA,
    LEADER_1,
    LEADER_2,
    TOKEN,
    USER_ALPHA,
    USER_BETA,
    build_account_state,
    follow,
    make_config,
    runtime_user,
)


def _leader_event(tx_hash: str = "0xevent-1") -> dict:
    return {
        "transactionHash": tx_hash,
        "type": "TRADE",
        "side": "BUY",
        "asset": TOKEN,
        "conditionId": CONDITION,
        "size": 10,
        "price": 0.5,
        "usdcSize": 10,
        "timestamp": 1_760_160_000,
    }


@pytest.mark.asyncio
async def test_follow_whitelists_isolate_against_a_global_roster():
    """A→L1 only, B→L2 only: the roster contains both, routing is per-follow."""
    config = make_config()
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    beta = runtime_user(USER_BETA, FUNDER_BETA, {LEADER_2: follow(USER_BETA, LEADER_2)})
    states = {
        USER_ALPHA: build_account_state(alpha, config),
        USER_BETA: build_account_state(beta, config),
    }

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        [alpha, beta], states, [LEADER_1, LEADER_2], config
    )

    # The global roster contains both leaders; only the subscribed contexts
    # are indexed for each.
    assert [ctx.user_id for ctx in contexts_by_leader[LEADER_1]] == [USER_ALPHA]
    assert [ctx.user_id for ctx in contexts_by_leader[LEADER_2]] == [USER_BETA]
    # Each account has exactly one risk context — its own.
    assert set(risk_contexts_by_user) == {USER_ALPHA, USER_BETA}
    assert risk_contexts_by_user[USER_ALPHA].user_id == USER_ALPHA
    assert risk_contexts_by_user[USER_BETA].user_id == USER_BETA

    # The dispatcher fans a leader batch out to exactly that leader's
    # contexts — B never receives L1's event and vice versa.
    executed: list[tuple[str, str]] = []

    async def executor(context, leader_id, trades, now_ts):
        executed.append((context.user_id, leader_id))
        return []

    dispatcher_l1 = LeaderBatchDispatcher(
        leader=LEADER_1,
        contexts=contexts_by_leader[LEADER_1],
        legacy_executor=executor,
    )
    dispatcher_l2 = LeaderBatchDispatcher(
        leader=LEADER_2,
        contexts=contexts_by_leader[LEADER_2],
        legacy_executor=executor,
    )
    await dispatcher_l1.dispatch([_leader_event()])
    await dispatcher_l2.dispatch([_leader_event("0xevent-2")])

    assert executed == [(USER_ALPHA, LEADER_1), (USER_BETA, LEADER_2)]


@pytest.mark.asyncio
async def test_disabled_follow_entry_routes_nothing_but_keeps_the_account_alive():
    """A disabled follow is explicit configuration: no copy contexts at all."""
    config = make_config()
    alpha = runtime_user(
        USER_ALPHA,
        FUNDER_ALPHA,
        {LEADER_1: follow(USER_ALPHA, LEADER_1, enabled=False)},
    )
    state = build_account_state(alpha, config)
    state.positions_repo.list_open_positions = AsyncMock(return_value=[])

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        [alpha], {USER_ALPHA: state}, [LEADER_1], config
    )

    assert contexts_by_leader[LEADER_1] == []
    # No open positions and no enabled follows: no risk-only context is built
    # (nothing to protect), and no exception escapes.
    assert risk_contexts_by_user == {}


@pytest.mark.asyncio
async def test_empty_follows_keep_existing_position_protection():
    """An empty explicit whitelist stops new entries but keeps monitoring.

    The risk-only context is added to ``risk_contexts_by_user`` ONLY (never to
    ``contexts_by_leader``) and shares the account-owned coordinator, so
    protective exits and reconciliation keep their serialization domain while
    no new copied entry can be routed. (Composition twin of
    ``tests/test_account_owned_position_coordinator.py::test_empty_follows_stop_new_entries_but_keep_protection_on_account_coordinator``
    for the empty-follows and disabled-whitelist matrix rows.)
    """
    config = make_config()
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {})  # explicit empty whitelist
    state = build_account_state(alpha, config)
    state.positions_repo.list_open_positions = AsyncMock(
        return_value=[
            SimpleNamespace(token_id=TOKEN, condition_id=CONDITION, size_tokens=Decimal("5"))
        ]
    )

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        [alpha], {USER_ALPHA: state}, [LEADER_1, LEADER_2], config
    )

    assert contexts_by_leader[LEADER_1] == []
    assert contexts_by_leader[LEADER_2] == []
    risk_context = risk_contexts_by_user[USER_ALPHA]
    assert risk_context is not None
    # Protective monitoring keeps the account-owned coordination domain.
    assert risk_context.position_coordinator is state.position_coordinator
    # Reconciliation collaborators stay wired on the risk-only context.
    assert risk_context.execution_service is state.execution_service
    assert risk_context.orders_repo is state.orders_repo
    assert risk_context.fills_repo is state.fills_repo


@pytest.mark.asyncio
async def test_one_accounts_pipeline_failure_never_blocks_the_other():
    """A's executor crash is contained to A's dispatch result (§9 bullet 4).

    The production dispatcher fans a batch out with per-context exception
    capture, so one account's failure surfaces as a LeaderDispatchFailure for
    that account only while B's execution completes.
    """
    config = make_config()
    alpha = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: follow(USER_ALPHA, LEADER_1)})
    beta = runtime_user(USER_BETA, FUNDER_BETA, {LEADER_1: follow(USER_BETA, LEADER_1)})
    states = {
        USER_ALPHA: build_account_state(alpha, config),
        USER_BETA: build_account_state(beta, config),
    }
    contexts_by_leader, _ = await build_context_maps([alpha, beta], states, [LEADER_1], config)
    contexts = contexts_by_leader[LEADER_1]
    assert [ctx.user_id for ctx in contexts] == [USER_ALPHA, USER_BETA]

    completed: list[str] = []

    async def executor(context, leader_id, trades, now_ts):
        if context.user_id == USER_ALPHA:
            raise RuntimeError("alpha history fetch failed")
        completed.append(context.user_id)
        return []

    dispatcher = LeaderBatchDispatcher(leader=LEADER_1, contexts=contexts, legacy_executor=executor)
    failures = await dispatcher.dispatch([_leader_event()])

    assert completed == [USER_BETA]
    assert len(failures) == 1
    assert failures[0].context.user_id == USER_ALPHA
    assert isinstance(failures[0].error, RuntimeError)


@pytest.mark.asyncio
async def test_same_leader_two_copiers_compose_independent_sizing_and_identity():
    """§9 bullet 1 (composition face): same leader, one event.

    Each binding carries its own follow sizing inputs and produces its own
    per-account trade identity; the reservation/budget state each sized intent
    will contend on belongs to exactly one account. The full money-path proof
    (independently sized intents + DISTINCT persisted reservations through
    ``execute_trades``) runs against real SQL in
    ``test_multicopier_postgres.py``.
    """
    config = make_config()
    alpha = runtime_user(
        USER_ALPHA,
        FUNDER_ALPHA,
        {LEADER_1: follow(USER_ALPHA, LEADER_1, allocation_pct="0.10", max_trade_usdc="25")},
    )
    beta = runtime_user(
        USER_BETA,
        FUNDER_BETA,
        {LEADER_1: follow(USER_BETA, LEADER_1, allocation_pct="0.50", max_trade_usdc="10")},
    )
    states = {
        USER_ALPHA: build_account_state(alpha, config),
        USER_BETA: build_account_state(beta, config),
    }
    contexts_by_leader, _ = await build_context_maps([alpha, beta], states, [LEADER_1], config)
    ctx_a, ctx_b = contexts_by_leader[LEADER_1]

    # Independent sizing inputs per binding, from each account's own follow.
    assert ctx_a.allocation_pct == Decimal("0.10")
    assert ctx_b.allocation_pct == Decimal("0.50")
    assert ctx_a.buy_max_amount == Decimal("25")
    assert ctx_b.buy_max_amount == Decimal("10")

    # Independent venue boundaries and order repositories: B's orders cannot
    # be attached to A's persistence and vice versa.
    assert ctx_a.execution_service is states[USER_ALPHA].execution_service
    assert ctx_b.execution_service is states[USER_BETA].execution_service
    assert ctx_a.orders_repo is states[USER_ALPHA].orders_repo
    assert ctx_b.orders_repo is states[USER_BETA].orders_repo
    assert ctx_a.orders_repo is not ctx_b.orders_repo

    # The claim identity is per-account: the SAME event yields distinct
    # per-account trade keys (no cross-account dedup by construction), while
    # each account's key is stable for its own replay protection.
    event = _leader_event()
    key_a = build_trade_key(USER_ALPHA, LEADER_1, event)
    key_b = build_trade_key(USER_BETA, LEADER_1, event)
    assert key_a != key_b
    assert key_a == build_trade_key(USER_ALPHA, LEADER_1, dict(event))

    # Distinct budget domains: each binding contends on its OWN account's
    # reservation service, never the other's.
    assert ctx_a.extra["reservation_service"] is not ctx_b.extra["reservation_service"]
    assert (
        ctx_a.extra["reservation_service"]
        is states[USER_ALPHA].execution_coordinator.reservation_service
    )
    assert (
        ctx_b.extra["reservation_service"]
        is states[USER_BETA].execution_coordinator.reservation_service
    )


@pytest.mark.asyncio
async def test_two_leaders_one_token_share_the_account_coordination_domain():
    """§9 bullet 3 (composition face): two leaders, one wallet, one token.

    Both leader bindings of one account serialize on the ONE account-owned
    coordinator and contend on the ONE reservation service, so concurrent
    same-token actions cannot bypass shared limits through a private lock map.
    (Lock semantics twin:
    ``tests/test_account_owned_position_coordinator.py::test_same_token_scopes_serialize_across_leader_bindings_and_coordinator``.)
    """
    config = make_config()
    alpha = runtime_user(
        USER_ALPHA,
        FUNDER_ALPHA,
        {
            LEADER_1: follow(USER_ALPHA, LEADER_1),
            LEADER_2: follow(USER_ALPHA, LEADER_2),
        },
    )
    state = build_account_state(alpha, config)
    contexts_by_leader, _ = await build_context_maps(
        [alpha], {USER_ALPHA: state}, [LEADER_1, LEADER_2], config
    )
    ctx_l1 = contexts_by_leader[LEADER_1][0]
    ctx_l2 = contexts_by_leader[LEADER_2][0]

    # Canonical coordination: ONE lock map + ONE intent registry for the
    # account regardless of how many leaders target the same token.
    assert ctx_l1.position_coordinator is ctx_l2.position_coordinator
    assert ctx_l1.position_coordinator is state.position_coordinator
    scope = PositionScope(user_id=USER_ALPHA, condition_id=CONDITION, token_id=TOKEN)
    async with ctx_l1.position_coordinator.acquire(scope):
        assert ctx_l2.position_coordinator.is_locked(scope)
    assert not ctx_l2.position_coordinator.is_locked(scope)

    # Shared limits: both leaders contend on the same reservation service and
    # the same budget repository — a second leader cannot multiply spendable
    # balance by routing through a separate budget domain.
    assert ctx_l1.extra["reservation_service"] is ctx_l2.extra["reservation_service"]
    assert ctx_l1.extra["reservations_repo"] is ctx_l2.extra["reservations_repo"]
    assert ctx_l1.execution_coordinator is ctx_l2.execution_coordinator
