"""A05 — per-copier follows are explicit whitelists, not a global-leader union.

Before this fix ``ensure_runtime_follows`` unioned every global leader into
every runtime user's follows with ``enabled=True`` — a multi-account
deployment silently copied every leader into every sub-account. Follows are
now the source of truth: absence of a follow entry means NOT following.

The single documented exception (the legacy migration, see
``docs/02-configuration.md``): when exactly ONE user account exists AND its
follows configuration is empty/absent, the historical default of auto-following
the global leaders applies so single-account operators keep today's behavior
without a follows configuration.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import config.app_config as app_config_mod
from orchestrators.runtime_bootstrap import ensure_runtime_follows
from tests.config_helpers import load_test_config

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40

LEADER_1 = "0x" + "1" * 40
LEADER_2 = "0x" + "2" * 40
LEADER_3 = "0x" + "3" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "USER_ADDRESS": _VALID_USER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "PERCENTAGE": "7",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _user(user_id: str, follows: dict) -> SimpleNamespace:
    return SimpleNamespace(
        user_id=user_id,
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
        follows=follows,
    )


def _configured_follow(user_id: str, leader_id: str, *, enabled: bool = True):
    return SimpleNamespace(enabled=enabled, user_id=user_id, leader_id=leader_id)


# ---------------------------------------------------------------------------
# Legacy migration: single account + no explicit follows → auto-follow
# ---------------------------------------------------------------------------


def test_single_account_with_empty_follows_applies_legacy_default():
    user = _user("user-1", {})

    ensure_runtime_follows([user], [LEADER_1, LEADER_2], _config())

    assert set(user.follows) == {LEADER_1, LEADER_2}
    for leader in (LEADER_1, LEADER_2):
        follow = user.follows[leader]
        assert follow.enabled is True
        assert follow.user_id == "user-1"
        assert follow.leader_id == leader
        # Legacy default sizing comes from the global config, unchanged.
        assert follow.allocation_pct == 0.07
        assert follow.max_trade_usdc == 42.0
        assert follow.max_market_exposure_usdc == 123.0


def test_legacy_default_does_not_duplicate_existing_entries():
    from services.multiUserConfig import UserFollowConfig

    existing = UserFollowConfig("user-1", LEADER_1, True, 0.5, 10, 77)
    user = _user("user-1", {LEADER_1: existing})

    ensure_runtime_follows([user], [LEADER_1, LEADER_2], _config())

    assert len(user.follows) == 2
    assert user.follows[LEADER_1] is existing, "existing explicit entry must be preserved"


def test_legacy_default_with_no_leaders_leaves_follows_empty():
    user = _user("user-1", {})

    ensure_runtime_follows([user], [], _config())

    assert user.follows == {}


# ---------------------------------------------------------------------------
# Explicit whitelists: no implicit union of global leaders
# ---------------------------------------------------------------------------


def test_single_account_with_explicit_follows_never_unions_other_leaders():
    user = _user("user-1", {LEADER_1: _configured_follow("user-1", LEADER_1)})

    ensure_runtime_follows([user], [LEADER_1, LEADER_2, LEADER_3], _config())

    # LEADER_2/LEADER_3 have no follow entry → NOT following. The whitelist
    # is the source of truth even for a single account.
    assert set(user.follows) == {LEADER_1}


def test_single_account_with_disabled_entry_is_explicit_configuration():
    # A disabled entry still counts as explicit follows configuration: the
    # operator expressed a copy policy, so the legacy default must not fire
    # and silently re-enable copying of every global leader.
    user = _user("user-1", {LEADER_1: _configured_follow("user-1", LEADER_1, enabled=False)})

    ensure_runtime_follows([user], [LEADER_1, LEADER_2], _config())

    assert set(user.follows) == {LEADER_1}
    assert user.follows[LEADER_1].enabled is False


def test_multi_account_never_auto_follows_even_with_empty_follows():
    user_a = _user("user-a", {})
    user_b = _user("user-b", {})

    ensure_runtime_follows([user_a, user_b], [LEADER_1, LEADER_2], _config())

    assert user_a.follows == {}
    assert user_b.follows == {}


def test_multi_account_mixed_configuration_never_auto_follows():
    user_a = _user("user-a", {})  # no explicit follows at all
    user_b = _user("user-b", {LEADER_1: _configured_follow("user-b", LEADER_1)})

    ensure_runtime_follows([user_a, user_b], [LEADER_1, LEADER_2], _config())

    assert user_a.follows == {}, "multi-account deployments never auto-follow"
    assert set(user_b.follows) == {LEADER_1}, "LEADER_2 must not be unioned in"
