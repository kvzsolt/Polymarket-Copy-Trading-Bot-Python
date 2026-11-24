"""A05 — per-copier follows are explicit whitelists, not a global-leader union.

``ensure_runtime_follows`` must never union every global leader into every
runtime user's follows: follows are the source of truth and absence of a
follow entry means NOT following.

The single documented exception (the legacy migration, see
``docs/02-configuration.md``) keys on EXPLICIT CONFIGURATION, not on
in-memory follows emptiness: when exactly ONE user account exists AND its
follows did not come from explicit operator configuration (the config-less
fallback of ``load_runtime_users``), that lone account auto-follows every
leader it is shown — at startup AND at every dynamic refresh — exactly like
the pre-whitelist behavior. Any user loaded from a ``MULTI_USER_CONFIG_FILE``
payload (any follow entry, even disabled, or an explicitly empty list), or a
second account, means whitelist semantics with no union.
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pmcopy.config.app_config as app_config_mod
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import ensure_runtime_follows
from pmcopy.services.accounts.multi_user_config import load_runtime_users
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


def _user(user_id: str, follows: dict, *, follows_explicit: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        user_id=user_id,
        wallet=SimpleNamespace(funder_address=_VALID_FUNDER),
        follows=follows,
        follows_explicit=follows_explicit,
    )


def _configured_follow(user_id: str, leader_id: str, *, enabled: bool = True):
    return SimpleNamespace(enabled=enabled, user_id=user_id, leader_id=leader_id)


def _multi_user_config_file(tmp_path, monkeypatch, entries: list[dict]) -> str:
    path = tmp_path / "users.json"
    path.write_text(json.dumps({"users": entries}), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return str(path)


def _payload_entry(
    user_id: str,
    follows: list | None,
    *,
    private_key: str = _VALID_PRIVATE_KEY,
    funder: str = _VALID_FUNDER,
) -> dict:
    # A06 — distinct copier entries must carry distinct wallet identities;
    # the strict loader rejects duplicate funders/keys instead of letting a
    # second account silently share (or overwrite) the first's state.
    entry: dict = {
        "user_id": user_id,
        "private_key": private_key,
        "funder_address": funder,
        "signature_type": 3,
    }
    if follows is not None:
        entry["follows"] = follows
    return entry


# ---------------------------------------------------------------------------
# Legacy migration: single config-less account → dynamic auto-follow
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
        # Legacy default sizing comes from the global config, unchanged —
        # exact Decimal since A06 (money is never a binary float here).
        assert follow.allocation_pct == Decimal("0.07")
        assert follow.max_trade_usdc == Decimal("42")
        assert follow.max_market_exposure_usdc == Decimal("123")


def test_legacy_config_less_account_auto_follows_new_leaders_at_refresh():
    # The legacy default keys on EXPLICIT CONFIGURATION absence, not on the
    # in-memory follows map being empty. A config-less single account whose
    # follows were already populated at startup (load_runtime_users seeds
    # them from the startup leader set) must STILL auto-follow a leader that
    # appears later via dynamic discovery/promotion — matching the
    # pre-whitelist behavior, which unioned at every refresh.
    startup_follow = _configured_follow("user-1", LEADER_1)
    user = _user("user-1", {LEADER_1: startup_follow})

    ensure_runtime_follows([user], [LEADER_2], _config())

    assert set(user.follows) == {LEADER_1, LEADER_2}
    # The pre-existing startup entry is never overwritten.
    assert user.follows[LEADER_1] is startup_follow
    added = user.follows[LEADER_2]
    assert added.enabled is True
    assert added.user_id == "user-1"
    assert added.leader_id == LEADER_2


def test_legacy_default_never_overwrites_existing_entries():
    from pmcopy.services.accounts.multi_user_config import UserFollowConfig

    existing = UserFollowConfig("user-1", LEADER_1, True, 0.5, 10, 77)
    user = _user("user-1", {LEADER_1: existing})

    ensure_runtime_follows([user], [LEADER_1, LEADER_2], _config())

    # Refresh repeats are idempotent: LEADER_1 keeps its original entry
    # (values untouched, never replaced by a config-derived default) while
    # the new leader is added.
    assert user.follows[LEADER_1] is existing
    assert existing.allocation_pct == 0.5
    assert existing.max_trade_usdc == 10
    assert LEADER_2 in user.follows


def test_legacy_default_with_no_leaders_leaves_follows_empty():
    user = _user("user-1", {})

    ensure_runtime_follows([user], [], _config())

    assert user.follows == {}


# ---------------------------------------------------------------------------
# Explicit whitelists: no implicit union of global leaders
# ---------------------------------------------------------------------------


def test_single_account_with_explicit_follows_never_unions_other_leaders():
    user = _user("user-1", {LEADER_1: _configured_follow("user-1", LEADER_1)}, follows_explicit=True)

    ensure_runtime_follows([user], [LEADER_1, LEADER_2, LEADER_3], _config())

    # LEADER_2/LEADER_3 have no follow entry → NOT following. The whitelist
    # is the source of truth even for a single account.
    assert set(user.follows) == {LEADER_1}


def test_single_account_with_disabled_entry_is_explicit_configuration():
    # A disabled entry still counts as explicit follows configuration: the
    # operator expressed a copy policy, so the legacy default must not fire
    # and silently re-enable copying of every global leader.
    user = _user(
        "user-1",
        {LEADER_1: _configured_follow("user-1", LEADER_1, enabled=False)},
        follows_explicit=True,
    )

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


# ---------------------------------------------------------------------------
# Loader threading: explicitness is derived at the config layer
# ---------------------------------------------------------------------------


def test_loader_config_less_single_account_stays_dynamic_at_refresh(tmp_path, monkeypatch):
    # No MULTI_USER_CONFIG_FILE: the single-account fallback synthesizes
    # follows from the startup leader list. Those follows are NOT explicit
    # configuration, so the legacy default keeps applying — a refresh call
    # with a newly discovered leader adds the follow.
    cfg = _config()

    users = load_runtime_users([LEADER_1], cfg)

    assert len(users) == 1
    assert set(users[0].follows) == {LEADER_1}
    assert users[0].follows_explicit is False

    ensure_runtime_follows(users, [LEADER_2], cfg)

    assert set(users[0].follows) == {LEADER_1, LEADER_2}
    added = users[0].follows[LEADER_2]
    assert added.enabled is True
    assert added.user_id == users[0].user_id


def test_loader_payload_with_disabled_entry_is_explicit_and_frozen(tmp_path, monkeypatch):
    # A MULTI_USER_CONFIG_FILE user with a single disabled follow entry is
    # explicit configuration: the legacy default must not fire and newly
    # shown leaders must NOT be added at refresh.
    config_path = _multi_user_config_file(
        tmp_path,
        monkeypatch,
        [
            _payload_entry(
                "explicit-user",
                [{"leader_id": LEADER_1, "enabled": False}],
            )
        ],
    )
    cfg = _config(MULTI_USER_CONFIG_FILE=config_path)

    users = load_runtime_users([], cfg)

    assert len(users) == 1
    assert users[0].follows_explicit is True

    ensure_runtime_follows(users, [LEADER_1, LEADER_2], cfg)

    assert set(users[0].follows) == {LEADER_1}
    assert users[0].follows[LEADER_1].enabled is False


def test_loader_payload_with_empty_follows_list_is_explicit(tmp_path, monkeypatch):
    # An operator who writes a multi-user configuration entry without a
    # follows list has EXPLICITLY configured an empty whitelist: nothing new
    # is ever auto-followed, at startup or at refresh.
    config_path = _multi_user_config_file(
        tmp_path,
        monkeypatch,
        [_payload_entry("explicit-empty-user", None)],
    )
    cfg = _config(MULTI_USER_CONFIG_FILE=config_path)

    users = load_runtime_users([], cfg)

    assert len(users) == 1
    assert users[0].follows_explicit is True

    ensure_runtime_follows(users, [LEADER_1, LEADER_2], cfg)

    assert users[0].follows == {}


def test_loader_multi_account_config_never_auto_follows(tmp_path, monkeypatch):
    config_path = _multi_user_config_file(
        tmp_path,
        monkeypatch,
        [
            _payload_entry("user-a", None),
            _payload_entry(
                "user-b",
                [{"leader_id": LEADER_1, "enabled": True}],
                private_key="0x" + "e" * 64,
                funder="0x" + "f" * 40,
            ),
        ],
    )
    cfg = _config(MULTI_USER_CONFIG_FILE=config_path)

    users = load_runtime_users([], cfg)

    assert len(users) == 2

    ensure_runtime_follows(users, [LEADER_1, LEADER_2], cfg)

    assert users[0].follows == {}
    assert set(users[1].follows) == {LEADER_1}
