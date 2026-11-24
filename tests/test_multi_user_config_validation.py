"""A06 — strict multi-user configuration boundary contracts.

``services/accounts/multi_user_config.py`` is the operator-facing boundary for
``MULTI_USER_CONFIG_FILE``. These tests pin the strict input contracts:

* ``enabled`` accepts only JSON booleans or the exact strings
  ``"true"``/``"false"`` (case-insensitive); ``bool("false")`` must never
  enable a follow. Absent → enabled (documented default).
* Money (``allocation_pct`` / ``max_trade_usdc`` /
  ``max_market_exposure_usdc``) must arrive as decimal TEXT (JSON string) or
  a ``Decimal``; JSON numbers are rejected because ``json`` parses them into
  binary floats. Values must be finite, positive, and within bounds
  (``allocation_pct`` is a FRACTION of leader notional: ``0 < x <= 1``).
* Duplicate user ids, duplicate wallet identities (case-insensitive
  funder-address compare, and private-key compare for live entries without a
  funder — the key compare strips surrounding whitespace so a padded
  copy-paste cannot pose as a second wallet while the stored SecretStr keeps
  the value verbatim), and duplicate leader ids inside one user's follows
  are hard errors — never silent overwrites.
* ``chain_id`` is a JSON number or decimal text; JSON ``true``/``false`` is
  rejected (``int(True) == 1`` must never select a network).
* Leader ids and funders are validated as Ethereum addresses (stripped;
  identity compared case-insensitively).
* Unknown keys at the top level, in user objects, and in follow objects are
  rejected (catches ``max_trade_usd``-style typos). A payload without a
  ``users`` list is rejected instead of silently loading zero users.
* Credential-shaped keys beyond the sanctioned ``private_key`` are rejected
  with secret-safe messages that never echo the supplied value.
* Money is exact ``Decimal`` end-to-end: the loader emits ``Decimal`` fields
  (defaults derived from the typed AppConfig are converted at that trusted
  boundary).
* T10 regression: ``follows_explicit`` semantics (payload → True, even for
  an absent/empty follows list; config-less fallback → False) are preserved.
* ``app.py`` must fail fast when two runtime users would overwrite the same
  ``user_states`` entry.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import patch

import pytest
import pmcopy.config.app_config as app_config_mod
from pmcopy.services.accounts.multi_user_config import load_runtime_users
from tests.config_helpers import load_test_config

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_OTHER_PRIVATE_KEY = "0x" + "b" * 64
_VALID_FUNDER = "0x" + "c" * 40
_OTHER_FUNDER = "0x" + "d" * 40

LEADER_1 = "0x" + "1" * 40
LEADER_2 = "0x" + "2" * 40


def _config(**overrides):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "TRADING_MODE": "paper",
        "PERCENTAGE": "7",
        "BUY_MAX_AMOUNT": "42",
        "MAX_MARKET_EXPOSURE_USDC": "123",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _config_file(tmp_path, monkeypatch, payload) -> str:
    path = tmp_path / "users.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return str(path)


def _entry(user_id: str = "user-1", **overrides) -> dict:
    entry = {
        "user_id": user_id,
        "private_key": _VALID_PRIVATE_KEY,
        "funder_address": _VALID_FUNDER,
        "signature_type": 3,
    }
    entry.update(overrides)
    return entry


def _follow(leader_id: str = LEADER_1, **overrides) -> dict:
    follow = {"leader_id": leader_id}
    follow.update(overrides)
    return follow


def _load(payload, tmp_path, monkeypatch, **config_overrides):
    # config_overrides MUST be forwarded: the helper used to accept and
    # silently drop them, so a test's TRADING_MODE="live" never reached the
    # loader and live-path tests unknowingly ran in paper mode.
    overrides = {"MULTI_USER_CONFIG_FILE": _config_file(tmp_path, monkeypatch, payload)}
    overrides.update(config_overrides)
    cfg = _config(**overrides)
    return load_runtime_users([], cfg)


# ---------------------------------------------------------------------------
# Booleans: only real bools or the exact strings true/false
# ---------------------------------------------------------------------------


def test_enabled_absent_defaults_to_true(tmp_path, monkeypatch):
    users = _load({"users": [_entry(follows=[_follow()])]}, tmp_path, monkeypatch)
    assert users[0].follows[LEADER_1].enabled is True


def test_enabled_json_false_disables(tmp_path, monkeypatch):
    users = _load(
        {"users": [_entry(follows=[_follow(enabled=False)])]}, tmp_path, monkeypatch
    )
    assert users[0].follows[LEADER_1].enabled is False


def test_enabled_string_false_never_enables(tmp_path, monkeypatch):
    # THE headline defect: bool("false") is True in Python, so a templated
    # "false" used to silently ENABLE a follow.
    users = _load(
        {"users": [_entry(follows=[_follow(enabled="false")])]}, tmp_path, monkeypatch
    )
    assert users[0].follows[LEADER_1].enabled is False


def test_enabled_string_is_case_insensitive(tmp_path, monkeypatch):
    users = _load(
        {
            "users": [
                _entry(
                    follows=[
                        _follow(enabled="FALSE"),
                        _follow(LEADER_2, enabled="True"),
                    ]
                )
            ]
        },
        tmp_path,
        monkeypatch,
    )
    assert users[0].follows[LEADER_1].enabled is False
    assert users[0].follows[LEADER_2].enabled is True


@pytest.mark.parametrize("bad", [1, 0, "yes", "on", "1", "", None, [], 1.0])
def test_enabled_anything_else_is_rejected(tmp_path, monkeypatch, bad):
    with pytest.raises(ValueError, match="enabled"):
        _load(
            {"users": [_entry(follows=[_follow(enabled=bad)])]}, tmp_path, monkeypatch
        )


# ---------------------------------------------------------------------------
# Money: decimal text or Decimal, never JSON numbers; finite/positive/bounded
# ---------------------------------------------------------------------------


def test_money_decimal_text_is_kept_exact(tmp_path, monkeypatch):
    users = _load(
        {
            "users": [
                _entry(
                    follows=[
                        _follow(
                            allocation_pct="0.12",
                            max_trade_usdc="12.5",
                            max_market_exposure_usdc="77.25",
                        )
                    ]
                )
            ]
        },
        tmp_path,
        monkeypatch,
    )
    follow = users[0].follows[LEADER_1]
    assert follow.allocation_pct == Decimal("0.12")
    assert follow.max_trade_usdc == Decimal("12.5")
    assert follow.max_market_exposure_usdc == Decimal("77.25")
    assert isinstance(follow.allocation_pct, Decimal)
    assert isinstance(follow.max_trade_usdc, Decimal)
    assert isinstance(follow.max_market_exposure_usdc, Decimal)


def test_money_json_float_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="allocation_pct.*decimal"):
        _load(
            {"users": [_entry(follows=[_follow(allocation_pct=0.12)])]},
            tmp_path,
            monkeypatch,
        )


def test_money_json_int_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="max_trade_usdc.*decimal"):
        _load(
            {"users": [_entry(follows=[_follow(max_trade_usdc=50)])]},
            tmp_path,
            monkeypatch,
        )


@pytest.mark.parametrize("bad_text", ["nan", "NaN", "inf", "-inf", "Infinity"])
def test_money_non_finite_rejected(tmp_path, monkeypatch, bad_text):
    with pytest.raises(ValueError, match="max_trade_usdc"):
        _load(
            {"users": [_entry(follows=[_follow(max_trade_usdc=bad_text)])]},
            tmp_path,
            monkeypatch,
        )


def test_money_negative_rejected_names_user_and_field(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match=r"user-1.*max_trade_usdc"):
        _load(
            {"users": [_entry(follows=[_follow(max_trade_usdc="-5")])]},
            tmp_path,
            monkeypatch,
        )


@pytest.mark.parametrize(
    "field", ["allocation_pct", "max_trade_usdc", "max_market_exposure_usdc"]
)
def test_money_zero_rejected(tmp_path, monkeypatch, field):
    with pytest.raises(ValueError, match=field):
        _load(
            {"users": [_entry(follows=[_follow(**{field: "0"})])]},
            tmp_path,
            monkeypatch,
        )


def test_money_garbage_text_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="allocation_pct"):
        _load(
            {"users": [_entry(follows=[_follow(allocation_pct="half")])]},
            tmp_path,
            monkeypatch,
        )


def test_allocation_pct_is_a_fraction_bounded_to_one(tmp_path, monkeypatch):
    # allocation_pct is a FRACTION of leader notional (0.07 = 7%), matching
    # the config-less fallback (PERCENTAGE=7 → 0.07). Over-100% copying is a
    # misconfiguration and must be rejected.
    users = _load(
        {"users": [_entry(follows=[_follow(allocation_pct="1")])]},
        tmp_path,
        monkeypatch,
    )
    assert users[0].follows[LEADER_1].allocation_pct == Decimal("1")

    with pytest.raises(ValueError, match="allocation_pct"):
        _load(
            {"users": [_entry(follows=[_follow(allocation_pct="1.5")])]},
            tmp_path,
            monkeypatch,
        )


def test_money_defaults_come_from_typed_config_as_decimal(tmp_path, monkeypatch):
    users = _load({"users": [_entry(follows=[_follow()])]}, tmp_path, monkeypatch)
    follow = users[0].follows[LEADER_1]
    assert follow.allocation_pct == Decimal("0.07")
    assert follow.max_trade_usdc == Decimal("42")
    assert follow.max_market_exposure_usdc == Decimal("123")
    assert isinstance(follow.allocation_pct, Decimal)


def test_configless_fallback_money_is_decimal(tmp_path, monkeypatch):
    cfg = _config()
    users = load_runtime_users([LEADER_1], cfg)
    follow = users[0].follows[LEADER_1]
    assert follow.allocation_pct == Decimal("0.07")
    assert follow.max_trade_usdc == Decimal("42")
    assert follow.max_market_exposure_usdc == Decimal("123")


# ---------------------------------------------------------------------------
# Duplicates: user ids, wallet identities, leader ids — never silent overwrite
# ---------------------------------------------------------------------------


def test_duplicate_user_id_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="duplicate.*user-1"):
        _load(
            {
                "users": [
                    _entry("user-1", funder_address=_VALID_FUNDER),
                    _entry("user-1", funder_address=_OTHER_FUNDER),
                ]
            },
            tmp_path,
            monkeypatch,
        )


def test_duplicate_funder_address_rejected_case_insensitive(tmp_path, monkeypatch):
    # Same address, different casing: normalized identity compare must catch
    # the second wallet before it silently shares state with the first.
    mixed_case_funder = "0x" + "C" * 40
    with pytest.raises(ValueError, match="funder_address"):
        _load(
            {
                "users": [
                    _entry("user-1", funder_address=_VALID_FUNDER),
                    _entry("user-2", funder_address=mixed_case_funder),
                ]
            },
            tmp_path,
            monkeypatch,
        )


def test_duplicate_private_key_rejected_without_echoing_secret(tmp_path, monkeypatch):
    # Live entries may omit funder_address; the private key is then the only
    # wallet identity. A shared key across two entries is a duplicate wallet
    # and must fail closed — naming the users, never the key material.
    # (funder_address is deleted so the PRIVATE-KEY compare is the one that
    # fires; leaving the shared default funder in would exercise the
    # funder-duplicate gate instead.)
    entry_a = _entry("live-a", private_key=_OTHER_PRIVATE_KEY)
    del entry_a["funder_address"]
    entry_b = _entry("live-b", private_key=_OTHER_PRIVATE_KEY)
    del entry_b["funder_address"]
    payload = {"users": [entry_a, entry_b]}
    with pytest.raises(ValueError) as excinfo:
        _load(payload, tmp_path, monkeypatch, TRADING_MODE="live")
    message = str(excinfo.value)
    assert "live-a" in message
    assert "live-b" in message
    assert "private_key" in message
    assert _OTHER_PRIVATE_KEY not in message
    assert _OTHER_PRIVATE_KEY.replace("0x", "") not in message


def test_duplicate_private_key_whitespace_only_difference_rejected(tmp_path, monkeypatch):
    # Keys differing only by surrounding whitespace are the SAME wallet:
    # the duplicate compare normalizes (strip) before comparing so a
    # copy-pasted "  <key>  " cannot defeat duplicate-wallet detection.
    padded_key = "  " + _OTHER_PRIVATE_KEY + " \n"
    entry_a = _entry("live-a", private_key=_OTHER_PRIVATE_KEY)
    del entry_a["funder_address"]
    entry_b = _entry("live-b", private_key=padded_key)
    del entry_b["funder_address"]
    payload = {"users": [entry_a, entry_b]}
    with pytest.raises(ValueError) as excinfo:
        _load(payload, tmp_path, monkeypatch, TRADING_MODE="live")
    message = str(excinfo.value)
    assert "live-a" in message
    assert "live-b" in message
    assert "private_key" in message
    assert _OTHER_PRIVATE_KEY not in message
    assert _OTHER_PRIVATE_KEY.replace("0x", "") not in message


def test_private_key_stored_verbatim_only_compare_normalizes(tmp_path, monkeypatch):
    # Only the COMPARISON normalizes. The stored SecretStr keeps the value
    # exactly as written (byte-identical transport to the wallet layer), so
    # whitespace-padded single entries still load unchanged.
    padded_key = "  " + _OTHER_PRIVATE_KEY + "  "
    users = _load(
        {"users": [_entry(private_key=padded_key, follows=[_follow()])]},
        tmp_path,
        monkeypatch,
    )
    assert users[0].wallet.private_key.get_secret_value() == padded_key


def test_duplicate_leader_id_in_one_users_follows_rejected(tmp_path, monkeypatch):
    # The old loader silently kept the LAST duplicate follow.
    with pytest.raises(ValueError, match="leader"):
        _load(
            {
                "users": [
                    _entry(
                        follows=[
                            _follow(LEADER_1, max_trade_usdc="10"),
                            _follow(LEADER_1, max_trade_usdc="20"),
                        ]
                    )
                ]
            },
            tmp_path,
            monkeypatch,
        )


def test_duplicate_leader_id_case_insensitive_rejected(tmp_path, monkeypatch):
    # Same address written with different casing: one wallet, two entries.
    lower_leader = "0x" + "a" * 40
    upper_leader = "0x" + "A" * 40
    with pytest.raises(ValueError, match="leader"):
        _load(
            {
                "users": [
                    _entry(follows=[_follow(lower_leader), _follow(upper_leader)])
                ]
            },
            tmp_path,
            monkeypatch,
        )


def test_same_leader_across_different_users_is_allowed(tmp_path, monkeypatch):
    # Two DISTINCT wallets (different funder and different key) may both
    # follow the same leader; only duplicate identities are rejected.
    users = _load(
        {
            "users": [
                _entry(
                    "user-1",
                    funder_address=_VALID_FUNDER,
                    follows=[_follow(LEADER_1)],
                ),
                _entry(
                    "user-2",
                    private_key=_OTHER_PRIVATE_KEY,
                    funder_address=_OTHER_FUNDER,
                    follows=[_follow(LEADER_1)],
                ),
            ]
        },
        tmp_path,
        monkeypatch,
    )
    assert LEADER_1 in users[0].follows
    assert LEADER_1 in users[1].follows


# ---------------------------------------------------------------------------
# Address validation and normalization
# ---------------------------------------------------------------------------


def test_invalid_leader_id_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="leader_id"):
        _load(
            {"users": [_entry(follows=[_follow("not-an-address")])]},
            tmp_path,
            monkeypatch,
        )


def test_leader_id_survives_as_validated_stripped_address(tmp_path, monkeypatch):
    users = _load(
        {"users": [_entry(follows=[_follow(f"  {LEADER_1}  ")])]},
        tmp_path,
        monkeypatch,
    )
    assert users[0].follows[LEADER_1].leader_id == LEADER_1


def test_invalid_funder_address_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="funder_address"):
        _load(
            {"users": [_entry(funder_address="0x123")]},
            tmp_path,
            monkeypatch,
        )


def test_invalid_funder_address_rejected_without_echoing_value(tmp_path, monkeypatch):
    # Final-review fix: an operator who pastes a private key (66 hex) into a
    # funder-address field must get a rejection whose text carries a
    # length/shape hint ONLY. The error flows to startup output, doctor
    # details, and ``pmcopy config validate`` stdout — echoing the supplied
    # value would print the key material.
    canary = "0x" + "e" * 64  # runtime-built; private-key-shaped, not an address
    with pytest.raises(ValueError, match="funder_address") as excinfo:
        _load(
            {"users": [_entry(funder_address=canary)]},
            tmp_path,
            monkeypatch,
        )
    message = str(excinfo.value)
    assert canary not in message
    assert canary.replace("0x", "") not in message
    assert "66" in message  # a length hint is given instead of the value


# ---------------------------------------------------------------------------
# Strict schema: unknown keys, missing structure
# ---------------------------------------------------------------------------


def test_unknown_user_key_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="unknown.*nickname"):
        _load({"users": [_entry(nickname="alpha")]}, tmp_path, monkeypatch)


def test_unknown_follow_key_typo_rejected(tmp_path, monkeypatch):
    # "max_trade_usd" (missing the trailing c) must not be silently ignored.
    with pytest.raises(ValueError, match="max_trade_usd"):
        _load(
            {"users": [_entry(follows=[_follow(max_trade_usd="10")])]},
            tmp_path,
            monkeypatch,
        )


def test_unknown_top_level_key_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="unknown.*version_note"):
        _load({"users": [], "version_note": "v2"}, tmp_path, monkeypatch)


def test_missing_users_key_rejected(tmp_path, monkeypatch):
    # A file without a "users" list used to load as zero users — silently
    # disabling every copier. Fail closed instead.
    with pytest.raises(ValueError, match="users"):
        _load({"accounts": []}, tmp_path, monkeypatch)


def test_missing_user_id_rejected_with_context(tmp_path, monkeypatch):
    entry = _entry()
    del entry["user_id"]
    with pytest.raises(ValueError, match="user_id"):
        _load({"users": [entry]}, tmp_path, monkeypatch)


def test_missing_private_key_rejected_with_context(tmp_path, monkeypatch):
    entry = _entry()
    del entry["private_key"]
    with pytest.raises(ValueError, match="private_key"):
        _load({"users": [entry]}, tmp_path, monkeypatch)


@pytest.mark.parametrize("value", [True, False])
def test_chain_id_json_bool_rejected(tmp_path, monkeypatch, value):
    # int(True) == 1 and int(False) == 0, so a templated JSON boolean used
    # to load as a chain id (true -> 1). A network selector must never
    # accept a boolean; reject naming the field and the JSON type.
    with pytest.raises(ValueError, match="chain_id") as excinfo:
        _load(
            {"users": [_entry(chain_id=value, follows=[_follow()])]},
            tmp_path,
            monkeypatch,
        )
    message = str(excinfo.value)
    assert "chain_id" in message
    assert "bool" in message


def test_chain_id_numeric_loads_as_int(tmp_path, monkeypatch):
    # Positive control for the bool guard: numeric chain ids keep loading
    # (both JSON numbers and decimal text) with the exact int value.
    users = _load(
        {"users": [_entry(chain_id=80002, follows=[_follow()])]},
        tmp_path,
        monkeypatch,
    )
    assert users[0].wallet.chain_id == 80002
    users_text = _load(
        {"users": [_entry(chain_id="137", follows=[_follow()])]},
        tmp_path,
        monkeypatch,
    )
    assert users_text[0].wallet.chain_id == 137


def test_empty_user_id_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="user_id"):
        _load({"users": [_entry("   ")]}, tmp_path, monkeypatch)


def test_follows_must_be_a_list(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="follows"):
        _load({"users": [_entry(follows={"leader_id": LEADER_1})]}, tmp_path, monkeypatch)


def test_follow_without_leader_id_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="leader_id"):
        _load({"users": [_entry(follows=[{"enabled": True}])]}, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Credential-shaped keys: never accepted, values never echoed
# ---------------------------------------------------------------------------


def test_credential_shaped_follow_key_rejected_secret_safe(tmp_path, monkeypatch):
    # Build the synthetic credential-shaped key name from parts so no
    # literal credential-shaped string appears in this file.
    shaped_key = "_".join(["api", "key"])
    shaped_value = "-".join(["sk", "test", "0000"])
    with pytest.raises(ValueError) as excinfo:
        _load(
            {"users": [_entry(follows=[_follow(**{shaped_key: shaped_value})])]},
            tmp_path,
            monkeypatch,
        )
    message = str(excinfo.value)
    assert shaped_key in message  # the FIELD name may be named
    assert shaped_value not in message  # the VALUE must never be echoed


def test_credential_shaped_user_key_rejected_secret_safe(tmp_path, monkeypatch):
    shaped_key = "_".join(["bearer", "token"])
    shaped_value = "-".join(["tok", "test", "1111"])
    with pytest.raises(ValueError) as excinfo:
        _load({"users": [_entry(**{shaped_key: shaped_value})]}, tmp_path, monkeypatch)
    message = str(excinfo.value)
    assert shaped_value not in message


# ---------------------------------------------------------------------------
# T10 regression: follows_explicit semantics preserved
# ---------------------------------------------------------------------------


def test_payload_follows_list_marks_explicit(tmp_path, monkeypatch):
    users = _load({"users": [_entry(follows=[_follow()])]}, tmp_path, monkeypatch)
    assert users[0].follows_explicit is True


def test_payload_without_follows_key_is_explicit_empty(tmp_path, monkeypatch):
    users = _load({"users": [_entry()]}, tmp_path, monkeypatch)
    assert users[0].follows_explicit is True
    assert users[0].follows == {}


def test_payload_empty_follows_list_is_explicit(tmp_path, monkeypatch):
    users = _load({"users": [_entry(follows=[])]}, tmp_path, monkeypatch)
    assert users[0].follows_explicit is True
    assert users[0].follows == {}


def test_configless_fallback_is_not_explicit(tmp_path, monkeypatch):
    users = load_runtime_users([LEADER_1], _config())
    assert users[0].follows_explicit is False


# ---------------------------------------------------------------------------
# app.py: duplicate user-state registration fails fast
# ---------------------------------------------------------------------------


def test_app_rejects_duplicate_user_state_registration():
    from pmcopy.app import _reject_duplicate_user_state

    states: dict[str, object] = {"dup-user": object()}
    with pytest.raises(ValueError, match="dup-user"):
        _reject_duplicate_user_state(states, "dup-user")


def test_app_allows_first_user_state_registration():
    from pmcopy.app import _reject_duplicate_user_state

    states: dict[str, object] = {}
    # A fresh id raises nothing; the caller then performs the assignment.
    _reject_duplicate_user_state(states, "fresh-user")


# ---------------------------------------------------------------------------
# Compatibility: existing payload shapes keep loading
# ---------------------------------------------------------------------------


def test_minimal_deposit_wallet_entry_still_loads(tmp_path, monkeypatch):
    users = _load({"users": [_entry()]}, tmp_path, monkeypatch)
    assert users[0].user_id == "user-1"
    assert users[0].wallet.funder_address == _VALID_FUNDER
    assert users[0].follows_explicit is True
