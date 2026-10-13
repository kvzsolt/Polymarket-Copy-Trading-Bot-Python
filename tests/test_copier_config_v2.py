"""Task 22 — multi-copier configuration schema v2 (copiers YAML) contracts.

``services/copierConfig.py`` is the operator-facing boundary for the
``app.copiers_config_file`` YAML roster (schema_version 2, addon §3). These
tests pin the strict input contracts on top of the Task 14 primitives:

* **Schema** — ``schema_version`` must be exactly ``2``; unknown keys at
  every level (top level, copier, wallet, limits, follow, sizing) are
  rejected; ``copiers`` and per-copier ``wallet``/``limits``/``follows``
  keys are required (an explicitly empty ``copiers``/``follows`` list is
  valid; a missing one is not).
* **Booleans** — only real YAML booleans or the exact strings
  ``"true""/"false"`` (case-insensitive). ``"yes"``/``"1"`` are rejected.
* **Money** — decimal text only (the loader parses the YAML with a
  string-scalar loader, so quoting is irrelevant and no binary float can
  ever exist); values must be finite and strictly positive;
  ``leader_share`` is a FRACTION of leader notional (``0 < x <= 1``);
  ``quote_amount`` must not exceed the copier ``max_order_quote`` cap.
* **Identity** — duplicate copier ids, duplicate resolved wallet
  identities (case-insensitive funder compare, non-empty private-key
  compare), duplicate follow leaders per copier, and reserved/unsafe ids
  (``default``, path-shaped) are rejected. Leader addresses must be
  Ethereum addresses.
* **Mode** — every copier declares ``paper``/``live``; mixed modes across
  copiers are rejected; a uniform mode disagreeing with the authoritative
  ``execution.trading_mode`` is rejected.
* **Secrets** — wallet fields are env-var REFERENCES (names only, never
  values). Paper must not demand signing secrets: ``private_key_env`` is
  optional and a declared-but-unset reference resolves empty instead of
  failing paper startup. Live requires the private-key reference, its env
  presence, and a valid ``0x`` + 64-hex key; errors name the VARIABLE,
  never the value. Resolved private keys are registered with the Task 5
  redaction registry.
* **Migration** — the v2 file is authoritative when present; setting both
  ``app.copiers_config_file`` and ``app.multi_user_config_file`` is a
  config error; with no roster file the legacy single-account config maps
  deterministically to one copier with the EXISTING persisted id.
* **Selection** — ``select_runtime_copiers`` filters the loaded users to
  one copier id and rejects unknown ids.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import pmcopy.config.app_config as app_config_mod
from pmcopy.services.multiUserConfig import RuntimeUser, load_runtime_users
from tests.config_helpers import load_test_config

# Synthetic values only — no real credentials ever appear in this file.
LEADER_1 = "0x" + "1" * 40
LEADER_2 = "0x" + "2" * 40
FUNDER_A = "0x" + "c" * 40
FUNDER_B = "0x" + "d" * 40
KEY_A = "0x" + "a" * 64
KEY_B = "0x" + "b" * 64


def _paper_yaml(*copiers: str) -> str:
    body = "\n".join(copiers) if copiers else _DEFAULT_PAPER_COPIERS
    return f"schema_version: 2\ncopiers:\n{body}"


_DEFAULT_PAPER_COPIERS = f"""  - id: primary
    enabled: true
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        enabled: true
        sizing:
          mode: fixed
          quote_amount: "2.00"
  - id: secondary
    enabled: true
    mode: paper
    wallet:
      funder_address_env: SECONDARY_FUNDER_ADDRESS
      private_key_env: SECONDARY_PRIVATE_KEY
    limits:
      max_order_quote: "3.00"
      max_open_exposure_quote: "20.00"
    follows:
      - leader_address: "{LEADER_1}"
        enabled: true
        sizing:
          mode: fixed
          quote_amount: "1.00"
      - leader_address: "{LEADER_2}"
        enabled: true
        sizing:
          mode: fixed
          quote_amount: "1.00"
"""


def _config(**overrides):
    env = {
        "PRIVATE_KEY": "",
        "FUNDER_ADDRESS": "",
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "TRADING_MODE": "paper",
    }
    env.update(overrides)
    with patch.object(app_config_mod, "_runtime_python_supported", return_value=True):
        return load_test_config(env)


def _copiers_file(tmp_path, monkeypatch, text: str) -> str:
    path = tmp_path / "copiers.yml"
    path.write_text(text, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return str(path)


def _set_default_env(monkeypatch) -> None:
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("SECONDARY_FUNDER_ADDRESS", FUNDER_B)
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", KEY_A)
    monkeypatch.setenv("SECONDARY_PRIVATE_KEY", KEY_B)


def _load(
    text: str,
    tmp_path,
    monkeypatch,
    *,
    set_env: bool = True,
    **config_overrides,
) -> list[RuntimeUser]:
    if set_env:
        _set_default_env(monkeypatch)
    cfg = _config(COPIERS_CONFIG_FILE=_copiers_file(tmp_path, monkeypatch, text), **config_overrides)
    return load_runtime_users([], cfg)


# ---------------------------------------------------------------------------
# Happy path: two copiers load as RuntimeUser rows through the existing seam
# ---------------------------------------------------------------------------


def test_two_paper_copiers_load_with_runtime_user_shape(tmp_path, monkeypatch):
    users = _load(_paper_yaml(), tmp_path, monkeypatch)

    assert [u.user_id for u in users] == ["primary", "secondary"]
    primary = users[0]
    assert primary.wallet.funder_address == FUNDER_A
    assert primary.wallet.private_key.get_secret_value() == KEY_A
    assert int(primary.wallet.signature_type) == 3
    assert primary.wallet.chain_id == 137
    assert primary.follows_explicit is True
    assert set(primary.follows) == {LEADER_1}

    follow = primary.follows[LEADER_1]
    assert follow.user_id == "primary"
    assert follow.enabled is True
    # fixed sizing → 1:1 mirror of leader notional, capped at quote_amount
    assert follow.allocation_pct == Decimal("1")
    assert follow.max_trade_usdc == Decimal("2.00")
    assert follow.max_market_exposure_usdc == Decimal("50.00")
    assert isinstance(follow.allocation_pct, Decimal)

    secondary = users[1]
    assert set(secondary.follows) == {LEADER_1, LEADER_2}
    assert secondary.follows[LEADER_1].max_trade_usdc == Decimal("1.00")


def test_enabled_absent_defaults_true_and_false_disables(tmp_path, monkeypatch):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
          mode: proportional
          leader_share: "0.07"
  - id: secondary
    enabled: false
    mode: paper
    wallet:
      funder_address_env: SECONDARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    users = _load(text, tmp_path, monkeypatch)

    # Absent enabled → true; the disabled copier is validated but NOT started.
    assert [u.user_id for u in users] == ["primary"]
    follow = users[0].follows[LEADER_1]
    # proportional sizing → the bounded fraction flows through unchanged
    assert follow.allocation_pct == Decimal("0.07")
    assert follow.max_trade_usdc == Decimal("5.00")
    assert follow.max_market_exposure_usdc == Decimal("50.00")


def test_proportional_leader_share_is_required_no_global_default(tmp_path, monkeypatch):
    # Schema v2 keeps sizing fully explicit: no implicit global default.
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
          mode: proportional
"""
    )
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    with pytest.raises(ValueError, match="leader_share"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_disabled_copier_does_not_demand_secret_resolution(tmp_path, monkeypatch):
    # A disabled copier's env references are not required to resolve.
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
  - id: parked
    enabled: false
    mode: paper
    wallet:
      funder_address_env: PARKED_FUNDER_ADDRESS_MISSING
      private_key_env: PARKED_PRIVATE_KEY_MISSING
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    users = _load(text, tmp_path, monkeypatch, set_env=False)
    assert [u.user_id for u in users] == ["primary"]


def test_empty_copiers_list_is_valid_and_empty_follows_permits_no_entries(tmp_path, monkeypatch):
    users = _load("schema_version: 2\ncopiers: []", tmp_path, monkeypatch)
    assert users == []

    empty_follows = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    users = _load(empty_follows, tmp_path, monkeypatch, set_env=False)
    assert users[0].follows == {}
    assert users[0].follows_explicit is True


# ---------------------------------------------------------------------------
# Paper must not demand signing secrets
# ---------------------------------------------------------------------------


def test_paper_copier_without_private_key_env_loads(tmp_path, monkeypatch):
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    users = _load(text, tmp_path, monkeypatch, set_env=False)
    assert users[0].wallet.private_key.get_secret_value() == ""


def test_paper_declared_but_unset_private_key_env_resolves_empty(tmp_path, monkeypatch):
    # THE addon §3 rule: a declared OPTIONAL secret reference must not make
    # paper startup demand live credentials.
    monkeypatch.delenv("PRIMARY_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(_DEFAULT_PAPER_COPIERS.split("  - id: secondary")[0])
    users = _load(text, tmp_path, monkeypatch, set_env=False)
    assert users[0].wallet.private_key.get_secret_value() == ""


def test_paper_never_validates_key_material(tmp_path, monkeypatch):
    # A garbage key VALUE in paper is never parsed or demanded — paper does
    # not authenticate, so key material is irrelevant to paper startup.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", "not-a-key-at-all")
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    users = _load(text, tmp_path, monkeypatch, set_env=False)
    assert users[0].user_id == "primary"


def test_paper_requires_funder_reference(tmp_path, monkeypatch):
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="funder_address_env.*required in paper"):
        _load(text, tmp_path, monkeypatch)


def test_paper_missing_funder_env_is_rejected(tmp_path, monkeypatch):
    monkeypatch.delenv("PRIMARY_FUNDER_ADDRESS", raising=False)
    with pytest.raises(ValueError, match="PRIMARY_FUNDER_ADDRESS"):
        _load(
            _paper_yaml(_DEFAULT_PAPER_COPIERS.split("  - id: secondary")[0]),
            tmp_path,
            monkeypatch,
            set_env=False,
        )


# ---------------------------------------------------------------------------
# Live credentials: fail clearly, name the variable, never the value
# ---------------------------------------------------------------------------


def _live_copier(key_env: str = "PRIMARY_PRIVATE_KEY") -> str:
    return f"""  - id: primary
    mode: live
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: {key_env}
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""


def test_live_copier_loads_with_resolved_key(tmp_path, monkeypatch):
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", KEY_A)
    users = _load(
        _paper_yaml(_live_copier()), tmp_path, monkeypatch, set_env=False, TRADING_MODE="live"
    )
    assert users[0].wallet.private_key.get_secret_value() == KEY_A
    assert users[0].wallet.funder_address == FUNDER_A


def test_live_without_private_key_env_is_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        """  - id: primary
    mode: live
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="private_key_env.*required in live"):
        _load(text, tmp_path, monkeypatch, set_env=False, TRADING_MODE="live")


def test_live_missing_private_key_env_value_names_the_variable(tmp_path, monkeypatch):
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.delenv("PRIMARY_PRIVATE_KEY", raising=False)
    with pytest.raises(ValueError, match="PRIMARY_PRIVATE_KEY") as excinfo:
        _load(
            _paper_yaml(_live_copier()),
            tmp_path,
            monkeypatch,
            set_env=False,
            TRADING_MODE="live",
        )
    assert KEY_A not in str(excinfo.value)


def test_live_invalid_private_key_format_is_rejected_without_echo(tmp_path, monkeypatch):
    canary = "0x" + "e" * 40  # wrong length — also our canary value
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", canary)
    with pytest.raises(ValueError, match="66-char hex") as excinfo:
        _load(
            _paper_yaml(_live_copier()),
            tmp_path,
            monkeypatch,
            set_env=False,
            TRADING_MODE="live",
        )
    assert canary not in str(excinfo.value)


def test_live_funder_reference_is_optional_but_validated(tmp_path, monkeypatch):
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", KEY_A)
    # live may omit funder_address_env (derived from the key) …
    text = _paper_yaml(
        """  - id: primary
    mode: live
    wallet:
      private_key_env: PRIMARY_PRIVATE_KEY
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    users = _load(text, tmp_path, monkeypatch, set_env=False, TRADING_MODE="live")
    assert users[0].wallet.funder_address == ""

    # … but a declared reference must resolve to a valid address.
    text_with_funder = _paper_yaml(_live_copier())
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", "0x1234")
    with pytest.raises(ValueError, match="funder_address_env"):
        _load(text_with_funder, tmp_path, monkeypatch, set_env=False, TRADING_MODE="live")


def test_resolved_private_keys_are_registered_for_redaction(tmp_path, monkeypatch):
    from pmcopy.utils.log_redaction import REDACTED_PLACEHOLDER, redact_text, reset_secret_registry

    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PRIMARY_PRIVATE_KEY", KEY_A)
    reset_secret_registry()
    try:
        _load(_paper_yaml(_live_copier()), tmp_path, monkeypatch, set_env=False, TRADING_MODE="live")
        redacted = redact_text(f"leak {KEY_A}")
        assert KEY_A not in redacted
        assert REDACTED_PLACEHOLDER in redacted
    finally:
        reset_secret_registry()


# ---------------------------------------------------------------------------
# Booleans: strict — "yes"/"1" must never enable anything
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["yes", "on", "1", "0", "", "null", "maybe"])
def test_enabled_strict_boolean_rejections(tmp_path, monkeypatch, bad):
    text = _paper_yaml(
        f"""  - id: primary
    enabled: {bad}
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="enabled"):
        _load(text, tmp_path, monkeypatch)


def test_enabled_string_false_and_true_text_accepted(tmp_path, monkeypatch):
    text = _paper_yaml(
        f"""  - id: primary
    enabled: "false"
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        enabled: "True"
        sizing:
          mode: fixed
          quote_amount: "1.00"
"""
    )
    users = _load(text, tmp_path, monkeypatch)
    # disabled copier is not started; the follow parsing still validated.
    assert users == []


# ---------------------------------------------------------------------------
# Money: decimal text, finite, positive, bounded
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    ["abc", "-5.00", "0", "NaN", "Infinity", "-Infinity", ""],
)
def test_limit_money_rejections(tmp_path, monkeypatch, bad):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "{bad}"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="max_order_quote"):
        _load(text, tmp_path, monkeypatch)


def test_missing_limits_block_or_field_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    follows: []
"""
    )
    with pytest.raises(ValueError, match="limits"):
        _load(text, tmp_path, monkeypatch)

    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="max_open_exposure_quote"):
        _load(text, tmp_path, monkeypatch)


@pytest.mark.parametrize("bad", ["0", "1.5", "-0.1", "NaN", "abc"])
def test_leader_share_bounds(tmp_path, monkeypatch, bad):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
          mode: proportional
          leader_share: "{bad}"
"""
    )
    with pytest.raises(ValueError, match="leader_share"):
        _load(text, tmp_path, monkeypatch)


def test_fixed_quote_amount_above_account_cap_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
          mode: fixed
          quote_amount: "6.00"
"""
    )
    with pytest.raises(ValueError, match="max_order_quote"):
        _load(text, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Sizing schema: mode + exactly the matching amount field
# ---------------------------------------------------------------------------


def test_sizing_mode_required_and_known(tmp_path, monkeypatch):
    base = f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
{{body}}
"""
    with pytest.raises(ValueError, match="sizing"):
        _load(_paper_yaml(base.format(body="          quote_amount: \"1.00\"")), tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="mode"):
        _load(
            _paper_yaml(base.format(body='          mode: "mirror-everything"')), tmp_path, monkeypatch
        )


def test_fixed_requires_quote_amount_and_forbids_leader_share(tmp_path, monkeypatch):
    base = f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
{{body}}
"""
    with pytest.raises(ValueError, match="quote_amount"):
        _load(_paper_yaml(base.format(body="          mode: fixed")), tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="leader_share"):
        _load(
            _paper_yaml(
                base.format(
                    body='          mode: fixed\n          quote_amount: "1.00"\n          leader_share: "0.5"'
                )
            ),
            tmp_path,
            monkeypatch,
        )


def test_proportional_requires_leader_share_and_forbids_quote_amount(tmp_path, monkeypatch):
    base = f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
        sizing:
{{body}}
"""
    with pytest.raises(ValueError, match="leader_share"):
        _load(_paper_yaml(base.format(body="          mode: proportional")), tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="quote_amount"):
        _load(
            _paper_yaml(
                base.format(
                    body='          mode: proportional\n          quote_amount: "1.00"'
                )
            ),
            tmp_path,
            monkeypatch,
        )


def test_sizing_block_required(tmp_path, monkeypatch):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{LEADER_1}"
"""
    )
    with pytest.raises(ValueError, match="sizing"):
        _load(text, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Identity: duplicates and invalid addresses rejected
# ---------------------------------------------------------------------------


def test_duplicate_copier_ids_rejected(tmp_path, monkeypatch):
    one = """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    with pytest.raises(ValueError, match="duplicate.*primary"):
        _load(_paper_yaml(one, one), tmp_path, monkeypatch)


def _copier_block(
    copier_id: str,
    *,
    enabled: str = "true",
    funder_env: str = "PRIMARY_FUNDER_ADDRESS",
    key_env: str = "",
) -> str:
    key_line = f"      private_key_env: {key_env}\n" if key_env else ""
    return f"""  - id: {copier_id}
    enabled: {enabled}
    mode: paper
    wallet:
      funder_address_env: {funder_env}
{key_line}    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""


def test_duplicate_id_spanning_enabled_and_disabled_rejected(tmp_path, monkeypatch):
    # addon §3: duplicate ids are rejected unqualified — a DISABLED twin of
    # an enabled copier id is the same config mistake.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(
        _copier_block("primary"),
        _copier_block("primary", enabled="false"),
    )
    with pytest.raises(ValueError, match="duplicate.*primary"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_duplicate_id_disabled_only_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        _copier_block("primary", enabled="false"),
        _copier_block("primary", enabled="false"),
    )
    with pytest.raises(ValueError, match="duplicate.*primary"):
        _load(text, tmp_path, monkeypatch)


def test_duplicate_wallet_env_name_spanning_enabled_and_disabled_rejected(
    tmp_path, monkeypatch,
):
    # One env var holds one address, so two copiers referencing the SAME
    # funder variable share a wallet identity — enabled or not.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(
        _copier_block("primary", funder_env="PRIMARY_FUNDER_ADDRESS"),
        _copier_block("secondary", enabled="false", funder_env="PRIMARY_FUNDER_ADDRESS"),
    )
    with pytest.raises(ValueError, match="PRIMARY_FUNDER_ADDRESS"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_duplicate_resolved_wallet_spanning_enabled_and_disabled_rejected(
    tmp_path, monkeypatch,
):
    # Different env NAMES resolving to the same address: the disabled
    # entry's public funder identity is resolved non-demandingly (the
    # variable is set), so the collision is caught without demanding any
    # of the disabled copier's secrets.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PARKED_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(
        _copier_block("primary", funder_env="PRIMARY_FUNDER_ADDRESS"),
        _copier_block("parked", enabled="false", funder_env="PARKED_FUNDER_ADDRESS"),
    )
    with pytest.raises(ValueError, match="funder"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_duplicate_private_key_env_name_spanning_enabled_and_disabled_rejected(
    tmp_path, monkeypatch,
):
    # One env var holds one key: sharing the private_key reference is
    # sharing the wallet, even when one copier is disabled.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("PARKED_FUNDER_ADDRESS", FUNDER_B)
    monkeypatch.setenv("SHARED_PRIVATE_KEY", KEY_A)
    text = _paper_yaml(
        _copier_block("primary", key_env="SHARED_PRIVATE_KEY"),
        _copier_block(
            "parked", enabled="false", funder_env="PARKED_FUNDER_ADDRESS",
            key_env="SHARED_PRIVATE_KEY",
        ),
    )
    with pytest.raises(ValueError, match="private_key"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_duplicate_resolved_funder_rejected_across_env_names(tmp_path, monkeypatch):
    # Two DIFFERENT env var names resolving to the SAME address are one
    # canonical wallet identity — rejected.
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("ALIAS_FUNDER_ADDRESS", FUNDER_A)
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
  - id: secondary
    mode: paper
    wallet:
      funder_address_env: ALIAS_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="funder"):
        _load(text, tmp_path, monkeypatch, set_env=False)


def test_duplicate_live_private_keys_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("KEY_ONE", KEY_A)
    monkeypatch.setenv("KEY_TWO", KEY_A)
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("SECONDARY_FUNDER_ADDRESS", FUNDER_B)
    text = _paper_yaml(
        """  - id: primary
    mode: live
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: KEY_ONE
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
  - id: secondary
    mode: live
    wallet:
      funder_address_env: SECONDARY_FUNDER_ADDRESS
      private_key_env: KEY_TWO
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="private_key"):
        _load(text, tmp_path, monkeypatch, set_env=False, TRADING_MODE="live")


def test_two_paper_copiers_without_keys_are_not_duplicate_identities(tmp_path, monkeypatch):
    # Regression guard: both keys resolve EMPTY in paper; empty must not be
    # treated as a shared wallet identity (paper identity is the funder).
    monkeypatch.setenv("PRIMARY_FUNDER_ADDRESS", FUNDER_A)
    monkeypatch.setenv("SECONDARY_FUNDER_ADDRESS", FUNDER_B)
    monkeypatch.delenv("PRIMARY_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("SECONDARY_PRIVATE_KEY", raising=False)
    users = _load(_paper_yaml(), tmp_path, monkeypatch, set_env=False)
    assert len(users) == 2


def test_duplicate_follow_leader_rejected_case_insensitive(tmp_path, monkeypatch):
    # Same wallet written twice with different letter casing is ONE leader.
    leader_mixed = "0x" + "Ab12" * 10
    upper = "0x" + leader_mixed[2:].upper()
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "{leader_mixed}"
        sizing:
          mode: fixed
          quote_amount: "1.00"
      - leader_address: "{upper}"
        sizing:
          mode: fixed
          quote_amount: "1.00"
"""
    )
    with pytest.raises(ValueError, match="duplicate"):
        _load(text, tmp_path, monkeypatch)


def test_invalid_leader_address_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows:
      - leader_address: "0x1234"
        sizing:
          mode: fixed
          quote_amount: "1.00"
"""
    )
    with pytest.raises(ValueError, match="leader_address"):
        _load(text, tmp_path, monkeypatch)


@pytest.mark.parametrize("bad_id", ["default", "", "has space", "with/slash", "../escape", "a" * 65])
def test_unsafe_or_reserved_copier_ids_rejected(tmp_path, monkeypatch, bad_id):
    text = _paper_yaml(
        f"""  - id: "{bad_id}"
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="id"):
        _load(text, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Unknown fields rejected at every level
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("fragment", "match"),
    [
        ("schema_version: 2\nextra_top: true\ncopiers: []", "unknown"),
        (
            '  - id: primary\n    mode: paper\n    sig_type: 3\n    wallet:\n      funder_address_env: PRIMARY_FUNDER_ADDRESS\n    limits:\n      max_order_quote: "5.00"\n      max_open_exposure_quote: "50.00"\n    follows: []',
            "unknown",
        ),
        (
            '  - id: primary\n    mode: paper\n    wallet:\n      funder_address_env: PRIMARY_FUNDER_ADDRESS\n      private_key: "0xabc"\n    limits:\n      max_order_quote: "5.00"\n      max_open_exposure_quote: "50.00"\n    follows: []',
            "credential",
        ),
        (
            '  - id: primary\n    mode: paper\n    wallet:\n      funder_address_env: PRIMARY_FUNDER_ADDRESS\n    limits:\n      max_order_quote: "5.00"\n      max_open_exposure_quote: "50.00"\n      max_order_usd: "5.00"\n    follows: []',
            "unknown",
        ),
        (
            '  - id: primary\n    mode: paper\n    wallet:\n      funder_address_env: PRIMARY_FUNDER_ADDRESS\n    limits:\n      max_order_quote: "5.00"\n      max_open_exposure_quote: "50.00"\n    follows:\n      - leader_address: "%s"\n        enabled: true\n        allocation_pct: "0.5"\n        sizing:\n          mode: fixed\n          quote_amount: "1.00"'
            % LEADER_1,
            "unknown",
        ),
        (
            '  - id: primary\n    mode: paper\n    wallet:\n      funder_address_env: PRIMARY_FUNDER_ADDRESS\n    limits:\n      max_order_quote: "5.00"\n      max_open_exposure_quote: "50.00"\n    follows:\n      - leader_address: "%s"\n        sizing:\n          mode: fixed\n          quote_amount: "1.00"\n          extra: 1'
            % LEADER_1,
            "unknown",
        ),
    ],
)
def test_unknown_fields_rejected_at_every_level(tmp_path, monkeypatch, fragment, match):
    text = fragment if fragment.startswith("schema_version") else f"schema_version: 2\ncopiers:\n{fragment}"
    with pytest.raises(ValueError, match=match):
        _load(text, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Schema version and required keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "copiers: []",
        'schema_version: "1"\ncopiers: []',
        "schema_version: 1\ncopiers: []",
        "schema_version: 3\ncopiers: []",
        "schema_version: 2",
    ],
)
def test_bad_schema_version_or_missing_copiers_rejected(tmp_path, monkeypatch, text):
    with pytest.raises(ValueError, match="schema_version|copiers"):
        _load(text, tmp_path, monkeypatch)


def test_missing_wallet_or_follows_key_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="wallet"):
        _load(
            _paper_yaml(
                """  - id: primary
    mode: paper
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
            ),
            tmp_path,
            monkeypatch,
        )
    with pytest.raises(ValueError, match="follows"):
        _load(
            _paper_yaml(
                """  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
"""
            ),
            tmp_path,
            monkeypatch,
        )


# ---------------------------------------------------------------------------
# Mode: single process-wide paper/live
# ---------------------------------------------------------------------------


def test_mixed_modes_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(_DEFAULT_PAPER_COPIERS.replace("    mode: paper", "    mode: live", 1))
    with pytest.raises(ValueError, match="mixed"):
        _load(text, tmp_path, monkeypatch)


def test_uniform_mode_disagreeing_with_trading_mode_rejected(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="trading_mode"):
        _load(_paper_yaml(_live_copier()), tmp_path, monkeypatch, set_env=False, TRADING_MODE="paper")

    with pytest.raises(ValueError, match="trading_mode"):
        _load(_paper_yaml(), tmp_path, monkeypatch, TRADING_MODE="live")


def test_unknown_mode_value_rejected(tmp_path, monkeypatch):
    text = _paper_yaml(
        """  - id: primary
    mode: shadow
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="mode"):
        _load(text, tmp_path, monkeypatch)


# ---------------------------------------------------------------------------
# Env-var reference validation (names, never values)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_name", ["", "0XKEY", "HAS SPACE", "DOLLAR$"])
def test_invalid_env_reference_names_rejected(tmp_path, monkeypatch, bad_name):
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: "{bad_name}"
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="funder_address_env"):
        _load(text, tmp_path, monkeypatch)


def test_private_key_env_value_that_is_actually_a_key_is_rejected(tmp_path, monkeypatch):
    # An operator pasting the KEY instead of the variable name must get a
    # config error, not a silently unset reference.
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: "{KEY_A}"
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="private_key_env"):
        _load(text, tmp_path, monkeypatch)


def test_env_reference_error_never_echoes_supplied_value(tmp_path, monkeypatch):
    # T14 invariant: no error message ever echoes a supplied secret value.
    # A raw key pasted into private_key_env (the exact mistake the env-name
    # guard exists for) must be rejected WITHOUT the canary appearing in
    # the error text (which flows to startup print_error, doctor detail,
    # and config validate stdout).
    import uuid

    canary = "0x" + (uuid.uuid4().hex + uuid.uuid4().hex)[:64]
    text = _paper_yaml(
        f"""  - id: primary
    mode: paper
    wallet:
      funder_address_env: PRIMARY_FUNDER_ADDRESS
      private_key_env: "{canary}"
    limits:
      max_order_quote: "5.00"
      max_open_exposure_quote: "50.00"
    follows: []
"""
    )
    with pytest.raises(ValueError, match="private_key_env") as excinfo:
        _load(text, tmp_path, monkeypatch)
    assert canary not in str(excinfo.value)
    assert canary[:16] not in str(excinfo.value)


# ---------------------------------------------------------------------------
# File path safety
# ---------------------------------------------------------------------------


def test_copiers_file_must_be_yaml_suffix(tmp_path, monkeypatch):
    path = tmp_path / "copiers.json"
    path.write_text("{}")
    monkeypatch.chdir(tmp_path)
    cfg = _config(COPIERS_CONFIG_FILE=str(path))
    with pytest.raises(ValueError, match="\\.yml"):
        load_runtime_users([], cfg)


def test_copiers_file_confined_to_home_or_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cfg = _config(COPIERS_CONFIG_FILE="/etc/copiers.yml")
    with pytest.raises(ValueError, match="\\$HOME|working directory"):
        load_runtime_users([], cfg)


def test_copiers_file_must_parse_as_mapping(tmp_path, monkeypatch):
    path = tmp_path / "copiers.yml"
    path.write_text("- just\n- a\n- list\n")
    monkeypatch.chdir(tmp_path)
    cfg = _config(COPIERS_CONFIG_FILE=str(path))
    with pytest.raises(ValueError, match="mapping|object"):
        load_runtime_users([], cfg)


# ---------------------------------------------------------------------------
# Migration: v2 authoritative; both files = error; legacy → one copier
# ---------------------------------------------------------------------------


def test_both_copiers_file_and_v1_multi_user_file_rejected(tmp_path, monkeypatch):
    import dataclasses

    v1_path = tmp_path / "users.json"
    v1_path.write_text(json.dumps({"users": []}))
    copiers_path = _copiers_file(tmp_path, monkeypatch, _paper_yaml())

    # Rejected at config-load time so `pmcopy config validate` catches it.
    with pytest.raises(Exception, match="copiers_config_file"):
        _config(COPIERS_CONFIG_FILE=copiers_path, MULTI_USER_CONFIG_FILE=str(v1_path))

    # Defense-in-depth: the roster loader refuses a hand-built AppConfig
    # carrying both files even when semantic validation was bypassed.
    base = _config(COPIERS_CONFIG_FILE=copiers_path)
    conflicted = dataclasses.replace(
        base,
        app=dataclasses.replace(base.app, multi_user_config_file=str(v1_path)),
    )
    with pytest.raises(ValueError, match="multi_user_config_file"):
        load_runtime_users([], conflicted)


def test_semantic_validator_waives_single_account_wallet_requirements(tmp_path, monkeypatch):
    # Paper without any legacy FUNDER_ADDRESS/PRIVATE_KEY is valid when the
    # copiers file is the wallet authority; the per-copier references are
    # validated by the roster loader instead.
    cfg = _config(COPIERS_CONFIG_FILE=_copiers_file(tmp_path, monkeypatch, _paper_yaml()))
    assert cfg.app.copiers_config_file.endswith("copiers.yml")

    # ...and live without the global key is equally waived with the roster.
    cfg = _config(
        COPIERS_CONFIG_FILE=_copiers_file(tmp_path, monkeypatch, _paper_yaml()),
        TRADING_MODE="live",
    )
    assert cfg.execution.trading_mode == "live"

    # Without the roster file the existing requirements still fail closed.
    with pytest.raises(Exception, match="FUNDER_ADDRESS"):
        _config(FUNDER_ADDRESS="")
    with pytest.raises(Exception, match="PRIVATE_KEY"):
        _config(FUNDER_ADDRESS=FUNDER_A, TRADING_MODE="live", PRIVATE_KEY="")


def test_copiers_file_is_authoritative_over_legacy_env_wallet(monkeypatch, tmp_path):
    # Legacy PMCOPY__WALLET__* values may still be set in the environment;
    # with a copiers roster they must NOT become an extra or merged account.
    legacy_key = "0x" + "e" * 64
    legacy_funder = "0x" + "e" * 40
    assert legacy_key not in {KEY_A, KEY_B} and legacy_funder not in {FUNDER_A, FUNDER_B}
    monkeypatch.setenv("PMCOPY__WALLET__PRIVATE_KEY", legacy_key)
    monkeypatch.setenv("PMCOPY__WALLET__FUNDER_ADDRESS", legacy_funder)
    _set_default_env(monkeypatch)
    cfg = _config(COPIERS_CONFIG_FILE=_copiers_file(tmp_path, monkeypatch, _paper_yaml()))
    users = load_runtime_users([], cfg)

    assert [u.user_id for u in users] == ["primary", "secondary"]
    resolved_keys = {u.wallet.private_key.get_secret_value() for u in users}
    resolved_funders = {u.wallet.funder_address for u in users}
    assert resolved_keys == {KEY_A, KEY_B}
    assert resolved_funders == {FUNDER_A, FUNDER_B}
    assert legacy_key not in resolved_keys
    assert legacy_funder not in resolved_funders


def test_legacy_single_account_maps_to_one_deterministic_copier(caplog):
    # No roster file: the legacy single-account config is exactly ONE
    # copier whose id is the EXISTING persisted identity (the funder
    # address) — never combined with a copiers list — and the mapping is
    # announced in the log.
    import logging

    cfg = _config(FUNDER_ADDRESS=FUNDER_A, PRIVATE_KEY=KEY_A)
    with caplog.at_level(logging.INFO, logger="pmcopy.services.multiUserConfig"):
        users = load_runtime_users([LEADER_1], cfg)
    assert len(users) == 1
    assert users[0].user_id == FUNDER_A
    assert users[0].follows_explicit is False
    assert any(
        "legacy single-account" in record.message.lower() for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Selection (--copier): routing AND runtime ownership
# ---------------------------------------------------------------------------


def test_select_runtime_copiers_filters_subset():
    from pmcopy.services.copierConfig import select_runtime_copiers

    users = [
        RuntimeUser(user_id="primary", wallet=MagicMock()),
        RuntimeUser(user_id="secondary", wallet=MagicMock()),
    ]
    selected = select_runtime_copiers(users, "secondary")
    assert [u.user_id for u in selected] == ["secondary"]
    assert select_runtime_copiers(users, None) is users


def test_select_runtime_copiers_rejects_unknown_id():
    from pmcopy.services.copierConfig import select_runtime_copiers

    users = [RuntimeUser(user_id="primary", wallet=MagicMock())]
    with pytest.raises(ValueError, match="primary"):
        select_runtime_copiers(users, "ghost")


def test_load_respects_copier_selection_through_seam(tmp_path, monkeypatch):
    users = _load(_paper_yaml(), tmp_path, monkeypatch)
    from pmcopy.services.copierConfig import select_runtime_copiers

    assert [u.user_id for u in select_runtime_copiers(users, "secondary")] == ["secondary"]


# ---------------------------------------------------------------------------
# End-to-end smoke: schema v2 → RuntimeUser → build_user_state for 2 copiers
# ---------------------------------------------------------------------------


def test_schema_v2_to_build_user_state_two_copiers(tmp_path, monkeypatch):
    from pmcopy.orchestrators import runtime_bootstrap

    users = _load(_paper_yaml(), tmp_path, monkeypatch)
    assert len(users) == 2

    states = {}
    with (
        patch("pmcopy.orchestrators.runtime_bootstrap.PositionManager"),
        patch("pmcopy.orchestrators.runtime_bootstrap.ExecutionService"),
    ):
        for runtime_user in users:
            states[runtime_user.user_id] = runtime_bootstrap.build_user_state(
                runtime_user,
                client=MagicMock(),
                pool=MagicMock(),
                config=_config(
                    COPIERS_CONFIG_FILE=_copiers_file(tmp_path, monkeypatch, _paper_yaml())
                ),
                clob_gateway=MagicMock(),
            )

    assert set(states) == {"primary", "secondary"}
    # Account-owned collaborators are distinct per copier — no shared state.
    assert states["primary"].execution_coordinator is not states["secondary"].execution_coordinator
    assert states["primary"].position_coordinator is not states["secondary"].position_coordinator
    assert states["primary"].pnl_tracker is not states["secondary"].pnl_tracker
