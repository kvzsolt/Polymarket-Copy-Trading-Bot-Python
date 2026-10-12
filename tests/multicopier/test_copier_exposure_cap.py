"""Copier open-exposure cap (schema v2 ``limits.max_open_exposure_quote``).

Config that documents a limit that does not exist is dangerous: an operator
relies on it. These are the focused unit-seam tests for the Task 24 fix —
the flag that marks an EXPLICITLY configured copier-level exposure cap must
survive every roster surface (schema v2 copiers file, v1 multi-user JSON
with the field present) while the legacy default-derived surfaces (v1 field
absent, config-less single-account fallback) stay unmarked so their behavior
is exactly as before the fix. The behavioral proof (rejection at admission,
no reservation consumed, in-flight reservations counting toward the cap,
protective exits never blocked) runs against real SQL in
``test_multicopier_postgres.py``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.orchestrators.runtime_bootstrap import build_trading_context
from pmcopy.services.copierConfig import load_runtime_users_from_copiers
from pmcopy.services.multiUserConfig import load_runtime_users

from tests.multicopier.conftest import (
    FUNDER_ALPHA,
    LEADER_1,
    USER_ALPHA,
    build_account_state,
    follow,
    make_config,
    runtime_user,
    synth_private_key,
)


def _write_copiers(tmp_path, body: str) -> str:
    path = tmp_path / "copiers.yml"
    path.write_text("schema_version: 2\ncopiers:\n" + body, encoding="utf-8")
    return str(path)


_GOOD_BODY = f"""\
    - id: {USER_ALPHA}
      enabled: true
      mode: paper
      wallet:
        funder_address_env: ALPHA_FUNDER
      limits:
        max_order_quote: "5.00"
        max_open_exposure_quote: "50.00"
      follows:
        - leader_address: "{LEADER_1}"
          enabled: true
          sizing: {{mode: fixed, quote_amount: "2.00"}}
    """


def test_schema_v2_limit_is_marked_explicit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALPHA_FUNDER", FUNDER_ALPHA)
    users = load_runtime_users_from_copiers(_write_copiers(tmp_path, _GOOD_BODY), make_config())

    assert len(users) == 1
    entry = users[0].follows[LEADER_1]
    assert entry.max_market_exposure_usdc == Decimal("50.00")
    assert entry.market_exposure_cap_explicit is True


def test_v1_follow_with_explicit_field_is_marked_explicit(tmp_path, monkeypatch):
    import json

    monkeypatch.chdir(tmp_path)

    payload = {
        "users": [
            {
                "user_id": USER_ALPHA,
                "private_key": synth_private_key("f"),
                "funder_address": FUNDER_ALPHA,
                "follows": [
                    {
                        "leader_id": LEADER_1,
                        "max_market_exposure_usdc": "75.00",
                    }
                ],
            }
        ]
    }
    roster = tmp_path / "users.json"
    roster.write_text(json.dumps(payload), encoding="utf-8")
    config = make_config(**{"app": {"multi_user_config_file": str(roster)}})
    users = load_runtime_users([], config)

    entry = users[0].follows[LEADER_1]
    assert entry.max_market_exposure_usdc == Decimal("75.00")
    assert entry.market_exposure_cap_explicit is True


def test_v1_follow_without_the_field_stays_unmarked(tmp_path, monkeypatch):
    import json

    monkeypatch.chdir(tmp_path)

    payload = {
        "users": [
            {
                "user_id": USER_ALPHA,
                "private_key": synth_private_key("f"),
                "funder_address": FUNDER_ALPHA,
                "follows": [{"leader_id": LEADER_1}],
            }
        ]
    }
    roster = tmp_path / "users.json"
    roster.write_text(json.dumps(payload), encoding="utf-8")
    config = make_config(**{"app": {"multi_user_config_file": str(roster)}})
    users = load_runtime_users([], config)

    entry = users[0].follows[LEADER_1]
    # Legacy default-derived value: present, but NOT an enforced cap.
    assert entry.max_market_exposure_usdc == Decimal("300")
    assert entry.market_exposure_cap_explicit is False


def test_legacy_single_account_fallback_stays_unmarked():
    config = make_config()
    users = load_runtime_users([LEADER_1], config)

    entry = users[0].follows[LEADER_1]
    assert entry.market_exposure_cap_explicit is False


@pytest.mark.asyncio
async def test_build_trading_context_propagates_the_explicit_flag():
    config = make_config()
    explicit_follow = follow(
        USER_ALPHA, LEADER_1, max_market_exposure_usdc="50", explicit_exposure_cap=True
    )
    legacy_follow = follow(USER_ALPHA, LEADER_1, max_market_exposure_usdc="50")
    user = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: explicit_follow})
    state = build_account_state(user, config)
    ctx = await build_trading_context(state, explicit_follow, config)

    assert ctx.max_market_exposure_usdc == Decimal("50")
    assert ctx.market_exposure_cap_explicit is True

    legacy_user = runtime_user(USER_ALPHA, FUNDER_ALPHA, {LEADER_1: legacy_follow})
    legacy_state = build_account_state(legacy_user, config)
    legacy_ctx = await build_trading_context(legacy_state, legacy_follow, config)
    assert legacy_ctx.max_market_exposure_usdc == Decimal("50")
    assert legacy_ctx.market_exposure_cap_explicit is False


# ---------------------------------------------------------------------------
# Headroom arithmetic (the exposure-accounting seam the pipeline consumes)
# ---------------------------------------------------------------------------


def test_copier_exposure_headroom_arithmetic():
    from pmcopy.orchestrators.trade_pipeline import copier_open_exposure_headroom

    # open + reserved + nothing left under the cap.
    assert copier_open_exposure_headroom(
        cap=Decimal("60"), open_exposure=Decimal("50"), reserved_quote=Decimal("5")
    ) == Decimal("5")
    # At the cap → zero headroom (a new entry must be rejected).
    assert copier_open_exposure_headroom(
        cap=Decimal("60"), open_exposure=Decimal("50"), reserved_quote=Decimal("10")
    ) == Decimal("0")
    # Over the cap already → negative headroom.
    assert copier_open_exposure_headroom(
        cap=Decimal("60"), open_exposure=Decimal("70"), reserved_quote=Decimal("0")
    ) < Decimal("0")
    # Unknown open exposure is NOT zero — fail closed.
    assert (
        copier_open_exposure_headroom(
            cap=Decimal("60"), open_exposure=None, reserved_quote=Decimal("0")
        )
        is None
    )
