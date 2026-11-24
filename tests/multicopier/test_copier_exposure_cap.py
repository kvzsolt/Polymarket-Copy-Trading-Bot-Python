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

from pmcopy.orchestrators.bootstrap.runtime_bootstrap import build_trading_context
from pmcopy.services.accounts.copier_config import load_runtime_users_from_copiers
from pmcopy.services.accounts.multi_user_config import load_runtime_users

from tests.multicopier.conftest import (
    CONDITION,
    FUNDER_ALPHA,
    LEADER_1,
    TOKEN,
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
    from pmcopy.orchestrators.pipeline.trade_pipeline import copier_open_exposure_headroom

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


# ---------------------------------------------------------------------------
# Fix round 2 — the reservation TRANSACTION must be cap-authoritative
# ---------------------------------------------------------------------------


class _CapRecordingRepo:
    """Repo double capturing what the checked BUY transaction receives."""

    def __init__(self, existing=None):
        self.existing = existing
        self.checked_calls: list[dict] = []
        self.sum_calls: list[str] = []

    async def get_active_by_intent_key(self, _key):
        return self.existing

    async def sum_active_quote_for_user(self, user_id):
        self.sum_calls.append(user_id)
        return Decimal("0")

    async def create_buy_reservation_checked_by_intent(
        self,
        intent,
        reserved_usdc,
        expires_at,
        available_balance,
        *,
        open_exposure_cap=None,
        confirmed_open_exposure=None,
    ):
        from datetime import datetime, timezone

        from pmcopy.domain.order_lifecycle import Reservation

        self.checked_calls.append(
            {
                "reserved_usdc": reserved_usdc,
                "open_exposure_cap": open_exposure_cap,
                "confirmed_open_exposure": confirmed_open_exposure,
            }
        )
        return (
            None,
            Reservation(
                id="res-1",
                user_id=intent.key.user_id,
                intent_key=intent.key.stable_key(),
                condition_id=intent.key.condition_id,
                token_id=intent.key.token_id,
                side=intent.key.side,
                reason=intent.key.reason,
                reserved_usdc=Decimal(str(reserved_usdc)),
                reserved_size=Decimal("0"),
                status="active",
                created_at=datetime.now(timezone.utc),
                expires_at=datetime.now(timezone.utc),
            ),
            None,
        )


def _buy_intent(user_id: str = USER_ALPHA):
    from datetime import datetime, timezone

    from pmcopy.domain.order_lifecycle import (
        IntentReason,
        IntentSide,
        OrderIntent,
        OrderIntentKey,
    )

    return OrderIntent(
        key=OrderIntentKey(
            user_id=user_id,
            leader_wallet=LEADER_1,
            condition_id=CONDITION,
            token_id=TOKEN,
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
            source_event_id="0xsig-cap",
        ),
        size=Decimal("100"),
        price=Decimal("0.50"),
        max_slippage_bps=200,
        created_at=datetime.fromtimestamp(1_000, tz=timezone.utc),
    )


def _service(repo, *, cap=None, reader=None):
    from pmcopy.services.execution.reservation_service import ReservationService

    async def _balance(_address):
        return Decimal("1000")

    return ReservationService(
        reservations_repo=repo,
        positions_repo=None,
        get_balance_func=_balance,
        funder_address=FUNDER_ALPHA,
        open_exposure_cap=cap,
        confirmed_exposure_reader=reader,
    )


@pytest.mark.asyncio
async def test_capped_reservation_forwards_cap_and_confirmed_exposure_to_the_transaction():
    repo = _CapRecordingRepo()

    async def _reader(user_id):
        return Decimal("25")

    service = _service(repo, cap=Decimal("60"), reader=_reader)
    reservation = await service.reserve_for_intent(_buy_intent())

    assert reservation.id == "res-1"
    assert repo.checked_calls == [
        {
            "reserved_usdc": Decimal("50"),
            "open_exposure_cap": Decimal("60"),
            "confirmed_open_exposure": Decimal("25"),
        }
    ]


@pytest.mark.asyncio
async def test_uncapped_reservation_passes_no_cap_parameters_and_never_reads_exposure():
    repo = _CapRecordingRepo()
    reader_calls: list[str] = []

    async def _reader(user_id):
        reader_calls.append(user_id)
        return Decimal("0")

    service = _service(repo, cap=None, reader=_reader)
    await service.reserve_for_intent(_buy_intent())

    # Legacy accounts: NO cap parameters reach the reservation transaction
    # and the exposure reader is never invoked — byte-identical overhead.
    assert repo.checked_calls == [
        {
            "reserved_usdc": Decimal("50"),
            "open_exposure_cap": None,
            "confirmed_open_exposure": None,
        }
    ]
    assert reader_calls == []


@pytest.mark.asyncio
async def test_transaction_cap_rejection_maps_to_the_cap_error():
    from pmcopy.services.execution.reservation_service import (
        CopierOpenExposureCapExceeded,
    )

    class _CapRejectingRepo(_CapRecordingRepo):
        async def create_buy_reservation_checked_by_intent(
            self, intent, reserved_usdc, expires_at, available_balance, **_kwargs
        ):
            self.checked_calls.append({"rejected": True})
            return None, None, "copier_open_exposure_cap"

    async def _reader(_user_id):
        return Decimal("0")

    service = _service(_CapRejectingRepo(), cap=Decimal("60"), reader=_reader)
    with pytest.raises(CopierOpenExposureCapExceeded):
        await service.reserve_for_intent(_buy_intent())


@pytest.mark.asyncio
async def test_cap_with_unreadable_confirmed_exposure_fails_closed():
    from pmcopy.services.execution.reservation_service import (
        CopierOpenExposureCapExceeded,
    )

    async def _broken_reader(_user_id):
        raise RuntimeError("positions unavailable")

    repo = _CapRecordingRepo()
    service = _service(repo, cap=Decimal("60"), reader=_broken_reader)
    with pytest.raises(CopierOpenExposureCapExceeded):
        await service.reserve_for_intent(_buy_intent())
    assert repo.checked_calls == [], "no transaction may open without evidence"
