"""Tests for the opt-in on-chain CTF auto-redemption service.

These exercise the eligibility/grouping/idempotency logic in pure form:
the data-api fetch and the Web3 redeem call are both injected, so no test
touches the network or Polygon. The two-phase gate is the contract under
test — a position is only redeemed when the data-api flags it ``redeemable``
AND the local market cache shows the token pinned at a terminal price.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from config.app_config import load_config
from services.redemption.redemption_service import (
    RedemptionService,
    RedemptionTxResult,
    _condition_id_bytes32,
    _parse_position,
)

_VALID_PRIVATE_KEY = "0x" + "a" * 64
_VALID_FUNDER = "0x" + "b" * 40
_VALID_USER = "0x" + "c" * 40


def _make_config(**overrides: str):
    env = {
        "PRIVATE_KEY": _VALID_PRIVATE_KEY,
        "FUNDER_ADDRESS": _VALID_FUNDER,
        "RPC_URL": "https://polygon-rpc.example.com",
        "CHAIN_ID": "137",
        "USER_ADDRESS": _VALID_USER,
        "ENABLE_AUTO_REDEEM": "true",
        "REDEEM_REQUIRE_SIGNER_MATCH": "false",
    }
    env.update(overrides)
    return load_config(env)


class _FakeStore:
    """Minimal MarketStateStore stand-in.

    ``prices`` maps token_id -> Decimal terminal price, or None to model a
    token the cache has never seen (get_state returns None).
    """

    def __init__(self, prices: dict[str, Decimal | None]) -> None:
        self._prices = prices

    def get_state(self, token_id: str):
        price = self._prices.get(token_id)
        if price is None:
            return None
        return SimpleNamespace(
            last_trade_price=price,
            best_bid=price,
            best_ask=price,
        )


def _position(
    token_id: str,
    condition_id: str,
    *,
    size: str = "100",
    value: str = "99",
    redeemable: bool = True,
) -> dict[str, Any]:
    return {
        "asset": token_id,
        "conditionId": condition_id,
        "size": size,
        "currentValue": value,
        "redeemable": redeemable,
        "title": "Test Market",
        "outcome": "Yes",
    }


def _build_service(
    *,
    positions: list[dict[str, Any]],
    prices: dict[str, Decimal | None],
    redeem_results: dict[str, RedemptionTxResult] | None = None,
    calls_out: list[str] | None = None,
    config=None,
):
    cfg = config or _make_config()

    async def _fetcher(_wallet: str) -> list[dict[str, Any]]:
        return positions

    async def _redeemer(condition_id: str) -> RedemptionTxResult:
        if calls_out is not None:
            calls_out.append(condition_id)
        if redeem_results and condition_id in redeem_results:
            return redeem_results[condition_id]
        return RedemptionTxResult(
            success=True, condition_id=condition_id, tx_hash="0xdeadbeef"
        )

    return RedemptionService(
        config=cfg,
        wallet_address=_VALID_FUNDER,
        market_store=_FakeStore(prices),
        private_key=_VALID_PRIVATE_KEY,
        position_fetcher=_fetcher,
        condition_redeemer=_redeemer,
    )


# ---------------------------------------------------------------------------
# Two-phase eligibility gate
# ---------------------------------------------------------------------------


def test_redeems_only_terminal_cached_price():
    """Redeemable position at a terminal cache price fires; mid-price does not."""
    calls: list[str] = []
    svc = _build_service(
        positions=[
            _position("tokA", "condWin"),
            _position("tokB", "condMid"),
        ],
        prices={"tokA": Decimal("0.99"), "tokB": Decimal("0.50")},
        calls_out=calls,
    )
    results = asyncio.run(svc.scan_once())

    assert calls == ["condWin"]
    assert [r.condition_id for r in results] == ["condWin"]
    assert all(r.success for r in results)


def test_loss_side_terminal_price_redeems():
    """A position pinned at REDEEM_RESOLVED_LOW (0.01) is also redeemable."""
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokL", "condLoss")],
        prices={"tokL": Decimal("0.01")},
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    assert calls == ["condLoss"]


def test_skips_non_redeemable_even_at_terminal_price():
    """Cache says terminal, but data-api ``redeemable`` is false → no redeem."""
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokA", "condX", redeemable=False)],
        prices={"tokA": Decimal("0.99")},
        calls_out=calls,
    )
    results = asyncio.run(svc.scan_once())
    assert calls == []
    assert results == []


def test_skips_when_cache_has_no_state():
    """Redeemable, but the cache never saw the token → fail-closed skip."""
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokA", "condX")],
        prices={"tokA": None},
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    assert calls == []


def test_skips_dust_below_zero_threshold():
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokA", "condX", size="0.00001")],
        prices={"tokA": Decimal("0.99")},
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    assert calls == []


# ---------------------------------------------------------------------------
# Grouping, idempotency, caps
# ---------------------------------------------------------------------------


def test_groups_by_condition_single_redeem():
    """Two outcome tokens under one condition → one redeemPositions call."""
    calls: list[str] = []
    svc = _build_service(
        positions=[
            _position("tokYes", "condShared"),
            _position("tokNo", "condShared"),
        ],
        prices={"tokYes": Decimal("0.99"), "tokNo": Decimal("0.01")},
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    assert calls == ["condShared"]


def test_idempotent_does_not_resubmit_after_success():
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokA", "condX")],
        prices={"tokA": Decimal("0.99")},
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    asyncio.run(svc.scan_once())
    assert calls == ["condX"]  # second scan is a no-op


def test_failed_redeem_is_retried_next_scan():
    """A reverted/failed redeem is NOT marked done, so it retries."""
    calls: list[str] = []
    svc = _build_service(
        positions=[_position("tokA", "condX")],
        prices={"tokA": Decimal("0.99")},
        redeem_results={
            "condX": RedemptionTxResult(
                success=False, condition_id="condX", error="transaction_reverted"
            )
        },
        calls_out=calls,
    )
    asyncio.run(svc.scan_once())
    asyncio.run(svc.scan_once())
    assert calls == ["condX", "condX"]


def test_max_conditions_per_tick_cap():
    calls: list[str] = []
    positions = [_position(f"tok{i}", f"cond{i}") for i in range(5)]
    prices = {f"tok{i}": Decimal("0.99") for i in range(5)}
    svc = _build_service(
        positions=positions,
        prices=prices,
        calls_out=calls,
        config=_make_config(REDEEM_MAX_CONDITIONS_PER_TICK="2"),
    )
    asyncio.run(svc.scan_once())
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# Runtime support / signer gate
# ---------------------------------------------------------------------------


def test_signer_mismatch_blocks_when_required():
    cfg = _make_config(REDEEM_REQUIRE_SIGNER_MATCH="true")
    svc = RedemptionService(
        config=cfg,
        wallet_address=_VALID_FUNDER,  # not the EOA derived from the key
        market_store=_FakeStore({}),
        private_key=_VALID_PRIVATE_KEY,
    )
    assert svc._validate_runtime_support() is False


def test_signer_match_not_required_passes():
    cfg = _make_config(REDEEM_REQUIRE_SIGNER_MATCH="false")
    svc = RedemptionService(
        config=cfg,
        wallet_address=_VALID_FUNDER,
        market_store=_FakeStore({}),
        private_key=_VALID_PRIVATE_KEY,
    )
    assert svc._validate_runtime_support() is True


def test_disabled_loop_returns_immediately():
    cfg = _make_config(ENABLE_AUTO_REDEEM="false")
    svc = RedemptionService(
        config=cfg,
        wallet_address=_VALID_FUNDER,
        market_store=_FakeStore({}),
        private_key=_VALID_PRIVATE_KEY,
    )
    # Returns without scanning or sleeping.
    asyncio.run(asyncio.wait_for(svc.loop(), timeout=1.0))


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_condition_id_bytes32_hex():
    cond = "0x" + "ab" * 32
    out = _condition_id_bytes32(cond)
    assert isinstance(out, bytes)
    assert len(out) == 32
    assert out == bytes.fromhex("ab" * 32)


def test_condition_id_bytes32_decimal():
    out = _condition_id_bytes32("255")
    assert len(out) == 32
    assert out == (255).to_bytes(32, "big")


def test_condition_id_bytes32_short_hex_left_padded():
    out = _condition_id_bytes32("0x01")
    assert len(out) == 32
    assert out[-1] == 1
    assert out[:-1] == b"\x00" * 31


def test_condition_id_bytes32_rejects_empty():
    with pytest.raises(ValueError):
        _condition_id_bytes32("")


def test_parse_position_alt_keys():
    raw = {
        "tokenID": "tok9",
        "condition_id": "cond9",
        "size": "12.5",
        "currentValue": "11.0",
        "redeemable": True,
        "slug": "some-market",
    }
    pos = _parse_position(raw)
    assert pos is not None
    assert pos.token_id == "tok9"
    assert pos.condition_id == "cond9"
    assert pos.size == Decimal("12.5")
    assert pos.current_value == Decimal("11.0")
    assert pos.redeemable is True
    assert pos.title == "some-market"


def test_parse_position_missing_ids_returns_none():
    assert _parse_position({"size": "10", "redeemable": True}) is None


def test_parse_position_redeemable_must_be_true_bool():
    # Truthy-but-not-True values must not be coerced into a redeemable flag.
    pos = _parse_position(
        {"asset": "t", "conditionId": "c", "redeemable": "yes"}
    )
    assert pos is not None
    assert pos.redeemable is False
