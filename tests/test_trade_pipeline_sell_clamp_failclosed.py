"""P2-m: SELL clamp must fail closed when live holdings are unavailable.

The SELL path clamps the sell size to the live on-chain holdings fetched via
``_fetch_live_position_size``. When that fetch returns ``None`` (transport
error / unknown) the bot cannot determine the real sellable size, so selling
against the (possibly stale) DB position size is unsafe — it can oversell or
sell shares we no longer hold. The pipeline must FAIL CLOSED: skip the SELL,
log ``sell_clamp_live_holdings_unavailable``, submit nothing, and leave the
trade unprocessed so a later poll can retry once the venue recovers.

This mirrors ``test_sell_not_blocked_when_risk_block_active`` from
``test_trade_pipeline_safety`` (which pins the live-size = real-number happy
path) and reuses its Postgres-backed harness, so the assertions exercise the
real ``execute_trades`` SELL control flow. The harness ``pytest.skip``s when
Docker / testcontainers are unavailable, consistent with the other
trade-pipeline tests.
"""

from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from pmcopy.orchestrators.trade_pipeline import build_trade_key, execute_trades

# Reuse the proven Postgres-backed harness + fixtures from the safety suite.
from tests.test_trade_pipeline_safety import (  # noqa: E402
    FAKE_TOKEN_ID,
    _make_context,
    _pg_pool,
    _signal,
)


@pytest.mark.asyncio
async def test_sell_skipped_failclosed_when_live_holdings_unavailable(tmp_path):
    """live-holdings fetch -> None must skip the SELL and submit nothing."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        # Seed a non-zero DB position so the *only* thing that stops the SELL is
        # the unavailable live-holdings read (the fail-closed branch under test).
        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.50"),
            )
        )

        signal = _signal(side="SELL", tx_hash="0xsell-live-unavailable")
        trade_key = build_trade_key("user1", "leader1", signal)

        # None = live size unknown (transport error). MUST NOT sell on DB size.
        with patch(
            "pmcopy.orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=None,
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "sell_clamp_live_holdings_unavailable"

        # Fail closed: no order was ever submitted.
        exec_svc.execute.assert_not_called()

        # Transient/unknown: the trade must remain retryable (NOT marked
        # processed), so the next poll can retry the protective SELL.
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_sell_proceeds_when_live_holdings_known(tmp_path):
    """Control: a real live size (not None) still flows through to submit.

    Pins that the fail-closed branch is scoped strictly to ``None`` and does not
    regress the known-size happy path.
    """
    async with _pg_pool() as pool:
        from pmcopy.services.executionService import ExecutionResult

        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=True,
            order_id="ord1",
            status="filled",
            requested_amount=Decimal("10"),
            filled_amount=Decimal("10"),
            avg_price=Decimal("0.49"),
            fees_paid=Decimal("0.01"),
            error=None,
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.50"),
            )
        )

        signal = _signal(side="SELL", tx_hash="0xsell-live-known")
        with patch(
            "pmcopy.orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("10"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].reason != "sell_clamp_live_holdings_unavailable"
        exec_svc.execute.assert_awaited_once()


# ---------------------------------------------------------------------------
# execution.sell_fraction_missing_policy: a SELL whose leader fraction is
# unknown (positions API outage) must not dump the whole position by default.
# ---------------------------------------------------------------------------


def test_derive_sell_tokens_policy_unit():
    from types import SimpleNamespace

    from pmcopy.orchestrators.trade_pipeline import _derive_sell_tokens

    sig = SimpleNamespace(condition_id="cond1")
    pos = SimpleNamespace(size_tokens=Decimal("10"))

    assert _derive_sell_tokens(
        sig, pos, {"_leader_sell_fraction": 0.5}, missing_policy="skip"
    ) == Decimal("5")
    assert _derive_sell_tokens(sig, pos, {}, missing_policy="skip") is None
    assert (
        _derive_sell_tokens(
            sig,
            pos,
            {"_leader_sell_fraction_unavailable": True},
            missing_policy="skip",
        )
        is None
    )
    assert _derive_sell_tokens(sig, pos, {}, missing_policy="full_close") == Decimal("10")


@pytest.mark.asyncio
async def test_sell_skipped_retryable_when_fraction_missing_policy_skip(tmp_path):
    """No fraction annotation + policy=skip -> retryable skip, nothing sold."""
    async with _pg_pool() as pool:
        exec_svc = AsyncMock()
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc)

        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.50"),
            )
        )

        signal = _signal(
            side="SELL",
            tx_hash="0xsell-fraction-missing",
            leader_sell_fraction=None,
        )
        trade_key = build_trade_key("user1", "leader1", signal)

        with patch(
            "pmcopy.orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("10"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].status == "skipped"
        assert results[0].reason == "sell_fraction_missing_skipped"
        exec_svc.execute.assert_not_called()
        # Retryable: the claim is released so a later poll (with a recovered
        # positions API and a fresh annotation) can copy the exit late.
        assert await ctx.orders_repo.has_processed_trade(trade_key) is False


@pytest.mark.asyncio
async def test_sell_full_close_policy_preserves_legacy_dump(tmp_path):
    """policy=full_close keeps the legacy behaviour: unknown fraction sells all."""
    async with _pg_pool() as pool:
        import dataclasses

        from pmcopy.services.executionService import ExecutionResult
        from tests.test_trade_pipeline_safety import _config

        exec_svc = AsyncMock()
        exec_svc.execute.return_value = ExecutionResult(
            accepted=True,
            order_id="ord1",
            status="filled",
            requested_amount=Decimal("10"),
            filled_amount=Decimal("10"),
            avg_price=Decimal("0.49"),
            fees_paid=Decimal("0.01"),
            error=None,
        )
        cfg = _config()
        cfg = dataclasses.replace(
            cfg,
            execution=dataclasses.replace(cfg.execution, sell_fraction_missing_policy="full_close"),
        )
        ctx = _make_context(pool, tmp_path, execution_service=exec_svc, config=cfg)

        from pmcopy.storage.postgres.positions_repo import PositionRecord

        await ctx.positions_repo.upsert_position(
            PositionRecord(
                user_id="user1",
                condition_id="cond1",
                token_id=FAKE_TOKEN_ID,
                leader_id="leader1",
                size_tokens=Decimal("10"),
                avg_entry_price=Decimal("0.50"),
            )
        )

        signal = _signal(
            side="SELL",
            tx_hash="0xsell-fraction-full-close",
            leader_sell_fraction=None,
        )
        with patch(
            "pmcopy.orchestrators.trade_pipeline._fetch_live_position_size",
            new_callable=AsyncMock,
            return_value=Decimal("10"),
        ):
            results = await execute_trades(ctx, "leader1", [signal], time.time())

        assert len(results) == 1
        assert results[0].reason != "sell_fraction_missing_skipped"
        exec_svc.execute.assert_awaited_once()
