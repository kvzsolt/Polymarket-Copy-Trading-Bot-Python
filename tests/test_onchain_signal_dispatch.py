"""Tests for ``orchestrators.onchain_signal_dispatch.OnchainSignalDispatchAdapter``.

The adapter replaces the ``_onchain_signal_sink`` closure in ``app.main``:
it logs per-signal provenance, counts the signal per source, and routes it
either to the v1 copy-task runtime or to the legacy per-context pipeline.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace

import pytest

from orchestrators.onchain_signal_dispatch import OnchainSignalDispatchAdapter
from services.metrics import METRIC_DETECTION_SIGNALS, MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)
from storage.postgres.orders_repo import CROSS_SOURCE_CLAIM_STATUS

_LEADER = "0x" + "ab" * 20
_TX = "0x" + "cd" * 32


def _signal(*, provisional: bool = False) -> OnchainTradeSignal:
    return OnchainTradeSignal(
        leader_wallet=_LEADER,
        token_id="123456",
        side="BUY",
        size=Decimal("10"),
        price=Decimal("0.5"),
        usdc_size=Decimal("5"),
        tx_hash=_TX,
        confidence=(
            DetectionConfidence.PENDING_PROBABLE
            if provisional
            else DetectionConfidence.MINED_AUTHORITATIVE
        ),
        maker_side=0,
        watched_role="maker",
        event_ts=1_700_000_000,
        log_index=None if provisional else 7,
        condition_id="0x" + "ef" * 32,
        provisional=provisional,
        exchange="0x" + "11" * 20,
        order_hash=None,
    )


class _Pipeline:
    """Fake ``execute_trades``: records calls and returns configurable outcomes.

    By default every call reports one ``executed`` result so the tx-level
    cross-source claim is kept (the release path is exercised explicitly).
    """

    def __init__(
        self,
        fail_for: set[str] | None = None,
        *,
        outcomes: list | None = None,
    ) -> None:
        self.calls: list[tuple[str, str, list, float]] = []
        self._fail_for = fail_for or set()
        self._outcomes = (
            outcomes if outcomes is not None else [SimpleNamespace(status="executed", reason=None)]
        )

    async def __call__(self, ctx, leader_id, raws, now_ts):
        self.calls.append((ctx.user_id, leader_id, raws, now_ts))
        if ctx.user_id in self._fail_for:
            raise RuntimeError("boom")
        return list(self._outcomes)


class _V1Runtime:
    def __init__(self) -> None:
        self.signals: list[OnchainTradeSignal] = []

    async def submit_onchain(self, signal) -> None:
        self.signals.append(signal)


class _FakeOrdersRepo:
    """In-memory ``processed_trades`` claim with the production conflict rules."""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.claims: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.release_attempts: list[str] = []

    async def try_claim_processed_trade(
        self,
        *,
        trade_key: str,
        user_id: str,
        leader_id: str | None,
        tx_hash: str | None,
        side: str | None,
        asset: str | None,
        condition_id: str | None,
        status: str = "in_progress",
        stale_claim_ttl_seconds: float = 30.0,
    ) -> bool:
        self.claims.append((trade_key, status))
        existing = self.rows.get(trade_key)
        if existing is not None and not str(existing["status"]).startswith("retryable_failed"):
            return False
        self.rows[trade_key] = {
            "trade_key": trade_key,
            "user_id": user_id,
            "leader_id": leader_id,
            "tx_hash": tx_hash,
            "side": side,
            "asset": asset,
            "condition_id": condition_id,
            "status": status,
            "order_id": None,
        }
        return True

    async def get_processed_trade(self, trade_key: str) -> dict | None:
        return self.rows.get(trade_key)

    async def release_cross_source_claim(self, trade_key: str) -> bool:
        """Status-guarded delete — mirrors ``OrdersRepo.release_cross_source_claim``."""
        self.release_attempts.append(trade_key)
        row = self.rows.get(trade_key)
        if row is None or str(row.get("status") or "") != CROSS_SOURCE_CLAIM_STATUS:
            return False
        self.deleted.append(trade_key)
        self.rows.pop(trade_key, None)
        return True

    def seed_pipeline_fill_row(self, trade_key: str, *, order_id: str | None) -> None:
        """Simulate the per-fill ``processed_trades`` row the pipeline writes."""
        self.rows[trade_key] = {
            "trade_key": trade_key,
            "status": "executed" if order_id else "in_progress",
            "order_id": order_id,
        }


def _ctx(user_id: str) -> SimpleNamespace:
    return SimpleNamespace(user_id=user_id, orders_repo=_FakeOrdersRepo())


def _adapter(
    contexts_by_leader,
    *,
    v1_runtime=None,
    pipeline=None,
    metrics=None,
) -> tuple[OnchainSignalDispatchAdapter, _Pipeline, MetricsRegistry]:
    pipeline = pipeline or _Pipeline()
    metrics = metrics or MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    adapter = OnchainSignalDispatchAdapter(
        contexts_by_leader=contexts_by_leader,
        v1_runtime=v1_runtime,
        execute_trades=pipeline,
        clock=lambda: 1234.5,
        metrics=metrics,
        logger=logging.getLogger("OnchainSignalDispatch"),
    )
    return adapter, pipeline, metrics


@pytest.mark.asyncio
async def test_dispatches_to_all_follower_contexts_case_insensitive() -> None:
    checksummed = "0x" + "AB" * 20
    contexts = {checksummed: [_ctx("u1"), _ctx("u2")], "0x" + "99" * 20: [_ctx("u3")]}
    adapter, pipeline, _ = _adapter(contexts)

    await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    for _user, leader_id, raws, now_ts in pipeline.calls:
        assert leader_id == _LEADER
        assert now_ts == 1234.5
        assert len(raws) == 1
        assert raws[0]["tx_hash"] == _TX
        assert raws[0]["side"] == "BUY"


@pytest.mark.asyncio
async def test_no_followers_is_a_noop() -> None:
    adapter, pipeline, _ = _adapter({"0x" + "99" * 20: [_ctx("u3")]})

    await adapter(_signal())

    assert pipeline.calls == []


@pytest.mark.asyncio
async def test_prefers_v1_runtime_when_present() -> None:
    v1 = _V1Runtime()
    adapter, pipeline, _ = _adapter({_LEADER: [_ctx("u1")]}, v1_runtime=v1)

    signal = _signal()
    await adapter(signal)

    assert v1.signals == [signal]
    assert pipeline.calls == []


@pytest.mark.asyncio
async def test_logs_provenance_and_increments_metric(caplog: pytest.LogCaptureFixture) -> None:
    adapter, _pipeline, metrics = _adapter({_LEADER: [_ctx("u1")]})

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=True))
        await adapter(_signal(provisional=False))

    lines = [
        record.getMessage()
        for record in caplog.records
        if "detection_signal |" in record.getMessage()
    ]
    assert len(lines) == 2
    assert lines[0].startswith("detection_signal | source=on_chain_pending ")
    assert f"leader={_LEADER}" in lines[0]
    assert f"tx={_TX}" in lines[0]
    assert "side=BUY" in lines[0]
    assert "token=123456" in lines[0]
    assert "confidence=pending_probable" in lines[0]
    assert "provisional=True" in lines[0]
    assert lines[1].startswith("detection_signal | source=on_chain_confirmed ")
    assert "confidence=mined_authoritative" in lines[1]
    assert "provisional=False" in lines[1]

    assert metrics.counters[f'{METRIC_DETECTION_SIGNALS}{{source="on_chain_pending"}}'] == 1
    assert metrics.counters[f'{METRIC_DETECTION_SIGNALS}{{source="on_chain_confirmed"}}'] == 1


@pytest.mark.asyncio
async def test_pipeline_error_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _pipeline, _ = _adapter({_LEADER: [_ctx("u1"), _ctx("u2")]}, pipeline=pipeline)

    with caplog.at_level(logging.WARNING, logger="OnchainSignalDispatch"):
        await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    warnings = [
        r.getMessage() for r in caplog.records if "onchain_signal_pipeline_error" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "user=u1" in warnings[0]


def test_detection_signals_metric_schema_is_bounded() -> None:
    from domain.detection import DetectionMode
    from services.metrics import DETECTION_SOURCE_LABEL_VALUES, MetricKind

    schema = {s.name: s for s in TELEMETRY_METRIC_SCHEMAS}[METRIC_DETECTION_SIGNALS]
    assert schema.kind is MetricKind.COUNTER
    assert schema.allowed_label_values == (
        ("source", ("polling", "on_chain_pending", "on_chain_confirmed")),
    )
    # The label domain is derived from the enum, so a new mode cannot drift.
    assert DETECTION_SOURCE_LABEL_VALUES == tuple(mode.value for mode in DetectionMode)
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    with pytest.raises(ValueError):
        registry.inc(METRIC_DETECTION_SIGNALS, labels={"source": "mempool"})


# ---------------------------------------------------------------------------
# Cross-source dedup (Slice 4): tx-level claim before per-fill dispatch
# ---------------------------------------------------------------------------


def _ctx_with_repo(user_id: str, repo: _FakeOrdersRepo) -> SimpleNamespace:
    return SimpleNamespace(user_id=user_id, orders_repo=repo)


def _polling_row() -> dict:
    """data-api ``/activity`` shape: no ``logIndex`` / ``orderHash``."""
    return {
        "transactionHash": _TX,
        "side": "BUY",
        "asset": "123456",
        "conditionId": "0x" + "ef" * 32,
        "size": 10,
        "price": 0.5,
        "usdcSize": 5,
        "timestamp": 1_700_000_000,
        "type": "TRADE",
    }


@pytest.mark.asyncio
async def test_onchain_signal_skipped_when_polling_claimed_tx(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from orchestrators.trade_pipeline import build_trade_key
    from services.metrics import METRIC_DETECTION_SIGNALS_DEDUPED

    repo = _FakeOrdersRepo()
    # Polling already claimed the tx-level key (its per-fill key IS the tx-level key).
    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert await repo.try_claim_processed_trade(
        trade_key=polling_key,
        user_id="u1",
        leader_id=_LEADER,
        tx_hash=_TX,
        side="BUY",
        asset="123456",
        condition_id="c",
        status="in_progress",
    )
    adapter, pipeline, metrics = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]})

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    assert pipeline.calls == []
    deduped = [
        r.getMessage() for r in caplog.records if "detection_signal_deduped" in r.getMessage()
    ]
    assert len(deduped) == 1
    assert deduped[0].startswith(
        "detection_signal_deduped | source=on_chain_confirmed winner=polling "
    )
    assert f"tx={_TX}" in deduped[0]
    assert (
        metrics.counters[f'{METRIC_DETECTION_SIGNALS_DEDUPED}{{source="on_chain_confirmed"}}'] == 1
    )


@pytest.mark.asyncio
async def test_polling_skips_when_onchain_claimed_tx() -> None:
    """The on-chain tx-level claim collides with the key polling would build."""
    from orchestrators.onchain_signal_dispatch import CrossSourceTradeClaimPolicy
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    adapter, pipeline, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]})

    await adapter(_signal(provisional=False))

    # On-chain was dispatched; the pipeline (faked here) claims its per-fill key.
    assert [call[0] for call in pipeline.calls] == ["u1"]
    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    fill_key = build_trade_key("u1", _LEADER, pipeline.calls[0][2][0])
    assert fill_key.endswith("|log:7")
    assert polling_key != fill_key
    assert repo.rows[polling_key]["status"] == CrossSourceTradeClaimPolicy.STATUS
    # Polling arriving later hits the same tx-level key and is refused.
    assert (
        await repo.try_claim_processed_trade(
            trade_key=polling_key,
            user_id="u1",
            leader_id=_LEADER,
            tx_hash=_TX,
            side="BUY",
            asset="123456",
            condition_id="c",
            status="in_progress",
        )
        is False
    )


@pytest.mark.asyncio
async def test_pending_signal_without_fill_discriminator_is_not_preclaimed() -> None:
    """When the per-fill key equals the tx-level key the pipeline claim is the dedup."""
    repo = _FakeOrdersRepo()
    adapter, pipeline, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]})

    await adapter(_signal(provisional=True))

    assert [call[0] for call in pipeline.calls] == ["u1"]
    assert repo.claims == []


def _polling_claim(repo: _FakeOrdersRepo, polling_key: str):
    return repo.try_claim_processed_trade(
        trade_key=polling_key,
        user_id="u1",
        leader_id=_LEADER,
        tx_hash=_TX,
        side="BUY",
        asset="123456",
        condition_id="c",
        status="in_progress",
    )


@pytest.mark.asyncio
async def test_second_onchain_fill_in_same_tx_is_dispatched_as_sibling(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """C1: two ``OrderFilled`` fills of one tx/side/asset are two pipeline dispatches.

    The tx-level claim is held by this process, so the sibling fill is our own
    (sizes are additive; the pipeline's per-fill ``|log:N`` claim keeps
    idempotency). Polling's aggregate for that tx is still refused.
    """
    from dataclasses import replace

    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    adapter, pipeline, metrics = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]})

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))
        await adapter(replace(_signal(provisional=False), log_index=8))

    assert [call[0] for call in pipeline.calls] == ["u1", "u1"]
    fill_keys = [build_trade_key("u1", _LEADER, call[2][0]) for call in pipeline.calls]
    assert fill_keys[0].endswith("|log:7") and fill_keys[1].endswith("|log:8")
    assert not any("detection_signal_deduped" in r.getMessage() for r in caplog.records)
    sibling = [
        r.getMessage() for r in caplog.records if "cross_source_sibling_fill" in r.getMessage()
    ]
    assert len(sibling) == 1
    assert f"tx={_TX}" in sibling[0]
    assert (
        metrics.counters.get('detection_signals_deduped_total{source="on_chain_confirmed"}', 0) == 0
    )
    # Polling's aggregate for the same tx is still refused.
    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert await _polling_claim(repo, polling_key) is False


@pytest.mark.asyncio
async def test_sibling_fill_recognised_from_row_status_after_restart() -> None:
    """A fresh policy instance (no in-process memory) probes the row status."""
    from dataclasses import replace

    repo = _FakeOrdersRepo()
    ctx = _ctx_with_repo("u1", repo)
    first, pipeline_a, _ = _adapter({_LEADER: [ctx]})
    await first(_signal(provisional=False))
    assert [call[0] for call in pipeline_a.calls] == ["u1"]

    second, pipeline_b, _ = _adapter({_LEADER: [ctx]})  # new adapter == new policy
    await second(replace(_signal(provisional=False), log_index=8))

    assert [call[0] for call in pipeline_b.calls] == ["u1"]


@pytest.mark.asyncio
async def test_lost_claim_with_probe_error_skips_with_unknown_winner(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Never attribute a lost claim to polling when the probe fails."""
    from orchestrators.trade_pipeline import build_trade_key

    class _ProbeBrokenRepo(_FakeOrdersRepo):
        async def get_processed_trade(self, trade_key: str) -> dict | None:
            raise RuntimeError("pg down")

    repo = _ProbeBrokenRepo()
    assert await _polling_claim(repo, build_trade_key("u1", _LEADER, _polling_row()))
    adapter, pipeline, metrics = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]})

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    assert pipeline.calls == []
    unresolved = [r for r in caplog.records if "cross_source_claim_unresolved" in r.getMessage()]
    assert len(unresolved) == 1
    assert unresolved[0].levelno == logging.WARNING
    assert "winner=unknown" in unresolved[0].getMessage()
    assert not any("winner=polling" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# I2: release the tx-level claim when a context produced no executed outcome
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_released_when_pipeline_raises(caplog: pytest.LogCaptureFixture) -> None:
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == [polling_key]
    assert polling_key not in repo.rows
    released = [
        r.getMessage() for r in caplog.records if "cross_source_claim_released" in r.getMessage()
    ]
    assert len(released) == 1
    assert "reason=pipeline_error" in released[0]
    # Polling can now copy the fill.
    assert await _polling_claim(repo, polling_key) is True


@pytest.mark.asyncio
async def test_claim_released_when_every_result_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(outcomes=[SimpleNamespace(status="skipped", reason="risk_blocked")])
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == [polling_key]
    released = [
        r.getMessage() for r in caplog.records if "cross_source_claim_released" in r.getMessage()
    ]
    assert len(released) == 1
    assert "reason=no_executed_outcome" in released[0]


@pytest.mark.asyncio
async def test_claim_kept_when_a_result_executed() -> None:
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(
        outcomes=[
            SimpleNamespace(status="skipped", reason="x"),
            SimpleNamespace(status="executed", reason=None),
        ]
    )
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    await adapter(_signal(provisional=False))

    assert repo.deleted == []
    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.rows[polling_key]["status"] == "cross_source_claimed"


# ---------------------------------------------------------------------------
# N1: a post-submission outcome must never release the tx-level claim
# ---------------------------------------------------------------------------


def _onchain_fill_key(user_id: str = "u1", *, log_index: int = 7) -> str:
    """Per-fill key the pipeline claims for the on-chain raw signal."""
    from dataclasses import replace

    from orchestrators.trade_pipeline import build_trade_key

    signal = replace(_signal(provisional=False), log_index=log_index)
    return build_trade_key(user_id, _LEADER, signal.as_raw_signal_dict())


@pytest.mark.asyncio
async def test_claim_kept_when_skip_is_post_submission_duplicate(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """N1: ``skipped/duplicate_ignored`` means an order already reached the venue.

    ``trade_pipeline`` returns that shape when the coordinator/execution dedup
    fires or the copy fill was already persisted. Releasing the tx-level claim
    there lets the next REST polling tick claim the same key and submit a
    second copy of the same leader fill.
    """
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(
        outcomes=[
            SimpleNamespace(status="skipped", reason="duplicate_ignored", order_id="ORD-LIVE-1")
        ]
    )
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == []
    assert repo.rows[polling_key]["status"] == CROSS_SOURCE_CLAIM_STATUS
    assert not any("cross_source_claim_released" in r.getMessage() for r in caplog.records)
    # Polling must stay refused — the copy already happened.
    assert await _polling_claim(repo, polling_key) is False


@pytest.mark.asyncio
async def test_claim_kept_when_skip_is_reservation_conflict_without_order_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``ReservationConflict`` yields ``duplicate_ignored`` with no ``order_id``.

    ``services/execution/coordinator.py`` returns the duplicate status without
    an order id, so the reason — not only the order id — has to block release.
    """
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(
        outcomes=[SimpleNamespace(status="skipped", reason="duplicate_ignored", order_id=None)]
    )
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == []
    assert repo.rows[polling_key]["status"] == CROSS_SOURCE_CLAIM_STATUS
    assert not any("cross_source_claim_released" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_claim_kept_when_exception_after_fill_persisted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """N5: the pipeline raised after its own per-fill claim (row present).

    ``mark_processed_trade`` (``orchestrators/trade_pipeline.py``) never
    writes ``order_id`` on the per-fill row, so a real post-claim row looks
    exactly like this: ``status="executed"``, no ``order_id``. The row's mere
    presence — not its ``order_id`` — is what proves the pipeline reached its
    own claim, so the tx-level claim must stay kept.
    """
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    repo.seed_pipeline_fill_row(_onchain_fill_key(), order_id=None)
    repo.rows[_onchain_fill_key()]["status"] = "executed"
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == []
    assert repo.rows[polling_key]["status"] == CROSS_SOURCE_CLAIM_STATUS
    assert not any("cross_source_claim_released" in r.getMessage() for r in caplog.records)
    kept = [r.getMessage() for r in caplog.records if "cross_source_claim_kept" in r.getMessage()]
    assert len(kept) == 1
    assert "reason=pipeline_error" in kept[0]
    assert "detail=per_fill_row_present" in kept[0]
    assert "status=executed" in kept[0]
    assert await _polling_claim(repo, polling_key) is False


@pytest.mark.asyncio
async def test_claim_kept_when_exception_after_pipeline_claim_in_progress(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """N5: a per-fill row still ``in_progress`` also proves a claim was taken.

    ``try_claim_processed_trade`` (``orchestrators/trade_pipeline.py:1761``)
    inserts the per-fill row *before* any submission attempt. Any row at all
    — regardless of status or ``order_id`` — means the pipeline's own claim
    already exists, so an absent row is the only proof that nothing happened.
    """
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    repo.seed_pipeline_fill_row(_onchain_fill_key(), order_id=None)
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == []
    assert repo.rows[polling_key]["status"] == CROSS_SOURCE_CLAIM_STATUS
    assert not any("cross_source_claim_released" in r.getMessage() for r in caplog.records)
    kept = [r.getMessage() for r in caplog.records if "cross_source_claim_kept" in r.getMessage()]
    assert len(kept) == 1
    assert "reason=pipeline_error" in kept[0]
    assert "detail=per_fill_row_present" in kept[0]
    assert "status=in_progress" in kept[0]
    assert await _polling_claim(repo, polling_key) is False


@pytest.mark.asyncio
async def test_claim_released_when_exception_before_pipeline_claim(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """I2 preserved: no per-fill row means the pipeline never submitted anything."""
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == [polling_key]
    released = [
        r.getMessage() for r in caplog.records if "cross_source_claim_released" in r.getMessage()
    ]
    assert len(released) == 1
    assert "reason=pipeline_error" in released[0]
    assert await _polling_claim(repo, polling_key) is True


@pytest.mark.asyncio
async def test_claim_kept_when_the_per_fill_probe_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unreadable per-fill row cannot prove "nothing happened" — fail closed."""
    from orchestrators.trade_pipeline import build_trade_key

    class _ProbeBrokenRepo(_FakeOrdersRepo):
        async def get_processed_trade(self, trade_key: str) -> dict | None:
            raise RuntimeError("pg down")

    repo = _ProbeBrokenRepo()
    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    with caplog.at_level(logging.INFO, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.deleted == []
    assert repo.rows[polling_key]["status"] == CROSS_SOURCE_CLAIM_STATUS
    kept = [r for r in caplog.records if "cross_source_claim_kept" in r.getMessage()]
    assert len(kept) == 1
    assert kept[0].levelno == logging.WARNING


@pytest.mark.asyncio
async def test_release_goes_through_the_status_guarded_repo_method() -> None:
    """The release must never use an unguarded ``DELETE`` on the tx-level key."""
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    pipeline = _Pipeline(outcomes=[SimpleNamespace(status="skipped", reason="risk_blocked")])
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", repo)]}, pipeline=pipeline)

    await adapter(_signal(provisional=False))

    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert repo.release_attempts == [polling_key]


@pytest.mark.asyncio
async def test_decision_carries_the_per_fill_key() -> None:
    """``CrossSourceClaimDecision`` exposes the key the pipeline itself claims."""
    from domain.detection import DetectionMode
    from orchestrators.onchain_signal_dispatch import CrossSourceTradeClaimPolicy
    from orchestrators.trade_pipeline import build_trade_key

    repo = _FakeOrdersRepo()
    ctx = _ctx_with_repo("u1", repo)
    raw = _signal(provisional=False).as_raw_signal_dict()

    decision = await CrossSourceTradeClaimPolicy().try_claim(
        ctx, _LEADER, raw, source=DetectionMode.ON_CHAIN_CONFIRMED
    )

    assert decision.claimed is True
    assert decision.fill_key == build_trade_key("u1", _LEADER, raw)
    assert decision.fill_key.endswith("|log:7")
    assert decision.fill_key != decision.trade_key


@pytest.mark.asyncio
async def test_sibling_fill_never_releases_the_first_fills_claim() -> None:
    """A skipped sibling must not free the tx-level key held for the executed fill."""
    from dataclasses import replace

    repo = _FakeOrdersRepo()
    ctx = _ctx_with_repo("u1", repo)
    executed, _p, _ = _adapter({_LEADER: [ctx]})
    await executed(_signal(provisional=False))
    skipped = _Pipeline(outcomes=[SimpleNamespace(status="skipped", reason="already_processed")])
    sibling_adapter, _p2, _ = _adapter({_LEADER: [ctx]}, pipeline=skipped)

    await sibling_adapter(replace(_signal(provisional=False), log_index=8))

    assert repo.deleted == []
    assert [call[0] for call in _p2.calls] == ["u1"]


@pytest.mark.asyncio
async def test_claim_not_released_when_a_sibling_was_dispatched_meanwhile() -> None:
    """Fill A errors after sibling B was admitted on A's claim: keep the key.

    Releasing would let polling's aggregate (A+B) copy B a second time.
    """
    import asyncio
    from dataclasses import replace

    repo = _FakeOrdersRepo()
    ctx = _ctx_with_repo("u1", repo)
    gate = asyncio.Event()

    class _SlowFailingPipeline(_Pipeline):
        async def __call__(self, ctx, leader_id, raws, now_ts):
            self.calls.append((ctx.user_id, leader_id, raws, now_ts))
            if raws[0]["log_index"] == 7:
                await gate.wait()
                raise RuntimeError("boom")
            return [SimpleNamespace(status="executed", reason=None)]

    pipeline = _SlowFailingPipeline()
    adapter, _p, _ = _adapter({_LEADER: [ctx]}, pipeline=pipeline)

    first = asyncio.create_task(adapter(_signal(provisional=False)))
    await asyncio.sleep(0)  # let A claim and enter the pipeline
    await adapter(replace(_signal(provisional=False), log_index=8))
    gate.set()
    await first

    assert repo.deleted == []
    assert len(pipeline.calls) == 2


@pytest.mark.asyncio
async def test_release_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    class _NoDeleteRepo(_FakeOrdersRepo):
        async def release_cross_source_claim(self, trade_key: str) -> bool:
            raise RuntimeError("pg down")

    pipeline = _Pipeline(fail_for={"u1"})
    adapter, _p, _ = _adapter({_LEADER: [_ctx_with_repo("u1", _NoDeleteRepo())]}, pipeline=pipeline)

    with caplog.at_level(logging.WARNING, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    assert any("cross_source_claim_release_failed" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_claim_policy_failure_fails_closed_for_that_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BrokenRepo(_FakeOrdersRepo):
        async def try_claim_processed_trade(self, **kwargs) -> bool:
            raise RuntimeError("pg down")

    adapter, pipeline, _ = _adapter({_LEADER: [_ctx_with_repo("u1", _BrokenRepo())]})

    with caplog.at_level(logging.WARNING, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=False))

    assert pipeline.calls == []
    assert any("cross_source_claim_failed" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Pending-copy confirmation handoff (provisional signals only)
# ---------------------------------------------------------------------------


class _RecordingTracker:
    """Stands in for ``PendingCopyConfirmationTracker.track`` (sync, bool)."""

    def __init__(self, *, raises: bool = False) -> None:
        self.entries: list = []
        self._raises = raises

    def track(self, entry) -> bool:
        self.entries.append(entry)
        if self._raises:
            raise RuntimeError("tracker exploded")
        return True


def _tracking_adapter(
    contexts_by_leader,
    *,
    tracker,
    pipeline=None,
    monotonic=None,
):
    pipeline = pipeline or _Pipeline()
    adapter = OnchainSignalDispatchAdapter(
        contexts_by_leader=contexts_by_leader,
        v1_runtime=None,
        execute_trades=pipeline,
        clock=lambda: 1234.5,
        metrics=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        logger=logging.getLogger("OnchainSignalDispatch"),
        pending_copy_tracker=tracker,
        **({"monotonic": monotonic} if monotonic is not None else {}),
    )
    return adapter, pipeline


@pytest.mark.asyncio
async def test_provisional_copy_that_reached_the_venue_is_tracked() -> None:
    tracker = _RecordingTracker()
    pipeline = _Pipeline(outcomes=[SimpleNamespace(status="executed", reason=None, order_id="o-1")])
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1"), _ctx("u2")]},
        tracker=tracker,
        pipeline=pipeline,
        monotonic=lambda: 4242.0,
    )

    await adapter(_signal(provisional=True))

    assert len(tracker.entries) == 1
    entry = tracker.entries[0]
    assert entry.tx_hash == _TX
    assert entry.leader_wallet == _LEADER
    assert entry.user_ids == ("u1", "u2")
    assert entry.token_id == "123456"
    assert entry.condition_id == "0x" + "ef" * 32
    assert entry.side == "BUY"
    assert entry.recorded_at == 4242.0


@pytest.mark.asyncio
async def test_tracked_statuses_are_the_submitted_ones() -> None:
    for status in ("executed", "submitted_unconfirmed"):
        tracker = _RecordingTracker()
        pipeline = _Pipeline(outcomes=[SimpleNamespace(status=status, reason=None, order_id=None)])
        adapter, _pipeline = _tracking_adapter(
            {_LEADER: [_ctx("u1")]}, tracker=tracker, pipeline=pipeline
        )

        await adapter(_signal(provisional=True))

        assert [entry.user_ids for entry in tracker.entries] == [("u1",)], status


@pytest.mark.asyncio
async def test_the_paper_tier_gate_result_is_not_tracked() -> None:
    """``status="paper"`` is the tier gate, not a submission.

    ``trade_pipeline`` returns it from the "Tier gate: paper follow" branch
    with ``order_id=None``, *before* any execution — and
    ``execution.paper_trade_only`` downgrades every ``live_follow`` leader to
    that tier. Tracking it would make a paper soak alarm
    ``pending_copy_unconfirmed`` for positions that were never opened.
    """
    tracker = _RecordingTracker()
    pipeline = _Pipeline(
        outcomes=[SimpleNamespace(status="paper", reason="tier_paper_follow", order_id=None)]
    )
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1")]}, tracker=tracker, pipeline=pipeline
    )

    await adapter(_signal(provisional=True))

    assert [call[0] for call in pipeline.calls] == ["u1"]
    assert tracker.entries == []


@pytest.mark.asyncio
async def test_a_provisional_signal_that_only_skipped_is_not_tracked() -> None:
    tracker = _RecordingTracker()
    pipeline = _Pipeline(
        outcomes=[SimpleNamespace(status="skipped", reason="risk_rejected", order_id=None)]
    )
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1")]}, tracker=tracker, pipeline=pipeline
    )

    await adapter(_signal(provisional=True))

    assert tracker.entries == []


@pytest.mark.asyncio
async def test_only_the_users_whose_copy_reached_the_venue_are_tracked() -> None:
    tracker = _RecordingTracker()

    class _MixedPipeline(_Pipeline):
        async def __call__(self, ctx, leader_id, raws, now_ts):
            self.calls.append((ctx.user_id, leader_id, raws, now_ts))
            if ctx.user_id == "u1":
                return [SimpleNamespace(status="executed", reason=None, order_id="o-1")]
            return [SimpleNamespace(status="skipped", reason="risk_rejected", order_id=None)]

    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1"), _ctx("u2")]},
        tracker=tracker,
        pipeline=_MixedPipeline(),
    )

    await adapter(_signal(provisional=True))

    assert len(tracker.entries) == 1
    assert tracker.entries[0].user_ids == ("u1",)


@pytest.mark.asyncio
async def test_a_confirmed_signal_is_never_tracked() -> None:
    """Only provisional copies need follow-up: a mined fill is already final."""
    tracker = _RecordingTracker()
    adapter, _pipeline = _tracking_adapter({_LEADER: [_ctx("u1")]}, tracker=tracker)

    await adapter(_signal(provisional=False))

    assert tracker.entries == []


@pytest.mark.asyncio
async def test_a_pipeline_exception_leaves_that_user_untracked() -> None:
    tracker = _RecordingTracker()
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1")]},
        tracker=tracker,
        pipeline=_Pipeline(fail_for={"u1"}),
    )

    await adapter(_signal(provisional=True))

    assert tracker.entries == []


@pytest.mark.asyncio
async def test_a_tracker_failure_never_breaks_dispatch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tracker = _RecordingTracker(raises=True)
    pipeline = _Pipeline(outcomes=[SimpleNamespace(status="executed", reason=None, order_id="o-1")])
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1")]}, tracker=tracker, pipeline=pipeline
    )

    with caplog.at_level(logging.DEBUG, logger="OnchainSignalDispatch"):
        await adapter(_signal(provisional=True))

    assert [call[0] for call in pipeline.calls] == ["u1"]
    assert len(tracker.entries) == 1
    assert any("pending_copy_track_skipped" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_dispatch_without_a_tracker_is_unchanged() -> None:
    adapter, pipeline, _ = _adapter({_LEADER: [_ctx("u1")]})

    await adapter(_signal(provisional=True))

    assert [call[0] for call in pipeline.calls] == ["u1"]


@pytest.mark.asyncio
async def test_the_recorded_stamp_shares_the_trackers_monotonic_time_base() -> None:
    """``recorded_at`` feeds ``now - recorded_at`` inside the tracker.

    The tracker's default clock is ``time.monotonic``; stamping the entry with
    wall-clock ``time.time`` would make every age hugely negative and no copy
    would ever be polled.
    """
    import time

    tracker = _RecordingTracker()
    pipeline = _Pipeline(outcomes=[SimpleNamespace(status="executed", reason=None, order_id="o-1")])
    adapter, _pipeline = _tracking_adapter(
        {_LEADER: [_ctx("u1")]}, tracker=tracker, pipeline=pipeline
    )

    await adapter(_signal(provisional=True))

    assert len(tracker.entries) == 1
    assert abs(tracker.entries[0].recorded_at - time.monotonic()) < 1.0


def test_pending_copy_metric_schemas_are_bounded() -> None:
    from services.metrics import (
        GAUGE_PENDING_COPY_TRACKED,
        METRIC_PENDING_COPY_CONFIRMED,
        METRIC_PENDING_COPY_TRACKER_DROPPED,
        METRIC_PENDING_COPY_UNCONFIRMED,
        MetricKind,
    )

    schemas = {schema.name: schema for schema in TELEMETRY_METRIC_SCHEMAS}

    assert schemas[METRIC_PENDING_COPY_CONFIRMED].kind is MetricKind.COUNTER
    assert schemas[METRIC_PENDING_COPY_CONFIRMED].allowed_label_values == ()
    assert schemas[GAUGE_PENDING_COPY_TRACKED].kind is MetricKind.GAUGE
    assert schemas[METRIC_PENDING_COPY_UNCONFIRMED].allowed_label_values == (
        ("reason", ("reverted", "not_mined", "rpc_error")),
    )
    assert schemas[METRIC_PENDING_COPY_TRACKER_DROPPED].allowed_label_values == (
        ("reason", ("full",)),
    )

    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    with pytest.raises(ValueError):
        registry.inc(METRIC_PENDING_COPY_UNCONFIRMED, labels={"reason": "replaced"})
    with pytest.raises(ValueError):
        registry.inc(METRIC_PENDING_COPY_TRACKER_DROPPED, labels={"reason": "stale"})


# ---------------------------------------------------------------------------
# C2 — detection telemetry emits
# ---------------------------------------------------------------------------


def _telemetry_adapter(
    contexts_by_leader,
    *,
    telemetry,
    pipeline=None,
    v1_runtime=None,
):
    """Build an adapter whose claim policy shares the detection emitter."""
    pipeline = pipeline or _Pipeline()
    adapter = OnchainSignalDispatchAdapter(
        contexts_by_leader=contexts_by_leader,
        v1_runtime=v1_runtime,
        execute_trades=pipeline,
        clock=lambda: 1234.5,
        metrics=MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS),
        logger=logging.getLogger("OnchainSignalDispatch"),
        telemetry=telemetry,
    )
    return adapter, pipeline


@pytest.mark.asyncio
async def test_dispatch_emits_one_signal_received_per_signal() -> None:
    """Exactly one detection event per signal, correlated by transaction hash."""
    from services.telemetry import LeaderEvent, LeaderOutcome, TelemetryProducer, safe_ref
    from tests.telemetry_helpers import recording_emitters

    emitters, service = recording_emitters()
    adapter, pipeline = _telemetry_adapter(
        {_LEADER: [_ctx("u1"), _ctx("u2")]},
        telemetry=emitters.detection,
    )

    await adapter(_signal(provisional=True))

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    leader_events = service.payloads(LeaderEvent)
    assert len(leader_events) == 1
    assert leader_events[0].outcome is LeaderOutcome.RECEIVED
    assert leader_events[0].source == "on_chain_pending"
    assert leader_events[0].leader_ref == safe_ref(_LEADER)
    assert service.calls[0].producer is TelemetryProducer.DETECTION
    assert service.calls[0].correlation_id == safe_ref(_TX)


@pytest.mark.asyncio
async def test_dispatch_emits_signal_received_on_the_v1_path_too() -> None:
    """The v1 runtime short-circuits the legacy fan-out, never the detection emit."""
    from services.telemetry import LeaderEvent, LeaderOutcome
    from tests.telemetry_helpers import recording_emitters

    emitters, service = recording_emitters()
    v1 = _V1Runtime()
    adapter, pipeline = _telemetry_adapter(
        {_LEADER: [_ctx("u1")]},
        telemetry=emitters.detection,
        v1_runtime=v1,
    )

    await adapter(_signal(provisional=False))

    assert len(v1.signals) == 1
    assert pipeline.calls == []
    [event] = service.payloads(LeaderEvent)
    assert event.outcome is LeaderOutcome.RECEIVED
    assert event.source == "on_chain_confirmed"


@pytest.mark.asyncio
async def test_cross_source_dedup_emits_signal_deduplicated() -> None:
    """A fill polling already claimed reports a DEDUP leader event."""
    from orchestrators.trade_pipeline import build_trade_key
    from services.telemetry import LeaderEvent, LeaderOutcome, safe_ref
    from tests.telemetry_helpers import recording_emitters

    emitters, service = recording_emitters()
    repo = _FakeOrdersRepo()
    polling_key = build_trade_key("u1", _LEADER, _polling_row())
    assert await repo.try_claim_processed_trade(
        trade_key=polling_key,
        user_id="u1",
        leader_id=_LEADER,
        tx_hash=_TX,
        side="BUY",
        asset="123456",
        condition_id="c",
        status="in_progress",
    )
    adapter, pipeline = _telemetry_adapter(
        {_LEADER: [_ctx_with_repo("u1", repo)]},
        telemetry=emitters.detection,
    )

    await adapter(_signal(provisional=False))

    assert pipeline.calls == []
    received, deduped = service.payloads(LeaderEvent)
    assert received.outcome is LeaderOutcome.RECEIVED
    assert deduped.outcome is LeaderOutcome.DEDUP
    assert deduped.source == "on_chain_confirmed"
    assert deduped.leader_ref == safe_ref(_LEADER.lower())


@pytest.mark.asyncio
async def test_raising_telemetry_backend_never_stops_dispatch() -> None:
    """A failing telemetry service must not change dispatch behaviour."""
    from tests.telemetry_helpers import raising_emitters

    emitters, service = raising_emitters()
    adapter, pipeline = _telemetry_adapter(
        {_LEADER: [_ctx("u1"), _ctx("u2")]},
        telemetry=emitters.detection,
    )

    await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1", "u2"]
    assert service.calls == 1


@pytest.mark.asyncio
async def test_dispatch_without_telemetry_is_unchanged() -> None:
    """Omitting the emitter keeps the adapter on its pre-telemetry behaviour."""
    adapter, pipeline, _metrics = _adapter({_LEADER: [_ctx("u1")]})

    await adapter(_signal())

    assert [call[0] for call in pipeline.calls] == ["u1"]
