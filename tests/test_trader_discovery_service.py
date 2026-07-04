from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.discovery.falcon_candidate_service import FalconCandidate
from services.traderDiscoveryService import TraderDiscoveryService, TraderProfile


def _candidate(wallet_char: str = "a") -> FalconCandidate:
    return FalconCandidate(
        wallet="0x" + wallet_char * 40,
        h_score=80.0,
        roi_15d=25.0,
        win_rate_15d=60.0,
        sharpe_15d=2.0,
        pnl_15d=125.0,
        total_trades_15d=42,
        markets_traded_15d=9,
        trajectory="stable",
        raw={"source": "falcon"},
    )


def _config():
    return SimpleNamespace(
        discovery=SimpleNamespace(
            falcon_api_token="token",
            discovery_history_trade_limit=500,
            discovery_candidate_eval_concurrency=1,
        )
    )


def _service(*, logger_name: str = "test.trader_discovery", run_history: bool = False):
    candidate = _candidate()
    evaluation_repo = SimpleNamespace(upsert_evaluation=AsyncMock())
    if run_history:
        evaluation_repo.create_discovery_run = AsyncMock()
        evaluation_repo.complete_discovery_run = AsyncMock()
        evaluation_repo.upsert_run_candidate = AsyncMock()
    service = TraderDiscoveryService(
        config=_config(),
        falcon_candidates=SimpleNamespace(
            fetch_candidates=AsyncMock(return_value=[candidate])
        ),
        history_backfill=SimpleNamespace(
            backfill_wallet=AsyncMock(
                return_value=SimpleNamespace(
                    fetched=1,
                    inserted=1,
                    skipped=0,
                    earliest_ts=1,
                    latest_ts=1,
                    errors=0,
                )
            )
        ),
        discovery_repo=SimpleNamespace(
            list_trades=AsyncMock(return_value=[SimpleNamespace(trade_id="1")]),
        ),
        evaluation_repo=evaluation_repo,
        grouper=SimpleNamespace(group=lambda _trades: []),
        classifier=SimpleNamespace(),
        replay_service=SimpleNamespace(
            replay_wallet=AsyncMock(side_effect=RuntimeError("boom"))
        ),
        scoring_service=SimpleNamespace(),
        logger_instance=logging.getLogger(logger_name),
    )
    return service, candidate, evaluation_repo


def _concurrent_service(
    candidates: list[FalconCandidate],
    *,
    concurrency: int,
    run_history: bool = False,
) -> tuple[TraderDiscoveryService, SimpleNamespace]:
    evaluation_repo = SimpleNamespace(upsert_evaluation=AsyncMock())
    if run_history:
        evaluation_repo.create_discovery_run = AsyncMock()
        evaluation_repo.complete_discovery_run = AsyncMock()
        evaluation_repo.upsert_run_candidate = AsyncMock()
    service = TraderDiscoveryService(
        config=SimpleNamespace(
            discovery=SimpleNamespace(
                falcon_api_token="token",
                discovery_history_trade_limit=500,
                discovery_candidate_eval_concurrency=concurrency,
                falcon_sort_modes=("h_score",),
            )
        ),
        falcon_candidates=SimpleNamespace(
            fetch_candidates=AsyncMock(return_value=candidates)
        ),
        history_backfill=object(),
        discovery_repo=object(),
        evaluation_repo=evaluation_repo,
        grouper=object(),
        classifier=object(),
        replay_service=object(),
        scoring_service=object(),
        logger_instance=logging.getLogger("test.trader_discovery.concurrent"),
    )
    return service, evaluation_repo


def _outcome(wallet: str, score: float):
    return SimpleNamespace(
        profile=TraderProfile(
            wallet=wallet,
            composite_score=score,
            tags="live",
        ),
        status="live",
        final_score=score,
        eligible_live=True,
        eligible_shadow=True,
        rejection_reason=None,
    )


@pytest.mark.asyncio
async def test_candidate_eval_exception_upserts_rejected_evaluation():
    service, candidate, evaluation_repo = _service()

    profiles = await service.discover_candidates()

    assert profiles == []
    evaluation_repo.upsert_evaluation.assert_awaited_once()
    stored = evaluation_repo.upsert_evaluation.await_args.args[0]
    assert stored.wallet == candidate.wallet
    assert stored.status == "rejected"
    assert stored.rejection_reason == "candidate_eval_exception"
    assert stored.eligible_live is False
    assert stored.eligible_shadow is False
    assert stored.replay_json["error_type"] == "RuntimeError"
    assert "boom" in stored.replay_json["error"]


@pytest.mark.asyncio
async def test_candidate_eval_failed_logs_wallet_and_error_type(caplog):
    service, candidate, _evaluation_repo = _service(logger_name="test.trader_discovery.log")

    with caplog.at_level(logging.WARNING, logger="test.trader_discovery.log"):
        await service.discover_candidates()

    records = [
        record
        for record in caplog.records
        if "candidate_eval_failed" in record.getMessage()
    ]
    assert len(records) == 1
    record = records[0]
    assert candidate.wallet in record.getMessage()
    assert "RuntimeError" in record.getMessage()
    assert record.exc_info is not None


@pytest.mark.asyncio
async def test_discovery_run_history_is_best_effort_for_failed_candidate():
    service, _candidate, evaluation_repo = _service(run_history=True)

    await service.discover_candidates()

    evaluation_repo.create_discovery_run.assert_awaited_once()
    assert evaluation_repo.upsert_run_candidate.await_count == 2
    evaluation_repo.complete_discovery_run.assert_awaited_once()
    assert evaluation_repo.complete_discovery_run.await_args.kwargs["status"] == "completed"
    assert evaluation_repo.complete_discovery_run.await_args.kwargs["error_count"] == 1


@pytest.mark.asyncio
async def test_candidate_evaluations_run_concurrently_with_semaphore():
    candidates = [_candidate(ch) for ch in ("a", "b", "c", "d")]
    service, _evaluation_repo = _concurrent_service(candidates, concurrency=2)
    state = {"in_flight": 0, "max_in_flight": 0}
    scores = {
        candidates[0].wallet: 1.0,
        candidates[1].wallet: 4.0,
        candidates[2].wallet: 2.0,
        candidates[3].wallet: 3.0,
    }

    async def evaluate_one(cand, *, run_id=None):  # noqa: ARG001
        state["in_flight"] += 1
        state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
        try:
            await asyncio.sleep(0.02)
            return _outcome(cand.wallet, scores[cand.wallet])
        finally:
            state["in_flight"] -= 1

    service._evaluate_one = evaluate_one  # type: ignore[method-assign]

    profiles = await service.discover_candidates()

    assert state["max_in_flight"] == 2
    assert [profile.composite_score for profile in profiles] == [4.0, 3.0, 2.0, 1.0]


@pytest.mark.asyncio
async def test_candidate_eval_exception_does_not_cancel_other_candidates():
    good = _candidate("a")
    bad = _candidate("b")
    service, evaluation_repo = _concurrent_service(
        [good, bad],
        concurrency=2,
        run_history=True,
    )

    async def evaluate_one(cand, *, run_id=None):  # noqa: ARG001
        await asyncio.sleep(0)
        if cand.wallet == bad.wallet:
            raise RuntimeError("candidate failed")
        return _outcome(cand.wallet, 2.0)

    service._evaluate_one = evaluate_one  # type: ignore[method-assign]

    profiles = await service.discover_candidates()

    assert [profile.wallet for profile in profiles] == [good.wallet]
    evaluation_repo.upsert_evaluation.assert_awaited_once()
    evaluation_repo.complete_discovery_run.assert_awaited_once()
    assert evaluation_repo.complete_discovery_run.await_args.kwargs["status"] == "completed"
    assert evaluation_repo.complete_discovery_run.await_args.kwargs["error_count"] == 1
