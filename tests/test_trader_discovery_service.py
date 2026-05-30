from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from services.discovery.falcon_candidate_service import FalconCandidate
from services.traderDiscoveryService import TraderDiscoveryService


def _candidate() -> FalconCandidate:
    return FalconCandidate(
        wallet="0x" + "a" * 40,
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
        )
    )


def _service(*, logger_name: str = "test.trader_discovery"):
    candidate = _candidate()
    evaluation_repo = SimpleNamespace(upsert_evaluation=AsyncMock())
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
