"""Execution metadata misses report actual fetch time, excluding cache hits."""

from unittest.mock import AsyncMock

import pytest

from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from pmcopy.integrations.polymarket.errors import VenueUnavailableError
from pmcopy.services.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from pmcopy.services.telemetry.latency import PipelineLatency
from tests.telemetry.test_pipeline_latency import ReceiptClock
from tests.test_ccxt_prediction_gateway_contract import FakeExchange, TOKEN


@pytest.mark.asyncio
async def test_cold_and_expired_metadata_fetches_are_observed_but_cache_hits_are_not() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    exchange = FakeExchange()

    async def fetch(parameters):
        clock.nanoseconds += 20_000_000
        return exchange.clob_market

    exchange.clobPublicGetClobMarketsConditionId = AsyncMock(side_effect=fetch)
    gateway = CcxtPredictionGateway(
        exchange,
        metadata_ttl_seconds=1,
        monotonic=lambda: clock.nanoseconds / 1_000_000_000,
        latency=PipelineLatency(clock=clock, metrics=registry),
    )
    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    clock.nanoseconds += 2_000_000_000
    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    metric = registry.histograms['telemetry_latency_seconds{phase="metadata_cache_miss"}']
    assert metric.count == 2
    assert metric.total == 0.04
    assert exchange.clobPublicGetClobMarketsConditionId.await_count == 2


@pytest.mark.asyncio
async def test_failed_metadata_attempt_is_measured_without_successful_cache_entry() -> None:
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    exchange = FakeExchange()

    async def fetch(parameters):
        clock.nanoseconds += 20_000_000
        raise VenueUnavailableError("metadata unavailable")

    exchange.clobPublicGetClobMarketsConditionId = AsyncMock(side_effect=fetch)
    gateway = CcxtPredictionGateway(
        exchange, latency=PipelineLatency(clock=clock, metrics=registry)
    )
    for _ in range(2):
        with pytest.raises(VenueUnavailableError):
            await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    metric = registry.histograms['telemetry_latency_seconds{phase="metadata_cache_miss"}']
    assert metric.count == 2
    assert metric.total == 0.04
