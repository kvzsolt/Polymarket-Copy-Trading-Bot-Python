"""Old outcome metadata is evicted without changing execution metadata values."""

from collections import Counter

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from tests.test_ccxt_prediction_gateway_contract import FakeExchange


class OutcomeReader(FakeExchange):
    """Return validated outcome metadata and count public source reads."""

    def __init__(self) -> None:
        super().__init__()
        self.reads: Counter[str] = Counter()

    async def load_outcome(self, token_id):
        """Return the fixture metadata for the requested raw outcome identity."""
        self.reads[token_id] += 1
        return self.outcome_value


@pytest.mark.asyncio
async def test_execution_metadata_cache_evicts_old_outcomes_and_refetches_exact_metadata():
    source = OutcomeReader()
    gateway = CcxtPredictionGateway(source, metadata_cache_size=2)
    first = await gateway.get_execution_metadata("1")
    await gateway.get_execution_metadata("2")
    await gateway.get_execution_metadata("3")
    assert await gateway.get_execution_metadata("1") == first
    assert source.reads["1"] == 2
    assert source.reads["2"] == 1
    assert source.reads["3"] == 1


@pytest.mark.asyncio
async def test_execution_metadata_cache_keeps_recently_used_outcomes() -> None:
    source = OutcomeReader()
    gateway = CcxtPredictionGateway(source, metadata_cache_size=2)
    await gateway.get_execution_metadata("1")
    await gateway.get_execution_metadata("2")
    await gateway.get_execution_metadata("1")
    await gateway.get_execution_metadata("3")
    await gateway.get_execution_metadata("1")
    await gateway.get_execution_metadata("2")
    assert source.reads == Counter({"1": 1, "2": 2, "3": 1})


@pytest.mark.parametrize("bound", [0, -1, True, 1.5])
def test_execution_metadata_cache_requires_a_positive_integer_bound(bound):
    with pytest.raises(ValueError, match="metadata_cache_size"):
        CcxtPredictionGateway(OutcomeReader(), metadata_cache_size=bound)
