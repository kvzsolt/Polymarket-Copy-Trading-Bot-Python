"""Paper observation covers both dispatch paths without awaiting analytics."""

from unittest.mock import AsyncMock, Mock

from pmcopy.orchestrators.lifecycle.runtime_monitor import LeaderBatchDispatcher
from pmcopy.orchestrators.detection.onchain_signal_dispatch import OnchainSignalDispatchAdapter
from tests.test_onchain_signal_dispatch import _signal


async def test_polling_observes_before_selected_pipeline_without_changing_dispatch():
    observed = Mock()
    sink = AsyncMock()
    dispatcher = LeaderBatchDispatcher(
        leader="leader", contexts=(), legacy_executor=AsyncMock(), v1_sink=sink, observer=observed
    )
    rows = [{"side": "BUY"}]
    await dispatcher.dispatch(rows)
    observed.assert_called_once_with("leader", rows)
    sink.assert_awaited_once_with("leader", rows)


async def test_onchain_observes_only_confirmed_signals_on_both_paths():
    observed = Mock()
    sink = Mock(submit_onchain=AsyncMock())
    adapter = OnchainSignalDispatchAdapter(
        contexts_by_leader={}, v1_runtime=sink, execute_trades=AsyncMock(), observer=observed
    )
    await adapter(_signal(provisional=True))
    observed.assert_not_called()
    signal = _signal(provisional=False)
    await adapter(signal)
    observed.assert_called_once_with(signal.leader_wallet, [signal.as_raw_signal_dict()])
    assert sink.submit_onchain.await_count == 2
