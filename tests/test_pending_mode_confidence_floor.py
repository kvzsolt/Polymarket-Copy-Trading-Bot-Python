"""Cross-layer pin for the ``on_chain_pending`` confidence floor.

``config.app_config._PENDING_MODE_CONFIDENCE_VALUES`` exists because a floor
above what a mempool sighting can reach silently drops 100 % of pending
signals. The facts that make it true live in two other modules:

* ``services/onchain_detection/pending_tx_detector.py`` stamps the confidence
  on every pending signal;
* ``orchestrators/detection/onchain_detection_runtime.py`` owns ``_CONFIDENCE_RANK`` and
  drops any signal ranked below the configured floor.

So the expected set is *derived* from those two modules here — the emitted
confidence by running the real detector, the ordering from the real rank map —
rather than restated as literals. Raising the detector's confidence (or
reordering the ladder) without widening the accepted floors then fails here
instead of in production.
"""

from __future__ import annotations

from pmcopy.config.app_config import (
    _ONCHAIN_DETECTION_CONFIDENCE_VALUES,
    _PENDING_MODE_CONFIDENCE_VALUES,
)
from pmcopy.orchestrators.detection.onchain_detection_runtime import _CONFIDENCE_RANK
from pmcopy.services.onchain_detection.pending_tx_detector import build_signals_from_pending_tx
from pmcopy.services.onchain_detection.trade_detection_models import (
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    DetectionConfidence,
)
from tests.test_pending_tx_detector import _UNWATCHED, _WATCHED, _encode, _mk_order, _tx


def _emitted_pending_confidence() -> DetectionConfidence:
    """Return the confidence the real detector stamps on a pending signal."""
    taker = _mk_order(_WATCHED, side=MAKER_SIDE_BUY, token_id=789)
    calldata = _encode(taker, [_mk_order(_UNWATCHED, side=MAKER_SIDE_SELL)])

    signals = build_signals_from_pending_tx(_tx(calldata), {_WATCHED}, recv_ts_ms=1000)

    assert len(signals) == 1, "fixture must produce exactly one watched-taker signal"
    return signals[0].confidence


def test_pending_mode_floor_is_exactly_what_a_mempool_sighting_can_clear() -> None:
    """The accepted floors are the ranks at or below the emitted confidence."""
    emitted = _emitted_pending_confidence()
    emitted_rank = _CONFIDENCE_RANK[emitted]

    clearable = {level.value for level, rank in _CONFIDENCE_RANK.items() if rank <= emitted_rank}

    assert set(_PENDING_MODE_CONFIDENCE_VALUES) == clearable


def test_rejected_floors_are_exactly_the_ranks_above_the_emitted_confidence() -> None:
    """The complement is non-empty, so the validation rule is not vacuous."""
    emitted_rank = _CONFIDENCE_RANK[_emitted_pending_confidence()]

    rejected = {level.value for level, rank in _CONFIDENCE_RANK.items() if rank > emitted_rank}

    assert rejected
    assert rejected.isdisjoint(_PENDING_MODE_CONFIDENCE_VALUES)
    # Every rejected floor is still a globally valid value, i.e. the pending
    # rule narrows the generic one rather than duplicating it.
    assert rejected <= _ONCHAIN_DETECTION_CONFIDENCE_VALUES


def test_pending_mode_floors_are_a_subset_of_the_globally_accepted_values() -> None:
    """A typo in the pending tuple would otherwise be unreachable config."""
    assert set(_PENDING_MODE_CONFIDENCE_VALUES) <= _ONCHAIN_DETECTION_CONFIDENCE_VALUES
    assert set(_PENDING_MODE_CONFIDENCE_VALUES) == {
        level.value for level in _CONFIDENCE_RANK if level.value in _PENDING_MODE_CONFIDENCE_VALUES
    }
