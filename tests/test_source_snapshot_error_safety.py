"""Malformed restart snapshots cannot expose payloads through chained exceptions."""

import traceback

import pytest

from pmcopy.adapters.persistence.source_transaction_state_store import SourceTransactionSnapshotCodec


def test_invalid_snapshot_has_no_secret_bearing_validation_cause():
    payload = '{"records":"synthetic-private-key"}'
    with pytest.raises(ValueError) as captured:
        SourceTransactionSnapshotCodec().decode(payload)
    assert "synthetic-private-key" not in "".join(traceback.format_exception(captured.value))
