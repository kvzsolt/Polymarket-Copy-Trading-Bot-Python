"""Removed or unidentified chain logs cannot become canonical copied fills."""

import pytest

from services.onchain_detection.order_filled_detector import decode_order_filled_log
from tests.test_order_filled_detector import _build_log


@pytest.mark.parametrize("removed", [True, "true", "false", 1, 0, None])
def test_removed_or_malformed_removal_evidence_never_decodes_as_a_new_fill(removed) -> None:
    log = _build_log()
    log["removed"] = removed
    assert decode_order_filled_log(log) is None


@pytest.mark.parametrize("index", [None, True, False, -1, "-1", "bad", "0x", 1.5, "0x_1", "0x1_0"])
def test_unidentified_log_never_defaults_to_log_zero(index) -> None:
    log = _build_log()
    log["logIndex"] = index
    assert decode_order_filled_log(log) is None


def test_missing_log_index_never_defaults_to_log_zero() -> None:
    log = _build_log()
    del log["logIndex"]
    assert decode_order_filled_log(log) is None


@pytest.mark.parametrize("transaction", [None, True, "", "0xshort"])
def test_mined_fill_requires_transaction_identity(transaction) -> None:
    log = _build_log()
    log["transactionHash"] = transaction
    assert decode_order_filled_log(log) is None


@pytest.mark.parametrize("index", [0, "0x0", "0xa"])
def test_valid_zero_index_and_omitted_removal_flag_preserve_canonical_fill(index) -> None:
    log = _build_log()
    log["logIndex"] = index
    decoded = decode_order_filled_log(log)
    assert decoded is not None
    assert decoded.log_index == (10 if index == "0xa" else 0)
