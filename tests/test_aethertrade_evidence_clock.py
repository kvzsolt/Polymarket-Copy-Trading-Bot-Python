"""Producer receipt evidence retains the actual wall-clock nanosecond sample."""

import pytest

from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.domain.source_time import TransportReceipt


class TestEvidenceClock:
    """Nanosecond units do not imply kernel timestamp or clock accuracy claims."""

    def test_clock_retains_native_sample_once(self, monkeypatch):
        import pmcopy.adapters.clock as clock_module
        calls=[]
        def wall():
            calls.append(True)
            return 1234567890123456789
        monkeypatch.setattr(clock_module, 'time_ns', wall)
        monkeypatch.setattr(clock_module, 'monotonic_ns', lambda: 81)
        receipt=SystemReceiptClock(boot_id='fixture-boot').capture()
        assert receipt.wall_timestamp_ns == 1234567890123456789
        assert receipt.wall_timestamp_ms == 1234567890123
        assert receipt.monotonic_ns == 81
        assert len(calls) == 1

    def test_legacy_receipt_does_not_claim_native_nanoseconds(self):
        assert TransportReceipt(1234, 5, 'fixture').wall_timestamp_ns is None

    @pytest.mark.parametrize('ns', [True, -1, 1.1, '1234000000', 1235000000])
    def test_inexact_or_inconsistent_clock_is_rejected(self, ns):
        with pytest.raises(ValueError):
            TransportReceipt(1234, 5, 'fixture', wall_timestamp_ns=ns)
