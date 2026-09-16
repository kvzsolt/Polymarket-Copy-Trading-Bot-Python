"""Tests for ``clients.network.web3_receipt_fetcher.Web3ReceiptStatusFetcher``.

The pending-copy confirmation tracker needs exactly one fact about a leader
transaction: mined-and-successful (1), mined-and-reverted (0), or not mined
yet (``None``). web3.py signals "not mined yet" by raising
``TransactionNotFound`` (its ``eth_getTransactionReceipt`` null-result
formatter), so that exception — and only that one — maps to ``None``; every
other RPC failure propagates so the tracker can distinguish an outage from a
verdict.

RPC URLs carry the provider key and are never logged.
"""

from __future__ import annotations

import pytest
from web3.exceptions import TransactionNotFound

from clients.network.web3_adapter import AsyncWeb3Adapter
from clients.network.web3_receipt_fetcher import Web3ReceiptStatusFetcher

_TX = "0x" + "cd" * 32


class _FakeEth:
    def __init__(self, receipt: object = None, error: BaseException | None = None) -> None:
        self.calls: list[str] = []
        self._receipt = receipt
        self._error = error

    def get_transaction_receipt(self, tx_hash: str) -> object:
        self.calls.append(tx_hash)
        if self._error is not None:
            raise self._error
        return self._receipt


class _FakeWeb3:
    def __init__(self, eth: _FakeEth) -> None:
        self.eth = eth


def _fetcher(receipt: object = None, error: BaseException | None = None):
    eth = _FakeEth(receipt=receipt, error=error)
    adapter = AsyncWeb3Adapter(_FakeWeb3(eth))
    return Web3ReceiptStatusFetcher(adapter), eth


async def test_successful_receipt_reports_one() -> None:
    fetcher, eth = _fetcher(receipt={"status": 1})

    assert await fetcher.fetch_status(_TX) == 1
    assert eth.calls == [_TX]


async def test_reverted_receipt_reports_zero() -> None:
    fetcher, _eth = _fetcher(receipt={"status": 0})

    assert await fetcher.fetch_status(_TX) == 0


async def test_transaction_not_found_reports_none() -> None:
    fetcher, _eth = _fetcher(error=TransactionNotFound("not mined"))

    assert await fetcher.fetch_status(_TX) is None


async def test_a_receipt_without_a_status_field_reports_none() -> None:
    """Pre-Byzantium/atypical receipts carry no status; treat as undecided."""
    fetcher, _eth = _fetcher(receipt={"blockNumber": 42})

    assert await fetcher.fetch_status(_TX) is None


async def test_a_null_receipt_reports_none() -> None:
    fetcher, _eth = _fetcher(receipt=None)

    assert await fetcher.fetch_status(_TX) is None


async def test_other_rpc_errors_propagate() -> None:
    fetcher, _eth = _fetcher(error=RuntimeError("rpc down"))

    with pytest.raises(RuntimeError, match="rpc down"):
        await fetcher.fetch_status(_TX)


async def test_a_blank_tx_hash_never_reaches_the_node() -> None:
    fetcher, eth = _fetcher(receipt={"status": 1})

    assert await fetcher.fetch_status("") is None
    assert eth.calls == []


def test_from_rpc_url_builds_a_fallback_backed_fetcher() -> None:
    """The production factory reuses the shared read-only failover provider."""
    fetcher = Web3ReceiptStatusFetcher.from_rpc_url(
        "https://polygon.example.invalid/v2/primary-key",
        "https://polygon-backup.example.invalid/v2/backup-key",
    )

    assert isinstance(fetcher, Web3ReceiptStatusFetcher)


def test_from_rpc_url_requires_a_primary_endpoint() -> None:
    with pytest.raises(ValueError) as excinfo:
        Web3ReceiptStatusFetcher.from_rpc_url("", "")

    # The message must not echo any URL: they carry the provider API key.
    assert "primary" in str(excinfo.value).lower()


def test_from_rpc_url_error_never_echoes_the_endpoint() -> None:
    secret = "SuperSecretProviderKey123"
    try:
        Web3ReceiptStatusFetcher.from_rpc_url("   ", f"https://x.invalid/v2/{secret}")
    except ValueError as exc:
        assert secret not in str(exc)
    else:  # pragma: no cover - the call above must raise
        raise AssertionError("expected a ValueError")


async def test_adapter_get_transaction_receipt_is_offloaded_not_called_inline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``AsyncWeb3Adapter`` must route the blocking RPC through ``to_thread``.

    web3's ``eth.get_transaction_receipt`` is a synchronous HTTP call; calling
    it directly from the loop would stall detection and execution for the
    duration of the request.
    """
    import asyncio

    import clients.network.web3_adapter as web3_adapter_module

    offloaded: list[str] = []

    async def _recording_to_thread(function, *args, **kwargs):
        offloaded.append(getattr(function, "__name__", repr(function)))
        await asyncio.sleep(0)
        return function(*args, **kwargs)

    monkeypatch.setattr(web3_adapter_module.asyncio, "to_thread", _recording_to_thread)

    eth = _FakeEth(receipt={"status": 1})
    adapter = AsyncWeb3Adapter(_FakeWeb3(eth))

    receipt = await adapter.get_transaction_receipt(_TX)

    assert receipt == {"status": 1}
    assert offloaded == ["get_transaction_receipt"]
    assert eth.calls == [_TX]
