"""Receipt-status probe for the pending-copy confirmation tracker.

The gen-1 on-chain detection client is WebSocket-only: it can see a
transaction in the mempool but cannot ask whether it later mined. This adapter
supplies that one fact over HTTP RPC, reusing the repository's read-only
failover provider (``FallbackWeb3Provider``) and the thread-offloading
``AsyncWeb3Adapter`` so the call never blocks the event loop.

Mapping (verified against web3.py 7.15.0
``web3/_utils/method_formatters.py`` ``NULL_RESULT_FORMATTERS``, which maps a
null ``eth_getTransactionReceipt`` result to ``raise_transaction_not_found``):

* receipt with ``status == 1`` -> ``1`` (mined, succeeded);
* receipt with ``status == 0`` -> ``0`` (mined, reverted);
* ``TransactionNotFound``      -> ``None`` (not mined yet);
* any other RPC failure        -> raised, so the caller can tell an outage
  from a verdict.

RPC URLs carry the provider API key and are never logged or echoed in errors.
"""

from __future__ import annotations

from typing import Any

from web3.exceptions import TransactionNotFound

from clients.network.rpc_fallback import FallbackWeb3Provider
from clients.network.web3_adapter import AsyncWeb3Adapter


class Web3ReceiptStatusFetcher:
    """Report whether one transaction mined, reverted, or is still pending."""

    def __init__(self, web3_adapter: AsyncWeb3Adapter) -> None:
        """Bind the async web3 adapter used for the receipt lookup.

        Args:
            web3_adapter: Adapter that runs web3 calls off the event loop.
        """

        self._adapter = web3_adapter

    @classmethod
    def from_rpc_url(
        cls, primary: str, fallback: str = "", *, provider_runtime=None
    ) -> "Web3ReceiptStatusFetcher":
        """Build a fetcher over the read-only primary+fallback RPC provider.

        Args:
            primary: Primary Polygon HTTP RPC endpoint (secret; never logged).
            fallback: Optional secondary endpoint used on transient primary
                failures. Ignored when blank or identical to ``primary``.

        Returns:
            A fetcher bound to a fresh ``Web3`` instance.

        Raises:
            ValueError: If ``primary`` is blank. The message never echoes an
                endpoint: the URL path carries the provider API key.
        """

        if not str(primary or "").strip():
            raise ValueError("a primary RPC endpoint is required for receipt confirmation")
        if provider_runtime is not None:
            return cls(AsyncWeb3Adapter(provider_runtime.web3()))
        provider = FallbackWeb3Provider(str(primary).strip(), str(fallback or "").strip())
        return cls(AsyncWeb3Adapter(provider.get_web3()))

    async def fetch_status(self, tx_hash: str) -> int | None:
        """Return the receipt status for ``tx_hash``.

        Args:
            tx_hash: Transaction hash to probe.

        Returns:
            ``1`` when mined successfully, ``0`` when mined and reverted, and
            ``None`` when the node has no receipt yet (or returned one without
            a ``status`` field, which is equally undecided).

        Raises:
            Exception: Any transport or provider failure other than
                ``TransactionNotFound``.
        """

        if not str(tx_hash or "").strip():
            return None
        try:
            receipt: Any = await self._adapter.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            return None
        if receipt is None:
            return None
        status = self._read_status(receipt)
        return None if status is None else int(status)

    @staticmethod
    def _read_status(receipt: Any) -> int | None:
        """Extract ``status`` from a mapping-like or attribute-style receipt."""

        try:
            status = receipt["status"]
        except (KeyError, TypeError, IndexError):
            status = getattr(receipt, "status", None)
        if status is None:
            return None
        try:
            return int(status)
        except (TypeError, ValueError):
            return None


__all__ = ["Web3ReceiptStatusFetcher"]
