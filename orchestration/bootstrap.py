"""Compose production runtime dependencies through narrow interfaces."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

from adapters.clock import SystemClock
from adapters.polygon.alchemy_connection_manager import (
    AlchemyConnectionManager,
    AsyncioSleeper,
    ExponentialReconnectPolicy,
    SystemRandomSource,
    WebsocketsJsonRpcConnector,
)
from adapters.polygon.alchemy_json_rpc import AlchemySubscriptionOptions
from application.confirmed_signal_execution_adapter import (
    CallableOnchainSignalSink,
    ConfirmedSignalExecutionAdapter,
)
from application.source_transaction_pipeline import SourceTransactionPipeline
from domain.source_transactions import EvmReceipt
from interfaces.source_transactions import EvmRpcPort, SourceTransactionStateStore
from interfaces.source_time import BlockTimestampReader
from orchestration.polygon_source_transaction_runtime import (
    PolygonSourceTransactionRuntime,
)
from services.onchain_detection.calldata_decoder import SourceTransactionCalldataDecoder
from services.onchain_detection.order_filled_receipt_decoder import OrderFilledReceiptDecoder
from services.onchain_detection.pending_transaction_factory import PendingTransactionFactory
from services.onchain_detection.source_transaction_gate import (
    SourceTransactionGate,
    SourceTransactionRegistry,
)
from services.onchain_detection.trade_detection_models import OnchainTradeSignal


class _AddressSet:
    """Normalize EVM address collections at the orchestration boundary."""

    @staticmethod
    def normalize(values: tuple[str, ...] | frozenset[str]) -> tuple[str, ...]:
        normalized: list[str] = []
        for value in values:
            address = str(value or "").strip().lower()
            if len(address) != 42 or not address.startswith("0x"):
                raise ValueError("runtime address must be a 20-byte hex address")
            try:
                int(address[2:], 16)
            except ValueError as exc:
                raise ValueError("runtime address must be a 20-byte hex address") from exc
            if address not in normalized:
                normalized.append(address)
        return tuple(normalized)


@dataclass(frozen=True, slots=True)
class PolygonSourceRuntimeConfig:
    """Validated production settings for source-transaction confirmation."""

    chain_id: int
    websocket_url: str
    exchange_addresses: tuple[str, ...]
    watched_wallets: frozenset[str]
    confirmation_timeout: timedelta = timedelta(seconds=60)

    def __post_init__(self) -> None:
        """Validate chain, URL, addresses, and confirmation lifetime."""

        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if not str(self.websocket_url or "").strip():
            raise ValueError("websocket_url is required")
        if self.confirmation_timeout <= timedelta(0):
            raise ValueError("confirmation_timeout must be positive")
        exchanges = _AddressSet.normalize(self.exchange_addresses)
        watched = frozenset(_AddressSet.normalize(self.watched_wallets))
        if not exchanges:
            raise ValueError("exchange_addresses must not be empty")
        object.__setattr__(self, "exchange_addresses", exchanges)
        object.__setattr__(self, "watched_wallets", watched)

    @property
    def pending_targets(self) -> tuple[str, ...]:
        """Return direct exchange plus watched Safe destination filters."""

        return tuple(dict.fromkeys((*self.exchange_addresses, *sorted(self.watched_wallets))))


class _BoundEvmRpcPort:
    """Bind the circular runtime RPC edge exactly once during composition."""

    def __init__(self) -> None:
        self._delegate: EvmRpcPort | None = None

    def bind(self, delegate: EvmRpcPort) -> None:
        """Bind the production RPC delegate exactly once.

        Args:
            delegate: Connected-session manager serving receipt and nonce reads.

        Raises:
            RuntimeError: If already bound.
        """

        if self._delegate is not None:
            raise RuntimeError("EVM RPC port is already bound")
        self._delegate = delegate

    async def get_transaction_receipt(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> EvmReceipt | None:
        """Delegate a receipt lookup to the bound runtime manager."""

        return await self._require_delegate().get_transaction_receipt(
            chain_id,
            transaction_hash,
        )

    async def get_confirmed_nonce(self, chain_id: int, address: str) -> int:
        """Delegate a confirmed nonce lookup to the bound runtime manager."""

        return await self._require_delegate().get_confirmed_nonce(chain_id, address)

    def _require_delegate(self) -> EvmRpcPort:
        if self._delegate is None:
            raise RuntimeError("EVM RPC port is not bound")
        return self._delegate


class PolygonSourceRuntimeBootstrap:
    """Build the Polygon confirmation graph without lazy hot-path setup."""

    def build(
        self,
        config: PolygonSourceRuntimeConfig,
        signal_callback: Callable[[OnchainTradeSignal], Awaitable[None]],
        state_store: SourceTransactionStateStore | None = None,
        block_timestamp_reader: BlockTimestampReader | None = None,
    ) -> PolygonSourceTransactionRuntime:
        """Return a fully composed but not-yet-started runtime.

        Args:
            config: Validated Polygon source runtime configuration.
            signal_callback: Existing authoritative leader-signal consumer.
            state_store: Durable source reservation/idempotency snapshot adapter.
            block_timestamp_reader: Public immutable block-time evidence.

        Returns:
            Runtime ready to connect, subscribe, and resynchronize at startup.
        """

        clock = SystemClock()
        registry = SourceTransactionRegistry(clock=clock, state_store=state_store)
        rpc = _BoundEvmRpcPort()
        signal_sink = CallableOnchainSignalSink(signal_callback)
        execution = ConfirmedSignalExecutionAdapter(
            signal_sink,
            clock,
            block_timestamp_reader=block_timestamp_reader,
        )
        gate = SourceTransactionGate(
            registry=registry,
            evm_rpc=rpc,
            receipt_decoder=OrderFilledReceiptDecoder(),
            execution=execution,
            clock=clock,
        )
        calldata_decoder = SourceTransactionCalldataDecoder(
            exchange_addresses=frozenset(config.exchange_addresses)
        )
        pending_builder = PendingTransactionFactory(
            chain_id=config.chain_id,
            calldata_decoder=calldata_decoder,
            clock=clock,
            confirmation_timeout=config.confirmation_timeout,
        )
        pipeline = SourceTransactionPipeline(
            chain_id=config.chain_id,
            watched_wallets=config.watched_wallets,
            pending_builder=pending_builder,
            gate=gate,
        )
        manager = AlchemyConnectionManager(
            url=config.websocket_url,
            connector=WebsocketsJsonRpcConnector(),
            options=AlchemySubscriptionOptions(
                chain_id=config.chain_id,
                pending_targets=config.pending_targets,
                log_addresses=config.exchange_addresses,
            ),
            sink=pipeline,
            clock=clock,
            reconnect_policy=ExponentialReconnectPolicy(SystemRandomSource()),
            sleeper=AsyncioSleeper(),
        )
        rpc.bind(manager)
        return PolygonSourceTransactionRuntime(manager=manager, pipeline=pipeline)


__all__ = [
    "PolygonSourceRuntimeBootstrap",
    "PolygonSourceRuntimeConfig",
]
