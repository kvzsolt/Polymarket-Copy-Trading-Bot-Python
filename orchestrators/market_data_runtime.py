"""Runtime owner for WSS-backed market data.

This module wires together:

    MarketStateStore
    MarketWebSocket
    ClobGateway REST snapshot bootstrap
    ReadinessTracker

It does not make trading decisions. It only keeps token market state fresh.

Trade detection remains HTTP polling. Market WSS is only for quote/book/tick/
market-state data.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from cache.market_state_store import MarketStateStore
from clients.market_ws import DEFAULT_MARKET_WS_URL, MarketWebSocket
from interfaces.clob_gateway import ClobGateway
from services.healthServer import ReadinessTracker
from utils.logger import get_logger

logger = get_logger("MarketDataRuntime")


@dataclass
class MarketDataRuntimeStatus:
    running: bool = False
    subscribed_token_count: int = 0
    stale_token_count: int = 0
    dirty_token_count: int = 0
    last_error: str | None = None


@dataclass
class MarketDataRuntime:
    """Owns the market WSS task for one runtime/user state.

    This runtime is intentionally small. It should be created by app/bootstrap
    and stopped during graceful shutdown.

    It subscribes to token ids, bootstraps each token with REST snapshots, then
    lets MarketWebSocket keep the MarketStateStore fresh.
    """

    market_store: MarketStateStore
    gateway: ClobGateway
    readiness: ReadinessTracker
    config: Any
    ws_url: str = DEFAULT_MARKET_WS_URL

    _ws_client: MarketWebSocket | None = field(default=None, init=False)
    _ws_task: asyncio.Task | None = field(default=None, init=False)
    _subscribed_tokens: set[str] = field(default_factory=set, init=False)
    _status: MarketDataRuntimeStatus = field(
        default_factory=MarketDataRuntimeStatus,
        init=False,
    )

    async def start(self, initial_token_ids: list[str]) -> None:
        """Start market WSS runtime.

        If no token ids are available, the runtime stays not-ready. That is
        safer than marking market data healthy without subscriptions.
        """
        token_ids = _normalize_token_ids(initial_token_ids)

        self._subscribed_tokens.update(token_ids)
        self._status.running = True
        self._status.subscribed_token_count = len(self._subscribed_tokens)
        self._status.last_error = None

        if not token_ids:
            logger.warning("market_data_runtime_start_empty_token_set")
            self._update_readiness(
                wss_market_ok=False,
                wss_market_connecting=False,
                market_data_ready=False,
                last_error="no tokens subscribed",
            )
            return

        self._ws_client = MarketWebSocket(
            cache=self.market_store,
            ws_url=self.ws_url,
            rest_bootstrap=self.bootstrap_tokens,
            on_connected=self._on_wss_connected,
            on_disconnected=self._on_wss_disconnected,
        )

        self._ws_task = asyncio.create_task(
            self._run_ws(token_ids),
            name="market-wss-runtime",
        )

        self._update_readiness(
            wss_market_ok=False,
            wss_market_connecting=True,
            market_data_ready=False,
            subscribed_token_count=len(self._subscribed_tokens),
        )

        logger.info(
            "market_data_runtime_starting | subscribed_tokens=%d",
            len(self._subscribed_tokens),
        )

    async def stop(self) -> None:
        """Stop market WSS runtime."""
        self._status.running = False

        if self._ws_client is not None:
            try:
                await self._ws_client.stop()
            except Exception as exc:
                logger.warning("market_ws_stop_error | error=%r", exc)

        if self._ws_task is not None:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning("market_ws_task_stop_error | error=%r", exc)

        self._ws_client = None
        self._ws_task = None

        self.market_store.mark_all_stale("market_data_runtime_stopped")

        self._update_readiness(
            wss_market_ok=False,
            market_data_ready=False,
            last_error="stopped",
        )

        logger.info("market_data_runtime_stopped")

    async def subscribe_tokens(self, token_ids: list[str]) -> None:
        """Subscribe additional token ids at runtime."""
        normalized = _normalize_token_ids(token_ids)
        new_tokens = [token for token in normalized if token not in self._subscribed_tokens]
        if not new_tokens:
            return

        self._subscribed_tokens.update(new_tokens)
        self._status.subscribed_token_count = len(self._subscribed_tokens)

        if self._ws_client is not None:
            await self._ws_client.subscribe(new_tokens)

        self._update_readiness(
            subscribed_token_count=len(self._subscribed_tokens),
        )

        logger.info(
            "market_data_runtime_subscribed | new=%d total=%d",
            len(new_tokens),
            len(self._subscribed_tokens),
        )

    async def unsubscribe_tokens(self, token_ids: list[str]) -> None:
        """Unsubscribe token ids.

        This should generally be called only after positions/orders using those
        tokens are fully closed and no protective exit depends on them.
        """
        normalized = _normalize_token_ids(token_ids)
        remove_tokens = [token for token in normalized if token in self._subscribed_tokens]
        if not remove_tokens:
            return

        for token in remove_tokens:
            self._subscribed_tokens.discard(token)

        if self._ws_client is not None:
            await self._ws_client.unsubscribe(remove_tokens)

        self._status.subscribed_token_count = len(self._subscribed_tokens)

        self._update_readiness(
            subscribed_token_count=len(self._subscribed_tokens),
        )

        logger.info(
            "market_data_runtime_unsubscribed | removed=%d total=%d",
            len(remove_tokens),
            len(self._subscribed_tokens),
        )

    async def bootstrap_tokens(self, token_ids: list[str]) -> None:
        """REST bootstrap/resync for tokens before WSS reconnect resumes.

        REST is used here to seed/repair local market state. Whether REST state
        may be used for live execution is still controlled by MarketDataService
        policy. In live mode, quote/book should become WSS-fresh before orders.
        """
        normalized = _normalize_token_ids(token_ids)

        for token_id in normalized:
            try:
                await self._bootstrap_one_token(token_id)
            except Exception as exc:
                self.market_store.mark_dirty(token_id, "rest_bootstrap_failed")
                self._status.last_error = repr(exc)
                logger.warning(
                    "market_data_bootstrap_failed | token_id=%s error=%r",
                    token_id,
                    exc,
                )

        self._refresh_counts()
        self._update_readiness_from_store()

    def status(self) -> MarketDataRuntimeStatus:
        self._refresh_counts()
        return MarketDataRuntimeStatus(
            running=self._status.running,
            subscribed_token_count=self._status.subscribed_token_count,
            stale_token_count=self._status.stale_token_count,
            dirty_token_count=self._status.dirty_token_count,
            last_error=self._status.last_error,
        )

    async def _run_ws(self, token_ids: list[str]) -> None:
        assert self._ws_client is not None

        try:
            await self._ws_client.connect(token_ids)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._status.last_error = repr(exc)
            self.market_store.mark_all_stale("market_ws_task_failed")
            self._update_readiness(
                wss_market_ok=False,
                market_data_ready=False,
                last_error=repr(exc),
            )
            logger.exception("market_ws_task_failed")
            raise

    async def _on_wss_connected(self) -> None:
        self._status.last_error = None
        self._update_readiness(
            wss_market_ok=True,
            wss_market_connecting=False,
            market_data_ready=False,
            subscribed_token_count=len(self._subscribed_tokens),
        )
        logger.info(
            "market_wss_connected",
            extra={"subscribed_token_count": len(self._subscribed_tokens)},
        )

    async def _on_wss_disconnected(self) -> None:
        self.market_store.mark_all_stale("market_wss_disconnected")
        self._update_readiness(
            wss_market_ok=False,
            wss_market_connecting=False,
            market_data_ready=False,
            last_error="market websocket disconnected",
        )
        logger.warning("market_wss_disconnected")

    async def _bootstrap_one_token(self, token_id: str) -> None:
        book = await self.gateway.get_orderbook(token_id)

        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}

        for level in getattr(book, "bids", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                bids[price] = size

        for level in getattr(book, "asks", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                asks[price] = size

        if not bids or not asks:
            self.market_store.mark_dirty(token_id, "rest_bootstrap_empty_book")
            return

        self.market_store.replace_book_snapshot(
            token_id,
            bids=bids,
            asks=asks,
            raw_event={"source": "rest_snapshot"},
        )

        info = await self.gateway.get_market_info(token_id)

        tick_size = _to_decimal(
            info.get("tick_size")
            or info.get("minimum_tick_size")
        )
        min_order_size = _to_decimal(
            info.get("min_order_size")
            or info.get("minimum_order_size")
            or info.get("order_min_size")
        )

        if tick_size is None or tick_size <= 0:
            self.market_store.mark_dirty(token_id, "rest_bootstrap_missing_tick")
            return

        if min_order_size is None or min_order_size <= 0:
            self.market_store.mark_dirty(token_id, "rest_bootstrap_missing_min_order")
            return

        self.market_store.set_tick(
            token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=bool(info.get("neg_risk", False)),
            source="rest_snapshot",
        )

        # REST bootstrap gives a baseline only. It must not satisfy WSS-fresh
        # execution policy. The next WSS `book` / price event must make the token
        # executable.
        self.market_store.mark_dirty(token_id, "awaiting_wss_after_rest_bootstrap")

    def _refresh_counts(self) -> None:
        stale = 0
        dirty = 0

        for token_id in self._subscribed_tokens:
            if self.market_store.is_stale(token_id):
                stale += 1
            if self.market_store.is_dirty(token_id):
                dirty += 1

        self._status.subscribed_token_count = len(self._subscribed_tokens)
        self._status.stale_token_count = stale
        self._status.dirty_token_count = dirty

    def _update_readiness_from_store(self) -> None:
        self._refresh_counts()

        market_ready = (
            self._status.running
            and self._status.subscribed_token_count > 0
            and self._status.stale_token_count == 0
            and self._status.dirty_token_count == 0
        )

        self._update_readiness(
            market_data_ready=market_ready,
            subscribed_token_count=self._status.subscribed_token_count,
            stale_token_count=self._status.stale_token_count,
            dirty_token_count=self._status.dirty_token_count,
        )

    def _update_readiness(self, **fields: Any) -> None:
        try:
            self.readiness.update(**fields)
        except Exception as exc:
            logger.debug("market_data_readiness_update_failed | error=%r", exc)


def extract_token_ids_from_live_positions(positions: list[dict[str, Any]]) -> list[str]:
    """Best-effort token-id extraction from Polymarket positions API rows."""
    token_ids: list[str] = []

    for position in positions:
        token_id = (
            position.get("asset")
            or position.get("asset_id")
            or position.get("assetId")
            or position.get("token_id")
            or position.get("tokenId")
            or position.get("clobTokenId")
        )

        if token_id:
            token_ids.append(str(token_id))
            continue

        # Some payloads contain clobTokenIds as list/string.
        clob_token_ids = position.get("clobTokenIds")
        if isinstance(clob_token_ids, list):
            token_ids.extend(str(item) for item in clob_token_ids if item)
        elif isinstance(clob_token_ids, str) and clob_token_ids.strip():
            token_ids.append(clob_token_ids.strip())

    return _normalize_token_ids(token_ids)


def extract_token_ids_from_contexts(contexts: list[Any]) -> list[str]:
    """Extract known token ids from runtime contexts.

    This is intentionally conservative. It only uses obvious position manager
    shapes and avoids making assumptions about leader trade polling.
    """
    token_ids: list[str] = []

    for context in contexts:
        position_manager = getattr(context, "position_manager", None)
        if position_manager is None:
            continue

        for attr_name in ("positions", "_positions"):
            positions = getattr(position_manager, attr_name, None)
            if isinstance(positions, dict):
                for key, value in positions.items():
                    token_id = getattr(key, "token_id", None) or getattr(value, "token_id", None)
                    if token_id:
                        token_ids.append(str(token_id))

    return _normalize_token_ids(token_ids)


def _normalize_token_ids(token_ids: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for raw in token_ids:
        token_id = str(raw).strip()
        if not token_id or token_id in seen:
            continue
        seen.add(token_id)
        result.append(token_id)

    return result


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
