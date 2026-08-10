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
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from domain.units import BASIS_POINTS_PER_UNIT
from typing import Any

from cache.market_state_store import MarketStateStore
from clients.market_ws import DEFAULT_MARKET_WS_URL, MarketWebSocket
from integrations.polymarket.errors import VenueOrderbookNotFoundError
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
    _last_wss_message_at: float = field(default=0.0, init=False)
    _wss_connected: bool = field(default=False, init=False)
    _wss_degraded_log_last: float = field(default=0.0, init=False)

    async def start(self, initial_token_ids: list[str]) -> None:
        """Start market WSS runtime.

        The WSS connection is opened unconditionally, even with zero initial
        tokens. Trade detection is HTTP-polled, so token ids only become known
        when a trade is observed; the connection must already be live so that
        dynamic ``subscribe_tokens`` / ``ensure_token_ready_or_bootstrapped``
        calls can attach tokens and REST-bootstrap them. Returning early on an
        empty token set would leave ``_ws_client`` unset and permanently block
        every new market.
        """
        token_ids = _normalize_token_ids(initial_token_ids)

        self._subscribed_tokens.update(token_ids)
        self._status.running = True
        self._status.subscribed_token_count = len(self._subscribed_tokens)
        self._status.last_error = None

        if not token_ids:
            logger.info("market_data_runtime_start_empty_token_set")

        self._ws_client = MarketWebSocket(
            cache=self.market_store,
            ws_url=self.ws_url,
            rest_bootstrap=self.bootstrap_tokens,
            on_connected=self._on_wss_connected,
            on_disconnected=self._on_wss_disconnected,
            on_state_change=self._on_market_state_change,
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
        self._wss_connected = False

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

    async def ensure_token_ready_or_bootstrapped(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
        side: str | None = None,
    ) -> bool:
        """Subscribe token and bootstrap from REST /book if WSS state is missing.

        Returns True when the token has executable data (WSS or fresh rest_bootstrap).
        Does NOT mark dirty — rest_bootstrap data is immediately usable if policy allows.
        """
        t_start = time.monotonic()

        if self._token_executable(token_id):
            return True

        if token_id not in self._subscribed_tokens:
            self._subscribed_tokens.add(token_id)
            self._status.subscribed_token_count = len(self._subscribed_tokens)
            if self._ws_client is not None:
                await self._ws_client.subscribe([token_id])
            logger.info("market_data_runtime_subscribe_sent | token_id=%s", token_id)
        else:
            logger.info("market_data_wss_bootstrap_repair_start | token_id=%s", token_id)

        cfg = self._bootstrap_config()
        if not cfg["enable"]:
            return self._token_executable(token_id)

        if cfg["require_wss_connected"] and self.is_wss_down():
            # A trade can arrive in the startup window before ``on_connected``
            # fires. Connecting is not the same as down -- wait briefly within
            # the bootstrap budget before failing closed.
            await self._await_wss_connected(
                timeout_seconds=min(cfg["total_budget_ms"] / 2000.0, 0.5)
            )

        if cfg["require_wss_connected"] and self.is_wss_down():
            debug = self.market_store.get_debug_state(token_id)
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=wss_disconnected debug=%s",
                token_id,
                debug,
            )
            return False

        elapsed_ms = (time.monotonic() - t_start) * 1000
        if elapsed_ms >= cfg["total_budget_ms"]:
            logger.warning(
                "market_data_bootstrap_timeout | token_id=%s elapsed_ms=%.1f",
                token_id,
                elapsed_ms,
            )
            return False

        logger.info(
            "market_data_rest_bootstrap_start | token_id=%s cond=%s", token_id, condition_id
        )
        fetch_start = time.monotonic()

        try:
            book = await asyncio.wait_for(
                self.gateway.get_orderbook(token_id),
                timeout=cfg["request_timeout_ms"] / 1000.0,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=request_timeout",
                token_id,
            )
            return False
        except VenueOrderbookNotFoundError:
            self.market_store.mark_resolved(token_id, condition_id)
            logger.info(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=no_orderbook",
                token_id,
            )
            return False
        except Exception as exc:
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=fetch_error error=%r",
                token_id,
                exc,
            )
            return False

        request_ms = (time.monotonic() - fetch_start) * 1000

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
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=empty_book",
                token_id,
            )
            return False

        data_age_ms = request_ms
        if data_age_ms > cfg["max_age_ms"]:
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=stale"
                " age_ms=%.1f max_ms=%d",
                token_id,
                data_age_ms,
                cfg["max_age_ms"],
            )
            return False

        best_bid = max(bids)
        best_ask = min(asks)
        if best_bid >= best_ask or best_ask <= 0:
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=invalid_spread",
                token_id,
            )
            return False
        spread_bps = int(((best_ask - best_bid) / best_ask) * BASIS_POINTS_PER_UNIT)
        if cfg["max_spread_bps"] > 0 and spread_bps > cfg["max_spread_bps"]:
            logger.warning(
                "market_data_rest_bootstrap_rejected | token_id=%s reason=wide_spread"
                " spread_bps=%d configured_max_bps=%d",
                token_id,
                spread_bps,
                cfg["max_spread_bps"],
            )
            return False

        self.market_store.replace_book_snapshot(
            token_id,
            bids=bids,
            asks=asks,
            raw_event={"source": "rest_bootstrap", "condition_id": condition_id},
        )

        prefetch_elapsed_ms = (time.monotonic() - t_start) * 1000
        tick = self.market_store.get_tick(token_id)
        if tick is None:
            remaining_ms = cfg["total_budget_ms"] - prefetch_elapsed_ms
            if remaining_ms > 10:
                try:
                    info = await asyncio.wait_for(
                        self.gateway.get_market_info(token_id),
                        timeout=min(
                            remaining_ms / 1000.0,
                            cfg["request_timeout_ms"] / 1000.0,
                        ),
                    )
                    tick_size = _to_decimal(info.get("tick_size") or info.get("minimum_tick_size"))
                    min_order_size = _to_decimal(
                        info.get("min_order_size")
                        or info.get("minimum_order_size")
                        or info.get("order_min_size")
                    )
                    neg_risk_raw = info.get("neg_risk")
                    if (
                        tick_size is not None
                        and tick_size > 0
                        and min_order_size is not None
                        and min_order_size > 0
                        and isinstance(neg_risk_raw, bool)
                    ):
                        self.market_store.set_tick(
                            token_id,
                            tick_size=tick_size,
                            min_order_size=min_order_size,
                            neg_risk=neg_risk_raw,
                            source="rest_bootstrap",
                        )
                except Exception as exc:
                    # Tick missing → MDS rejects on the read side; the bot
                    # still fails closed via the freshness gate. Log so
                    # operators see chronic tick-fetch failures (e.g. SDK
                    # auth issue, network partition) instead of silent drop.
                    logger.debug(
                        "market_data_bootstrap_tick_fetch_failed | token_id=%s error=%r",
                        token_id,
                        exc,
                    )

        prefetch_elapsed_ms = (time.monotonic() - t_start) * 1000

        if prefetch_elapsed_ms > cfg["total_budget_ms"]:
            logger.warning(
                "market_data_bootstrap_timeout | token_id=%s elapsed_ms=%.1f budget_ms=%d",
                token_id,
                prefetch_elapsed_ms,
                cfg["total_budget_ms"],
            )
            return False

        logger.info(
            "market_data_rest_bootstrap_used | token_id=%s cond=%s"
            " request_ms=%.1f data_age_ms=%.1f prefetch_elapsed_ms=%.1f"
            " bid_count=%d ask_count=%d best_bid=%s best_ask=%s spread_bps=%d source=rest_bootstrap",
            token_id,
            condition_id,
            request_ms,
            data_age_ms,
            prefetch_elapsed_ms,
            len(bids),
            len(asks),
            best_bid,
            best_ask,
            spread_bps,
        )
        return True

    def _bootstrap_config(self) -> dict:
        cfg_obj = getattr(self.config, "market_data", None)
        return {
            "enable": bool(getattr(cfg_obj, "enable_rest_bootstrap", True)),
            "max_age_ms": int(getattr(cfg_obj, "rest_bootstrap_max_age_ms", 500)),
            "request_timeout_ms": int(getattr(cfg_obj, "rest_bootstrap_request_timeout_ms", 750)),
            "total_budget_ms": int(getattr(cfg_obj, "rest_bootstrap_total_budget_ms", 1000)),
            "max_spread_bps": int(getattr(cfg_obj, "rest_bootstrap_max_spread_bps", 300)),
            "require_wss_connected": bool(
                getattr(cfg_obj, "rest_bootstrap_require_wss_connected", True)
            ),
        }

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
            except VenueOrderbookNotFoundError:
                self.market_store.mark_resolved(token_id)
                logger.info(
                    "market_data_bootstrap_skipped | token_id=%s reason=no_orderbook",
                    token_id,
                )
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
        self._wss_connected = True
        self._last_wss_message_at = time.monotonic()
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
        self._wss_connected = False
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

        tick_size = _to_decimal(info.get("tick_size") or info.get("minimum_tick_size"))
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

        neg_risk = info.get("neg_risk")
        if not isinstance(neg_risk, bool):
            self.market_store.mark_dirty(token_id, "rest_bootstrap_missing_neg_risk")
            return

        self.market_store.set_tick(
            token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            source="rest_snapshot",
        )

        # PR10.4: REST snapshot is the primary execution source. A healthy
        # REST bootstrap leaves the token executable immediately; we no
        # longer hold it dirty waiting for a WSS upgrade.

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

    def _token_executable(self, token_id: str) -> bool:
        """Return True only when this token has executable WSS state.

        Live execution requires:
          - not resolved
          - not stale, not dirty
          - quote_source == "wss"
          - book_source == "wss"
          - tick present (any source; metadata is REST-allowed)
        """
        store = self.market_store
        if store.is_resolved(token_id):
            return False
        if store.is_stale(token_id) or store.is_dirty(token_id):
            return False

        quote = store.get_quote(token_id)
        book = store.get_book(token_id)
        tick = store.get_tick(token_id)
        if quote is None or book is None or tick is None:
            return False
        if (quote.source or "unknown") != "wss":
            return False
        if (book.source or "unknown") != "wss":
            return False
        return True

    def _update_readiness_from_store(self) -> None:
        self._refresh_counts()

        all_executable = bool(self._subscribed_tokens) and all(
            self._token_executable(token_id) for token_id in self._subscribed_tokens
        )

        market_ready = (
            self._status.running
            and self._status.subscribed_token_count > 0
            and self._status.stale_token_count == 0
            and self._status.dirty_token_count == 0
            and all_executable
        )

        self._update_readiness(
            market_data_ready=market_ready,
            subscribed_token_count=self._status.subscribed_token_count,
            stale_token_count=self._status.stale_token_count,
            dirty_token_count=self._status.dirty_token_count,
        )

    def is_wss_down(self, threshold_seconds: float = 15.0) -> bool:
        """Return True when the market WSS connection is down or silent.

        Used by MarketDataService to gate REST degraded fallback, and by
        ``ensure_token_ready_or_bootstrapped`` to gate REST bootstrap. Down means:
          - runtime not running, or
          - WSS not connected (explicit connection state), or
          - connected but silent beyond ``threshold_seconds``.

        A connected stream that has not yet delivered a message is NOT down --
        ``_on_wss_connected`` sets ``_last_wss_message_at``, so a live connection
        always has a non-zero timestamp.
        """
        if not self._status.running:
            return True
        if not self._wss_connected:
            return True
        if self._last_wss_message_at == 0.0:
            return False
        return (time.monotonic() - self._last_wss_message_at) > threshold_seconds

    async def _await_wss_connected(self, timeout_seconds: float) -> bool:
        """Wait up to ``timeout_seconds`` for the market WSS to report connected.

        Absorbs the startup race where a trade arrives before the
        ``on_connected`` callback has fired. Returns True if connected within
        the window, False if it gave up or the runtime stopped.
        """
        if self._wss_connected:
            return True
        if timeout_seconds <= 0:
            return False
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self._wss_connected:
                return True
            if not self._status.running:
                return False
            await asyncio.sleep(0.05)
        return self._wss_connected

    async def _on_market_state_change(self, token_id: str, event_type: str) -> None:
        """WSS-driven recompute hook.

        Fired by ``MarketWebSocket`` after book / price_change / best_bid_ask /
        tick_size_change / market_resolved events. Cheap; just recomputes the
        readiness gate from the store snapshot.
        """
        self._last_wss_message_at = time.monotonic()
        try:
            self._update_readiness_from_store()
        except Exception as exc:
            logger.debug(
                "market_data_state_change_recompute_failed | token=%s event=%s error=%r",
                token_id,
                event_type,
                exc,
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
