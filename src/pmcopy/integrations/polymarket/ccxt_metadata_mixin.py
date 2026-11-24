"""Public market-data, execution-metadata caching, and private-pagination mixin."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.domain.units import BASIS_POINTS_PER_UNIT
from pmcopy.integrations.polymarket.ccxt_compat.tick_size import (
    TickSizeResolver,
)
from pmcopy.integrations.polymarket.ccxt_gateway_helpers import (
    _MAX_PRIVATE_HISTORY_PAGES,
    _PRIVATE_CURSOR_TERMINALS,
    _READ_TIMEOUT_SECONDS,
    _await_call,
    _datetime,
    _decimal,
    _first,
    _maybe_dict,
    _nested,
    _nested_dict,
    _nested_dict_or_self,
    _normalize_side,
    _raise_read_error,
)
from pmcopy.integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
from pmcopy.integrations.polymarket.errors import (
    VenueHistoryIncompleteError,
    VenueInvalidOrderError,
    VenueOrderbookNotFoundError,
    VenueSchemaError,
    VenueUnavailableError,
)
from pmcopy.integrations.polymarket.models import (
    PRICE_TOKEN_NOT_FOUND,
    OrderBook,
    OrderBookLevel,
)
from pmcopy.services.observability.metrics import LatencyPhase
from pmcopy.services.telemetry.latency import PipelineLatency


class _CcxtMetadataAndReadMixin:
    """Read-only orderbook, tick/neg_risk metadata, and pagination helpers."""

    _client: CcxtPredictionClient | Any
    _exchange: Any
    _metadata_cache_size: int
    _metadata: OrderedDict[str, ExecutionMetadata]
    _metadata_fetched_at: dict[str, float]
    _metadata_ttl_seconds: float
    _monotonic: Callable[[], float]
    _latency: PipelineLatency | None
    _tick_resolver: TickSizeResolver | None

    def _cache_metadata(self, metadata: ExecutionMetadata) -> None:
        token = metadata.token_id
        self._metadata[token] = metadata
        self._metadata.move_to_end(token)
        self._metadata_fetched_at[token] = self._monotonic()
        while len(self._metadata) > self._metadata_cache_size:
            evicted, _metadata = self._metadata.popitem(last=False)
            self._metadata_fetched_at.pop(evicted, None)

    def _metadata_is_fresh(self, token: str) -> bool:
        """Return whether cached metadata is recent enough to sign against.

        Args:
            token: Outcome token id.

        Returns:
            True when a cached entry exists and was fetched within the TTL.
        """
        if self._metadata_ttl_seconds <= 0:
            return False
        fetched_at = self._metadata_fetched_at.get(token)
        if fetched_at is None:
            return False
        return (self._monotonic() - fetched_at) < self._metadata_ttl_seconds

    @property
    def client(self) -> CcxtPredictionClient | Any:
        """Return the wrapped CCXT prediction client."""
        return self._client

    @property
    def exchange(self) -> Any:
        """Return the wrapped CCXT exchange instance."""
        return self._exchange

    def _resolve_method(self, *names: str) -> Any:
        for name in names:
            method = getattr(self._exchange, name, None)
            if callable(method):
                return method
        return None

    async def _ensure_api_credentials(self) -> None:
        method = self._resolve_method("load_api_credentials", "loadApiCredentials")
        if method is None:
            return
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                await _await_call(method)
        except BaseException as exc:
            _raise_read_error(exc, not_found="order")

    async def _fetch_private_pages(
        self,
        *,
        raw_method_names: tuple[str, ...],
        fallback_method: Any,
        params: dict[str, Any],
        collection_keys: tuple[str, ...],
    ) -> list[Any]:
        """Read a complete authenticated cursor collection.

        CCXT's unified ``fetch_open_orders`` and ``fetch_my_trades``
        wrappers (verified in 4.5.70–4.5.73) only consume the first CLOB
        response page. Reconciliation must
        not silently accept partial private history, so use CCXT's generated raw
        endpoint when present and walk ``next_cursor`` until the documented
        terminal value. The unified method remains a compatibility fallback for
        public/paper test doubles, but authenticated compatibility rejects a
        client that lacks the raw paginated methods.

        Envelope authority (audit A03): an HTTP-200 page whose payload carries
        no recognized collection key — or whose rows are not objects — is an
        INVALID page, not an empty one.  Reconciliation and absence proofs are
        built on these collections, so a schema-drifted payload must fail
        closed as :class:`VenueSchemaError` instead of being read as
        authoritative emptiness.
        """

        raw_method = self._resolve_method(*raw_method_names)
        if raw_method is None:
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    values = await _await_call(
                        fallback_method,
                        None,
                        None,
                        None,
                        dict(params),
                    )
            except BaseException as exc:
                _raise_read_error(exc, not_found="order")
            if isinstance(values, dict):
                for key in collection_keys:
                    collection = values.get(key)
                    if isinstance(collection, list):
                        return collection
                raise VenueSchemaError(
                    "CCXT private-history fallback response had no recognized "
                    f"collection key {collection_keys}; refusing to treat an "
                    "unrecognized payload as empty history"
                )
            if not isinstance(values, (list, tuple)):
                raise VenueSchemaError(
                    "CCXT private-history fallback response was not a "
                    f"collection ({type(values).__name__}); refusing to treat "
                    "an unrecognized payload as empty history"
                )
            return list(values)

        await self._ensure_api_credentials()
        rows: list[Any] = []
        seen_cursors: set[str] = set()
        request = dict(params)
        options = getattr(self._exchange, "options", {}) or {}
        max_pages_raw = options.get("privateHistoryMaxPages", _MAX_PRIVATE_HISTORY_PAGES)
        try:
            max_pages = int(max_pages_raw)
        except (TypeError, ValueError):
            max_pages = _MAX_PRIVATE_HISTORY_PAGES
        if max_pages <= 0:
            raise VenueInvalidOrderError("privateHistoryMaxPages must be positive")

        for _page in range(max_pages):
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    response = await _await_call(raw_method, dict(request))
            except BaseException as exc:
                _raise_read_error(exc, not_found="order")

            page_rows = self._private_page_rows(response, collection_keys=collection_keys)
            rows.extend(page_rows)

            payload = _maybe_dict(response)
            cursor_raw = _first(payload, "next_cursor", "nextCursor")
            cursor = str(cursor_raw or "").strip()
            if cursor in _PRIVATE_CURSOR_TERMINALS:
                return rows
            if cursor in seen_cursors:
                raise VenueHistoryIncompleteError(
                    "CCXT private-history pagination cursor repeated; "
                    "refusing partial reconciliation data"
                )
            seen_cursors.add(cursor)
            request = dict(params)
            request["next_cursor"] = cursor

        raise VenueHistoryIncompleteError(
            "CCXT private-history pagination exceeded privateHistoryMaxPages; "
            "refusing partial reconciliation data"
        )

    @staticmethod
    def _private_page_rows(
        response: Any,
        *,
        collection_keys: tuple[str, ...],
    ) -> list[Any]:
        """Validate and return one raw cursor page's rows.

        The raw CLOB data endpoints answer with a keyed envelope
        (``{"data": [...], "next_cursor": ...}``).  A payload without a
        recognized collection key, a null/absent collection, a bare list, or
        non-object rows is schema drift: none of it may be read as history —
        and especially not as empty history, which would manufacture absence
        evidence.
        """

        payload = _maybe_dict(response)
        for key in collection_keys:
            value = payload.get(key)
            if isinstance(value, list):
                for row in value:
                    if not isinstance(row, dict):
                        raise VenueSchemaError(
                            "CCXT private-history page row was not an object "
                            f"({type(row).__name__}); refusing malformed history"
                        )
                return value
        raise VenueSchemaError(
            "CCXT private-history page had no recognized collection key "
            f"{collection_keys} ({type(response).__name__}); refusing to treat "
            "an unrecognized payload as empty history"
        )

    async def _fetch_raw_order_book(self, token_id: str) -> dict[str, Any]:
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(self._exchange.fetch_order_book, str(token_id))
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        return _maybe_dict(value)

    async def _fetch_raw_clob_book(self, token_id: str) -> dict[str, Any] | None:
        """Fetch the raw CLOB book, which carries the execution metadata.

        CCXT's unified ``fetch_order_book`` drops the raw response's
        ``tick_size``, ``min_order_size``, and ``neg_risk`` fields during
        parsing, so metadata reads use the generated implicit endpoint for the
        same CLOB route. Returns ``None`` when the implicit endpoint is absent
        (degraded installs and unified-only test doubles) so callers can fall
        back to the parsed book plus the outcome cache.
        """
        method = self._resolve_method("clobPublicGetBook", "clob_public_get_book")
        if method is None:
            return None
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(method, {"token_id": str(token_id)})
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        return _maybe_dict(value)

    async def _fetch_metadata_book(self, token_id: str) -> dict[str, Any]:
        """Return the freshest book usable as an execution-metadata source."""
        raw = await self._fetch_raw_clob_book(token_id)
        if raw is not None:
            return raw
        return await self._fetch_raw_order_book(token_id)

    async def _fetch_clob_market_metadata(self, condition_id: str) -> dict[str, Any]:
        """Fetch V2 market metadata without reading order-book levels."""

        method = self._resolve_method(
            "clobPublicGetClobMarketsConditionId",
            "clob_public_get_clob_markets_condition_id",
        )
        if method is None:
            raise VenueUnavailableError(
                "CCXT does not expose clob-markets/{condition_id}; refusing execution"
            )
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(method, {"condition_id": str(condition_id)})
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        payload = _maybe_dict(value)
        return _nested_dict_or_self(payload, "data")

    async def _fetch_neg_risk_metadata(self, token_id: str) -> dict[str, Any]:
        """Fetch and validate the token's explicit negative-risk flag."""

        method = self._resolve_method(
            "clobPublicGetNegRiskTokenId",
            "clob_public_get_neg_risk_token_id",
        )
        if method is None:
            raise VenueUnavailableError(
                "CCXT does not expose neg-risk/{token_id}; refusing execution"
            )
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(method, {"token_id": str(token_id)})
        except BaseException as exc:
            _raise_read_error(exc, not_found="orderbook")
        if isinstance(value, bool):
            return {"neg_risk": value}
        payload = _nested_dict_or_self(_maybe_dict(value), "data")
        neg_risk = _first(payload, "neg_risk", "negRisk", "nr")
        if not isinstance(neg_risk, bool):
            raise VenueInvalidOrderError(
                "CCXT omitted neg_risk from token metadata; refusing execution"
            )
        return {"neg_risk": neg_risk}

    @staticmethod
    def _validate_metadata_token(
        token_id: str,
        market_metadata: dict[str, Any],
    ) -> None:
        """Ensure condition-scoped metadata contains the requested token."""

        tokens = _first(market_metadata, "tokens", "t")
        if not isinstance(tokens, list):
            raise VenueInvalidOrderError(
                "CCXT omitted tokens from condition metadata; refusing execution"
            )
        token_ids: set[str] = set()
        for value in tokens:
            if isinstance(value, (str, int)):
                token_ids.add(str(value))
                continue
            item = _maybe_dict(value)
            candidate = _first(item, "token_id", "tokenId", "asset_id", "assetId", "id", "t")
            if candidate is not None:
                token_ids.add(str(candidate))
        if str(token_id) not in token_ids:
            raise VenueInvalidOrderError(
                "condition metadata does not contain the requested token; refusing execution"
            )

    async def _fetch_execution_metadata_without_book(
        self,
        token_id: str,
        condition_id: str,
    ) -> ExecutionMetadata:
        """Return fresh signed-order metadata without fetching `/book`."""

        market_metadata, neg_risk_metadata = await asyncio.gather(
            self._fetch_clob_market_metadata(condition_id),
            self._fetch_neg_risk_metadata(token_id),
        )
        self._validate_metadata_token(token_id, market_metadata)
        metadata = self._metadata_from_sources(
            token_id,
            market_metadata,
            neg_risk_metadata,
        )
        if metadata is None:
            raise VenueInvalidOrderError(
                "CCXT omitted tick_size, min_order_size, or neg_risk; refusing execution"
            )
        self._cache_metadata(metadata)
        return metadata

    async def _load_outcome(self, token_id: str) -> dict[str, Any]:
        method = getattr(self._exchange, "load_outcome", None)
        outcome: Any = None
        if callable(method):
            try:
                async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                    outcome = await _await_call(method, token_id)
            except BaseException as exc:
                _raise_read_error(exc, not_found="orderbook")
        if not isinstance(outcome, dict):
            resolver = getattr(self._exchange, "outcome", None)
            if callable(resolver):
                try:
                    outcome = resolver(token_id)
                except BaseException:
                    outcome = None
        return outcome if isinstance(outcome, dict) else {}

    @staticmethod
    def _metadata_from_sources(
        token_id: str,
        *sources: dict[str, Any],
    ) -> ExecutionMetadata | None:
        tick: Any = None
        minimum: Any = None
        neg_risk: Any = None
        for source in sources:
            if not source:
                continue
            info = _nested_dict(source, "info")
            if tick is None:
                tick = _first(source, "tick_size", "tickSize", "mts")
                tick = tick if tick is not None else _first(info, "tick_size", "tickSize")
                tick = tick if tick is not None else _nested(source, "precision", "price")
                # Gamma-market spellings carried in a CCXT outcome's raw info.
                tick = (
                    tick
                    if tick is not None
                    else _first(info, "orderPriceMinTickSize", "minimumTickSize")
                )
            if minimum is None:
                minimum = _first(source, "min_order_size", "minOrderSize", "mos")
                minimum = (
                    minimum
                    if minimum is not None
                    else _first(info, "min_order_size", "minOrderSize")
                )
                minimum = (
                    minimum if minimum is not None else _nested(source, "limits", "amount", "min")
                )
                # Gamma-market spelling carried in a CCXT outcome's raw info.
                minimum = minimum if minimum is not None else _first(info, "orderMinSize")
            if neg_risk is None:
                neg_risk = _first(source, "neg_risk", "negRisk", "nr")
                neg_risk = neg_risk if neg_risk is not None else _first(info, "neg_risk", "negRisk")

        if tick is None or minimum is None or not isinstance(neg_risk, bool):
            return None
        tick_dec = _decimal(tick, field="tick_size")
        min_dec = _decimal(minimum, field="min_order_size")
        if tick_dec <= 0 or min_dec <= 0:
            raise VenueInvalidOrderError("invalid execution metadata")
        return ExecutionMetadata(
            token_id=str(token_id),
            tick_size=tick_dec,
            min_order_size=min_dec,
            neg_risk=neg_risk,
        )

    async def _execution_metadata(
        self,
        token_id: str,
        raw_book: dict[str, Any] | None = None,
        *,
        condition_id: str | None = None,
        refresh: bool = False,
    ) -> ExecutionMetadata:
        token = str(token_id)

        # A supplied book is always newer than the local cache. Polymarket can
        # change tick size while a market is live, so execution paths must not
        # keep signing against indefinitely cached metadata.
        if raw_book is not None:
            metadata = self._metadata_from_sources(token, raw_book)
            if metadata is None:
                outcome = await self._load_outcome(token)
                metadata = self._metadata_from_sources(token, raw_book, outcome)
            if metadata is None:
                raise VenueInvalidOrderError(
                    "CCXT omitted tick_size, min_order_size, or neg_risk; refusing execution"
                )
            self._cache_metadata(metadata)
            return metadata

        if refresh and self._metadata_is_fresh(token):
            cached = self._metadata.get(token)
            if cached is not None:
                self._metadata.move_to_end(token)
                return cached

        if not refresh:
            cached = self._metadata.get(token)
            if cached is not None:
                self._metadata.move_to_end(token)
                return cached

        measurement = (
            self._latency.span(LatencyPhase.METADATA_CACHE_MISS)
            if self._latency is not None
            else nullcontext()
        )
        with measurement:
            if refresh:
                if condition_id:
                    return await self._fetch_execution_metadata_without_book(
                        token,
                        condition_id,
                    )
                # A fresh order-book snapshot is the execution authority. This is
                # intentionally done immediately before every signed order, using
                # the raw CLOB book because the parsed book drops the metadata.
                book = await self._fetch_metadata_book(token)
                return await self._execution_metadata(token, book)

            outcome = await self._load_outcome(token)
            metadata = self._metadata_from_sources(token, outcome)
            if metadata is None:
                # The CCXT outcome cache does not consistently include
                # min_order_size. Fall back to a fresh book and cache only after all
                # three execution-critical fields validate.
                book = await self._fetch_metadata_book(token)
                metadata = self._metadata_from_sources(token, book, outcome)
            if metadata is None:
                raise VenueInvalidOrderError(
                    "CCXT omitted tick_size, min_order_size, or neg_risk; refusing execution"
                )
            self._cache_metadata(metadata)
            return metadata

    async def get_orderbook(self, token_id: str) -> OrderBook:
        """Return a normalized order book for a raw outcome token."""
        raw = await self._fetch_raw_order_book(str(token_id))
        metadata = await self._execution_metadata(str(token_id), raw)

        def parse_levels(values: Any) -> list[OrderBookLevel]:
            levels: list[OrderBookLevel] = []
            for value in values or ():
                if isinstance(value, dict):
                    price = _first(value, "price")
                    size = _first(value, "size", "amount")
                elif isinstance(value, (list, tuple)) and len(value) >= 2:
                    price, size = value[0], value[1]
                else:
                    continue
                p = _decimal(price, field="orderbook price")
                s = _decimal(size, field="orderbook size")
                if s > 0:
                    levels.append(OrderBookLevel(price=p, size=s))
            return levels

        bids = sorted(parse_levels(raw.get("bids")), key=lambda level: level.price, reverse=True)
        asks = sorted(parse_levels(raw.get("asks")), key=lambda level: level.price)
        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None
        if best_bid is not None and best_ask is not None and best_ask < best_bid:
            raise VenueInvalidOrderError("inverted order book")
        spread_bps: int | None = None
        if best_bid is not None and best_ask is not None and best_bid > 0:
            midpoint = (best_bid + best_ask) / Decimal("2")
            if midpoint > 0:
                spread_bps = int(((best_ask - best_bid) / midpoint) * BASIS_POINTS_PER_UNIT)

        timestamp = _datetime(raw.get("datetime"), raw.get("timestamp")) or datetime.now(
            timezone.utc
        )
        return OrderBook(
            token_id=str(_first(raw, "outcomeId", "token_id", "asset_id") or token_id),
            tick_size=metadata.tick_size,
            min_order_size=metadata.min_order_size,
            neg_risk=metadata.neg_risk,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=spread_bps,
            timestamp=timestamp,
        )

    async def get_price(self, token_id: str, side: str) -> dict[str, str] | object:
        """Return the executable top-of-book price for legacy risk readers.

        Args:
            token_id: Raw Polymarket outcome token identity.
            side: ``BUY`` selects the best ask; ``SELL`` selects the best bid.

        Returns:
            A string-precise price mapping, zero for an empty side, or the
            existing terminal-market sentinel when the venue has no book.

        Raises:
            VenueInvalidOrderError: If ``side`` is neither BUY nor SELL.
        """

        normalized_side = _normalize_side(side)
        try:
            book = await self.get_orderbook(token_id)
        except VenueOrderbookNotFoundError:
            return PRICE_TOKEN_NOT_FOUND
        price = book.best_ask if normalized_side == "BUY" else book.best_bid
        return {"price": format(price if price is not None else Decimal("0"), "f")}

    async def get_market_info(self, token_id: str) -> dict[str, Any]:
        """Return normalized market information for an outcome token."""
        book = await self.get_orderbook(token_id)
        return {
            "token_id": book.token_id,
            "tick_size": str(book.tick_size),
            "min_order_size": str(book.min_order_size),
            "neg_risk": book.neg_risk,
            "best_bid": str(book.best_bid) if book.best_bid is not None else None,
            "best_ask": str(book.best_ask) if book.best_ask is not None else None,
        }

    async def get_execution_metadata(
        self,
        token_id: str,
        *,
        condition_id: str | None = None,
    ) -> ExecutionMetadata:
        """Return authoritative metadata without a book when condition is known."""

        return await self._execution_metadata(
            str(token_id),
            condition_id=condition_id,
            refresh=condition_id is not None,
        )

