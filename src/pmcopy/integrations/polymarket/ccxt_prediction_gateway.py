"""Typed anti-corruption gateway for ``ccxt.prediction.polymarket``.

This adapter implements the repository's existing :class:`ClobGateway` port.
It is deliberately conservative:

* raw outcome token IDs are used to avoid symbol/handle ambiguity;
* execution metadata is mandatory and cached only after validation;
* market orders always carry an explicit price limit;
* BUY market size is treated as USDC cost, SELL market size as shares, matching
  the official SDK gateway contract;
* ambiguous POST failures are never retried blindly;
* account fills preserve maker/taker leg attribution.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal, ROUND_DOWN
from http import HTTPStatus
from typing import Any, Iterable

from pmcopy.domain.execution import ExecutionMetadata
from pmcopy.integrations.polymarket.ccxt_compat.capabilities import build_ccxt_health_payload
from pmcopy.integrations.polymarket.ccxt_compat.precision import CcxtPrecisionError
from pmcopy.integrations.polymarket.ccxt_compat.tick_size import (
    InvalidTickSize,
    TickRounding,
    TickSizeResolver,
    quantize_price_to_tick,
)
from pmcopy.integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
from pmcopy.integrations.polymarket.errors import (
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
)
from pmcopy.integrations.polymarket.models import (
    LimitOrderRequest,
    MarketableOrderRequest,
    VenueCancelResult,
    VenueFill,
    VenueOrderState,
    VenueSubmitResult,
)
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.utils.logger import redact_error_detail

from pmcopy.integrations.polymarket.ccxt_gateway_helpers import (
    _DEFAULT_METADATA_CACHE_SIZE,
    _DEFAULT_METADATA_TTL_SECONDS,
    _EXPECTED_CHAIN_ID,
    _READ_TIMEOUT_SECONDS,
    _await_call,
    _datetime,
    _decimal,
    _exception_name,
    _first,
    _maybe_dict,
    _nested,
    _nested_dict,
    _nested_dict_or_self,
    _normalize_market_order_type,
    _normalize_side,
    _normalize_status,
    _raise_read_error,
    _status_code,
    _submission_exception,
    _timestamp_ms,
    _venue_reason,  # noqa: F401
)
from pmcopy.integrations.polymarket.ccxt_metadata_mixin import _CcxtMetadataAndReadMixin



class CcxtPredictionGateway(_CcxtMetadataAndReadMixin):
    """Application gateway backed by ``ccxt.prediction.polymarket``."""

    def __init__(
        self,
        client: CcxtPredictionClient | Any,
        *,
        tick_size_resolver: TickSizeResolver | None = None,
        metadata_ttl_seconds: float = _DEFAULT_METADATA_TTL_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        latency: PipelineLatency | None = None,
        metadata_cache_size: int = _DEFAULT_METADATA_CACHE_SIZE,
    ) -> None:
        """Wrap one CCXT prediction client behind the venue gateway contract.

        Args:
            client: The CCXT prediction client or a bare exchange.
            tick_size_resolver: Authoritative per-token tick resolver.
            metadata_ttl_seconds: How long a metadata fetch stays usable for
                signing. ``0`` restores a fetch before every signed order.
            monotonic: Monotonic clock source, injected so the TTL is testable
                without wall-clock sleeps.
            latency: Optional bounded recorder for actual metadata fetch duration.
            metadata_cache_size: Maximum number of outcome metadata entries;
                least recently used entries are evicted with their fetch timestamps.

        Raises:
            ValueError: If the metadata cache bound is not a positive integer.
        """
        self._client = client
        self._exchange = client.exchange if isinstance(client, CcxtPredictionClient) else client
        if (
            isinstance(metadata_cache_size, bool)
            or not isinstance(metadata_cache_size, int)
            or metadata_cache_size <= 0
        ):
            raise ValueError("metadata_cache_size must be a positive integer")
        self._metadata_cache_size = metadata_cache_size
        self._metadata: OrderedDict[str, ExecutionMetadata] = OrderedDict()
        self._metadata_fetched_at: dict[str, float] = {}
        self._metadata_ttl_seconds = max(0.0, float(metadata_ttl_seconds))
        self._monotonic = monotonic
        self._latency = latency
        self._tick_size_resolver = tick_size_resolver


    @staticmethod
    def _validate_order(
        *,
        share_size: Decimal,
        price: Decimal,
        metadata: ExecutionMetadata,
    ) -> None:
        if share_size <= 0:
            raise VenueInvalidOrderError("order size must be positive")
        if share_size < metadata.min_order_size:
            raise VenueInvalidOrderError(
                f"order share size {share_size} is below minimum {metadata.min_order_size}"
            )
        if price <= 0 or price >= 1:
            raise VenueInvalidOrderError("price must be greater than 0 and less than 1")

    async def _resolved_tick_size(
        self,
        token_id: str,
        metadata: ExecutionMetadata,
    ) -> Decimal:
        """Return the tick size every signed order must be quantized against.

        The fresh execution-metadata fetch is the authoritative observation; it
        refreshes the resolver cache (invalidating a stale tick) and the
        resolver then returns the validated value that reaches the CCXT order
        parameters. Without a resolver the validated metadata tick is used
        directly — there is no default path either way.
        """

        if self._tick_size_resolver is None:
            return metadata.tick_size
        await self._tick_size_resolver.note_observed_tick(token_id, metadata.tick_size)
        return await self._tick_size_resolver.resolve(token_id)

    @staticmethod
    def _submission_from_order(value: Any) -> VenueSubmitResult:
        raw = _maybe_dict(value)
        info = _nested_dict(raw, "info")
        order_id = (
            str(_first(raw, "id", "orderID", "order_id") or _first(info, "orderID", "id") or "")
            or None
        )
        status = CcxtPredictionGateway._submission_status(raw=raw, info=info)
        success_marker = _first(raw, "success")
        if success_marker is None:
            success_marker = _first(info, "success")
        error = (
            str(
                _first(raw, "error", "errorMsg", "message")
                or _first(info, "error", "errorMsg", "message")
                or ""
            ).strip()
            or None
        )

        def _list(d: dict, *keys: str) -> list[str]:
            for k in keys:
                v = d.get(k)
                if isinstance(v, list):
                    return [str(x) for x in v if x]
                if isinstance(v, str) and v:
                    return [v]
            return []

        trade_ids = _list(raw, "trade_ids", "tradeIds") or _list(info, "trade_ids", "tradeIds")
        transactions_hashes = _list(
            raw, "transactions_hashes", "transactionHashes", "transaction_hashes"
        ) or _list(info, "transactions_hashes", "transactionHashes", "transaction_hashes")

        if success_marker is False or status in {"canceled", "rejected"}:
            return VenueSubmitResult(
                success=False,
                order_id=order_id,
                status=status or "rejected",
                raw=raw,
                error=error or "order_rejected",
                non_retryable=True,
                trade_ids=trade_ids or None,
                transactions_hashes=transactions_hashes or None,
            )
        if order_id is None:
            return VenueSubmitResult(
                success=False,
                status=status,
                raw=raw,
                error=error or "ambiguous_success_response_without_order_id",
                ambiguous_post_failure=True,
                trade_ids=trade_ids or None,
                transactions_hashes=transactions_hashes or None,
            )
        return VenueSubmitResult(
            success=True,
            order_id=order_id,
            status=status,
            raw=raw,
            trade_ids=trade_ids or None,
            transactions_hashes=transactions_hashes or None,
        )

    @staticmethod
    def _submission_status(*, raw: dict[str, Any], info: dict[str, Any]) -> str | None:
        """Return the raw CLOB lifecycle state when the POST preserved it."""

        venue_status = str(_first(info, "status") or "").strip().lower()
        if venue_status:
            return "canceled" if venue_status == "cancelled" else venue_status
        return _normalize_status(_first(raw, "status"))

    async def submit_limit_order(self, request: LimitOrderRequest) -> VenueSubmitResult:
        """Submit a validated limit order through CCXT Prediction."""
        if not request.condition_id:
            return VenueSubmitResult(
                success=False,
                error="missing_condition_id",
                non_retryable=True,
            )
        side = _normalize_side(request.side)
        metadata = await self._execution_metadata(request.token_id, refresh=True)
        tick_size = await self._resolved_tick_size(request.token_id, metadata)
        price = _decimal(request.price, field="price")
        size = _decimal(request.size, field="size")
        self._validate_order(
            share_size=size.quantize(Decimal("0.01"), rounding=ROUND_DOWN),
            price=price,
            metadata=metadata,
        )
        try:
            quantized = quantize_price_to_tick(price=price, tick_size=tick_size, side=side)
        except (CcxtPrecisionError, InvalidTickSize, ValueError) as exc:
            raise VenueInvalidOrderError(str(exc)) from exc
        params = {
            "orderType": "GTC",
            "timeInForce": "GTC",
            "tickSize": format(tick_size, "f"),
            "negRisk": metadata.neg_risk,
            "postOnly": False,
        }
        try:
            value = await _await_call(
                self._exchange.create_order,
                request.token_id,
                "limit",
                side.lower(),
                size,
                quantized,
                params,
            )
        except BaseException as exc:
            return replace(_submission_exception(exc), effective_price=quantized)
        return replace(self._submission_from_order(value), effective_price=quantized)

    async def submit_marketable_order(self, request: MarketableOrderRequest) -> VenueSubmitResult:
        """Submit a price-protected FAK order through CCXT Prediction."""
        if not request.condition_id:
            return VenueSubmitResult(
                success=False,
                error="missing_condition_id",
                non_retryable=True,
            )
        if request.price_limit is None:
            raise VenueInvalidOrderError("price_limit is required for marketable orders")
        side = _normalize_side(request.side)
        order_type = _normalize_market_order_type(request.order_type)
        metadata = await self._execution_metadata(
            request.token_id,
            condition_id=request.condition_id,
            refresh=True,
        )
        tick_size = await self._resolved_tick_size(request.token_id, metadata)
        size = _decimal(request.size, field="size")
        price_limit = _decimal(request.price_limit, field="price_limit")
        try:
            # A FAK/FOK order either crosses now or is killed, so the tick
            # snap must move toward the book. Rounding away from it shaves the
            # caller's band and turns an executable order into a venue kill.
            quantized = quantize_price_to_tick(
                price=price_limit,
                tick_size=tick_size,
                side=side,
                rounding=TickRounding.MARKETABLE,
            )
        except (CcxtPrecisionError, InvalidTickSize, ValueError) as exc:
            raise VenueInvalidOrderError(str(exc)) from exc
        signed_size = size.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        share_size = signed_size / quantized if side == "BUY" else signed_size
        self._validate_order(share_size=share_size, price=quantized, metadata=metadata)

        params: dict[str, Any] = {
            "orderType": order_type,
            "timeInForce": "IOC" if order_type == "FAK" else "FOK",
            "tickSize": format(tick_size, "f"),
            "negRisk": metadata.neg_risk,
        }
        # Preserve the official SDK gateway contract: BUY size is spend (USDC),
        # SELL size is shares. Passing cost together with an explicit price avoids
        # CCXT's cached-outcome-price fallback.
        #
        # The value must be an exact decimal *string*: ``Exchange.safe_string``
        # accepts only str/int/float, so a ``Decimal`` makes
        # ``safe_number(params, 'cost')`` return ``None`` and CCXT silently
        # re-reads the BUY as a share count. ``format(x, "f")`` keeps every
        # digit (no scientific notation, no binary-float rounding).
        exact_size = format(size, "f")
        if side == "BUY":
            params["cost"] = exact_size

        try:
            value = await _await_call(
                self._exchange.create_order,
                request.token_id,
                "market",
                side.lower(),
                exact_size,
                quantized,
                params,
            )
        except BaseException as exc:
            return replace(_submission_exception(exc), effective_price=quantized)
        return replace(self._submission_from_order(value), effective_price=quantized)

    @staticmethod
    def _order_state(value: Any) -> VenueOrderState:
        raw = _maybe_dict(value)
        info = _nested_dict(raw, "info")
        order_id = str(
            _first(raw, "id", "orderID", "order_id") or _first(info, "id", "orderID") or ""
        )
        if not order_id:
            if raw.get("info") == "null" and raw.get("status") is None:
                raise VenueOrderNotFoundError("CCXT returned a null order")
            raise VenueInvalidOrderError("CCXT order response omitted id")
        filled = _decimal(
            _first(raw, "filled", "size_matched") or _first(info, "size_matched"),
            field="filled",
            default=Decimal("0"),
        )
        average_raw = _first(raw, "average", "avg_price", "average_price")
        if average_raw is None:
            average_raw = _first(info, "avg_price", "average_price")
        average = _decimal(average_raw, field="average", default=Decimal("0"))
        # Outcome-token identity at the adapter boundary.  CCXT parsed
        # orders carry ``outcomeId`` (verified against the installed ccxt
        # ``parse_prediction_order``: it resolves the raw CLOB ``asset_id``
        # token and ``safe_outcome`` falls back to the raw token id whenever
        # the outcome cache misses), while raw ``/data/orders`` rows carry
        # ``asset_id`` directly.  No identity in the payload means ``None``:
        # the adapter never guesses a token mapping for the domain.
        token_raw = _first(
            raw, "outcomeId", "outcome_id", "token_id", "asset_id", "tokenId"
        ) or _first(info, "asset_id", "token_id", "tokenId")
        token_id = str(token_raw).strip() if token_raw is not None else None
        return VenueOrderState(
            order_id=order_id,
            status=_normalize_status(_first(raw, "status") or _first(info, "status")) or "unknown",
            side=str(_first(raw, "side") or _first(info, "side") or "").upper(),
            price=_decimal(
                _first(raw, "price") or _first(info, "price"), field="price", default=Decimal("0")
            ),
            size=_decimal(
                _first(raw, "amount", "original_size") or _first(info, "original_size"),
                field="size",
                default=Decimal("0"),
            ),
            filled_size=filled,
            # Never pretend the limit price is a weighted average fill price.
            avg_fill_price=average,
            created_at=_datetime(
                _first(raw, "datetime", "created_at"),
                _first(raw, "timestamp") or _first(info, "created_at"),
            ),
            token_id=token_id or None,
            raw=raw,
        )

    async def get_order(self, order_id: str) -> VenueOrderState:
        """Return one normalized venue order when it exists."""
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                value = await _await_call(self._exchange.fetch_order, order_id)
        except BaseException as exc:
            _raise_read_error(exc, not_found="order")
        return self._order_state(value)

    async def list_open_orders(
        self,
        *,
        order_id: str | None = None,
        market: str | None = None,
    ) -> list[VenueOrderState]:
        """Return normalized open orders with pagination deduplication."""
        params: dict[str, Any] = {}
        if order_id:
            params["id"] = order_id
        if market:
            params["market"] = market
        values = await self._fetch_private_pages(
            raw_method_names=(
                "clob_private_get_data_orders",
                "clobPrivateGetDataOrders",
            ),
            fallback_method=self._exchange.fetch_open_orders,
            params=params,
            collection_keys=("data", "orders"),
        )
        result: list[VenueOrderState] = []
        for value in values or ():
            state = self._order_state(value)
            if order_id and state.order_id != order_id:
                continue
            if market:
                state_raw = _maybe_dict(state.raw)
                raw_info = _nested_dict(state_raw, "info")
                raw_market = _first(state_raw, "market") or _first(
                    raw_info, "market", "condition_id"
                )
                # Strict scoping: a row whose payload carries no market
                # identity cannot prove which condition — let alone which
                # outcome token — it belongs to, so it must be rejected
                # rather than silently bypass the filter.
                if raw_market is None or str(raw_market) != market:
                    continue
            result.append(state)
        return result

    @staticmethod
    def _fee(value: dict[str, Any]) -> Decimal | None:
        fee = value.get("fee")
        if isinstance(fee, dict):
            raw = _first(fee, "cost", "amount")
            if raw is not None:
                return _decimal(raw, field="fee", default=Decimal("0"))
        info = _nested_dict_or_self(value, "info")
        raw = _first(info, "fee", "fee_amount", "feeAmount")
        if raw is not None and not isinstance(raw, dict):
            return _decimal(raw, field="fee", default=Decimal("0"))
        return None

    @staticmethod
    def _trade_legs(
        value: dict[str, Any],
        *,
        wallet_address: str | None = None,
    ) -> Iterable[dict[str, Any]]:
        info = _nested_dict_or_self(value, "info")
        role = str(
            _first(info, "trader_side", "traderSide") or value.get("takerOrMaker") or "TAKER"
        ).upper()
        makers = _first(info, "maker_orders", "makerOrders")
        own_wallet = str(wallet_address or "").strip().lower()
        if role == "MAKER" and isinstance(makers, list) and makers:
            for maker in makers:
                if not isinstance(maker, dict):
                    continue
                if own_wallet:
                    maker_addr = (
                        str(_first(maker, "maker_address", "makerAddress") or "").strip().lower()
                    )
                    if maker_addr and maker_addr != own_wallet:
                        continue
                maker_fee = CcxtPredictionGateway._fee(maker)
                if maker_fee is None and len(makers) == 1:
                    maker_fee = CcxtPredictionGateway._fee(value)
                yield {
                    "order_id": _first(maker, "order_id", "orderId", "id"),
                    "token_id": _first(maker, "token_id", "tokenId", "asset_id"),
                    "side": _first(maker, "side"),
                    "size": _first(maker, "matched_amount", "matchedAmount", "size"),
                    "price": _first(maker, "price"),
                    "role": role,
                    "fee": maker_fee,
                }
            return
        yield {
            "order_id": _first(value, "order") or _first(info, "taker_order_id", "takerOrderId"),
            "token_id": _first(value, "outcomeId", "token_id", "asset_id")
            or _first(info, "asset_id", "token_id", "tokenId"),
            "side": _first(value, "side") or _first(info, "side"),
            "size": _first(value, "amount", "size") or _first(info, "size"),
            "price": _first(value, "price") or _first(info, "price"),
            "role": role,
            "fee": CcxtPredictionGateway._fee(value),
        }

    async def get_fills(
        self,
        market: str | None = None,
        *,
        trade_id: str | None = None,
        order_id: str | None = None,
    ) -> list[VenueFill]:
        """Return normalized fills with exact maker and taker attribution."""
        params: dict[str, Any] = {}
        if market:
            params["market"] = market
        if trade_id:
            params["id"] = trade_id
        values = await self._fetch_private_pages(
            raw_method_names=(
                "clob_private_get_data_trades",
                "clobPrivateGetDataTrades",
            ),
            fallback_method=self._exchange.fetch_my_trades,
            params=params,
            collection_keys=("data", "trades"),
        )

        result: list[VenueFill] = []
        seen: set[tuple[str, str, int | None, str, str]] = set()
        for raw_value in values or ():
            value = _maybe_dict(raw_value)
            info = _nested_dict_or_self(value, "info")
            fill_id = str(
                _first(value, "id")
                or _first(info, "id", "transaction_hash", "transactionHash")
                or ""
            )
            if trade_id and fill_id != trade_id:
                continue
            bucket_raw = _first(info, "bucket_index", "bucketIndex")
            bucket_index = int(bucket_raw) if bucket_raw is not None else None
            timestamp_ms = _timestamp_ms(
                _first(value, "timestamp") or _first(info, "match_time", "matched_at", "timestamp")
            )
            timestamp = int((timestamp_ms or 0) / 1000)
            transaction_hash = (
                str(_first(info, "transaction_hash", "transactionHash") or "") or None
            )
            status = str(_first(info, "status") or _first(value, "status") or "") or None
            own_wallet = getattr(self._client, "wallet_address", None) or getattr(
                self._exchange, "walletAddress", None
            )
            for leg in self._trade_legs(value, wallet_address=own_wallet):
                leg_order_id = str(leg.get("order_id") or "")
                if order_id and leg_order_id != order_id:
                    continue
                token_id = str(leg.get("token_id") or "")
                size = _decimal(leg.get("size"), field="fill size", default=Decimal("0"))
                price = _decimal(leg.get("price"), field="fill price", default=Decimal("0"))
                dedupe = (fill_id, leg_order_id, bucket_index, str(size), str(price))
                if dedupe in seen:
                    continue
                seen.add(dedupe)
                result.append(
                    VenueFill(
                        fill_id=fill_id,
                        order_id=leg_order_id,
                        token_id=token_id,
                        side=str(leg.get("side") or "").upper(),
                        size=size,
                        price=price,
                        fee=leg["fee"],
                        timestamp=timestamp,
                        status=status,
                        transaction_hash=transaction_hash,
                        trader_side=str(leg.get("role") or "") or None,
                        bucket_index=bucket_index,
                        raw=value,
                    )
                )
        return result

    async def cancel_order(self, order_id: str) -> VenueCancelResult:
        """Cancel an order idempotently through the venue adapter."""
        try:
            value = await _await_call(self._exchange.cancel_order, order_id)
        except BaseException as exc:
            if _exception_name(exc) == "OrderNotFound" or _status_code(exc) == HTTPStatus.NOT_FOUND:
                return VenueCancelResult(
                    success=True,
                    order_id=order_id,
                    status="already_gone",
                )
            _raise_read_error(exc, not_found="order")

        raw = _maybe_dict(value)
        if isinstance(value, list):
            raw = {"canceled": value}
        canceled = _first(raw, "canceled", "cancelled")
        if isinstance(canceled, list) and order_id in {str(item) for item in canceled}:
            return VenueCancelResult(success=True, order_id=order_id, status="canceled")
        status = _normalize_status(_first(raw, "status") or _nested(raw, "info", "status"))
        if status == "canceled":
            return VenueCancelResult(success=True, order_id=order_id, status="canceled")
        not_canceled = _first(raw, "not_canceled", "notCanceled")
        error = None
        if isinstance(not_canceled, dict):
            error = str(not_canceled.get(order_id) or "") or None
        return VenueCancelResult(
            success=False,
            order_id=order_id,
            status="cancel_failed",
            error=error or "order was not canceled",
        )

    async def check_compatibility(self) -> dict[str, Any]:
        """Validate installed CCXT Prediction capabilities fail closed.

        The payload uses CCXT transport terminology only. ``live_execution_ready``
        additionally requires authentication, a verified type-3 Deposit Wallet
        identity, and the paginated private-history endpoints reconciliation
        depends on.
        """

        try:
            has = getattr(self._exchange, "has", {}) or {}
            options = getattr(self._exchange, "options", {}) or {}
            chain_id = int(options.get("chainId", _EXPECTED_CHAIN_ID))
            signature_type = options.get("signatureType")
            if signature_type is not None:
                signature_type = int(signature_type)
            wrapper = self._client if isinstance(self._client, CcxtPredictionClient) else None
            authenticated = (
                bool(wrapper.authenticated)
                if wrapper is not None
                else bool(getattr(self._exchange, "privateKey", None))
            )
            required = {
                "createOrder": True,
                "fetchOrderBook": True,
                "fetchOrder": True,
                "fetchOpenOrders": True,
                "fetchMyTrades": True,
                "cancelOrder": True,
            }
            missing = [name for name, expected in required.items() if has.get(name) is not expected]
            private_methods = {
                "orders": self._resolve_method(
                    "clob_private_get_data_orders",
                    "clobPrivateGetDataOrders",
                ),
                "trades": self._resolve_method(
                    "clob_private_get_data_trades",
                    "clobPrivateGetDataTrades",
                ),
            }
            missing_private_history = [
                name for name, method in private_methods.items() if method is None
            ]
            private_history_paginated = not missing_private_history
            patch = getattr(self._exchange, "_pmcopy_precision_patch", None)
            wallet = (
                wrapper.wallet_address
                if wrapper is not None
                else getattr(self._exchange, "walletAddress", None)
            )
            identity_ok = not authenticated or (
                signature_type == 3 and bool(str(wallet or "").strip())
            )
            capabilities = {
                "create_order": has.get("createOrder") is True,
                "cancel_order": has.get("cancelOrder") is True,
                "fetch_order": has.get("fetchOrder") is True,
                "fetch_open_orders": has.get("fetchOpenOrders") is True,
                "fetch_my_trades": has.get("fetchMyTrades") is True,
            }
            available = (
                bool(has.get("prediction"))
                and chain_id == _EXPECTED_CHAIN_ID
                and not missing
                and bool(patch)
                and identity_ok
                and (not authenticated or private_history_paginated)
            )
            live_execution_ready = (
                available
                and authenticated
                and signature_type == 3
                and bool(str(wallet or "").strip())
                and private_history_paginated
            )
            return build_ccxt_health_payload(
                available=available,
                ccxt_version=wrapper.ccxt_version if wrapper is not None else None,
                authenticated=authenticated,
                live_execution_ready=live_execution_ready,
                capabilities=capabilities,
                error=None if available else "CCXT prediction compatibility gate failed",
                details={
                    "chain_id": chain_id,
                    "prediction": bool(has.get("prediction")),
                    "wallet": wallet,
                    "signature_type": signature_type,
                    "private_history_paginated": private_history_paginated,
                    "missing_private_history_methods": missing_private_history,
                    "missing_methods": missing,
                },
            )
        except Exception as exc:  # compatibility probes must never raise
            # A probe failure reports a redacted category, never a CCXT
            # exception body that may echo the request.
            return build_ccxt_health_payload(
                available=False,
                ccxt_version=None,
                authenticated=False,
                live_execution_ready=False,
                capabilities={},
                error=redact_error_detail(exc),
            )

    async def close(self) -> None:
        """Close the injected provider or venue client."""
        if isinstance(self._client, CcxtPredictionClient):
            await self._client.close()
            return
        close = getattr(self._exchange, "close", None)
        if close is not None:
            await _await_call(close, True)


__all__ = ["CcxtPredictionGateway", "ExecutionMetadata"]
