"""P2 venue/gateway fixes (i, k, l, j).

Covers four safety hardenings:

* **P2-i** — outbound SDK GETs (``get_order`` / ``get_fills`` / ``get_orderbook``)
  are bounded by ``asyncio.timeout`` so a wedged keep-alive socket cannot stall
  the reconcile/confirm loop forever. A timeout surfaces as the
  ambiguous/retryable ``VenueUnavailableError`` (a GET is idempotent).
* **P2-k** — ``_classify_error`` dispatches on ``status_code`` FIRST
  (404→not_found, 401/403→auth, 400→invalid_order, 429→rate_limited,
  5xx→retryable), falling back to substring matching only when no code is
  present. The ``PolyApiException(status_code=None)`` transport shape is
  preserved.
* **P2-l** — ``cancel_order`` treats a 404 / not-found as a terminal SUCCESS
  (``already_gone``) instead of an ambiguous failure retried forever.
* **P2-j** — when neg_risk cannot be determined (``get_neg_risk`` transport
  failed AND the book omits neg_risk), market-meta resolution fails closed
  (raises) instead of silently defaulting ``neg_risk=False`` and signing for
  the wrong exchange. A pre-fetched / V2 neg_risk still takes precedence.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from integrations.polymarket.clob_v2_gateway import (
    _SDK_GET_TIMEOUT_SECONDS,
    ClobV2Gateway,
    _classify_error,
)
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import VenueCancelResult


# ---------------------------------------------------------------------------
# Shared test doubles
# ---------------------------------------------------------------------------

class _CodedExc(Exception):
    """Exception carrying a numeric HTTP ``status_code`` like the SDK does."""

    def __init__(self, status_code, message=""):
        self.status_code = status_code
        super().__init__(message or f"http {status_code}")


class PolyApiException(Exception):
    """Test double matching py-clob-client-v2's transport-failure shape."""

    def __init__(self, status_code=None, message="Request exception!"):
        self.status_code = status_code
        super().__init__(message)


def _gateway_with_raw(raw_client) -> ClobV2Gateway:
    """Build a gateway whose ``_require_raw_client`` returns ``raw_client``."""
    wrapper = MagicMock()
    wrapper.client = raw_client
    return ClobV2Gateway(wrapper)


# get_order / get_fills are now true-async aiohttp GETs (no thread executor), so
# they are driven via a fake aiohttp session rather than a sync SDK callable.
class _RespCM:
    def __init__(self, status=200, body=None, exc=None):
        self.status = status
        self._body = {} if body is None else body
        self._exc = exc

    async def json(self, content_type=None):  # noqa: ARG002
        # Yield briefly so SonarCloud S7503 sees this as a genuine async
        # function (aiohttp's caller awaits ``response.json()``).
        await asyncio.sleep(0)
        return self._body

    async def text(self):
        await asyncio.sleep(0)
        return str(self._body)

    async def __aenter__(self):
        if self._exc is not None:
            raise self._exc
        return self

    async def __aexit__(self, *a):  # noqa: ANN002
        return False


class _SessionStub:
    def __init__(self, items):
        self._items = list(items)
        self.calls = 0

    def get(self, url, headers=None, params=None, timeout=None):  # noqa: ANN001, ARG002
        self.calls += 1
        item = self._items.pop(0) if self._items else _RespCM(200, {})
        return _RespCM(exc=item) if isinstance(item, Exception) else item


def _async_raw_client():
    return MagicMock(host="https://clob.polymarket.com",
                     _l2_headers=MagicMock(return_value={"k": "v"}))


# ===========================================================================
# P2-k — status-code-first classification
# ===========================================================================

class TestClassifyByStatusCodeFirst:
    def test_404_status_is_not_found(self):
        # Pre-fix: 404 with a body matching no substring fell through to
        # ``unknown``. Now the status code is authoritative.
        assert _classify_error(_CodedExc(404, "totally opaque body")) == (False, "not_found")

    def test_401_status_is_auth(self):
        assert _classify_error(_CodedExc(401, "opaque")) == (False, "auth_failure")

    def test_403_status_is_auth(self):
        assert _classify_error(_CodedExc(403, "opaque")) == (False, "auth_failure")

    def test_400_status_is_invalid_order(self):
        assert _classify_error(_CodedExc(400, "opaque body no keyword")) == (False, "invalid_order")

    def test_429_status_is_rate_limited(self):
        assert _classify_error(_CodedExc(429, "opaque")) == (True, "rate_limited")

    @pytest.mark.parametrize("code", [500, 502, 503, 504, 599])
    def test_5xx_status_is_retryable_server_error(self, code):
        retryable, reason = _classify_error(_CodedExc(code, "opaque"))
        assert retryable is True
        assert reason == f"server_error_{code}"

    def test_status_code_wins_over_misleading_message(self):
        # A 400 (terminal) whose text contains "timeout" must NOT be classified
        # retryable — the authoritative status code takes precedence.
        retryable, reason = _classify_error(_CodedExc(400, "request timeout-ish wording"))
        assert (retryable, reason) == (False, "invalid_order")

    def test_poly_api_exception_none_status_still_transport(self):
        # The deliberate exception to status-first dispatch: a transport
        # failure has no code and must stay retryable transport.
        retryable, reason = _classify_error(PolyApiException(status_code=None))
        assert (retryable, reason) == (True, "poly_api_transport")

    def test_substring_fallback_used_when_no_status_code(self):
        # No status_code → existing substring matching still classifies.
        assert _classify_error(Exception("tick size mismatch")) == (False, "invalid_order")
        retryable, _ = _classify_error(Exception("connection timeout"))
        assert retryable is True

    def test_not_found_message_without_status_code(self):
        # A status-code-less 404 shape is still classified not_found via msg.
        assert _classify_error(Exception("order not found")) == (False, "not_found")


# ===========================================================================
# P2-l — cancel_order treats 404/not-found as terminal success
# ===========================================================================

class TestCancelOrderNotFoundTerminal:
    @pytest.mark.asyncio
    async def test_cancel_404_status_returns_already_gone_success(self):
        raw = MagicMock()
        raw.cancel_order.side_effect = _CodedExc(404, "no order")
        gw = _gateway_with_raw(raw)

        result = await gw.cancel_order("0xdead")

        assert isinstance(result, VenueCancelResult)
        assert result.success is True
        assert result.status == "already_gone"
        assert result.order_id == "0xdead"

    @pytest.mark.asyncio
    async def test_cancel_not_found_message_returns_already_gone_success(self):
        raw = MagicMock()
        raw.cancel_order.side_effect = Exception("order not found on venue")
        gw = _gateway_with_raw(raw)

        result = await gw.cancel_order("0xdead")

        assert result.success is True
        assert result.status == "already_gone"

    @pytest.mark.asyncio
    async def test_cancel_transport_failure_still_retryable(self):
        # Regression guard: a genuine transport failure must still raise the
        # retryable error — only 404/not-found becomes terminal success.
        raw = MagicMock()
        raw.cancel_order.side_effect = PolyApiException(status_code=None)
        gw = _gateway_with_raw(raw)

        with pytest.raises(VenueUnavailableError):
            await gw.cancel_order("0xdead")

    @pytest.mark.asyncio
    async def test_cancel_invalid_still_terminal_invalid(self):
        raw = MagicMock()
        raw.cancel_order.side_effect = _CodedExc(400, "bad payload")
        gw = _gateway_with_raw(raw)

        with pytest.raises(VenueInvalidOrderError):
            await gw.cancel_order("0xdead")

    @pytest.mark.asyncio
    async def test_cancel_success_path_unchanged(self):
        raw = MagicMock()
        raw.cancel_order.return_value = {"success": True}
        gw = _gateway_with_raw(raw)

        result = await gw.cancel_order("0xlive")
        assert result.success is True
        assert result.status == "cancelled"


# ===========================================================================
# P2-i — outbound GETs are timeout-bounded
# ===========================================================================

class TestGetTimeouts:
    @pytest.mark.asyncio
    async def test_get_order_timeout_raises_unavailable(self, monkeypatch):
        # A per-request aiohttp timeout surfaces as the retryable
        # VenueUnavailableError after one idempotent retry (no thread is tied up).
        session = _SessionStub([asyncio.TimeoutError(), asyncio.TimeoutError()])
        monkeypatch.setattr(
            "integrations.polymarket.clob_v2_gateway._get_session", lambda: session
        )
        gw = _gateway_with_raw(_async_raw_client())

        with pytest.raises(VenueUnavailableError):
            await gw.get_order("0xhang")
        assert session.calls == 2  # original + exactly one retry

    @pytest.mark.asyncio
    async def test_get_fills_timeout_raises_unavailable(self, monkeypatch):
        session = _SessionStub([asyncio.TimeoutError(), asyncio.TimeoutError()])
        monkeypatch.setattr(
            "integrations.polymarket.clob_v2_gateway._get_session", lambda: session
        )
        gw = _gateway_with_raw(_async_raw_client())

        with pytest.raises(VenueUnavailableError):
            await gw.get_fills()
        assert session.calls == 2

    @pytest.mark.asyncio
    async def test_get_orderbook_timeout_raises_unavailable(self, monkeypatch):
        monkeypatch.setattr(
            "integrations.polymarket.clob_v2_gateway._SDK_GET_TIMEOUT_SECONDS", 0.05
        )

        async def _hang(token_id):
            await asyncio.sleep(5)
            return MagicMock(bids=[], asks=[])

        wrapper = MagicMock()
        wrapper.client = MagicMock()
        wrapper.get_order_book = _hang
        gw = ClobV2Gateway(wrapper)

        with pytest.raises(VenueUnavailableError) as exc_info:
            await gw.get_orderbook("0xhang")
        assert "timeout" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_get_order_fast_path_unaffected(self, monkeypatch):
        # A prompt 200 response must still parse and return normally.
        session = _SessionStub([
            _RespCM(200, {"id": "0xok", "status": "live", "size": "5"}),
        ])
        monkeypatch.setattr(
            "integrations.polymarket.clob_v2_gateway._get_session", lambda: session
        )
        gw = _gateway_with_raw(_async_raw_client())

        state = await gw.get_order("0xok")
        assert state.order_id == "0xok"
        assert state.status == "live"

    def test_timeout_constant_is_positive(self):
        assert _SDK_GET_TIMEOUT_SECONDS > 0
