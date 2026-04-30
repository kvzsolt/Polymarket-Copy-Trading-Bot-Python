"""Tests for CLOB V1/V2 method-level adapter seams (Phase 2c).

Each adapter in clob_v1_compat dispatches to V1 or V2 client methods depending
on the client's shape at call time.  These tests verify:
- V1-shaped client → V1 method called with V1 signature
- V2-shaped client → V2 method called with V2 signature
- Exception propagation in both paths

Fakes use MagicMock(spec=[...]) so hasattr() returns False for absent names,
mirroring real V1/V2 duck-type detection.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, sentinel

import pytest

from utils.clob_v1_compat import (
    OrderType,
    cancel_order_by_id,
    derive_api_creds,
    submit_signed_order,
)

# ---------------------------------------------------------------------------
# Fake client factories
# ---------------------------------------------------------------------------

_V1_SPEC = ["create_or_derive_api_creds", "post_order", "cancel", "set_api_creds"]
_V2_SPEC = ["create_or_derive_api_key", "post_order", "cancel_order", "set_api_creds"]


def _v1_client() -> MagicMock:
    """V1-shaped client: has create_or_derive_api_creds, NOT create_or_derive_api_key."""
    return MagicMock(spec=_V1_SPEC)


def _v2_client() -> MagicMock:
    """V2-shaped client: has create_or_derive_api_key, NOT create_or_derive_api_creds."""
    return MagicMock(spec=_V2_SPEC)


# ---------------------------------------------------------------------------
# derive_api_creds
# ---------------------------------------------------------------------------


class TestDeriveApiCreds:
    def test_v1_delegates_to_create_or_derive_api_creds(self) -> None:
        client = _v1_client()
        client.create_or_derive_api_creds.return_value = sentinel.creds

        result = derive_api_creds(client)

        client.create_or_derive_api_creds.assert_called_once_with()
        assert result is sentinel.creds

    def test_v1_propagates_exception(self) -> None:
        client = _v1_client()
        client.create_or_derive_api_creds.side_effect = RuntimeError("auth failed")

        with pytest.raises(RuntimeError, match="auth failed"):
            derive_api_creds(client)

    def test_v2_delegates_to_create_or_derive_api_key(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.return_value = sentinel.api_key

        result = derive_api_creds(client)

        client.create_or_derive_api_key.assert_called_once_with()
        assert result is sentinel.api_key

    def test_v2_does_not_call_v1_method(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.return_value = sentinel.api_key

        derive_api_creds(client)

        assert not hasattr(client, "create_or_derive_api_creds")

    def test_v2_propagates_exception(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.side_effect = RuntimeError("v2 auth failed")

        with pytest.raises(RuntimeError, match="v2 auth failed"):
            derive_api_creds(client)


# ---------------------------------------------------------------------------
# submit_signed_order
# ---------------------------------------------------------------------------


class TestSubmitSignedOrder:
    def test_v1_uses_orderType_kwarg(self) -> None:
        client = _v1_client()
        client.post_order.return_value = sentinel.response

        result = submit_signed_order(client, sentinel.signed_order, OrderType.FOK)

        client.post_order.assert_called_once_with(
            sentinel.signed_order, orderType=OrderType.FOK
        )
        assert result is sentinel.response

    def test_v1_works_with_gtc_order_type(self) -> None:
        client = _v1_client()
        client.post_order.return_value = {"orderID": "abc123"}

        result = submit_signed_order(client, sentinel.order, OrderType.GTC)

        client.post_order.assert_called_once_with(
            sentinel.order, orderType=OrderType.GTC
        )
        assert result == {"orderID": "abc123"}

    def test_v1_propagates_exception(self) -> None:
        client = _v1_client()
        client.post_order.side_effect = RuntimeError("post failed")

        with pytest.raises(RuntimeError, match="post failed"):
            submit_signed_order(client, sentinel.order, OrderType.FOK)

    def test_v2_uses_order_type_kwarg(self) -> None:
        client = _v2_client()
        client.post_order.return_value = sentinel.v2_response

        result = submit_signed_order(client, sentinel.signed_order, OrderType.GTC)

        client.post_order.assert_called_once_with(
            sentinel.signed_order, order_type=OrderType.GTC
        )
        assert result is sentinel.v2_response

    def test_v2_does_not_use_orderType_kwarg(self) -> None:
        client = _v2_client()
        client.post_order.return_value = sentinel.v2_response

        submit_signed_order(client, sentinel.signed_order, OrderType.FOK)

        call_kwargs = client.post_order.call_args.kwargs
        assert "orderType" not in call_kwargs
        assert "order_type" in call_kwargs

    def test_v2_propagates_exception(self) -> None:
        client = _v2_client()
        client.post_order.side_effect = RuntimeError("v2 post failed")

        with pytest.raises(RuntimeError, match="v2 post failed"):
            submit_signed_order(client, sentinel.order, OrderType.FOK)


# ---------------------------------------------------------------------------
# cancel_order_by_id
# ---------------------------------------------------------------------------


class _FakeOrderPayload:
    """Minimal stand-in for V2 OrderPayload."""

    def __init__(self, orderID: str) -> None:
        self.orderID = orderID

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeOrderPayload) and self.orderID == other.orderID


class TestCancelOrderById:
    def test_v1_delegates_to_cancel_with_order_id_kwarg(self) -> None:
        client = _v1_client()
        client.cancel.return_value = sentinel.cancel_response

        result = cancel_order_by_id(client, "order-42")

        client.cancel.assert_called_once_with(order_id="order-42")
        assert result is sentinel.cancel_response

    def test_v1_propagates_exception(self) -> None:
        client = _v1_client()
        client.cancel.side_effect = RuntimeError("cancel failed")

        with pytest.raises(RuntimeError, match="cancel failed"):
            cancel_order_by_id(client, "order-99")

    def test_v2_delegates_to_cancel_order_with_payload(self) -> None:
        client = _v2_client()
        client.cancel_order.return_value = sentinel.v2_cancel

        with patch("utils.clob_v1_compat.OrderPayload", _FakeOrderPayload):
            result = cancel_order_by_id(client, "order-v2-55")

        client.cancel_order.assert_called_once_with(_FakeOrderPayload("order-v2-55"))
        assert result is sentinel.v2_cancel

    def test_v2_does_not_call_v1_cancel(self) -> None:
        client = _v2_client()
        client.cancel_order.return_value = sentinel.v2_cancel

        with patch("utils.clob_v1_compat.OrderPayload", _FakeOrderPayload):
            cancel_order_by_id(client, "order-v2-55")

        assert not hasattr(client, "cancel")

    def test_v2_propagates_exception_from_cancel_order(self) -> None:
        client = _v2_client()
        client.cancel_order.side_effect = RuntimeError("v2 cancel failed")

        with patch("utils.clob_v1_compat.OrderPayload", _FakeOrderPayload):
            with pytest.raises(RuntimeError, match="v2 cancel failed"):
                cancel_order_by_id(client, "order-v2-99")


# ---------------------------------------------------------------------------
# _is_v2_client detection
# ---------------------------------------------------------------------------


class TestIsV2ClientDetection:
    def test_v1_shape_not_detected_as_v2(self) -> None:
        from utils.clob_v1_compat import _is_v2_client

        client = _v1_client()
        assert _is_v2_client(client) is False

    def test_v2_shape_detected_as_v2(self) -> None:
        from utils.clob_v1_compat import _is_v2_client

        client = _v2_client()
        assert _is_v2_client(client) is True

    def test_plain_object_not_detected_as_v2(self) -> None:
        from utils.clob_v1_compat import _is_v2_client

        assert _is_v2_client(object()) is False


# ---------------------------------------------------------------------------
# Module-level __all__ includes the adapters
# ---------------------------------------------------------------------------


def test_compat_module_exports_adapter_functions() -> None:
    from utils import clob_v1_compat

    for name in ("derive_api_creds", "submit_signed_order", "cancel_order_by_id"):
        assert name in clob_v1_compat.__all__, f"Missing from __all__: {name}"
        assert hasattr(clob_v1_compat, name), f"Missing attribute: {name}"
        assert callable(getattr(clob_v1_compat, name)), f"Not callable: {name}"
