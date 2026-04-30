"""Tests for V2-native CLOB adapter helpers (Phase 4).

Verifies that each adapter in clob_v1_compat calls the V2 SDK method with
the correct signature and propagates exceptions.
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

_V2_SPEC = ["create_or_derive_api_key", "post_order", "cancel_order", "set_api_creds"]


def _v2_client() -> MagicMock:
    return MagicMock(spec=_V2_SPEC)


class _FakeOrderPayload:
    def __init__(self, orderID: str) -> None:
        self.orderID = orderID

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _FakeOrderPayload) and self.orderID == other.orderID


class TestDeriveApiCreds:
    def test_calls_create_or_derive_api_key(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.return_value = sentinel.api_key

        result = derive_api_creds(client)

        client.create_or_derive_api_key.assert_called_once_with()
        assert result is sentinel.api_key

    def test_propagates_exception(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.side_effect = RuntimeError("auth failed")

        with pytest.raises(RuntimeError, match="auth failed"):
            derive_api_creds(client)


class TestSubmitSignedOrder:
    def test_uses_order_type_kwarg(self) -> None:
        client = _v2_client()
        client.post_order.return_value = sentinel.response

        result = submit_signed_order(client, sentinel.signed_order, OrderType.GTC)

        client.post_order.assert_called_once_with(
            sentinel.signed_order, order_type=OrderType.GTC
        )
        assert result is sentinel.response

    def test_fok_order_type(self) -> None:
        client = _v2_client()
        client.post_order.return_value = {"orderID": "abc123"}

        result = submit_signed_order(client, sentinel.order, OrderType.FOK)

        client.post_order.assert_called_once_with(
            sentinel.order, order_type=OrderType.FOK
        )
        assert result == {"orderID": "abc123"}

    def test_kwarg_is_order_type_not_orderType(self) -> None:
        client = _v2_client()
        client.post_order.return_value = sentinel.v2_response

        submit_signed_order(client, sentinel.signed_order, OrderType.FOK)

        call_kwargs = client.post_order.call_args.kwargs
        assert "orderType" not in call_kwargs
        assert "order_type" in call_kwargs

    def test_propagates_exception(self) -> None:
        client = _v2_client()
        client.post_order.side_effect = RuntimeError("post failed")

        with pytest.raises(RuntimeError, match="post failed"):
            submit_signed_order(client, sentinel.order, OrderType.FOK)


class TestCancelOrderById:
    def test_calls_cancel_order_with_payload(self) -> None:
        client = _v2_client()
        client.cancel_order.return_value = sentinel.cancel

        with patch("utils.clob_v1_compat.OrderPayload", _FakeOrderPayload):
            result = cancel_order_by_id(client, "order-42")

        client.cancel_order.assert_called_once_with(_FakeOrderPayload("order-42"))
        assert result is sentinel.cancel

    def test_propagates_exception(self) -> None:
        client = _v2_client()
        client.cancel_order.side_effect = RuntimeError("cancel failed")

        with patch("utils.clob_v1_compat.OrderPayload", _FakeOrderPayload):
            with pytest.raises(RuntimeError, match="cancel failed"):
                cancel_order_by_id(client, "order-99")


def test_compat_module_exports_adapter_functions() -> None:
    from utils import clob_v1_compat

    for name in ("derive_api_creds", "submit_signed_order", "cancel_order_by_id"):
        assert name in clob_v1_compat.__all__, f"Missing from __all__: {name}"
        assert hasattr(clob_v1_compat, name), f"Missing attribute: {name}"
        assert callable(getattr(clob_v1_compat, name)), f"Not callable: {name}"
