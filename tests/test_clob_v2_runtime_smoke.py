"""V2-native runtime path smoke test (Phase 4).

Verifies the three active call paths (auth, order submit, cancel) exercise
V2 SDK methods without any V1 dispatch leftovers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, sentinel

import pytest

from utils.clob_v1_compat import (
    OrderPayload,
    OrderType,
    cancel_order_by_id,
    derive_api_creds,
    submit_signed_order,
)


def _v2_client() -> MagicMock:
    return MagicMock(
        spec=["create_or_derive_api_key", "post_order", "cancel_order", "set_api_creds"]
    )


class TestAuthPathV2Native:
    def test_derive_api_creds_calls_v2_method(self) -> None:
        client = _v2_client()
        client.create_or_derive_api_key.return_value = sentinel.key

        creds = derive_api_creds(client)

        client.create_or_derive_api_key.assert_called_once()
        assert creds is sentinel.key

    def test_no_v1_method_on_client_spec(self) -> None:
        client = _v2_client()
        assert not hasattr(client, "create_or_derive_api_creds")


class TestOrderSubmitPathV2Native:
    def test_post_order_uses_underscore_order_type(self) -> None:
        client = _v2_client()
        client.post_order.return_value = {"orderID": "v2-order-1"}

        result = submit_signed_order(client, sentinel.order, OrderType.FOK)

        client.post_order.assert_called_once_with(
            sentinel.order, order_type=OrderType.FOK
        )
        assert result["orderID"] == "v2-order-1"

    def test_kwarg_name_is_order_type_not_orderType(self) -> None:
        client = _v2_client()
        client.post_order.return_value = {}

        submit_signed_order(client, sentinel.order, OrderType.GTC)

        kwargs = client.post_order.call_args.kwargs
        assert "order_type" in kwargs
        assert "orderType" not in kwargs


class TestCancelPathV2Native:
    def test_cancel_uses_cancel_order_with_order_payload(self) -> None:
        client = _v2_client()
        client.cancel_order.return_value = sentinel.cancelled

        result = cancel_order_by_id(client, "oid-123")

        assert result is sentinel.cancelled
        call_args = client.cancel_order.call_args
        payload = call_args[0][0]
        assert isinstance(payload, OrderPayload)
        assert payload.orderID == "oid-123"

    def test_no_v1_cancel_method_on_client_spec(self) -> None:
        client = _v2_client()
        assert not hasattr(client, "cancel")


class TestNoV1DetectionLeftovers:
    def test_module_has_no_is_v2_client_function(self) -> None:
        import utils.clob_v1_compat as mod

        assert not hasattr(mod, "_is_v2_client"), (
            "V1/V2 dispatch detector _is_v2_client should be removed"
        )


class TestV2SDKInstalledEnvironment:
    """Verify the V2 SDK package is actually importable with expected types.

    These tests exercise the real installed environment, not mocks.
    They guard against packaging or dependency regressions that seam-only
    tests would miss.
    """

    def test_v2_client_class_importable(self) -> None:
        from py_clob_client_v2.client import ClobClient

        assert callable(ClobClient)

    def test_v2_order_types_are_real_enum(self) -> None:
        from utils.clob_v1_compat import OrderType

        assert hasattr(OrderType, "FOK")
        assert hasattr(OrderType, "GTC")
        assert hasattr(OrderType, "GTD")

    def test_v2_order_payload_is_real_dataclass(self) -> None:
        from utils.clob_v1_compat import OrderPayload

        payload = OrderPayload(orderID="test-id")
        assert payload.orderID == "test-id"

    def test_v2_order_args_is_real_dataclass(self) -> None:
        from utils.clob_v1_compat import OrderArgs

        args = OrderArgs(token_id="tok", side="BUY", size=10.0, price=0.5)
        assert args.token_id == "tok"
        assert args.side == "BUY"

    def test_v2_buy_sell_constants_exist(self) -> None:
        from utils.clob_v1_compat import BUY, SELL

        assert BUY is not None
        assert SELL is not None
        assert BUY != SELL

    def test_compat_layer_exports_match_all(self) -> None:
        import utils.clob_v1_compat as mod

        for name in mod.__all__:
            assert hasattr(mod, name), f"__all__ lists {name!r} but it is not exported"

    def test_order_gateway_resolve_order_type_roundtrip(self) -> None:
        from clients.order_gateway import _resolve_order_type

        for ot_name in ("FOK", "GTC", "GTD"):
            resolved = _resolve_order_type(ot_name)
            assert str(resolved) == ot_name, f"Expected {ot_name!r}, got {resolved!r}"
