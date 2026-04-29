"""Tests for CLOB V1 method-level adapter seams.

These tests verify that the three adapter functions in clob_v1_compat
correctly delegate to the underlying V1 ClobClient methods with the
expected argument signatures.  When V2 is wired in, only the adapter
implementations change — these tests will be updated to assert V2
delegation instead.
"""

from __future__ import annotations

from unittest.mock import MagicMock, sentinel

import pytest

from utils.clob_v1_compat import (
    OrderType,
    cancel_order_by_id,
    derive_api_creds,
    submit_signed_order,
)


# ---------------------------------------------------------------------------
# derive_api_creds
# ---------------------------------------------------------------------------


class TestDeriveApiCreds:
    def test_delegates_to_v1_create_or_derive_api_creds(self) -> None:
        client = MagicMock()
        client.create_or_derive_api_creds.return_value = sentinel.creds

        result = derive_api_creds(client)

        client.create_or_derive_api_creds.assert_called_once_with()
        assert result is sentinel.creds

    def test_propagates_exception_from_client(self) -> None:
        client = MagicMock()
        client.create_or_derive_api_creds.side_effect = RuntimeError("auth failed")

        with pytest.raises(RuntimeError, match="auth failed"):
            derive_api_creds(client)


# ---------------------------------------------------------------------------
# submit_signed_order
# ---------------------------------------------------------------------------


class TestSubmitSignedOrder:
    def test_delegates_to_v1_post_order_with_orderType_kwarg(self) -> None:
        client = MagicMock()
        client.post_order.return_value = sentinel.response

        result = submit_signed_order(client, sentinel.signed_order, OrderType.FOK)

        client.post_order.assert_called_once_with(
            sentinel.signed_order, orderType=OrderType.FOK
        )
        assert result is sentinel.response

    def test_works_with_gtc_order_type(self) -> None:
        client = MagicMock()
        client.post_order.return_value = {"orderID": "abc123"}

        result = submit_signed_order(client, sentinel.order, OrderType.GTC)

        client.post_order.assert_called_once_with(
            sentinel.order, orderType=OrderType.GTC
        )
        assert result == {"orderID": "abc123"}

    def test_propagates_exception_from_client(self) -> None:
        client = MagicMock()
        client.post_order.side_effect = RuntimeError("post failed")

        with pytest.raises(RuntimeError, match="post failed"):
            submit_signed_order(client, sentinel.order, OrderType.FOK)


# ---------------------------------------------------------------------------
# cancel_order_by_id
# ---------------------------------------------------------------------------


class TestCancelOrderById:
    def test_delegates_to_v1_cancel_with_order_id_kwarg(self) -> None:
        client = MagicMock()
        client.cancel.return_value = sentinel.cancel_response

        result = cancel_order_by_id(client, "order-42")

        client.cancel.assert_called_once_with(order_id="order-42")
        assert result is sentinel.cancel_response

    def test_propagates_exception_from_client(self) -> None:
        client = MagicMock()
        client.cancel.side_effect = RuntimeError("cancel failed")

        with pytest.raises(RuntimeError, match="cancel failed"):
            cancel_order_by_id(client, "order-99")


# ---------------------------------------------------------------------------
# Module-level __all__ includes the adapters
# ---------------------------------------------------------------------------


def test_compat_module_exports_adapter_functions() -> None:
    from utils import clob_v1_compat

    for name in ("derive_api_creds", "submit_signed_order", "cancel_order_by_id"):
        assert name in clob_v1_compat.__all__, f"Missing from __all__: {name}"
        assert hasattr(clob_v1_compat, name), f"Missing attribute: {name}"
        assert callable(getattr(clob_v1_compat, name)), f"Not callable: {name}"
