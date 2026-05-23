"""Issue #93 — CLOB V2 compatibility probe must prove live-trading readiness.

``ClobV2Gateway.check_compatibility`` is the structured readiness probe the
startup health gate keys on in live mode. These tests pin the contract the
audit demanded:

* Every SDK-exposed contract/identity field is probed and reported
  structurally (None when the SDK build does not expose it — informational,
  not an error by itself).
* The **hard-required** minimum — ``exchange_address`` present,
  ``chain_id == 137`` (Polygon mainnet), and a marketable-order method —
  flips ``sdk_available`` to False with a structured ``error`` so the
  startup gate fails closed.

Per-trade live state (collateral/conditional-token *balance* and *allowance*)
is intentionally NOT probed here: that data is wallet/RPC-bound and already
gated on the per-trade balance-regime / balance-client path.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from integrations.polymarket.clob_v2_gateway import ClobV2Gateway


def _make_gateway(*, sdk_attrs: dict | None = None) -> ClobV2Gateway:
    """Build a gateway whose raw client carries the given attrs/methods."""
    raw = MagicMock(spec=[*(sdk_attrs or {}).keys()])
    for name, value in (sdk_attrs or {}).items():
        setattr(raw, name, value)
    wrapper = MagicMock()
    wrapper.client = raw
    gw = ClobV2Gateway.__new__(ClobV2Gateway)
    gw._client = wrapper
    return gw


class TestRequiredMinimum:
    @pytest.mark.asyncio
    async def test_full_sdk_reports_sdk_available_true(self):
        gw = _make_gateway(sdk_attrs={
            "exchange_address": "0xExchange",
            "chain_id": 137,
            "neg_risk_exchange_address": "0xNegRiskExchange",
            "collateral_address": "0xUSDC",
            "conditional_token_address": "0xCTF",
            "fee_rate_bps": 0,
            "create_market_order": lambda **kw: None,
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is True
        assert report["error"] is None
        assert report["exchange_address"] == "0xExchange"
        assert report["chain_id"] == 137
        assert report["negative_risk_supported"] is True
        # Structured discovery — all extra fields populated.
        assert report["neg_risk_exchange_address"] == "0xNegRiskExchange"
        assert report["collateral_address"] == "0xUSDC"
        assert report["conditional_token_address"] == "0xCTF"
        assert report["fee_rate_bps"] == 0

    @pytest.mark.asyncio
    async def test_missing_exchange_address_fails_closed(self):
        gw = _make_gateway(sdk_attrs={
            "chain_id": 137,
            "create_market_order": lambda **kw: None,
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is False
        assert "exchange_address" in (report["error"] or "")

    @pytest.mark.asyncio
    async def test_wrong_chain_id_fails_closed(self):
        gw = _make_gateway(sdk_attrs={
            "exchange_address": "0xExchange",
            "chain_id": 1,                       # Ethereum mainnet — wrong domain
            "create_market_order": lambda **kw: None,
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is False
        assert "chain_id" in (report["error"] or "")
        assert "137" in (report["error"] or "")  # expected chain in error

    @pytest.mark.asyncio
    async def test_missing_marketable_order_method_fails_closed(self):
        gw = _make_gateway(sdk_attrs={
            "exchange_address": "0xExchange",
            "chain_id": 137,
            # neither create_market_order nor create_and_post_order present
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is False
        assert report["negative_risk_supported"] is False
        assert "create_market_order" in (report["error"] or "")


class TestStructuredDiscovery:
    """Optional fields default to None when the SDK does not expose them —
    that is informational, not an error, as long as the required minimum
    is intact."""

    @pytest.mark.asyncio
    async def test_optional_fields_none_when_sdk_silent(self):
        gw = _make_gateway(sdk_attrs={
            "exchange_address": "0xExchange",
            "chain_id": 137,
            "create_market_order": lambda **kw: None,
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is True
        assert report["neg_risk_exchange_address"] is None
        assert report["collateral_address"] is None
        assert report["conditional_token_address"] is None
        assert report["fee_rate_bps"] is None

    @pytest.mark.asyncio
    async def test_getter_methods_accepted_as_alternative_to_attrs(self):
        gw = _make_gateway(sdk_attrs={
            "get_exchange_address": lambda: "0xExchange",
            "get_chain_id": lambda: 137,
            "create_market_order": lambda **kw: None,
        })
        report = await gw.check_compatibility()
        assert report["sdk_available"] is True
        assert report["exchange_address"] == "0xExchange"
        assert report["chain_id"] == 137
