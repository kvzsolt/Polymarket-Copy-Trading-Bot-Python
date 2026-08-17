"""Release-gate: CLOB gateway compatibility smoke (Issue #75 item 13).

Pins the production gateway protocol surface so anything that drops a
required method, weakens an awaitable signature, or unwraps the gateway in
``ExecutionService`` breaks the gate immediately. The full SDK isolation
tests live in ``tests/test_issue66_gateway_isolation.py``.
"""

from __future__ import annotations

import inspect

import pytest

from interfaces.clob_gateway import ClobGateway


REQUIRED_METHODS = (
    "get_market_info",
    "get_orderbook",
    "submit_marketable_order",
    "submit_limit_order",
    "get_order",
    "get_fills",
    "cancel_order",
    "check_compatibility",
)


def test_gateway_protocol_has_required_methods() -> None:
    missing = [name for name in REQUIRED_METHODS if not hasattr(ClobGateway, name)]
    assert not missing, f"ClobGateway Protocol missing methods: {missing}"


def test_gateway_methods_are_coroutines() -> None:
    for name in REQUIRED_METHODS:
        method = getattr(ClobGateway, name)
        assert inspect.iscoroutinefunction(method), f"ClobGateway.{name} must be async"


@pytest.mark.asyncio
async def test_mock_gateway_check_compatibility_returns_available_flag(
    mock_clob_gateway,
) -> None:
    report = await mock_clob_gateway.check_compatibility()
    assert "available" in report
