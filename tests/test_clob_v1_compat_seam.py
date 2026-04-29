"""Tests validating the CLOB V1 compatibility seam module.

This test suite ensures that:
1. The compatibility seam exports all required V1 types/constants
2. Active code paths import exclusively through the seam (not directly from py_clob_client)
3. The seam is transparent — code using it behaves identically to pre-seam code
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

import pytest

# Test that seam module exists and exports all required symbols
def test_clob_v1_compat_exports_required_symbols() -> None:
    """Validate that clob_v1_compat exports all necessary V1 types and constants."""
    from utils.clob_v1_compat import (
        ClobClient,
        MarketOrderArgs,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
        BUY,
        SELL,
    )

    # Validate that imports succeeded and are not None
    assert ClobClient is not None
    assert MarketOrderArgs is not None
    assert OrderArgs is not None
    assert OrderType is not None
    assert PartialCreateOrderOptions is not None
    assert BUY is not None
    assert SELL is not None

    # Validate type names match expected V1 SDK
    assert ClobClient.__module__.startswith("py_clob_client")
    assert OrderType.__name__ == "OrderType"
    assert isinstance(BUY, str)
    assert isinstance(SELL, str)


def test_clob_v1_compat_module_all_export() -> None:
    """Validate that __all__ is properly defined for clean imports."""
    from utils import clob_v1_compat

    expected_exports = {
        "ClobClient",
        "MarketOrderArgs",
        "OrderArgs",
        "OrderType",
        "PartialCreateOrderOptions",
        "BUY",
        "SELL",
    }
    assert set(clob_v1_compat.__all__) == expected_exports


class AstImportVisitor(ast.NodeVisitor):
    """AST visitor to detect direct imports from py_clob_client."""

    def __init__(self) -> None:
        self.direct_imports: list[tuple[str, str]] = []  # (module, imported_name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module and node.module.startswith("py_clob_client"):
            if isinstance(node.names, list):
                for alias in node.names:
                    self.direct_imports.append((node.module, alias.name))
        self.generic_visit(node)


def test_active_code_imports_only_through_seam() -> None:
    """Validate that active runtime code imports V1 types only through the seam.

    This ensures we have a single controlled boundary for future V2 migration.
    Allowed direct imports: only in utils/clob_v1_compat.py itself.
    """
    repo_root = Path(__file__).parent.parent
    active_files = [
        repo_root / "utils" / "createClobClient.py",
        repo_root / "services" / "executionService.py",
        repo_root / "clients" / "order_gateway.py",
        repo_root / "utils" / "postOrder.py",  # deprecated but included for consistency
    ]

    violations = []
    for file_path in active_files:
        if not file_path.exists():
            continue

        with open(file_path, "r") as f:
            tree = ast.parse(f.read())

        visitor = AstImportVisitor()
        visitor.visit(tree)

        if visitor.direct_imports:
            violations.append(
                f"{file_path.relative_to(repo_root)}: "
                f"direct py_clob_client imports {visitor.direct_imports}"
            )

    assert (
        not violations
    ), f"Found direct py_clob_client imports (should use utils.clob_v1_compat instead):\n" + "\n".join(
        violations
    )


def test_seam_is_transparent_ordertype_enum() -> None:
    """Validate that OrderType from seam behaves identically to direct import."""
    from utils.clob_v1_compat import OrderType
    from py_clob_client.clob_types import OrderType as DirectOrderType

    # Must be the same type
    assert OrderType is DirectOrderType

    # Must support enum access (V1 SDK has FOK, GTC, FAK, GTD)
    assert hasattr(OrderType, "FOK")
    assert hasattr(OrderType, "GTC")
    assert hasattr(OrderType, "FAK")

    # Enum values must be usable
    fok = OrderType.FOK
    assert str(fok) in ["OrderType.FOK", "FOK"]


def test_seam_is_transparent_buy_sell_constants() -> None:
    """Validate that BUY/SELL from seam are identical to direct import."""
    from utils.clob_v1_compat import BUY, SELL
    from py_clob_client.order_builder.constants import BUY as DirectBUY
    from py_clob_client.order_builder.constants import SELL as DirectSELL

    # Must be identical strings
    assert BUY == DirectBUY
    assert SELL == DirectSELL
    assert BUY == "BUY"
    assert SELL == "SELL"


def test_seam_order_args_instantiation() -> None:
    """Validate that OrderArgs from seam can be instantiated correctly."""
    from utils.clob_v1_compat import OrderArgs

    order = OrderArgs(
        token_id="0x123",
        side="BUY",
        size=100.0,
        price=0.5,
    )

    assert order.token_id == "0x123"
    assert order.side == "BUY"
    assert order.size == 100.0
    assert order.price == 0.5


def test_seam_market_order_args_instantiation() -> None:
    """Validate that MarketOrderArgs from seam can be instantiated correctly."""
    from utils.clob_v1_compat import MarketOrderArgs

    order = MarketOrderArgs(
        token_id="0x123",
        side="SELL",
        amount=50.0,
        price=0.75,
    )

    assert order.token_id == "0x123"
    assert order.side == "SELL"
    assert order.amount == 50.0
    assert order.price == 0.75


@pytest.mark.parametrize(
    "expected_export",
    [
        "ClobClient",
        "MarketOrderArgs",
        "OrderArgs",
        "OrderType",
        "PartialCreateOrderOptions",
        "BUY",
        "SELL",
    ],
)
def test_seam_module_exports_all_symbols(expected_export: str) -> None:
    """Parametrized test to validate each required export exists."""
    from utils import clob_v1_compat

    assert hasattr(clob_v1_compat, expected_export), (
        f"Seam module missing export: {expected_export}"
    )
    assert expected_export in clob_v1_compat.__all__


def test_executeservice_imports_through_seam() -> None:
    """Smoke test that executionService can import OrderType, BUY, SELL from seam."""
    from services.executionService import OrderType, BUY, SELL

    assert OrderType is not None
    assert BUY == "BUY"
    assert SELL == "SELL"


def test_ordergateay_imports_through_seam() -> None:
    """Smoke test that order_gateway can import OrderArgs, OrderType from seam."""
    from clients.order_gateway import OrderGateway
    from utils.clob_v1_compat import OrderArgs, OrderType

    # Verify the gateway can be imported and has expected signature
    assert hasattr(OrderGateway, "post_order")

    # Verify types are available
    assert OrderArgs is not None
    assert OrderType is not None


def test_createclobclient_imports_through_seam() -> None:
    """Smoke test that createClobClient imports V1 types from seam."""
    from utils.createClobClient import (
        ClobClient,
        MarketOrderArgs,
        OrderArgs,
        OrderType,
        PartialCreateOrderOptions,
    )

    # All types should be available and callable
    assert ClobClient is not None
    assert MarketOrderArgs is not None
    assert OrderArgs is not None
    assert OrderType is not None
    assert PartialCreateOrderOptions is not None
