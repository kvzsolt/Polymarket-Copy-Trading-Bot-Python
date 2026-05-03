"""Single gateway for all CLOB V2 SDK access.

All CLOB constants and types must be imported from here, not from
py_clob_client directly, so that CLOB SDK usage is consolidated in one place.

The only allowed exception is `utils/createClobClient.py` which constructs
the raw CLOB client and wraps its full API behind `ClobClientWrapper`.
"""

from __future__ import annotations

from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

# V2 payload type — lazy import; may fail if V2 client is not installed.
try:
    from py_clob_client_v2.clob_types import OrderPayload  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    OrderPayload = None  # type: ignore[assignment,misc]

__all__ = ["BUY", "OrderArgs", "OrderPayload", "OrderType", "SELL"]
