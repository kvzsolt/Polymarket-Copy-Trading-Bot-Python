"""Idempotency guard for order execution.

Re-exports from the established idempotency module for new-style imports
under the execution subpackage.
"""
from __future__ import annotations

# Import directly from the idempotency module file, bypassing utils/__init__.py
# which eagerly imports createClobClient (requiring web3/py_clob_client_v2).
import importlib

_idempotency = importlib.import_module("utils.idempotency")
build_trade_key = _idempotency.build_trade_key
has_processed = _idempotency.has_processed
mark_processed = _idempotency.mark_processed

__all__ = ["build_trade_key", "has_processed", "mark_processed"]
