"""Release-gate: typed config fail-closed (Issue #75 item 12).

Production trading services must refuse to operate without a typed
``AppConfig``. This gate asserts:
* ``ExecutionService`` raises at construction when ``config`` is omitted
  or explicitly ``None`` — silent fallback to env defaults is forbidden;
* the typed ``AppConfig`` factory raises ``EnvValidationError`` on an empty
  environment (no silent default to live mode).

Pre-fix history: a bare ``except Exception: return`` swallowed wrong-
signature ``TypeError`` from a stale constructor call, allowing the test
to pass vacuously even when production accepted ``config=None``. The
asserts below use ``pytest.raises`` so any future regression that allows
configless construction will surface as a test failure rather than a
silent green.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pmcopy.config.app_config import EnvValidationError, load_config
from pmcopy.services.executionService import ExecutionService


def test_app_config_rejects_empty_env() -> None:
    with pytest.raises(EnvValidationError):
        load_config({})


def test_execution_service_omitting_config_raises_type_error() -> None:
    """Omitting the keyword-only ``config`` argument is a hard TypeError."""
    stub_clob_client = MagicMock()
    fee_service = MagicMock()
    order_repo = MagicMock()
    clob_gateway = MagicMock()

    with pytest.raises(TypeError, match=r"missing.*required.*config"):
        ExecutionService(
            stub_clob_client,
            fee_service,
            order_repo,
            clob_gateway=clob_gateway,
        )


def test_execution_service_explicit_config_none_raises() -> None:
    """Explicitly passing ``config=None`` must raise — no silent env fallback."""
    stub_clob_client = MagicMock()
    fee_service = MagicMock()
    order_repo = MagicMock()
    clob_gateway = MagicMock()

    with pytest.raises(TypeError, match="requires a typed AppConfig"):
        ExecutionService(
            stub_clob_client,
            fee_service,
            order_repo,
            config=None,
            clob_gateway=clob_gateway,
        )
