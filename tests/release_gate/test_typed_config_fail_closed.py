"""Release-gate: typed config fail-closed (Issue #75 item 12).

Production trading services must refuse to operate without a typed
``AppConfig``. This gate asserts:
* ``ExecutionService.execute`` returns an unaccepted result (or raises)
  when constructed without ``config``;
* the typed ``AppConfig`` factory raises ``EnvValidationError`` on an empty
  environment (no silent default to live mode).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from config.app_config import EnvValidationError, load_config
from interfaces.clob_gateway import ClobGateway
from services.executionService import ExecutionRequest, ExecutionService
from services.orderRepository import OrderRepository
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


def test_app_config_rejects_empty_env() -> None:
    with pytest.raises(EnvValidationError):
        load_config({})


@pytest.mark.asyncio
async def test_execution_service_without_config_fails_closed(
    tmp_path,
    stub_clob_client,
) -> None:
    """ExecutionService constructed without typed config must NOT submit."""
    db_path = str(tmp_path / "fail_closed.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()

    repo = OrderRepository(db_path)
    gateway = MagicMock(spec=ClobGateway)
    fee_svc = MagicMock()
    meta_svc = MagicMock()

    try:
        svc = ExecutionService(
            stub_clob_client, meta_svc, fee_svc, repo,
            config=None,
            clob_gateway=gateway,
        )
    except Exception:
        # Fail-closed at construction is the correct behaviour: a service
        # without typed config must not be usable.
        return

    request = ExecutionRequest(
        user_id="u1",
        leader_id="l1",
        token_id="t1",
        side="BUY",
        amount=Decimal("5"),
        max_slippage_bps=100,
        reason="copy_trade",
        signal_id="sig-fc",
    )
    try:
        result = await svc.execute(request)
    except Exception:
        return
    assert getattr(result, "accepted", True) is False, (
        "ExecutionService must refuse to submit without typed AppConfig"
    )
