"""DL-02: ``_get_position_in_conn(for_update=True)`` must refuse a missing
``token_id``.

Pre-fix the no-token branch picked the first row by ``ORDER BY token_id
ASC LIMIT 1`` and silently locked it. For a binary-outcome market that
holds two TrackedPositions sharing ``(user_id, condition_id)`` the wrong
leg could end up locked under ``apply_fill``. The guard surfaces the
ambiguity at the caller boundary instead of corrupting state.
"""

from __future__ import annotations

import asyncio

import pytest

from storage.postgres.positions_repo import PositionsRepo


def test_get_position_in_conn_refuses_for_update_without_token_id() -> None:
    async def _go() -> None:
        with pytest.raises(ValueError, match="for_update.*token_id"):
            await PositionsRepo._get_position_in_conn(
                conn=None,  # unreachable — guard fires before any conn use
                user_id="u",
                condition_id="cond",
                token_id=None,
                for_update=True,
            )

    asyncio.run(_go())


def test_get_position_in_conn_refuses_for_update_with_empty_token_id() -> None:
    async def _go() -> None:
        with pytest.raises(ValueError, match="for_update.*token_id"):
            await PositionsRepo._get_position_in_conn(
                conn=None,
                user_id="u",
                condition_id="cond",
                token_id="",
                for_update=True,
            )

    asyncio.run(_go())
