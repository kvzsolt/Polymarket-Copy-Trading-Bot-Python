"""Recovery inventory proof binds fresh complete holdings to the follower wallet."""

import importlib
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

WALLET = "0x" + "a" * 40


class TestRecoveryInventoryReader:
    """Validate zero-only evidence and avoid cached account observations."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("size", [None, Decimal("1"), Decimal("-1"), Decimal("NaN"), 0, False])
    async def test_only_exact_zero_is_absence(self, size: object) -> None:
        """Unknown, positive, malformed, or inexact evidence never proves absence."""
        reader = self._reader(AsyncMock(return_value=size))
        assert await reader.zero_inventory("user-1", "condition", "123") is False

    @pytest.mark.asyncio
    async def test_each_proof_fetches_fresh_shares_for_the_bound_wallet(self) -> None:
        """A prior zero result cannot authorize recovery after inventory changes."""
        shares = AsyncMock(side_effect=[Decimal(0), Decimal(2)])
        reader = self._reader(shares)
        assert await reader.zero_inventory("user-1", "condition", "123") is True
        assert await reader.zero_inventory("user-1", "condition", "123") is False
        assert shares.await_count == 2
        shares.assert_awaited_with(WALLET, "123")

    @pytest.mark.asyncio
    async def test_unknown_user_cannot_query_a_different_wallet(self) -> None:
        """Only explicitly bound follower identities receive an absence proof."""
        shares = AsyncMock(return_value=Decimal(0))
        assert await self._reader(shares).zero_inventory("other-user", "condition", "123") is False
        shares.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "condition, token", [(None, "123"), ("condition", None), ("", "123"), ("condition", "0")]
    )
    async def test_malformed_scope_cannot_authorize_recovery(self, condition, token) -> None:
        """Malformed identities remain unavailable without querying the provider."""
        shares = AsyncMock(return_value=Decimal(0))
        assert await self._reader(shares).zero_inventory("user-1", condition, token) is False
        shares.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reader_error_is_sanitized(self, caplog: pytest.LogCaptureFixture) -> None:
        """Provider exceptions must not expose their raw detail."""
        shares = AsyncMock(side_effect=RuntimeError("credential-secret"))
        assert await self._reader(shares).zero_inventory("user-1", "condition", "123") is False
        assert "credential-secret" not in caplog.text

    @staticmethod
    def _reader(shares: AsyncMock):
        module = importlib.import_module("services.execution.recovery_inventory")
        return module.CompleteRecoveryInventoryReader(
            SimpleNamespace(shares=shares), wallet_by_user={"user-1": WALLET}
        )
