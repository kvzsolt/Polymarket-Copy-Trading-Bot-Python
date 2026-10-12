"""Execution lifecycle amount-unit regression tests."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from pmcopy.services.execution.types import ExecutionRequest
from pmcopy.services.executionService import ExecutionService


def _request(*, side: str, amount: str) -> ExecutionRequest:
    """Build the smallest request needed by lifecycle projection."""
    return ExecutionRequest(
        user_id="user-1",
        leader_id="leader-1",
        token_id="token-1",
        side=side,
        amount=Decimal(amount),
        max_slippage_bps=0,
        reason="copy_trade",
        signal_id=f"signal-{side.lower()}",
        condition_id="condition-1",
    )


class TestExecutionLifecycleAmounts:
    """Verify BUY cost and SELL share requests retain their true units."""

    def test_buy_cost_is_usdc_and_token_size_is_estimated(self) -> None:
        """Project a BUY's USDC cost into tokens without multiplying twice."""
        request = _request(side="BUY", amount="17.406709")
        price = Decimal("0.88")

        with patch("pmcopy.utils.logger.log_order_lifecycle") as lifecycle_log:
            ExecutionService._emit_post_lifecycle(
                req=request,
                side="BUY",
                order_id="order-1",
                worst_price=price,
                venue_status="canceled",
                lifecycle_status="canceled",
            )

        payload = lifecycle_log.call_args.kwargs
        assert payload["size"] == float(request.amount / price)
        assert payload["usdc_value"] == float(request.amount)
        assert payload["meta"]["sizeEstimated"] is True
        assert payload["meta"]["requestAmount"] == "17.406709"
        assert payload["meta"]["requestAmountUnit"] == "USDC"

    def test_sell_amount_is_tokens_and_value_is_price_times_size(self) -> None:
        """Keep a SELL's request amount share-denominated in lifecycle logs."""
        request = _request(side="SELL", amount="5.9523")
        price = Decimal("0.84")

        with patch("pmcopy.utils.logger.log_order_lifecycle") as lifecycle_log:
            ExecutionService._emit_post_lifecycle(
                req=request,
                side="SELL",
                order_id="order-2",
                worst_price=price,
                venue_status="canceled",
                lifecycle_status="canceled",
            )

        payload = lifecycle_log.call_args.kwargs
        assert payload["size"] == float(request.amount)
        assert payload["usdc_value"] == float(request.amount * price)
        assert payload["meta"]["sizeEstimated"] is False
        assert payload["meta"]["requestAmount"] == "5.9523"
        assert payload["meta"]["requestAmountUnit"] == "tokens"
