from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
from typing import Any

from config.env import ENV
from py_clob_client.clob_types import OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from utils.logger import get_logger

logger = get_logger("ExecutionService")


@dataclass(frozen=True)
class ExecutionRequest:
    user_id: str
    leader_id: str
    token_id: str
    side: str  # BUY or SELL
    amount: Decimal
    max_slippage_bps: int
    reason: str  # copy_trade, stop_loss, take_profit, rebalance
    signal_id: str | None = None


@dataclass
class ExecutionResult:
    accepted: bool
    order_id: str | None
    status: str
    requested_amount: Decimal
    filled_amount: Decimal
    avg_price: Decimal | None
    fees_paid: Decimal
    error: str | None = None


def _normalize_side(side: str) -> str:
    value = (side or "").strip().upper()
    if value not in ("BUY", "SELL"):
        raise ValueError(f"Unsupported side: {side}")
    return value


def _quantize_price(price: Decimal, tick_size: Decimal, side: str) -> Decimal:
    if tick_size <= 0:
        return price
    units = price / tick_size
    if side == "BUY":
        rounded_units = units.to_integral_value(rounding=ROUND_CEILING)
    else:
        rounded_units = units.to_integral_value(rounding=ROUND_FLOOR)
    return rounded_units * tick_size


def _idempotency_key(req: ExecutionRequest) -> str:
    payload = "|".join(
        [
            req.signal_id or "",
            req.user_id,
            req.leader_id,
            req.token_id,
            req.side.upper(),
            str(req.amount),
            str(req.max_slippage_bps),
            req.reason,
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _is_retry_blocking_existing(existing: dict[str, Any] | None) -> bool:
    if not existing:
        return False
    status = str(existing.get("status") or existing.get("response", {}).get("status") or "").lower()
    if existing.get("order_id"):
        return True
    return status in {"submitted", "submitted_unconfirmed", "filled", "duplicate_ignored", "live", "matched"}


class ExecutionService:
    def __init__(self, clob_client, market_meta_service, fee_service, order_repo):
        self.clob_client = clob_client
        self.market_meta_service = market_meta_service
        self.fee_service = fee_service
        self.order_repo = order_repo

    async def _confirm_fill(self, order_id: str) -> dict[str, Any]:
        """Poll order status briefly and return best-effort fill details."""
        loop = asyncio.get_running_loop()
        last_error: Exception | None = None
        for _ in range(5):
            try:
                order = await loop.run_in_executor(
                    None,
                    lambda: self.clob_client.client.get_order(order_id),
                )
                if isinstance(order, dict):
                    status = str(order.get("status", "")).lower()
                    if status in {"filled", "matched"}:
                        return order
                    # Even canceled FOK may include matched/size.
                    if order.get("size_matched") or order.get("filled_size"):
                        return order
                await asyncio.sleep(0.4)
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(0.4)

        if last_error:
            logger.warning("fill confirmation warning for %s: %s", order_id, last_error)
        return {}

    async def execute(self, req: ExecutionRequest) -> ExecutionResult:
        side = _normalize_side(req.side)
        if req.amount <= 0:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="amount must be > 0",
            )

        meta = await self.market_meta_service.get(req.token_id)

        quote = await self.clob_client.get_price(req.token_id, side)
        if not quote or quote.get("price") is None:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error="unable to fetch live quote",
            )

        quote_price = Decimal(str(quote["price"]))
        min_share_price = Decimal(str(ENV.get("MIN_SHARE_PRICE", 0) or 0))
        if side == "BUY" and min_share_price > 0 and quote_price < min_share_price:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"price below min_share_price ({quote_price} < {min_share_price})",
            )

        slippage = Decimal(req.max_slippage_bps) / Decimal(10_000)
        if side == "BUY":
            worst_price = quote_price * (Decimal("1") + slippage)
        else:
            worst_price = quote_price * (Decimal("1") - slippage)
        worst_price = _quantize_price(worst_price, meta.tick_size, side)

        if side == "BUY" and meta.min_order_size > 0:
            requested_tokens = req.amount / worst_price if worst_price > 0 else Decimal("0")
            if requested_tokens + Decimal("1e-12") < meta.min_order_size:
                min_notional = meta.min_order_size * worst_price
                return ExecutionResult(
                    accepted=False,
                    order_id=None,
                    status="rejected",
                    requested_amount=req.amount,
                    filled_amount=Decimal("0"),
                    avg_price=None,
                    fees_paid=Decimal("0"),
                    error=(
                        "amount below min_order_size "
                        f"({meta.min_order_size} tokens ~= {min_notional:.4f} USDC at protected price {worst_price})"
                    ),
                )

        if side == "SELL" and meta.min_order_size > 0 and req.amount + Decimal("1e-12") < meta.min_order_size:
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="rejected",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=f"amount below min_order_size ({meta.min_order_size})",
            )

        idem_key = _idempotency_key(req)
        existing = self.order_repo.get_by_idempotency_key(idem_key)
        if _is_retry_blocking_existing(existing):
            existing_order_id = existing.get("order_id")
            return ExecutionResult(
                accepted=True,
                order_id=existing_order_id,
                status="duplicate_ignored",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=None,
            )

        side_const = BUY if side == "BUY" else SELL
        fee_bps = await self.fee_service.get_fee_bps(req.token_id)

        request_doc = asdict(req)
        request_doc["amount"] = str(req.amount)
        request_doc["worst_price"] = str(worst_price)
        request_doc["tick_size"] = str(meta.tick_size)
        request_doc["neg_risk"] = meta.neg_risk

        last_error: str | None = None
        response: dict[str, Any] = {}
        for attempt in range(1, 4):
            try:
                signed_order = await self.clob_client.create_market_order(
                    token_id=req.token_id,
                    side=side_const,
                    amount=float(req.amount),
                    max_price=float(worst_price),
                    tick_size=str(meta.tick_size),
                    neg_risk=meta.neg_risk,
                    order_type=OrderType.FOK,
                )
                response = await self.clob_client.post_signed_order(signed_order, OrderType.FOK)
                if response.get("success"):
                    break
                last_error = str(response.get("error") or "unknown order submission error")
            except Exception as exc:
                last_error = str(exc)
            await asyncio.sleep(0.3 * attempt)

        self.order_repo.save_submission(
            idempotency_key=idem_key,
            request=request_doc,
            response=response or {"success": False, "error": last_error, "status": "failed"},
        )

        if not response.get("success"):
            return ExecutionResult(
                accepted=False,
                order_id=None,
                status="failed",
                requested_amount=req.amount,
                filled_amount=Decimal("0"),
                avg_price=None,
                fees_paid=Decimal("0"),
                error=last_error or "order submission failed",
            )

        order_id = response.get("orderID")
        fill_info = await self._confirm_fill(order_id) if order_id else {}

        # Parse fill size — NEVER fallback to req.amount (that's notional for BUY, tokens for SELL)
        raw_fill = (
            fill_info.get("size_matched")
            or fill_info.get("filled_size")
            or fill_info.get("size")
        )
        fill_status = str(fill_info.get("status", "")).lower()

        if raw_fill and Decimal(str(raw_fill)) > 0 and fill_status in {"filled", "matched", ""}:
            filled_amount = Decimal(str(raw_fill))
            confirmed = True
        else:
            # Order accepted but fill not confirmed — do NOT fabricate fill from request
            filled_amount = Decimal("0")
            confirmed = False

        avg_price = Decimal(
            str(
                fill_info.get("avg_price")
                or fill_info.get("price")
                or worst_price
            )
        ) if confirmed else None

        if confirmed and avg_price and avg_price > 0:
            notional = filled_amount * avg_price
            fees_paid = (notional * Decimal(fee_bps) / Decimal(10_000)).quantize(
                Decimal("0.00000001")
            )
        else:
            notional = Decimal("0")
            fees_paid = Decimal("0")

        if order_id:
            self.order_repo.mark_fill(
                order_id=order_id,
                fill={
                    "filled_amount": str(filled_amount),
                    "avg_price": str(avg_price),
                    "fee_bps": fee_bps,
                    "fees_paid": str(fees_paid),
                    "raw": fill_info,
                },
            )

        result_status = "filled" if confirmed else "submitted_unconfirmed"
        if fill_status and fill_status not in {"", "filled", "matched"}:
            result_status = fill_status
        return ExecutionResult(
            accepted=True,
            order_id=order_id,
            status=result_status,
            requested_amount=req.amount,
            filled_amount=filled_amount,
            avg_price=avg_price,
            fees_paid=fees_paid,
            error=None,
        )
