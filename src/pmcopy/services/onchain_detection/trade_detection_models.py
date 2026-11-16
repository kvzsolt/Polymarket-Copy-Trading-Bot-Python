"""Detection-confidence ladder + canonical on-chain trade signal (PR2).

Pending-transaction signals are **provisional**: they may be dropped, replaced,
or reverted before inclusion. Only mined ``OrderFilled`` logs are
authoritative, and even then a single settlement transaction can emit multiple
fill logs, so the final dedupe key is ``(tx_hash, log_index)`` — never
``tx_hash`` alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pmcopy.domain.source_time import TransportReceipt


class DetectionConfidence(StrEnum):
    """Provisional-to-authoritative confidence ladder.

    Ordered weakest -> strongest:

    * ``MARKET_UNATTRIBUTED`` — a market-level event with no wallet attribution.
    * ``PENDING_PROBABLE`` — a pending transaction whose calldata references a
      watched wallet. Provisional: may be dropped/reverted/replaced.
    * ``MINED_AUTHORITATIVE`` — a mined ``OrderFilled`` log whose ``maker`` or
      ``taker`` is a watched wallet. Authoritative for copy-trigger purposes,
      but still deduped by ``(tx_hash, log_index)`` because one tx can emit
      multiple fill logs.
    * ``CONFIRMED_FINAL`` — a mined fill that has survived finality / re-org
      protection. Reserved for future use; PR2 emits up to
      ``MINED_AUTHORITATIVE``.
    """

    MARKET_UNATTRIBUTED = "market_unattributed"
    PENDING_PROBABLE = "pending_probable"
    MINED_AUTHORITATIVE = "mined_authoritative"
    CONFIRMED_FINAL = "confirmed_final"


# Maker-side enum values from the CTF Exchange V2 ``OrderFilled`` event.
# ``side`` is the *maker* side: 0 = BUY, 1 = SELL. The taker side is the
# opposite.
MAKER_SIDE_BUY = 0
MAKER_SIDE_SELL = 1


@dataclass(frozen=True)
class OnchainTradeSignal:
    """Canonical on-chain trade signal emitted by the detectors.

    Carries the raw maker side (from the event/calldata) plus the *watched
    wallet's* side (which is the maker side when the watched wallet is the
    maker, and the opposite when the watched wallet is the taker), so the
    copy path receives the side the watched wallet effectively traded.
    """

    leader_wallet: str
    token_id: str
    side: str
    size: Decimal
    price: Decimal
    usdc_size: Decimal
    tx_hash: str
    confidence: DetectionConfidence
    maker_side: int
    watched_role: str
    event_ts: int | None
    log_index: int | None
    condition_id: str
    provisional: bool
    exchange: str
    order_hash: str | None
    block_number: int | None = None
    block_hash: str = ""
    leader_sell_fraction: Decimal | None = None
    sell_fraction_resolution: str = ""
    source_receipt: TransportReceipt | None = None
    source_action_id: str | None = None
    token_holder_wallet: str = ""

    def as_raw_signal_dict(self) -> dict[str, object]:
        """Render as a raw signal dict shaped for the copy pipeline.

        ``orchestrators.trade_pipeline.execute_trades`` consumes
        ``leader_id``/``leader_wallet``, ``condition_id``, ``token_id``,
        ``side`` in {BUY,SELL}, ``size``/``size_tokens``, ``price``,
        ``usdc_size``/``notional``, ``tx_hash``, ``event_ts``/``timestamp``.
        A blank ``condition_id`` drops the signal downstream — acceptable for
        an inert-by-default feature; the runtime enriches condition_id where
        it can (pending: from calldata; mined: via Gamma lookup).
        """
        raw: dict[str, object] = {
            "leader_id": self.leader_wallet,
            "leader_wallet": self.leader_wallet,
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "asset_id": self.token_id,
            "side": self.side,
            "size": str(self.size),
            "size_tokens": str(self.size),
            "price": str(self.price),
            "usdc_size": str(self.usdc_size),
            "notional": str(self.usdc_size),
            "tx_hash": self.tx_hash,
            "event_ts": None if self.provisional else self.event_ts,
            "timestamp": None if self.provisional else self.event_ts,
            "timestamp_unit": "milliseconds",
            "timestamp_provenance": (
                "pending_receipt"
                if self.provisional
                else "block_timestamp"
                if self.event_ts is not None
                else "unknown"
            ),
            "_source_receipt": self.source_receipt,
            "source_action_id": self.source_action_id,
            "detection_confidence": str(self.confidence),
            "log_index": self.log_index,
            "provisional": self.provisional,
            "exchange": self.exchange,
            "watched_role": self.watched_role,
            "order_hash": self.order_hash,
            "block_number": self.block_number,
            "block_hash": self.block_hash,
            "token_holder_wallet": self.token_holder_wallet or self.leader_wallet,
        }
        if self.leader_sell_fraction is not None:
            raw["_leader_sell_fraction"] = str(self.leader_sell_fraction)
            raw["_leader_sell_fraction_resolution"] = self.sell_fraction_resolution
        return raw


def watched_side_from_maker_side(maker_side: int, watched_role: str) -> str:
    """Return the watched wallet's effective side.

    ``maker_side`` is the raw event/calldata side (0=BUY, 1=SELL). If the
    watched wallet is the maker, its side equals ``maker_side``. If the
    watched wallet is the taker, its side is the opposite.
    """
    if watched_role == "maker":
        return "BUY" if maker_side == MAKER_SIDE_BUY else "SELL"
    return "SELL" if maker_side == MAKER_SIDE_BUY else "BUY"


def price_from_fill(maker_side: int, maker_amount_filled: int, taker_amount_filled: int) -> Decimal:
    """Compute the fill price from the maker-side perspective.

    * maker BUY  -> ``makerAmountFilled / takerAmountFilled`` (tokens per USDC)
    * maker SELL -> ``takerAmountFilled / makerAmountFilled`` (tokens per USDC)

    Both amounts are raw uint256 from the event. CTF outcome tokens are
    6-decimal (matching USDC.e) so the ratio is dimensionless; callers that
    need a human-readable probability divide by ``10**6`` only when converting
    amounts to token/USDC quantities, not when computing the price ratio.
    """
    if taker_amount_filled == 0 or maker_amount_filled == 0:
        return Decimal(0)
    if maker_side == MAKER_SIDE_BUY:
        return Decimal(maker_amount_filled) / Decimal(taker_amount_filled)
    return Decimal(taker_amount_filled) / Decimal(maker_amount_filled)
