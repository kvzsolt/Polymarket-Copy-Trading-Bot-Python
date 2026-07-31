from __future__ import annotations

import base64
import json
from decimal import Decimal

from integrations.heisenberg.models import (
    Market360Snapshot,
    MarketSnapshot,
    TradeSnapshot,
    Wallet360Snapshot,
    parse_leaderboard_entry,
    select_latest_wallet360,
)


def encoded_categories() -> str:
    payload = [{"category": "Sports", "pnl": 12.5, "trades": 9}]
    return base64.b64encode(json.dumps(payload).encode()).decode()


def test_wallet360_selects_latest_snapshot_and_decodes_categories() -> None:
    rows = [
        {
            "proxy_wallet": "0xabc",
            "calculation_window_days": 3,
            "date_range_end": "2026-07-29T00:00:00Z",
            "last_active": "2026-07-29T12:00:00Z",
            "avg_position_size": 100,
            "market_concentration_ratio": 0.2,
            "performance_by_category": encoded_categories(),
        },
        {
            "proxy_wallet": "0xabc",
            "calculation_window_days": 3,
            "date_range_end": "2026-07-30T00:00:00Z",
            "last_active": "2026-07-30T12:00:00Z",
            "avg_position_size": 125,
            "market_concentration_ratio": 0.15,
            "performance_by_category": encoded_categories(),
        },
    ]

    snapshot = select_latest_wallet360(rows, expected_window_days=3)

    assert isinstance(snapshot, Wallet360Snapshot)
    assert snapshot.avg_position_size == Decimal("125")
    assert snapshot.market_concentration_ratio == Decimal("0.15")
    assert snapshot.performance_by_category[0]["category"] == "Sports"


def test_leaderboard_parser_accepts_common_field_aliases() -> None:
    entry = parse_leaderboard_entry(
        {
            "wallet": "0xABC",
            "hscore": "78.5",
            "roi": "0.22",
            "pnl": "1200",
            "win_rate": "0.64",
            "trades": 80,
            "trajectory": "rising",
        }
    )

    assert entry.wallet == "0xabc"
    assert entry.h_score == Decimal("78.5")
    assert entry.roi_15d == Decimal("0.22")
    assert entry.trajectory == "rising"


def test_trade_market_and_market360_parsers_preserve_exact_decimals() -> None:
    trade = TradeSnapshot.from_mapping(
        {
            "id": "t1",
            "proxy_wallet": "0xabc",
            "condition_id": "c1",
            "side": "BUY",
            "price": "0.4175",
            "size": "12.25",
            "timestamp": "2026-07-30T10:00:00Z",
            "outcome": "Yes",
        }
    )
    market = MarketSnapshot.from_mapping(
        {
            "condition_id": "c1",
            "slug": "market",
            "closed": False,
            "start_date": "2026-07-29T00:00:00Z",
            "end_date": "2026-08-02T00:00:00Z",
            "volume_total": "10000.12",
            "side_a_token_id": "yes-token",
            "side_b_token_id": "no-token",
        }
    )
    quality = Market360Snapshot.from_mapping(
        {
            "condition_id": "c1",
            "liquidity_percentile": "82.50",
            "current_volume_24h": "12345.67",
            "top1_wallet_pct": "12.5",
            "top10_wallet_pct": "45",
            "market_active": True,
            "market_closed": False,
            "volume_trend": "Normal",
        }
    )

    assert trade.notional == Decimal("5.114375")
    assert market.volume_total == Decimal("10000.12")
    assert quality.liquidity_percentile == Decimal("82.50")
    assert quality.top1_wallet_fraction == Decimal("0.125")


def test_wallet360_roi_percentage_is_normalized_to_fraction() -> None:
    snapshot = Wallet360Snapshot.from_mapping(
        {
            "proxy_wallet": "0xabc",
            "calculation_window_days": 3,
            "date_range_end": "2026-03-15T00:00:00Z",
            "roi": "3.8743",
            "total_pnl": "290085.37",
            "total_invested": "7487360.92",
        }
    )

    assert snapshot.roi == Decimal("0.038743")
