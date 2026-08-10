from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from integrations.heisenberg.errors import HeisenbergUnavailableError
from services.discovery.heisenberg_candidate_service import (
    HeisenbergCandidateService,
    HeisenbergDiscoveryPolicy,
)

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
GOOD = "0x" + "1" * 40
WHALE = "0x" + "2" * 40
SYBIL = "0x" + "3" * 40
STALE = "0x" + "4" * 40
INVALID = "not-an-evm-wallet"


def wallet360(
    wallet: str, window: int, *, sybil: bool = False, concentration: float = 0.2
) -> list[dict[str, Any]]:
    return [
        {
            "proxy_wallet": wallet,
            "calculation_window_days": window,
            "date_range_end": NOW.isoformat(),
            "last_active": NOW.isoformat(),
            "avg_position_size": 5 if wallet.endswith("1") else 10_000,
            "market_concentration_ratio": concentration,
            "max_drawdown": 0.08,
            "max_position_pct": 0.20,
            "profit_factor": 1.4,
            "roi": 0.18,
            "sharpe_ratio": 1.8,
            "statistical_confidence": 0.9,
            "performance_trend": "improving",
            "sybil_risk_flag": sybil,
            "sybil_risk_score": 90 if sybil else 0,
            "timing_anomaly_flag": False,
            "position_size_volatility_flag": False,
            "total_trades": 100,
            "win_rate": 0.62,
            "total_pnl": 1000,
        }
    ]


def recent_trades(wallet: str, *, short: bool = True, stale: bool = False) -> list[dict[str, Any]]:
    timestamp = NOW - (timedelta(hours=20) if stale else timedelta(minutes=30))
    condition = "short" if short else "long"
    size = "20" if wallet.endswith("1") else "20000"
    return [
        {
            "id": f"{wallet}-{i}",
            "proxy_wallet": wallet,
            "condition_id": condition,
            "side": "BUY",
            "price": "0.5",
            "size": size,
            "timestamp": timestamp.isoformat(),
            "outcome": "Yes",
        }
        for i in range(12)
    ]


class FakeHeisenberg:
    def __init__(self) -> None:
        self.unavailable_wallet: str | None = None

    async def leaderboard(self, **_kwargs):
        return [
            {
                "wallet": GOOD,
                "h_score": 82,
                "roi_15d": 0.20,
                "pnl_15d": 5000,
                "win_rate_15d": 0.62,
                "total_trades_15d": 100,
                "trajectory": "rising",
            },
            {
                "wallet": WHALE,
                "h_score": 88,
                "roi_15d": 0.25,
                "pnl_15d": 9000,
                "win_rate_15d": 0.66,
                "total_trades_15d": 120,
                "trajectory": "stable",
            },
            {
                "wallet": SYBIL,
                "h_score": 95,
                "roi_15d": 0.9,
                "pnl_15d": 100000,
                "win_rate_15d": 0.9,
                "total_trades_15d": 1000,
                "trajectory": "rising",
            },
            {
                "wallet": STALE,
                "h_score": 80,
                "roi_15d": 0.2,
                "pnl_15d": 4000,
                "win_rate_15d": 0.6,
                "total_trades_15d": 90,
                "trajectory": "stable",
            },
            {
                "wallet": INVALID,
                "h_score": 99,
                "roi_15d": 0.9,
                "pnl_15d": 999999,
                "win_rate_15d": 0.9,
                "total_trades_15d": 9999,
                "trajectory": "rising",
            },
        ]

    async def wallet_360(self, wallet: str, window_days: int, **_kwargs):
        if wallet == self.unavailable_wallet:
            raise HeisenbergUnavailableError("vendor unavailable")
        return wallet360(wallet, window_days, sybil=wallet == SYBIL)

    async def wallet_stats_all_time(self, wallet: str, **_kwargs):
        return [
            {
                "proxy_wallet": wallet,
                "total_trades": 1000,
                "avg_trade_size": "100",
                "roi_pct": "10",
                "total_pnl": "5000",
            }
        ]

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        return recent_trades(proxy_wallet, stale=proxy_wallet == STALE)

    async def markets(self, *, condition_id: str, **_kwargs):
        end = NOW + (timedelta(days=3) if condition_id == "short" else timedelta(days=90))
        return [
            {
                "condition_id": condition_id,
                "closed": False,
                "start_date": (NOW - timedelta(days=1)).isoformat(),
                "end_date": end.isoformat(),
                "volume_total": "100000",
                "side_a_token_id": "yes",
                "side_b_token_id": "no",
            }
        ]

    async def market_360(self, *, condition_id: str, **_kwargs):
        return [
            {
                "condition_id": condition_id,
                "liquidity_percentile": "85",
                "current_volume_24h": "100000",
                "top1_wallet_pct": "15",
                "top10_wallet_pct": "45",
                "market_active": True,
                "market_closed": False,
                "volume_trend": "Normal",
                "whale_control_flag": False,
                "trade_concentration_flag": False,
            }
        ]

    async def pnl(self, *, wallet: str, **_kwargs):
        return [
            {
                "proxy_wallet": wallet,
                "pnl": "500",
                "trades": 30,
                "wins": 20,
                "losses": 10,
                "win_rate": "0.666",
            }
        ]


@pytest.mark.asyncio
async def test_discovery_rejects_sybil_and_stale_wallets_and_returns_evidence() -> None:
    service = HeisenbergCandidateService(
        FakeHeisenberg(),
        policy=HeisenbergDiscoveryPolicy(candidate_limit=10),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    wallets = [candidate.wallet for candidate in result.ranked]
    assert GOOD in wallets
    assert WHALE in wallets
    assert SYBIL not in wallets
    assert STALE not in wallets
    assert INVALID not in wallets
    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "sybil_risk" in rejected[SYBIL]
    assert "inactive_or_stale" in rejected[STALE]
    assert "invalid_wallet_address" in rejected[INVALID]
    assert result.ranked[0].evidence.wallet360_windows == (1, 3, 7, 15)


@pytest.mark.asyncio
async def test_best_wallet_changes_with_available_funds() -> None:
    service = HeisenbergCandidateService(FakeHeisenberg(), now=lambda: NOW)

    small = await service.discover(Decimal("50"))
    large = await service.discover(Decimal("5000"))

    assert small.best is not None
    assert large.best is not None
    assert small.best.wallet == GOOD
    assert large.best.wallet == WHALE
    assert small.best.bankroll_fit.recommended_stake_usdc <= Decimal("50")


@pytest.mark.asyncio
async def test_partial_wallet360_failure_never_silently_promotes_candidate() -> None:
    client = FakeHeisenberg()
    client.unavailable_wallet = GOOD
    service = HeisenbergCandidateService(client, now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "insufficient_wallet360_evidence" in rejected[GOOD]


class PriceJumpHeisenberg(FakeHeisenberg):
    async def price_jumps(self, *, token_id: str, **_kwargs):
        return [{"token_id": token_id, "change_pct": "15"}] * 5


@pytest.mark.asyncio
async def test_excessive_recent_price_jumps_are_a_fail_closed_market_risk() -> None:
    service = HeisenbergCandidateService(
        PriceJumpHeisenberg(),
        policy=HeisenbergDiscoveryPolicy(max_recent_price_jumps=2),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("5000"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert any("excessive_recent_price_jumps" in reasons for reasons in rejected.values())


class MixedQualityHeisenberg(FakeHeisenberg):
    def __init__(self, toxic_conditions: set[str]) -> None:
        super().__init__()
        self.toxic_conditions = toxic_conditions

    async def leaderboard(self, **_kwargs):
        return [
            {
                "wallet": GOOD,
                "h_score": 82,
                "roi_15d": 0.20,
                "pnl_15d": 5000,
                "win_rate_15d": 0.62,
                "total_trades_15d": 100,
                "trajectory": "rising",
            }
        ]

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        return [
            {
                "id": f"{proxy_wallet}-{index}",
                "proxy_wallet": proxy_wallet,
                "condition_id": f"condition-{index % 4}",
                "side": "BUY",
                "price": "0.5",
                "size": "20",
                "timestamp": (NOW - timedelta(minutes=30)).isoformat(),
                "outcome": "Yes",
            }
            for index in range(12)
        ]

    async def markets(self, *, condition_id: str, **_kwargs):
        return [
            {
                "condition_id": condition_id,
                "closed": False,
                "start_date": (NOW - timedelta(days=1)).isoformat(),
                "end_date": (NOW + timedelta(days=3)).isoformat(),
                "volume_total": "100000",
                "side_a_token_id": f"yes-{condition_id}",
                "side_b_token_id": f"no-{condition_id}",
            }
        ]

    async def market_360(self, *, condition_id: str, **_kwargs):
        toxic = condition_id in self.toxic_conditions
        return [
            {
                "condition_id": condition_id,
                "liquidity_percentile": "85",
                "current_volume_24h": "100000",
                "top1_wallet_pct": "15",
                "top10_wallet_pct": "45",
                "market_active": True,
                "market_closed": False,
                "volume_trend": "Normal",
                "whale_control_flag": toxic,
                "trade_concentration_flag": False,
            }
        ]


@pytest.mark.asyncio
async def test_one_toxic_market_does_not_discard_otherwise_copyable_wallet() -> None:
    service = HeisenbergCandidateService(
        MixedQualityHeisenberg({"condition-0"}),
        policy=HeisenbergDiscoveryPolicy(max_toxic_market_share=Decimal("0.25")),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert result.best is not None
    assert result.best.wallet == GOOD
    assert result.best.evidence.toxic_market_share == Decimal("0.25")


@pytest.mark.asyncio
async def test_toxic_market_majority_fails_closed() -> None:
    service = HeisenbergCandidateService(
        MixedQualityHeisenberg({"condition-0", "condition-1"}),
        policy=HeisenbergDiscoveryPolicy(max_toxic_market_share=Decimal("0.25")),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "toxic_or_inactive_market_mix" in rejected[GOOD]


STRONG = "0x" + "b" * 40
WEAK = "0x" + "a" * 40


class CorroborationHeisenberg(FakeHeisenberg):
    async def leaderboard(self, **_kwargs):
        common = {
            "h_score": 82,
            "roi_15d": 0.15,
            "pnl_15d": 5000,
            "win_rate_15d": 0.60,
            "total_trades_15d": 100,
            "trajectory": "stable",
        }
        return [{"wallet": WEAK, **common}, {"wallet": STRONG, **common}]

    async def pnl_leaderboard(self, **_kwargs):
        return [
            {"address": STRONG, "rank": 1},
            {"address": WEAK, "rank": 50},
        ]

    async def wallet_360(self, wallet: str, window_days: int, **_kwargs):
        rows = wallet360(GOOD, window_days)
        rows[0]["proxy_wallet"] = wallet
        rows[0]["avg_position_size"] = 10
        return rows

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        rows = recent_trades(GOOD)
        for row in rows:
            row["proxy_wallet"] = proxy_wallet
            row["id"] = f"{proxy_wallet}-{row['id']}"
        return rows

    async def wallet_stats_all_time(self, wallet: str, **_kwargs):
        return [
            {
                "proxy_wallet": wallet,
                "total_trades": 1500 if wallet == STRONG else 100,
                "avg_trade_size": "10",
                "roi_pct": "12" if wallet == STRONG else "-5",
                "total_pnl": "12000" if wallet == STRONG else "-500",
            }
        ]

    async def pnl(self, *, wallet: str, **_kwargs):
        return [
            {
                "proxy_wallet": wallet,
                "pnl": "500" if wallet == STRONG else "-500",
                "invested": "2000",
                "trades": 30,
                "wins": 20 if wallet == STRONG else 10,
                "losses": 10 if wallet == STRONG else 20,
                "win_rate": "0.666" if wallet == STRONG else "0.333",
            }
        ]


@pytest.mark.asyncio
async def test_official_rank_and_realized_pnl_corroborate_but_do_not_replace_h_score() -> None:
    service = HeisenbergCandidateService(CorroborationHeisenberg(), now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    assert result.best is not None
    assert result.best.wallet == STRONG
    assert result.best.components["official_rank_corroboration"] > Decimal("0")
    assert result.best.components["realized_pnl_corroboration"] > Decimal("0.5")
    assert result.best.components["all_time_confidence"] > Decimal("0")


CACHE_A = "0x" + "5" * 40
CACHE_B = "0x" + "6" * 40


class SharedMarketFailureHeisenberg(FakeHeisenberg):
    async def leaderboard(self, **_kwargs):
        common = {
            "h_score": 80,
            "roi_15d": 0.1,
            "pnl_15d": 1000,
            "win_rate_15d": 0.6,
            "total_trades_15d": 100,
            "trajectory": "stable",
        }
        return [{"wallet": CACHE_A, **common}, {"wallet": CACHE_B, **common}]

    async def wallet_360(self, wallet: str, window_days: int, **_kwargs):
        rows = wallet360(GOOD, window_days)
        rows[0]["proxy_wallet"] = wallet
        rows[0]["avg_position_size"] = 10
        return rows

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        rows = recent_trades(GOOD)
        for row in rows:
            row["proxy_wallet"] = proxy_wallet
            row["condition_id"] = "shared-condition"
            row["id"] = f"{proxy_wallet}-{row['id']}"
        return rows

    async def market_360(self, *, condition_id: str, **_kwargs):
        raise RuntimeError("market quality unavailable")


@pytest.mark.asyncio
async def test_cached_market_failure_is_attributed_to_every_affected_candidate() -> None:
    service = HeisenbergCandidateService(
        SharedMarketFailureHeisenberg(),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item for item in result.rejected}
    assert CACHE_A in rejected and CACHE_B in rejected
    for wallet in (CACHE_A, CACHE_B):
        assert "market_liquidity_below_threshold" in rejected[wallet].reasons
        assert "market360:RuntimeError" in rejected[wallet].metrics["source_errors"]


class StaleWallet360Heisenberg(FakeHeisenberg):
    async def leaderboard(self, **_kwargs):
        return [
            {
                "wallet": GOOD,
                "h_score": 82,
                "roi_15d": 0.20,
                "pnl_15d": 5000,
                "win_rate_15d": 0.62,
                "total_trades_15d": 100,
                "trajectory": "rising",
            }
        ]

    async def wallet_360(self, wallet: str, window_days: int, **_kwargs):
        rows = wallet360(wallet, window_days)
        rows[0]["date_range_end"] = (NOW - timedelta(days=5)).isoformat()
        return rows


@pytest.mark.asyncio
async def test_stale_wallet360_snapshots_fail_closed_even_when_trades_are_fresh() -> None:
    service = HeisenbergCandidateService(
        StaleWallet360Heisenberg(),
        policy=HeisenbergDiscoveryPolicy(max_wallet360_snapshot_age_hours=72),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "stale_wallet360_evidence" in rejected[GOOD]
