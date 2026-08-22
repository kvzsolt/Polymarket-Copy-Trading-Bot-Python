"""Deterministic tests for Falcon candidate evidence and ranking."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from integrations.falcon.errors import FalconUnavailableError
from services.discovery.falcon_candidate_service import (
    FalconCandidateService,
    FalconDiscoveryPolicy,
)

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
GOOD = "0x" + "1" * 40
WHALE = "0x" + "2" * 40
SYBIL = "0x" + "3" * 40
STALE = "0x" + "4" * 40
INVALID = "not-an-evm-wallet"


def wallet360(
    wallet: str,
    window: int,
    *,
    sybil: bool = False,
    concentration: float = 0.2,
    at: datetime = NOW,
) -> list[dict[str, Any]]:
    return [
        {
            "proxy_wallet": wallet,
            "calculation_window_days": window,
            "date_range_end": at.isoformat(),
            "last_active": at.isoformat(),
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


def recent_trades(
    wallet: str,
    *,
    short: bool = True,
    stale: bool = False,
    at: datetime = NOW,
    age: timedelta | None = None,
) -> list[dict[str, Any]]:
    if age is None:
        age = timedelta(hours=40) if stale else timedelta(minutes=30)
    timestamp = at - age
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


class FakeFalcon:
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
            raise FalconUnavailableError("vendor unavailable")
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
    service = FalconCandidateService(
        FakeFalcon(),
        policy=FalconDiscoveryPolicy(candidate_limit=10),
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
    service = FalconCandidateService(FakeFalcon(), now=lambda: NOW)

    small = await service.discover(Decimal("50"))
    large = await service.discover(Decimal("5000"))

    assert small.best is not None
    assert large.best is not None
    assert small.best.wallet == GOOD
    assert large.best.wallet == WHALE
    assert small.best.bankroll_fit.recommended_stake_usdc <= Decimal("50")


@pytest.mark.asyncio
async def test_partial_wallet360_failure_never_silently_promotes_candidate() -> None:
    client = FakeFalcon()
    client.unavailable_wallet = GOOD
    service = FalconCandidateService(client, now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "insufficient_wallet360_evidence" in rejected[GOOD]


class PriceJumpFalcon(FakeFalcon):
    async def price_jumps(self, *, token_id: str, **_kwargs):
        return [{"token_id": token_id, "change_pct": "15"}] * 5


@pytest.mark.asyncio
async def test_excessive_recent_price_jumps_are_a_fail_closed_market_risk() -> None:
    service = FalconCandidateService(
        PriceJumpFalcon(),
        policy=FalconDiscoveryPolicy(max_recent_price_jumps=2),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("5000"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert any("excessive_recent_price_jumps" in reasons for reasons in rejected.values())


class MixedQualityFalcon(FakeFalcon):
    def __init__(
        self,
        toxic_conditions: set[str],
        *,
        flag: str = "whale_control_flag",
    ) -> None:
        super().__init__()
        self.toxic_conditions = toxic_conditions
        self.flag = flag

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
        row = {
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
        row[self.flag] = condition_id in self.toxic_conditions
        return [row]


@pytest.mark.asyncio
async def test_one_toxic_market_does_not_discard_otherwise_copyable_wallet() -> None:
    service = FalconCandidateService(
        MixedQualityFalcon({"condition-0"}),
        policy=FalconDiscoveryPolicy(max_toxic_market_share=Decimal("0.25")),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert result.best is not None
    assert result.best.wallet == GOOD
    assert result.best.evidence.toxic_market_share == Decimal("0.25")


@pytest.mark.asyncio
async def test_toxic_market_majority_fails_closed() -> None:
    service = FalconCandidateService(
        MixedQualityFalcon({"condition-0", "condition-1"}),
        policy=FalconDiscoveryPolicy(max_toxic_market_share=Decimal("0.25")),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "toxic_or_inactive_market_mix" in rejected[GOOD]


STRONG = "0x" + "b" * 40
WEAK = "0x" + "a" * 40


class CorroborationFalcon(FakeFalcon):
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
    service = FalconCandidateService(CorroborationFalcon(), now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    assert result.best is not None
    assert result.best.wallet == STRONG
    assert result.best.components["official_rank_corroboration"] > Decimal("0")
    assert result.best.components["realized_pnl_corroboration"] > Decimal("0.5")
    assert result.best.components["all_time_confidence"] > Decimal("0")


CACHE_A = "0x" + "5" * 40
CACHE_B = "0x" + "6" * 40


class SharedMarketFailureFalcon(FakeFalcon):
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
    service = FalconCandidateService(
        SharedMarketFailureFalcon(),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item for item in result.rejected}
    assert CACHE_A in rejected and CACHE_B in rejected
    for wallet in (CACHE_A, CACHE_B):
        assert "market_liquidity_below_threshold" in rejected[wallet].reasons
        assert "market360:RuntimeError" in rejected[wallet].metrics["source_errors"]


class StaleWallet360Falcon(FakeFalcon):
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
    service = FalconCandidateService(
        StaleWallet360Falcon(),
        policy=FalconDiscoveryPolicy(max_wallet360_snapshot_age_hours=72),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "stale_wallet360_evidence" in rejected[GOOD]


class OneWalletFalcon(FakeFalcon):
    """Single-wallet Falcon fake with per-window Wallet360 overrides."""

    def __init__(
        self,
        *,
        overrides: dict[int | None, dict[str, Any]] | None = None,
        at: datetime = NOW,
        trade_age: timedelta | None = None,
    ) -> None:
        super().__init__()
        self.overrides = dict(overrides or {})
        self.at = at
        self.trade_age = trade_age

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
        rows = wallet360(wallet, window_days, at=self.at)
        rows[0].update(self.overrides.get(None, {}))
        rows[0].update(self.overrides.get(window_days, {}))
        return rows

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        return recent_trades(proxy_wallet, at=self.at, age=self.trade_age)


@pytest.mark.asyncio
async def test_activity_window_is_rolling_not_utc_date() -> None:
    just_after_midnight = datetime(2026, 7, 30, 0, 30, tzinfo=timezone.utc)
    service = FalconCandidateService(
        OneWalletFalcon(at=just_after_midnight, trade_age=timedelta(minutes=40)),
        now=lambda: just_after_midnight,
    )

    result = await service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in result.ranked] == [GOOD]


@pytest.mark.asyncio
async def test_trades_older_than_activity_window_but_within_last_trade_age() -> None:
    client = OneWalletFalcon(trade_age=timedelta(hours=30))
    rejecting = FalconCandidateService(client, now=lambda: NOW)
    accepting = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(min_activity_window_trades=0),
        now=lambda: NOW,
    )

    rejected = await rejecting.discover(Decimal("500"))
    accepted = await accepting.discover(Decimal("500"))

    assert "inactive_or_stale" in {item.wallet: item.reasons for item in rejected.rejected}[GOOD]
    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]
    assert accepted.ranked[0].evidence.activity_window_trade_count == 0


@pytest.mark.asyncio
async def test_toxic_flags_are_configurable() -> None:
    client = MixedQualityFalcon(
        {"condition-0", "condition-1", "condition-2"},
        flag="trade_concentration_flag",
    )
    default_service = FalconCandidateService(client, now=lambda: NOW)
    strict_service = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(toxic_market_flags=("trade_concentration",)),
        now=lambda: NOW,
    )

    accepted = await default_service.discover(Decimal("500"))
    rejected = await strict_service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]
    assert accepted.ranked[0].evidence.toxic_market_share == Decimal("0")
    reasons = {item.wallet: item.reasons for item in rejected.rejected}
    assert "toxic_or_inactive_market_mix" in reasons[GOOD]


class ClosedAndUnknownMarketFalcon(OneWalletFalcon):
    """Serve closed short-horizon markets plus one market without a snapshot."""

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
        if condition_id == "condition-3":
            return []
        return [
            {
                "condition_id": condition_id,
                "closed": True,
                "start_date": (NOW - timedelta(days=1)).isoformat(),
                "end_date": (NOW + timedelta(days=3)).isoformat(),
                "volume_total": "100000",
                "side_a_token_id": f"yes-{condition_id}",
                "side_b_token_id": f"no-{condition_id}",
            }
        ]


@pytest.mark.asyncio
async def test_short_duration_share_counts_closed_markets_and_ignores_unknown_markets() -> None:
    service = FalconCandidateService(
        ClosedAndUnknownMarketFalcon(),
        policy=FalconDiscoveryPolicy(
            min_short_trade_share=Decimal("1"),
            min_short_trade_count=9,
        ),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in result.ranked] == [GOOD]
    assert result.ranked[0].evidence.short_trade_count == 9
    assert result.ranked[0].evidence.short_trade_share == Decimal("1")


@pytest.mark.asyncio
async def test_perfect_timing_flag_is_hard_rejected() -> None:
    client = OneWalletFalcon(overrides={None: {"perfect_timing_flag": True}})
    strict = FalconCandidateService(client, now=lambda: NOW)
    permissive = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(reject_perfect_timing_flag=False),
        now=lambda: NOW,
    )

    rejected = await strict.discover(Decimal("500"))
    accepted = await permissive.discover(Decimal("500"))

    assert "perfect_timing" in {item.wallet: item.reasons for item in rejected.rejected}[GOOD]
    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]


@pytest.mark.asyncio
async def test_suspicious_win_rate_flag_is_hard_rejected() -> None:
    client = OneWalletFalcon(overrides={None: {"suspicious_win_rate_flag": True}})
    strict = FalconCandidateService(client, now=lambda: NOW)
    permissive = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(reject_suspicious_win_rate_flag=False),
        now=lambda: NOW,
    )

    rejected = await strict.discover(Decimal("500"))
    accepted = await permissive.discover(Decimal("500"))

    assert "suspicious_win_rate" in {item.wallet: item.reasons for item in rejected.rejected}[GOOD]
    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]


@pytest.mark.asyncio
async def test_hyperactive_trader_is_rejected() -> None:
    service = FalconCandidateService(
        OneWalletFalcon(overrides={15: {"total_trades": 6000}}),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item for item in result.rejected}
    assert "hyperactive_trader" in rejected[GOOD].reasons
    assert rejected[GOOD].metrics["trades_per_day"] == "400"


@pytest.mark.asyncio
async def test_hyperactive_trader_uses_days_active_when_present() -> None:
    service = FalconCandidateService(
        OneWalletFalcon(overrides={15: {"total_trades": 6000, "days_active": 30}}),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in result.ranked] == [GOOD]
    assert result.ranked[0].metrics["trades_per_day"] == "200"


@pytest.mark.asyncio
async def test_micro_trade_bot_is_rejected() -> None:
    bot = FalconCandidateService(
        OneWalletFalcon(overrides={None: {"avg_trade_size": "2"}}),
        now=lambda: NOW,
    )
    human = FalconCandidateService(
        OneWalletFalcon(overrides={None: {"avg_trade_size": "5"}}),
        now=lambda: NOW,
    )

    rejected = await bot.discover(Decimal("500"))
    accepted = await human.discover(Decimal("500"))

    assert "micro_trade_bot" in {item.wallet: item.reasons for item in rejected.rejected}[GOOD]
    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]


@pytest.mark.asyncio
async def test_similar_wallets_above_max_is_sybil_risk() -> None:
    service = FalconCandidateService(
        OneWalletFalcon(overrides={None: {"similar_wallets_count": 3}}),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item for item in result.rejected}
    assert "sybil_risk" in rejected[GOOD].reasons
    assert rejected[GOOD].metrics["similar_wallets_count"] == 3


CRYPTO_CATEGORIES = [
    {"category": "Crypto", "trades": 80},
    {"category": "Crypto / Prices", "trades": 80},
    {"category": "Sports", "trades": 20},
]


@pytest.mark.asyncio
async def test_crypto_dominant_wallet_is_rejected() -> None:
    service = FalconCandidateService(
        OneWalletFalcon(overrides={None: {"performance_by_category": CRYPTO_CATEGORIES}}),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    rejected = {item.wallet: item for item in result.rejected}
    assert "excluded_category_dominant" in rejected[GOOD].reasons
    assert rejected[GOOD].metrics["excluded_category_share"] == "0.8"


@pytest.mark.asyncio
async def test_category_gate_falls_back_to_7d_and_skips_when_absent() -> None:
    fallback = FalconCandidateService(
        OneWalletFalcon(overrides={7: {"performance_by_category": CRYPTO_CATEGORIES}}),
        now=lambda: NOW,
    )
    absent = FalconCandidateService(OneWalletFalcon(), now=lambda: NOW)

    rejected = await fallback.discover(Decimal("500"))
    accepted = await absent.discover(Decimal("500"))

    reasons = {item.wallet: item.reasons for item in rejected.rejected}
    assert "excluded_category_dominant" in reasons[GOOD]
    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]
    assert accepted.ranked[0].metrics["excluded_category_share"] is None


@pytest.mark.asyncio
async def test_drawdown_gate_disabled_by_default() -> None:
    client = OneWalletFalcon(overrides={None: {"max_drawdown": 0.9}})
    default_service = FalconCandidateService(client, now=lambda: NOW)
    gated_service = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(drawdown_gate_enabled=True),
        now=lambda: NOW,
    )

    accepted = await default_service.discover(Decimal("500"))
    rejected = await gated_service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in accepted.ranked] == [GOOD]
    reasons = {item.wallet: item.reasons for item in rejected.rejected}
    assert "drawdown_above_threshold" in reasons[GOOD]


@pytest.mark.asyncio
async def test_evidence_reports_activity_window_trade_count() -> None:
    service = FalconCandidateService(OneWalletFalcon(), now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    assert result.ranked[0].evidence.activity_window_trade_count == 12
    assert "recent_activity" in result.ranked[0].components
    assert "today_activity" not in result.ranked[0].components


SPECIALIST_CATEGORIES = [
    {"category": "Sports", "trades": 90},
    {"category": "Sports / Soccer", "trades": 85},
    {"category": "Sports / Soccer / La Liga", "trades": 40},
    {"category": "Other", "trades": 8},
]

UMBRELLA_GENERALIST_CATEGORIES = [
    {"category": "Sports", "trades": 100},
    {"category": "Esports", "trades": 70},
    {"category": "Esports / MOBA", "trades": 60},
    {"category": "Esports / Counter-Strike", "trades": 10},
    {"category": "Sports / Tennis", "trades": 30},
]


def specialist_client(**overrides: Any) -> OneWalletFalcon:
    row: dict[str, Any] = {"performance_by_category": SPECIALIST_CATEGORIES}
    row.update(overrides)
    return OneWalletFalcon(overrides={None: row})


@pytest.mark.asyncio
async def test_specialist_bonus_adds_weight_without_gating() -> None:
    specialist = FalconCandidateService(
        specialist_client(win_rate_last_30day="0.61"),
        now=lambda: NOW,
    )
    generalist = FalconCandidateService(
        OneWalletFalcon(
            overrides={
                None: {
                    "performance_by_category": [
                        {"category": "Sports", "trades": 50},
                        {"category": "Politics", "trades": 50},
                    ],
                    "win_rate_last_30day": "0.61",
                }
            }
        ),
        now=lambda: NOW,
    )

    focused = await specialist.discover(Decimal("500"))
    broad = await generalist.discover(Decimal("500"))

    assert focused.ranked[0].components["specialist"] == Decimal("1")
    assert broad.ranked[0].components["specialist"] == Decimal("0")
    assert focused.ranked[0].score > broad.ranked[0].score
    assert focused.ranked[0].metrics["specialist_dominant_category"] == "Sports / Soccer"
    assert focused.ranked[0].metrics["specialist_dominant_share"] == "0.85"
    assert focused.ranked[0].metrics["specialist_win_rate"] == "0.61"
    assert "specialist_categories" not in focused.ranked[0].metrics


@pytest.mark.asyncio
async def test_specialist_bonus_measures_dominance_below_umbrella_top_level() -> None:
    """A Sports-umbrella generalist spread over sub-categories earns no bonus."""
    generalist = FalconCandidateService(
        OneWalletFalcon(
            overrides={
                None: {
                    "performance_by_category": UMBRELLA_GENERALIST_CATEGORIES,
                    "win_rate_last_30day": "0.61",
                }
            }
        ),
        now=lambda: NOW,
    )

    result = await generalist.discover(Decimal("500"))

    assert result.ranked[0].components["specialist"] == Decimal("0")
    assert result.ranked[0].metrics["specialist_dominant_category"] == "Esports / MOBA"
    assert result.ranked[0].metrics["specialist_dominant_share"] == "0.6"


@pytest.mark.asyncio
async def test_specialist_bonus_uses_15d_win_rate_when_30d_missing() -> None:
    service = FalconCandidateService(specialist_client(win_rate=0.62), now=lambda: NOW)

    result = await service.discover(Decimal("500"))

    assert result.ranked[0].components["specialist"] == Decimal("1")
    assert result.ranked[0].metrics["specialist_win_rate"] == "0.62"


@pytest.mark.asyncio
async def test_specialist_bonus_not_applied_below_win_rate() -> None:
    service = FalconCandidateService(
        specialist_client(win_rate=0.52, win_rate_last_30day="0.52"),
        now=lambda: NOW,
    )

    result = await service.discover(Decimal("500"))

    assert result.ranked[0].components["specialist"] == Decimal("0")


@pytest.mark.asyncio
async def test_specialist_bonus_disabled_by_policy() -> None:
    client = specialist_client(win_rate_last_30day="0.61")
    enabled = FalconCandidateService(client, now=lambda: NOW)
    disabled = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(specialist_bonus_enabled=False),
        now=lambda: NOW,
    )

    with_bonus = await enabled.discover(Decimal("500"))
    without_bonus = await disabled.discover(Decimal("500"))

    assert without_bonus.ranked[0].components["specialist"] == Decimal("0")
    assert without_bonus.ranked[0].score < with_bonus.ranked[0].score


SORT_A = "0x" + "a" * 39 + "1"
SORT_B = "0x" + "b" * 39 + "1"
SORT_C = "0x" + "c" * 39 + "1"


class MultiSortFalcon(FakeFalcon):
    """Falcon fake returning a different wallet set per leaderboard sort mode."""

    def __init__(
        self,
        rows_by_mode: dict[str, tuple[str, ...]],
        *,
        failing: tuple[str, ...] = (),
    ) -> None:
        super().__init__()
        self.rows_by_mode = rows_by_mode
        self.failing = set(failing)
        self.sort_calls: list[str] = []
        self.wallet360_calls: list[str] = []

    @staticmethod
    def _row(wallet: str) -> dict[str, Any]:
        return {
            "wallet": wallet,
            "h_score": 82,
            "roi_15d": 0.20,
            "pnl_15d": 5000,
            "win_rate_15d": 0.62,
            "total_trades_15d": 100,
            "trajectory": "rising",
        }

    async def leaderboard(self, *, sort_by: str = "h_score", **_kwargs):
        self.sort_calls.append(sort_by)
        if sort_by in self.failing:
            raise FalconUnavailableError("leaderboard outage")
        return [self._row(wallet) for wallet in self.rows_by_mode.get(sort_by, ())]

    async def wallet_360(self, wallet: str, window_days: int, **_kwargs):
        self.wallet360_calls.append(wallet)
        return wallet360(wallet, window_days)

    async def trades(self, *, proxy_wallet: str, **_kwargs):
        return recent_trades(proxy_wallet)


def _sort_mode_service(client: MultiSortFalcon, **overrides: Any) -> FalconCandidateService:
    return FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(
            leaderboard_sort_modes=("h_score", "roi"),
            **overrides,
        ),
        now=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_shortlist_unions_multiple_sort_modes_deduplicated() -> None:
    client = MultiSortFalcon({"h_score": (SORT_A, SORT_B), "roi": (SORT_B, SORT_C)})
    service = _sort_mode_service(client)

    result = await service.discover(Decimal("500"))

    assert client.sort_calls == ["h_score", "roi"]
    wallets = {candidate.wallet for candidate in result.ranked}
    assert wallets == {SORT_A, SORT_B, SORT_C}
    assert client.wallet360_calls.count(SORT_B) == 4


@pytest.mark.asyncio
async def test_shortlist_keeps_h_score_mode_first_when_truncated() -> None:
    client = MultiSortFalcon({"h_score": (SORT_A,), "roi": (SORT_C,)})
    service = _sort_mode_service(client, candidate_limit=1)

    result = await service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in result.ranked] == [SORT_A]


@pytest.mark.asyncio
async def test_shortlist_tolerates_one_failing_sort_mode() -> None:
    client = MultiSortFalcon({"h_score": (SORT_A,), "roi": (SORT_C,)}, failing=("roi",))
    service = _sort_mode_service(client)

    result = await service.discover(Decimal("500"))

    assert [candidate.wallet for candidate in result.ranked] == [SORT_A]
    assert "leaderboard:roi:FalconUnavailableError" in result.warnings


@pytest.mark.asyncio
async def test_shortlist_fails_closed_when_all_sort_modes_fail() -> None:
    client = MultiSortFalcon({}, failing=("h_score", "roi"))
    service = _sort_mode_service(client)

    with pytest.raises(FalconUnavailableError):
        await service.discover(Decimal("500"))


@pytest.mark.asyncio
async def test_shortlist_requests_at_most_the_provider_page_cap() -> None:
    class RecordingFalcon(MultiSortFalcon):
        def __init__(self) -> None:
            super().__init__({"h_score": (SORT_A,)})
            self.limits: list[int] = []

        async def leaderboard(self, *, sort_by: str = "h_score", limit: int = 50, **kwargs):
            self.limits.append(limit)
            return await super().leaderboard(sort_by=sort_by, **kwargs)

    client = RecordingFalcon()
    service = FalconCandidateService(
        client,
        policy=FalconDiscoveryPolicy(leaderboard_sort_modes=("h_score",), candidate_limit=500),
        now=lambda: NOW,
    )

    await service.discover(Decimal("500"))

    assert client.limits == [200]


@pytest.mark.asyncio
async def test_targeted_discovery_skips_the_leaderboard_and_keeps_stored_h_score() -> None:
    """A targeted re-check analyses exactly the supplied wallets."""

    class NoLeaderboardFalcon(FakeFalcon):
        async def leaderboard(self, **_kwargs):
            raise AssertionError("targeted discovery must not query the leaderboard")

    service = FalconCandidateService(
        NoLeaderboardFalcon(),
        policy=FalconDiscoveryPolicy(candidate_limit=10),
        now=lambda: NOW,
    )

    result = await service.discover(
        Decimal("500"),
        wallets=[{"wallet": GOOD, "h_score": "82"}],
    )

    assert [item.wallet for item in result.ranked] == [GOOD]
    assert result.ranked[0].h_score == Decimal("82")


@pytest.mark.asyncio
async def test_targeted_discovery_does_not_reapply_the_h_score_gate() -> None:
    """Stored rows already passed ``min_h_score`` when they were persisted."""

    class NoLeaderboardFalcon(FakeFalcon):
        async def leaderboard(self, **_kwargs):
            raise AssertionError("targeted discovery must not query the leaderboard")

    service = FalconCandidateService(
        NoLeaderboardFalcon(),
        policy=FalconDiscoveryPolicy(candidate_limit=10, min_h_score=Decimal("99")),
        now=lambda: NOW,
    )

    result = await service.discover(
        Decimal("500"),
        wallets=[{"wallet": GOOD, "h_score": "82"}],
    )

    assert [item.wallet for item in result.ranked] == [GOOD]
    assert not [item for item in result.rejected if item.wallet == GOOD]


@pytest.mark.asyncio
async def test_targeted_discovery_still_applies_every_other_gate() -> None:
    """Only the shortlist step is skipped; the evidence gates run fresh."""

    class NoLeaderboardFalcon(FakeFalcon):
        async def leaderboard(self, **_kwargs):
            raise AssertionError("targeted discovery must not query the leaderboard")

    service = FalconCandidateService(
        NoLeaderboardFalcon(),
        policy=FalconDiscoveryPolicy(candidate_limit=10),
        now=lambda: NOW,
    )

    result = await service.discover(
        Decimal("500"),
        wallets=[
            {"wallet": GOOD, "h_score": "82"},
            {"wallet": SYBIL, "h_score": "95"},
            {"wallet": INVALID, "h_score": "99"},
        ],
    )

    assert [item.wallet for item in result.ranked] == [GOOD]
    rejected = {item.wallet: item.reasons for item in result.rejected}
    assert "sybil_risk" in rejected[SYBIL]
    assert "invalid_wallet_address" in rejected[INVALID]
