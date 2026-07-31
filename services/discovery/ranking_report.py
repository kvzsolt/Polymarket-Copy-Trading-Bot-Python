"""Deterministic operator report for bankroll-aware wallet discovery."""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from typing import Any

from services.discovery.discovery_models import DiscoveryResult, RankedTrader


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _candidate_row(rank: int, candidate: RankedTrader) -> dict[str, Any]:
    fit = candidate.bankroll_fit
    evidence = candidate.evidence
    return {
        "rank": rank,
        "wallet": candidate.wallet,
        "source": candidate.source,
        "score": _decimal_text(candidate.score),
        "h_score": _decimal_text(candidate.h_score),
        "recommended_copy_ratio": _decimal_text(fit.recommended_copy_ratio),
        "recommended_stake_usdc": _decimal_text(fit.recommended_stake_usdc),
        "target_trade_budget_usdc": _decimal_text(fit.target_trade_budget_usdc),
        "max_trade_budget_usdc": _decimal_text(fit.max_trade_budget_usdc),
        "typical_trade_usdc": _decimal_text(fit.typical_trade_usdc),
        "avg_position_size_usdc": _decimal_text(fit.avg_position_size_usdc),
        "trade_scale_ratio": _decimal_text(fit.trade_scale_ratio),
        "position_scale_ratio": _decimal_text(fit.position_scale_ratio),
        "copyable_trade_share": _decimal_text(fit.copyable_trade_share),
        "bankroll_fit_score": _decimal_text(fit.fit_score),
        "bankroll_reasons": list(fit.reasons),
        "last_trade_at": (
            evidence.last_trade_at.isoformat() if evidence.last_trade_at is not None else None
        ),
        "recent_trade_count": evidence.recent_trade_count,
        "today_trade_count": evidence.today_trade_count,
        "short_trade_share": _decimal_text(evidence.short_trade_share),
        "median_liquidity_percentile": _decimal_text(
            evidence.median_liquidity_percentile
        ),
        "toxic_market_share": _decimal_text(evidence.toxic_market_share),
        "missing_field_ratio": _decimal_text(evidence.missing_field_ratio),
        "official_leaderboard_rank": evidence.official_leaderboard_rank,
        "recent_price_jump_count": evidence.recent_price_jump_count,
        "recent_realized_pnl": candidate.metrics.get("recent_realized_pnl"),
        "components": {
            key: _decimal_text(value)
            for key, value in sorted(candidate.components.items())
        },
    }


def build_ranking_report(
    result: DiscoveryResult,
    *,
    funds: Decimal,
    top: int = 10,
) -> dict[str, Any]:
    if top < 1:
        raise ValueError("top must be positive")
    ranked = [
        _candidate_row(index, candidate)
        for index, candidate in enumerate(result.ranked[:top], start=1)
    ]
    rejection_counts = Counter(
        reason for rejected in result.rejected for reason in rejected.reasons
    )
    return {
        "source": result.source,
        "generated_at": (
            result.generated_at.isoformat() if result.generated_at is not None else None
        ),
        "funds_usdc": _decimal_text(Decimal(str(funds))),
        "best_wallet": ranked[0]["wallet"] if ranked else None,
        "ranked": ranked,
        "rejected_count": len(result.rejected),
        "rejection_reasons": dict(sorted(rejection_counts.items())),
        "warnings": list(result.warnings),
    }


def render_ranking_report(report: dict[str, Any], *, as_json: bool) -> str:
    if as_json:
        return json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False)
    lines = [
        f"Discovery source: {report.get('source')}",
        f"Funds: {report.get('funds_usdc')} USDC",
    ]
    ranked = report.get("ranked") or []
    if not ranked:
        lines.append("No wallet passed every fail-closed gate.")
    for row in ranked:
        lines.extend(
            [
                "",
                f"#{row['rank']} {row['wallet']} — score {row['score']} (H {row['h_score']})",
                f"Recommended stake: {row['recommended_stake_usdc']} USDC",
                f"Copy ratio: {row['recommended_copy_ratio']}",
                (
                    "Copyable share: "
                    f"{row['copyable_trade_share']} | liquidity pctl "
                    f"{row['median_liquidity_percentile']} | toxic share "
                    f"{row['toxic_market_share']}"
                ),
            ]
        )
    if report.get("rejection_reasons"):
        lines.extend(["", f"Rejections: {report['rejection_reasons']}"])
    return "\n".join(lines) + "\n"


__all__ = ["build_ranking_report", "render_ranking_report"]
