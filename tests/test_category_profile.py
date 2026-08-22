"""Tests for hierarchical Wallet360 category share analysis."""

from __future__ import annotations

from decimal import Decimal

from integrations.falcon.models import Wallet360Snapshot
from services.discovery.category_profile import CategoryProfile


def profile(rows: list[dict[str, object]], total: int) -> CategoryProfile:
    return CategoryProfile.from_performance_by_category(rows, total_trades=total)


def test_top_level_is_text_before_slash() -> None:
    result = profile(
        [
            {"category": "Sports / Soccer", "trades": 30},
            {"category": "Crypto / Prices", "trades": 20},
        ],
        50,
    )

    assert result.top_level_categories() == ("Sports", "Crypto")


def test_parent_row_wins_over_duplicated_child_rows() -> None:
    result = profile(
        [
            {"category": "Crypto", "trades": 80},
            {"category": "Crypto / Prices", "trades": 80},
            {"category": "Crypto / Updown", "trades": 80},
        ],
        100,
    )

    assert result.top_level_share("Crypto") == Decimal("0.8")


def test_children_are_summed_when_parent_row_missing() -> None:
    result = profile(
        [
            {"category": "Sports / Soccer", "trades": 30},
            {"category": "Sports / Tennis", "trades": 10},
        ],
        100,
    )

    assert result.top_level_share("Sports") == Decimal("0.4")


def test_top_level_share_is_exact_decimal_and_case_insensitive() -> None:
    result = profile([{"category": "Crypto", "trades": 1}], 3)

    assert result.top_level_share("crypto") == Decimal(1) / Decimal(3)
    assert result.top_level_share("  CRYPTO  ") == Decimal(1) / Decimal(3)
    assert result.top_level_share("Sports") == Decimal("0")


def test_share_clamped_when_categories_overlap() -> None:
    result = profile(
        [
            {"category": "Sports", "trades": 536},
            {"category": "Esports", "trades": 375},
            {"category": "Other", "trades": 65},
            {"category": "Crypto", "trades": 60},
        ],
        400,
    )

    assert result.top_level_share("Sports") == Decimal("1")
    assert result.share_of(("Sports", "Esports")) == Decimal("1")


def test_empty_or_unparseable_rows_yield_empty_profile() -> None:
    assert profile([], 100).is_empty is True
    assert profile([{"category": "", "trades": 5}], 100).is_empty is True
    assert profile([{"category": "Crypto", "trades": "n/a"}], 100).is_empty is True
    assert profile([{"category": "Crypto", "trades": 5}], 0).is_empty is True
    assert profile([{"category": "Crypto", "trades": 5}], 0).top_level_share("Crypto") == Decimal(
        "0"
    )


def test_share_for_any_of_multiple_excluded_categories() -> None:
    result = profile(
        [
            {"category": "Crypto", "trades": 20},
            {"category": "Politics", "trades": 30},
            {"category": "Sports", "trades": 50},
        ],
        100,
    )

    assert result.share_of(("crypto", "politics")) == Decimal("0.5")
    assert result.share_of(()) == Decimal("0")
    assert result.share_of(("crypto", "Crypto")) == Decimal("0.2")


def test_profile_from_snapshot_uses_snapshot_total_trades() -> None:
    snapshot = Wallet360Snapshot.from_mapping(
        {
            "proxy_wallet": "0xabc",
            "calculation_window_days": 15,
            "date_range_end": "2026-07-30T00:00:00Z",
            "total_trades": 200,
            "performance_by_category": [
                {"category": "Crypto / Prices", "trades": 50},
                {"category": "Sports", "trades": 150},
            ],
        }
    )

    result = CategoryProfile.from_snapshot(snapshot)

    assert result.total_trades == 200
    assert result.top_level_share("Crypto") == Decimal("0.25")
    assert CategoryProfile.from_snapshot(None).is_empty is True


def test_dominant_share_and_category() -> None:
    result = profile(
        [
            {"category": "Sports", "trades": 90},
            {"category": "Sports / Soccer", "trades": 60},
            {"category": "Other", "trades": 10},
        ],
        100,
    )

    assert result.dominant_category() == "Sports"
    assert result.dominant_share() == Decimal("0.9")


def test_dominant_share_is_zero_for_empty_profile() -> None:
    empty = CategoryProfile.empty()

    assert empty.dominant_category() is None
    assert empty.dominant_share() == Decimal("0")


def test_dominant_share_is_clamped_to_one() -> None:
    overlapping = profile([{"category": "Sports", "trades": 150}], 100)

    assert overlapping.dominant_share() == Decimal("1")


def test_specialty_buckets_use_sub_categories_and_fold_deeper_rows() -> None:
    result = profile(
        [
            {"category": "Sports", "trades": 100},
            {"category": "Esports", "trades": 70},
            {"category": "Esports / MOBA", "trades": 70},
            {"category": "Esports / MOBA / League of Legends", "trades": 65},
            {"category": "Sports / Tennis", "trades": 30},
            {"category": "Other", "trades": 4},
        ],
        100,
    )

    assert result.trades_by_specialty == (
        ("Esports / MOBA", 70),
        ("Sports / Tennis", 30),
        ("Other", 4),
    )
    assert result.dominant_specialty() == "Esports / MOBA"
    assert result.dominant_specialty_share() == Decimal("0.7")


def test_specialty_share_sums_deeper_rows_when_sub_category_row_missing() -> None:
    result = profile(
        [
            {"category": "Sports / Soccer / La Liga", "trades": 30},
            {"category": "Sports / Soccer / Serie A", "trades": 50},
        ],
        100,
    )

    assert result.trades_by_specialty == (("Sports / Soccer", 80),)
    assert result.dominant_specialty_share() == Decimal("0.8")


def test_specialty_share_is_zero_and_none_for_empty_profile() -> None:
    empty = CategoryProfile.empty()

    assert empty.dominant_specialty() is None
    assert empty.dominant_specialty_share() == Decimal("0")
