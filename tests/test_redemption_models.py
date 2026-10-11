"""Resolved claims use exact on-chain units, including zero-value losing shares."""

from decimal import Decimal

import pytest

from domain.redemption import RedemptionMarket, RedemptionSnapshot


class TestRedemptionSnapshot:
    """Payout arithmetic follows integer collateral redemption semantics."""

    @staticmethod
    def market() -> RedemptionMarket:
        """Return an independently identified binary market."""
        return RedemptionMarket("0x" + "ab" * 32, ("123", "456"), False, "Example")

    def test_winner_and_loser_are_one_redeemable_condition(self) -> None:
        """Both full balances are redeemed although only one pays out."""
        snapshot = RedemptionSnapshot(self.market(), (2_000_001, 9_000_000), (1, 0), 1, True)
        assert snapshot.eligible
        assert snapshot.shares == (Decimal("2.000001"), Decimal("9"))
        assert snapshot.expected_payout == Decimal("2.000001")

    def test_losing_only_and_dust_remain_eligible(self) -> None:
        """Zero cash value must never filter out held resolved tokens."""
        snapshot = RedemptionSnapshot(self.market(), (0, 1), (1, 0), 1, False)
        assert snapshot.eligible
        assert snapshot.expected_payout == 0

    def test_large_units_preserve_every_digit_without_decimal_context_rounding(self) -> None:
        """All uint256 units retain their micro-unit precision in reports."""
        units = 123456789012345678901234567890123456789
        snapshot = RedemptionSnapshot(self.market(), (units, 0), (1, 0), 1, False)
        expected = Decimal("123456789012345678901234567890123.456789")
        assert snapshot.shares[0] == expected
        assert snapshot.expected_payout == expected

    def test_fractional_resolution_rounds_each_payout_down_in_base_units(self) -> None:
        """Void or fractional outcomes obey CTF per-outcome integer division."""
        snapshot = RedemptionSnapshot(self.market(), (3, 5), (1, 1), 2, False)
        assert snapshot.expected_payout == Decimal("0.000003")

    @pytest.mark.parametrize("balances,denominator", [((5, 0), 0), ((0, 0), 1)])
    def test_unresolved_and_empty_are_not_eligible(
        self, balances: tuple[int, int], denominator: int
    ) -> None:
        """Only resolved and nonempty positions can authorize redemption."""
        payouts = (0, 0) if denominator == 0 else (1, 0)
        assert not RedemptionSnapshot(self.market(), balances, payouts, denominator, False).eligible

    @pytest.mark.parametrize(
        "balances,payouts,denominator",
        [
            ((-1, 0), (1, 0), 1),
            ((True, 0), (1, 0), 1),
            ((1, 0), (1, 1), 1),
            ((1, 0), (1, 0), 0),
            ((1.5, 0), (1, 0), 1),
        ],
    )
    def test_invalid_chain_arithmetic_fails_closed(self, balances, payouts, denominator) -> None:
        """Malformed or inconsistent evidence cannot authorize a money operation."""
        with pytest.raises(ValueError):
            RedemptionSnapshot(self.market(), balances, payouts, denominator, False)

    @pytest.mark.parametrize(
        "condition,tokens,negative_risk",
        [
            ("bad", ("123", "456"), False),
            ("0x" + "ab" * 32, ("123", "123"), False),
            ("0x" + "ab" * 32, ("123", "456"), "false"),
        ],
    )
    def test_invalid_market_identity_fails_closed(self, condition, tokens, negative_risk) -> None:
        """Neither invented symbols nor coerced risk flags are allowed."""
        with pytest.raises(ValueError):
            RedemptionMarket(condition, tokens, negative_risk)
