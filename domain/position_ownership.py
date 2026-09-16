"""Conservative leader attribution for aggregate legacy account positions."""

from decimal import Decimal


class PositionOwnership:
    """Keep mixed or imported inventory distinct from a known single leader."""

    @staticmethod
    def matches(owner: str | None, leader: str | None) -> bool:
        """Return true only for two explicit matching leader identities."""
        return bool(
            owner
            and owner.strip()
            and leader
            and leader.strip()
            and owner.strip().casefold() == leader.strip().casefold()
        )

    @classmethod
    def after_buy(
        cls, owner: str | None, leader: str | None, previous_shares: Decimal
    ) -> str | None:
        """Attribute a new position, preserving unknown ownership after a mixed BUY."""
        cls._validate_shares(previous_shares)
        if previous_shares == 0:
            return leader if leader and leader.strip() else None
        return owner if cls.matches(owner, leader) else None

    @classmethod
    def after_snapshot(
        cls, owner: str | None, previous_shares: Decimal, snapshot_shares: Decimal
    ) -> str | None:
        """Preserve a known owner only when a snapshot adds no unexplained shares."""
        cls._validate_shares(previous_shares)
        cls._validate_shares(snapshot_shares)
        return owner if snapshot_shares <= previous_shares else None

    @staticmethod
    def _validate_shares(shares: Decimal) -> None:
        if not shares.is_finite() or shares < 0:
            raise ValueError("Position shares must be finite and non-negative")
