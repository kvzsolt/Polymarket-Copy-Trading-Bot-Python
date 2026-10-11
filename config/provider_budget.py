"""Restart-required local provider budget configuration without endpoint credentials."""

from dataclasses import dataclass, field

from domain.provider_budget import BudgetSettings, ProviderConfigurationError


@dataclass(frozen=True, slots=True)
class ProviderBudgetConfig:
    """Keep shared durable history outside ephemeral process state."""

    ledger_path: str = "data/provider-budget.sqlite3"
    limits: BudgetSettings = field(default_factory=BudgetSettings)

    def __post_init__(self) -> None:
        """Reject absent and in-memory ledgers that would reset usage on restart."""
        if not self.ledger_path or self.ledger_path == ":memory:" or "\x00" in self.ledger_path:
            raise ProviderConfigurationError("a durable provider ledger path is required")
