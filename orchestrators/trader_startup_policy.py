"""Select manual or discovery-driven leaders for one process startup."""

from __future__ import annotations

from dataclasses import dataclass, replace

from config.app_config import AppConfig
from config.env import EnvValidationError


@dataclass(frozen=True, slots=True)
class TraderStartupPolicy:
    """Resolve the process-local leader source selected by the CLI."""

    auto_trader: bool = False

    @property
    def force_discovery(self) -> bool:
        """Return whether provider discovery must run despite scheduler config."""
        return self.auto_trader

    @property
    def startup_roster_event(self) -> str:
        """Return the structured event describing the selected startup path."""
        if self.auto_trader:
            return "auto_trader_startup_burst_started"
        return "startup_waiting_for_discovery_roster"

    def validate(self, config: AppConfig) -> None:
        """Validate prerequisites for the selected startup source.

        Args:
            config: Immutable application configuration.

        Raises:
            EnvValidationError: If automatic startup cannot authenticate or
                consume the qualified discovery roster.
        """
        if not self.auto_trader:
            return
        if not config.discovery.api.has_token:
            raise EnvValidationError(
                "--auto-trader requires discovery.api.token (PMCOPY__DISCOVERY__API__TOKEN)."
            )
        if not config.discovery.scheduler.start_from_roster:
            raise EnvValidationError(
                "--auto-trader requires discovery.scheduler.start_from_roster: true."
            )

    def effective_config(self, config: AppConfig) -> AppConfig:
        """Return the process-local configuration selected by the CLI.

        Args:
            config: Immutable application configuration.

        Returns:
            The original configuration in manual mode, or an immutable copy
            with Falcon enabled for an automatic-trader process.
        """
        if not self.auto_trader or config.discovery.falcon.enabled:
            return config
        falcon = config.discovery.falcon.model_copy(update={"enabled": True})
        discovery = config.discovery.model_copy(update={"falcon": falcon})
        return replace(config, discovery=discovery)

    def configured_leaders(self, config: AppConfig) -> list[str]:
        """Return the effective manual roster for this startup.

        Args:
            config: Immutable application configuration.

        Returns:
            Enabled configured wallets, or an empty list in automatic mode.
        """
        if self.auto_trader:
            return []
        return list(config.copy_trading.enabled_wallets())

    def discovery_loop_enabled(self, config: AppConfig) -> bool:
        """Return whether provider discovery should refresh in the background.

        Args:
            config: Immutable application configuration.

        Returns:
            ``True`` for the CLI override or a configured scheduler.
        """
        return self.auto_trader or config.discovery.scheduler.enabled

    def should_wait_for_discovery_roster(self, config: AppConfig) -> bool:
        """Return whether a bounded startup wait can produce an active leader.

        Args:
            config: Immutable application configuration.

        Returns:
            ``True`` when discovery runs, roster reads are enabled, and the
            provider credential is present.
        """
        return bool(
            self.discovery_loop_enabled(config)
            and config.discovery.scheduler.start_from_roster
            and config.discovery.api.has_token
        )


__all__ = ["TraderStartupPolicy"]
