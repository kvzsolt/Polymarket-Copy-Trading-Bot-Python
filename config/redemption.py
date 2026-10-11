"""Optional secret-only configuration for explicit gasless settlement commands."""

from collections.abc import Mapping
from dataclasses import dataclass, field
import re

from pydantic import SecretStr

from config.env import EnvValidationError, secret_value


@dataclass(frozen=True)
class RedemptionConfig:
    """Carry Relayer credentials without enabling any automatic settlement."""

    relayer_api_key: SecretStr = field(default_factory=lambda: SecretStr(""))
    relayer_api_key_address: str = ""

    def require_credentials(self) -> None:
        """Reject absent or malformed credentials before constructing a signer.

        Raises:
            EnvValidationError: If either required Relayer setting is invalid.
        """
        key = secret_value(self.relayer_api_key)
        if not key or len(key) > 1024 or any(ord(char) < 33 or ord(char) > 126 for char in key):
            raise EnvValidationError(
                "Set PMCOPY__REDEMPTION__RELAYER_API_KEY in secrets.env before --execute"
            )
        if re.fullmatch(r"0x[0-9a-fA-F]{40}", self.relayer_api_key_address) is None:
            raise EnvValidationError(
                "Set PMCOPY__REDEMPTION__RELAYER_API_KEY_ADDRESS in secrets.env before --execute"
            )


class RedemptionSecretBoundary:
    """Keep the settlement API key out of all operator YAML layers."""

    @staticmethod
    def reject_yaml(payload: Mapping[str, object]) -> None:
        """Require nonempty credentials to come from secrets.env or process env.

        Args:
            payload: Unvalidated YAML configuration, never included in errors.

        Raises:
            EnvValidationError: If YAML contains a nonempty Relayer API key.
        """
        section = payload.get("redemption")
        if isinstance(section, Mapping) and section.get("relayer_api_key"):
            raise EnvValidationError("redemption.relayer_api_key belongs in secrets.env only")
