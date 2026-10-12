"""Immutable local evidence limits and private receiver configuration."""

from collections.abc import Mapping
from dataclasses import dataclass, field
import math

from pydantic import SecretStr

from pmcopy.domain.evidence import EvidenceError


@dataclass(frozen=True, slots=True)
class EvidenceConfig:
    """Durable publication is mandatory for a configured production observation source."""

    enabled: bool = True
    outbox_path: str = "data/evidence-outbox.sqlite3"
    max_events: int = 10_000
    max_bytes: int = 64 * 1024 * 1024
    max_history: int = 1_000_000
    origin: str = field(default="http://127.0.0.1:9104", repr=False)
    token: SecretStr = field(default_factory=lambda: SecretStr(""), repr=False)
    timeout_seconds: float = 5.0
    batch_size: int = 100
    retry_delay_seconds: float = 5.0
    history_journal_path: str | None = None

    def __post_init__(self) -> None:
        """Reject ephemeral storage and unbounded/malformed worker settings."""
        if type(self.enabled) is not bool:
            raise EvidenceError("evidence_invalid_enabled")
        if self.history_journal_path is not None and (
            not isinstance(self.history_journal_path, str)
            or not self.history_journal_path.strip()
            or self.history_journal_path == ":memory:"
            or "\x00" in self.history_journal_path
        ):
            raise EvidenceError("evidence_invalid_history_journal")
        if (
            not isinstance(self.outbox_path, str)
            or not self.outbox_path.strip()
            or self.outbox_path == ":memory:"
            or "\x00" in self.outbox_path
        ):
            raise EvidenceError("evidence_durable_path_required")
        if any(
            type(n) is not int or n <= 0
            for n in (self.max_events, self.max_bytes, self.max_history)
        ):
            raise EvidenceError("evidence_invalid_capacity")
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= 100:
            raise EvidenceError("evidence_invalid_batch_limit")
        for value, lower, upper in (
            (self.timeout_seconds, 0.01, 30),
            (self.retry_delay_seconds, 0.1, 300),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not lower <= value <= upper
            ):
                raise EvidenceError("evidence_invalid_worker_bound")
        if not isinstance(self.token, SecretStr):
            raise EvidenceError("evidence_private_credential_required")

    def require_runtime(self) -> None:
        """Fail source startup rather than installing a disabled or credentialless no-op."""
        if not self.enabled:
            raise EvidenceError("evidence_durable_sink_required")
        token = self.token.get_secret_value()
        if not 32 <= len(token) <= 512 or any(not 33 <= ord(c) <= 126 for c in token):
            raise EvidenceError("evidence_private_credential_required")


class EvidenceSecretBoundary:
    """Keep evidence service credentials out of all operator YAML layers."""

    @staticmethod
    def reject_yaml(payload: Mapping[str, object]) -> None:
        """Reject token-bearing YAML without rendering its value in errors."""
        section = payload.get("evidence")
        if isinstance(section, Mapping) and section.get("token"):
            raise EvidenceError("evidence_token_requires_private_environment")
