"""Compose typed application configuration with frontend settings storage.

The adapter validates partial environment overrides against the same immutable
section types and cross-section safety rules used during application startup.
It returns only fields supplied by the operator, preserving the layered YAML
model instead of materializing every runtime default into an override file.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from typing import Protocol

from pydantic import BaseModel, SecretStr, TypeAdapter, ValidationError

from config.app_config import AppConfig, ExecutionConfig
from services.control_plane.contracts import SettingsPath


class ConfigSemanticValidator(Protocol):
    """Validate fully typed application configuration candidates."""

    def validate(self, config: AppConfig) -> None:
        """Raise when ``config`` violates an application invariant."""

        ...


class AppConfigSettingsValidator:
    """Validate frontend-safe partial settings against runtime config types."""

    _EDITABLE_SECTIONS = frozenset(
        {
            "app",
            "control_plane",
            "copy_trading",
            "detection",
            "discovery",
            "execution",
            "logging",
            "market_data",
            "polymarket",
            "protective_exits",
            "reconciliation",
            "risk",
            "sizing",
        }
    )
    _EXCLUDED_KEYS: Mapping[str, frozenset[str]] = {
        "app": frozenset({"multi_user_config_file"}),
        "control_plane": frozenset({"auth"}),
        # Followed traders are owned by config/traders/<name>.yml files, not
        # by frontend settings documents.
        "copy_trading": frozenset({"traders"}),
        # The detection source is wired once at startup; a runtime edit could
        # not hot-swap it, so it is restart-only and never frontend-editable.
        "detection": frozenset({"mode"}),
        "logging": frozenset({"log_dir", "app_log_jsonl_file"}),
        "polymarket": frozenset({"rpc_url", "fallback_rpc_url"}),
    }
    _SECRET_NAMES = frozenset(
        {
            "api_key",
            "api_token",
            "auth",
            "authorization",
            "bearer_token",
            "credential",
            "credentials",
            "password",
            "passphrase",
            "private_key",
            "refresh_token",
            "rpc_url",
            "fallback_rpc_url",
            "secret",
            "secret_key",
            "token",
        }
    )

    def __init__(
        self,
        *,
        config: AppConfig,
        environment: str,
        semantic_validator: ConfigSemanticValidator,
    ) -> None:
        """Store the immutable runtime snapshot and validation collaborator.

        Args:
            config: Current effective application configuration.
            environment: Explicit environment whose override files are editable.
            semantic_validator: Cross-section invariant validator.

        Raises:
            ValueError: If the environment identity is empty.
        """

        normalized_environment = str(environment).strip().lower()
        if not normalized_environment:
            raise ValueError("settings environment must not be empty")
        self._config = config
        self._environment = normalized_environment
        self._semantic_validator = semantic_validator

    def known_paths(self) -> frozenset[SettingsPath]:
        """Return every exact environment/section path this adapter supports."""

        return frozenset(
            SettingsPath(environment=self._environment, section=section)
            for section in self._EDITABLE_SECTIONS
        )

    def allowed_keys(self, path: SettingsPath) -> frozenset[str]:
        """Return frontend-editable top-level keys for one settings section.

        Args:
            path: Exact environment and service section.

        Returns:
            A frozen set derived from the runtime model fields.

        Raises:
            ValueError: If the path is not owned by this adapter.
        """

        section = self._section(path)
        model_keys = self._model_keys(section)
        return model_keys.difference(self._EXCLUDED_KEYS.get(path.section, frozenset()))

    def is_secret(self, path: SettingsPath, key_path: tuple[str, ...]) -> bool:
        """Return whether a nested settings path is secret-backed.

        Args:
            path: Exact environment and service section.
            key_path: Nested key components relative to the section root.

        Returns:
            ``True`` for secret model values and conservative credential names.

        Raises:
            ValueError: If the settings path is unsupported.
        """

        section = self._section(path)
        if not key_path:
            return False
        normalized = tuple(str(part).strip().casefold().replace("-", "_") for part in key_path)
        if any(part in self._SECRET_NAMES for part in normalized):
            return True
        value = self._value_at_path(section, key_path)
        return isinstance(value, SecretStr)

    def validate(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Validate and normalize a partial environment section override.

        Args:
            path: Exact environment and service section.
            payload: Partial, secret-free operator-supplied values.

        Returns:
            Only supplied fields, normalized into JSON/YAML-safe primitives.

        Raises:
            ValueError: If keys, types, secrets, or semantic invariants fail.
        """

        section = self._section(path)
        if any(not isinstance(key, str) or not key for key in payload):
            raise ValueError("settings keys must be non-empty strings")
        allowed = self.allowed_keys(path)
        if not set(payload).issubset(allowed):
            raise ValueError("settings payload contains unsupported keys")
        if self._contains_secret(path, payload):
            raise ValueError("settings payload contains a secret-backed field")
        try:
            adapter = TypeAdapter(type(section))
            current = adapter.dump_python(section, mode="python")
            if not isinstance(current, Mapping):
                raise ValueError("settings section did not serialize as a mapping")
            merged = self._deep_merge(dict(current), payload)
            validated_section = adapter.validate_python(merged)
            candidate = self._candidate(path.section, validated_section)
            self._semantic_validator.validate(candidate)
            normalized = adapter.dump_python(validated_section, mode="json")
            if not isinstance(normalized, Mapping):
                raise ValueError("settings section did not normalize as a mapping")
            return self._project_supplied(normalized, payload)
        except (ValidationError, TypeError, ValueError):
            raise ValueError("settings payload failed typed validation") from None

    def _section(self, path: SettingsPath) -> object:
        """Return one supported section from the immutable runtime snapshot."""

        if path.environment != self._environment or path.section not in self._EDITABLE_SECTIONS:
            raise ValueError("settings path is not supported")
        return getattr(self._config, path.section)

    @staticmethod
    def _model_keys(section: object) -> frozenset[str]:
        """Return field names from a frozen dataclass or Pydantic model."""

        if isinstance(section, BaseModel):
            return frozenset(type(section).model_fields)
        if is_dataclass(section) and not isinstance(section, type):
            return frozenset(item.name for item in fields(section))
        raise ValueError("settings section does not expose a typed schema")

    def _candidate(self, section_name: str, section: object) -> AppConfig:
        """Return a full typed candidate for cross-section validation."""

        changes: dict[str, object] = {section_name: section}
        if section_name == "execution":
            if not isinstance(section, ExecutionConfig):
                raise ValueError("execution settings did not retain their typed model")
            changes["dry_run"] = section.paper_trade_only
        return replace(self._config, **changes)

    def _contains_secret(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
        prefix: tuple[str, ...] = (),
    ) -> bool:
        """Return whether any nested supplied key is secret-backed."""

        for key, value in payload.items():
            key_path = (*prefix, str(key))
            if self.is_secret(path, key_path):
                return True
            if isinstance(value, Mapping) and self._contains_secret(path, value, key_path):
                return True
        return False

    @classmethod
    def _deep_merge(
        cls,
        base: dict[str, object],
        override: Mapping[str, object],
    ) -> dict[str, object]:
        """Merge a partial override into a detached current section mapping."""

        merged = deepcopy(base)
        for key, value in override.items():
            current = merged.get(key)
            if isinstance(current, Mapping) and isinstance(value, Mapping):
                merged[key] = cls._deep_merge(dict(current), value)
            else:
                merged[key] = deepcopy(value)
        return merged

    @classmethod
    def _project_supplied(
        cls,
        normalized: Mapping[str, object],
        supplied: Mapping[str, object],
    ) -> dict[str, object]:
        """Project normalized output back to exactly the supplied key shape."""

        projected: dict[str, object] = {}
        for key, supplied_value in supplied.items():
            normalized_value = normalized[key]
            if isinstance(supplied_value, Mapping) and isinstance(normalized_value, Mapping):
                projected[key] = cls._project_supplied(normalized_value, supplied_value)
            else:
                projected[key] = normalized_value
        return projected

    @staticmethod
    def _value_at_path(section: object, key_path: tuple[str, ...]) -> object:
        """Resolve a typed current value for secret-type inspection."""

        value = section
        for component in key_path:
            if isinstance(value, Mapping):
                value = value.get(component)
            elif isinstance(value, (tuple, list)) and component.isdigit():
                index = int(component)
                value = value[index] if 0 <= index < len(value) else None
            else:
                value = getattr(value, component, None)
            if value is None:
                break
        return value


__all__ = ["AppConfigSettingsValidator", "ConfigSemanticValidator"]
