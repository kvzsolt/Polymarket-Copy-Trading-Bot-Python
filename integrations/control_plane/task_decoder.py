"""Decode strict frontend task payloads into immutable domain tasks.

The decoder accepts one explicit write shape and delegates policy-value
construction to the existing task codec. It performs no persistence and never
accepts lifecycle state, optimistic versions, arbitrary patches, or secrets.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import fields
from typing import get_type_hints

from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    TaskConfigurationView,
)
from services.copy_tasks.domain import CopyTask
from storage.postgres.copy_task_repository import CopyTaskCodec


class CopyTaskPayloadDecoder:
    """Validate and decode the bounded frontend task write contract."""

    _REQUIRED_FIELDS = frozenset(
        {
            "name",
            "leader_wallet",
            "follower_account",
            "monitoring_mode",
            "sizing_mode",
            "notifications_muted",
            "configuration",
        }
    )
    _OPTIONAL_FIELDS = frozenset({"task_id"})

    def __init__(self, codec: CopyTaskCodec | None = None) -> None:
        """Store the existing pure task policy codec.

        Args:
            codec: Optional injected JSON-safe task translator.
        """

        self._codec = codec if codec is not None else CopyTaskCodec()

    def decode(
        self,
        payload: Mapping[str, object],
        *,
        task_id: str | None,
    ) -> CopyTask:
        """Return one paused version-one task from a strict payload.

        Args:
            payload: Frontend write document with identity and configuration.
            task_id: Optional authoritative task identity from the URL path.

        Returns:
            A validated immutable task safe for the command service.

        Raises:
            ControlPlaneError: If shape, identity, or domain values are invalid.
        """

        try:
            self._validate_top_level(payload)
            payload_task_id = payload.get("task_id")
            resolved_task_id = self._resolve_task_id(payload_task_id, task_id)
            configuration = self._mapping(payload.get("configuration"))
            self._validate_configuration(configuration)
            return self._codec.decode_row(
                {
                    "task_id": resolved_task_id,
                    "name": payload["name"],
                    "leader_wallet": payload["leader_wallet"],
                    "follower_account": payload["follower_account"],
                    "status": "paused",
                    "monitoring_mode": payload["monitoring_mode"],
                    "sizing_mode": payload["sizing_mode"],
                    "notifications_muted": payload["notifications_muted"],
                    "version": 1,
                    "configuration": configuration,
                }
            )
        except ControlPlaneError:
            raise
        except (KeyError, TypeError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None

    def _validate_top_level(self, payload: Mapping[str, object]) -> None:
        """Reject missing and unknown top-level fields."""

        keys = frozenset(payload)
        if not self._REQUIRED_FIELDS.issubset(keys):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        if not keys.issubset(self._REQUIRED_FIELDS | self._OPTIONAL_FIELDS):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)

    def _validate_configuration(self, configuration: Mapping[str, object]) -> None:
        """Reject unknown or incomplete nested configuration fields."""

        section_types = get_type_hints(TaskConfigurationView)
        required = frozenset(section_types) - {"protective_risk"}
        if not required.issubset(configuration) or not frozenset(configuration).issubset(
            section_types
        ):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        for section_name, section_type in section_types.items():
            if section_name == "protective_risk" and section_name not in configuration:
                continue
            section = self._mapping(configuration.get(section_name))
            expected_fields = frozenset(item.name for item in fields(section_type))
            if frozenset(section) != expected_fields:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)

    @staticmethod
    def _resolve_task_id(payload_value: object, path_value: str | None) -> str:
        """Resolve create or update identity without permitting ambiguity."""

        payload_id = str(payload_value or "").strip()
        path_id = str(path_value or "").strip()
        if path_value is None:
            if not payload_id:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
            return payload_id
        if payload_id and payload_id != path_id:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        if not path_id:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return path_id

    @staticmethod
    def _mapping(value: object) -> Mapping[str, object]:
        """Return a mapping or reject the boundary value."""

        if not isinstance(value, Mapping):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return value


__all__ = ["CopyTaskPayloadDecoder"]
