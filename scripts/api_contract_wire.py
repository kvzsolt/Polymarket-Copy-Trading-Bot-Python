"""Generate closed client wire schemas from runtime projections and decoder fields.

Request schemas describe the supported client document, including accepted create
aliases. HTTP wrappers currently ignore unknown keys, while the task decoder rejects
them; generated clients deliberately send only declared keys. This distinction is
published separately rather than implying that server wrappers validate a schema.
"""

from __future__ import annotations

import dataclasses
import types
import typing
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Any, cast

from integrations.control_plane.task_decoder import CopyTaskPayloadDecoder
from scripts.api_contract_introspection import blank_payload
from scripts.api_contract_tables import PAYLOAD_FIELD_PLACEHOLDERS, RouteSpec
from services.control_plane.contracts import SettingsValues
from services.telemetry.contracts import TelemetryEvent, TelemetryPayload
from services.telemetry.query import TelemetryQueryService


class ApiWireSchemaExporter:
    """Own a self-contained graph of JSON schemas for HTTP and WebSocket clients."""

    _DECIMAL = {
        "type": "string",
        "pattern": r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$",
        "x-decimal": True,
    }
    _INTEGER = {
        "anyOf": [
            {"type": "integer", "minimum": -(2**53 - 1), "maximum": 2**53 - 1},
            {"type": "string", "pattern": r"^-?(?:0|[1-9][0-9]*)$", "x-large-integer": True},
        ]
    }
    _TEXT = {"type": "string", "minLength": 1, "maxLength": 512, "pattern": r"\S"}
    _REVISION = {"type": "integer", "minimum": 1}
    _EXTRA_DECIMALS = {
        "ProtectiveLevelView": ("trigger_percent", "exit_fraction"),
        "TaskProtectiveRiskView": (
            "stop_loss_percent",
            "stop_loss_price",
            "take_profit_percent",
            "take_profit_price",
            "trailing_stop_percent",
        ),
    }

    def __init__(
        self,
        *,
        views: Mapping[str, type],
        decimal_fields: Mapping[str, Sequence[str]],
        vocabularies: Mapping[str, Mapping[str, tuple[str, ...]]],
    ) -> None:
        """Store inspected runtime types and their verified field vocabularies.

        Args:
            views: Dataclasses available as runtime response projections.
            decimal_fields: Verified exact-decimal string fields per projection.
            vocabularies: Resolved closed vocabularies of string-typed fields.
        """

        self._views = dict(views)
        self._decimals = dict(decimal_fields) | self._EXTRA_DECIMALS
        self._vocabularies = vocabularies
        self._definitions: dict[str, Any] = {}
        self._requests: dict[str, dict[str, Any]] = {}
        self._server_frames: dict[str, dict[str, str]] = {}
        self._client_frames: dict[str, dict[str, str]] = {}

    def build(self, routes: Sequence[RouteSpec]) -> dict[str, Any]:
        """Return schemas and route projections with no imports of domain writes.

        Args:
            routes: Registered routes in deterministic export order.

        Returns:
            Self-contained definitions and request/response/frame references.

        Raises:
            RuntimeError: When a response contains an unsupported wire type.
        """

        self._telemetry()
        self._task_requests()
        self._command_requests()
        self._frames()
        rendered_routes = [
            route.to_json()
            | {
                "request_schema": None if route.action is None else self._requests[route.action],
                "response_schema": self._response(route.response),
                "request_unknown_fields": (
                    None
                    if route.action is None
                    else "HTTP wrapper ignores undeclared keys; task payload/configuration rejects them. "
                    "The published client schema allows only supported keys."
                ),
            }
            for route in routes
        ]
        return {
            "routes": rendered_routes,
            "json_schema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$defs": dict(sorted(self._definitions.items())),
                "description": (
                    "Self-contained supported wire schemas. Route response_schema describes data "
                    "inside the success envelope; request_schema null means no JSON request body. "
                    "Closed client schemas exclude server-owned actor/lifecycle fields. "
                    "Semantic domain validation and capability/permission checks remain authoritative."
                ),
            },
            "server_frame_schemas": self._server_frames,
            "client_frame_schemas": self._client_frames,
        }

    @staticmethod
    def _object(
        properties: Mapping[str, Any],
        *,
        optional: Sequence[str] = (),
    ) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": dict(properties),
            "required": [name for name in properties if name not in optional],
            "additionalProperties": False,
        }

    def _define(self, name: str, schema: dict[str, Any]) -> dict[str, str]:
        self._definitions[name] = schema
        return {"$ref": f"#/$defs/{name}"}

    def _dataclass(self, owner: type) -> dict[str, str]:
        name = owner.__name__
        reference = {"$ref": f"#/$defs/{name}"}
        if name in self._definitions:
            return reference
        self._definitions[name] = {}
        hints = typing.get_type_hints(owner)
        properties: dict[str, Any] = {}
        for field in dataclasses.fields(cast(Any, owner)):
            schema = self._type(hints[field.name])
            if field.name in self._decimals.get(name, ()):
                schema = (
                    {"anyOf": [dict(self._DECIMAL), {"type": "null"}]}
                    if type(None) in typing.get_args(hints[field.name])
                    else dict(self._DECIMAL)
                )
            vocabulary = self._vocabularies.get(name, {}).get(field.name)
            if vocabulary is not None:
                schema = {"type": "string", "enum": list(vocabulary)}
            if not field.init and isinstance(field.default, Enum):
                schema = {"type": "string", "const": field.default.value}
            properties[field.name] = schema
        if name == "SettingsApplyMetadata":
            properties["restart_required"] = {"type": "boolean", "const": True}
            properties["live_applied"] = {"type": "boolean", "const": False}
        self._definitions[name] = self._object(properties)
        return reference

    def _type(self, annotation: Any) -> dict[str, Any]:
        origin = typing.get_origin(annotation)
        arguments = typing.get_args(annotation)
        if origin is typing.Literal:
            value_types = {type(value) for value in arguments}
            if len(value_types) != 1 or next(iter(value_types)) not in (str, bool, int):
                raise RuntimeError("unsupported mixed literal wire vocabulary")
            wire_type = {str: "string", bool: "boolean", int: "integer"}[next(iter(value_types))]
            return {"type": wire_type, "enum": list(arguments)}
        if origin in (types.UnionType, typing.Union):
            return {"anyOf": [self._type(item) for item in arguments]}
        if origin in (tuple, list, Sequence):
            if origin is tuple and arguments and arguments[-1] is not Ellipsis:
                return {
                    "type": "array",
                    "prefixItems": [self._type(item) for item in arguments],
                    "minItems": len(arguments),
                    "maxItems": len(arguments),
                }
            return {"type": "array", "items": self._type(arguments[0])}
        if origin in (dict, Mapping):
            return {"type": "object", "additionalProperties": self._type(arguments[1])}
        if annotation is SettingsValues or annotation is object or annotation is Any:
            self._json_value()
            if annotation is SettingsValues:
                return {"type": "object", "additionalProperties": {"$ref": "#/$defs/JsonValue"}}
            return {"$ref": "#/$defs/JsonValue"}
        if annotation is type(None):
            return {"type": "null"}
        if annotation is Decimal:
            return dict(self._DECIMAL)
        if annotation is datetime:
            return {"type": "string", "format": "date-time"}
        if annotation is int:
            return deepcopy(self._INTEGER)
        if annotation in (str, bool, float):
            return {"type": {str: "string", bool: "boolean", float: "number"}[annotation]}
        if isinstance(annotation, type) and issubclass(annotation, Enum):
            return {"type": "string", "enum": [item.value for item in annotation]}
        if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
            return self._dataclass(annotation)
        raise RuntimeError(f"unsupported wire type: {annotation}")

    def _json_value(self) -> None:
        self._definitions["JsonValue"] = {
            "anyOf": [
                {"type": "null"},
                {"type": "boolean"},
                {"type": "string"},
                {"type": "number"},
                {"type": "array", "items": {"$ref": "#/$defs/JsonValue"}},
                {"type": "object", "additionalProperties": {"$ref": "#/$defs/JsonValue"}},
            ]
        }

    def _response(self, name: str) -> dict[str, Any] | None:
        if name == "WebSocket":
            return None
        if name.endswith("[]"):
            return {"type": "array", "items": self._response(name[:-2])}
        if name.startswith("Page<"):
            return self._object(
                {
                    "items": {
                        "type": "array",
                        "maxItems": 100,
                        "items": self._response(name[5:-1]),
                    },
                    "next_cursor": {"anyOf": [dict(self._TEXT), {"type": "null"}]},
                }
            )
        if name in self._definitions:
            return {"$ref": f"#/$defs/{name}"}
        return self._dataclass(self._views[name])

    def _task_requests(self) -> None:
        self._dataclass(self._views["TaskView"])
        self._dataclass(self._views["TaskConfigurationView"])
        configuration = deepcopy(self._definitions["TaskConfigurationView"])
        configuration["required"].remove("protective_risk")
        configuration_ref = self._define("CopyTaskConfigurationPayload", configuration)
        required = CopyTaskPayloadDecoder._REQUIRED_FIELDS
        optional = CopyTaskPayloadDecoder._OPTIONAL_FIELDS
        summary = self._definitions["TaskView"]["properties"]
        properties = {
            name: (configuration_ref if name == "configuration" else deepcopy(summary[name]))
            for name in sorted(required | optional)
        }
        update = self._define(
            "CopyTaskUpdatePayload",
            self._object(properties, optional=tuple(optional)),
        )
        create = self._define("CopyTaskCreatePayload", self._object(properties))
        self._requests["task.create"] = self._define(
            "TaskCreateRequest",
            {
                "anyOf": [self._object({"task": create}), create],
            },
        )
        self._requests["task.update"] = self._define(
            "TaskUpdateRequest",
            self._object(
                {
                    "task": update,
                    "expected_version": dict(self._REVISION),
                }
            ),
        )

    def _command_requests(self) -> None:
        version = self._define(
            "TaskVersionRequest",
            self._object(
                {
                    "expected_version": dict(self._REVISION),
                }
            ),
        )
        for action in ("task.pause", "task.resume", "task.delete"):
            self._requests[action] = version
        self._requests["task.duplicate"] = self._define(
            "TaskDuplicateRequest",
            self._object(
                {
                    "new_task_id": dict(self._TEXT),
                    "new_name": dict(self._TEXT),
                }
            ),
        )
        from services.control_plane.contracts import TaskAction

        item = self._define(
            "BulkTaskItemRequest",
            self._object(
                {
                    "task_id": dict(self._TEXT),
                    "action": self._type(TaskAction),
                    "expected_version": dict(self._REVISION),
                }
            ),
        )
        self._requests["task.bulk"] = self._define(
            "BulkTaskRequest",
            self._object(
                {
                    "items": {"type": "array", "minItems": 1, "maxItems": 100, "items": item},
                }
            ),
        )
        self._requests["discovery.run"] = self._define(
            "DiscoveryRunRequest",
            self._object(
                {
                    "force": {"type": "boolean", "default": False},
                },
                optional=("force",),
            ),
        )
        runtime = self._define(
            "RuntimeControlRequest",
            self._object(
                {
                    "expected_revision": dict(self._REVISION),
                    "reason_code": dict(self._TEXT),
                }
            ),
        )
        for action in ("runtime.pause", "runtime.resume", "runtime.full_stop"):
            self._requests[action] = runtime
        self._requests["lock.reset"] = self._define(
            "LockResetRequest",
            self._object(
                {
                    **{
                        name: dict(self._TEXT)
                        for name in (
                            "follower_account",
                            "task_id",
                            "leader_wallet",
                            "market_id",
                            "reason_code",
                        )
                    },
                    "expected_revision": dict(self._REVISION),
                }
            ),
        )
        self._requests["settings.write"] = self._define(
            "SettingsWriteRequest",
            self._object(
                {
                    "values": self._type(SettingsValues),
                    "expected_revision": dict(self._TEXT),
                }
            ),
        )
        self._requests["leader.promote"] = self._define(
            "LeaderPromoteRequest",
            self._object(
                {
                    **{
                        name: dict(self._TEXT)
                        for name in ("follower_account", "evaluation_id", "request_id")
                    },
                    "expected_revision": {"type": "integer"},
                }
            ),
        )

    def _telemetry(self) -> None:
        self._dataclass(TelemetryEvent)
        projections = []
        for payload_type in typing.get_args(TelemetryPayload):
            payload = blank_payload(payload_type, placeholders=PAYLOAD_FIELD_PLACEHOLDERS)
            projected = TelemetryQueryService._payload_to_json(
                cast(Any, SimpleNamespace(payload=payload))
            )
            original = self._definitions[payload_type.__name__]["properties"]
            projections.append(
                self._define(
                    f"TelemetrySnapshot{payload_type.__name__}",
                    self._object({name: original[name] for name in projected}),
                )
            )
        event = deepcopy(self._definitions["TelemetryEvent"])
        event["properties"]["payload"] = {"anyOf": projections}
        snapshot_event = self._define("TelemetrySnapshotEvent", event)
        number = {"type": "number"}
        nullable_number = {"anyOf": [number, {"type": "null"}]}
        histogram = self._object(
            {
                "name": {"type": "string"},
                "labels": {"type": "object", "additionalProperties": {"type": "string"}},
                "count": deepcopy(self._INTEGER),
                "sum": number,
                "buckets": {
                    "type": "array",
                    "items": self._object(
                        {"le": {"type": "string"}, "count": deepcopy(self._INTEGER)}
                    ),
                },
                "p50": nullable_number,
                "p95": nullable_number,
                "p99": nullable_number,
            }
        )
        metrics_properties = {
            "counters": {"type": "object", "additionalProperties": number},
            "gauges": {"type": "object", "additionalProperties": number},
            "histograms": {"type": "array", "items": histogram},
        }
        self._define(
            "MetricsSnapshot",
            self._object(
                {
                    "generated_at": self._type(datetime),
                    **metrics_properties,
                }
            ),
        )
        self._define(
            "TelemetrySnapshot",
            self._object(
                {
                    "generated_at": self._type(datetime),
                    "events": {"type": "array", "items": snapshot_event},
                    "metrics": self._object(metrics_properties),
                }
            ),
        )

    def _frames(self) -> None:
        ready = {
            "stream_boot_id": {"type": "string"},
            "ordering": {"type": "string", "const": "session_per_producer"},
            "durable_replay": {"type": "boolean", "const": False},
            "resync_required": {"type": "boolean", "const": True},
        }
        fields: dict[str, dict[str, Any]] = {
            "stream_ready": ready,
            "telemetry_event": {
                "stream_boot_id": {"type": "string"},
                "data": {"$ref": "#/$defs/TelemetryEvent"},
            },
            "telemetry_snapshot": {
                "stream_boot_id": {"anyOf": [{"type": "string"}, {"type": "null"}]},
                "authoritative_financial_state": {"type": "boolean", "const": False},
                "data": {"anyOf": [{"$ref": "#/$defs/TelemetrySnapshot"}, {"type": "null"}]},
            },
            "resync_required": {"dropped_count": {"type": "integer", "minimum": 0}},
            "pong": {},
        }
        for name, properties in fields.items():
            self._server_frames[name] = self._define(
                "Stream" + "".join(part.title() for part in name.split("_")),
                self._object({"type": {"type": "string", "const": name}, **properties}),
            )
        self._server_frames["error"] = self._define(
            "StreamError",
            {
                "anyOf": [
                    self._object(
                        {
                            "type": {"type": "string", "const": "error"},
                            "code": {
                                "type": "string",
                                "enum": ["invalid_stream_message", "unsupported_stream_message"],
                            },
                        }
                    ),
                    self._object(
                        {
                            "type": {"type": "string", "const": "error"},
                            "code": {"type": "string", "const": "rate_limited"},
                            "retry_after_seconds": {"type": "integer", "minimum": 0},
                        }
                    ),
                ]
            },
        )
        for name in ("ping", "resync"):
            self._client_frames[name] = self._define(
                f"StreamClient{name.title()}",
                self._object({"type": {"type": "string", "const": name}}),
            )
