#!/usr/bin/env python3
"""Export the operator control-plane API contract to ``docs/ui/api_contract.json``.

A UI generator is handed ``docs/ui/UI_GENERATOR_PROMPT.md`` plus this JSON file
and nothing else, so the JSON has to be the machine-readable truth about the
running backend rather than a hand-written summary that quietly rots. Every
section is therefore derived from the code that serves the API:

* view and telemetry field lists come from ``dataclasses.fields`` and
  ``typing.get_type_hints`` of the real frozen contracts;
* enum vocabularies come from the real ``StrEnum`` members;
* the HTTP status of every application error comes from the real
  ``ControlPlaneErrorMapper``;
* transport limits come from the real ``ControlPlaneHttpConfig`` defaults;
* the capability vocabulary comes from the real composition-root helper.

Everything that cannot be recovered from the running code is declared in
``scripts/api_contract_tables.py`` and cross-checked before anything is
written: the route table in both directions against the routes an
all-capabilities ``ControlPlaneHttpApplication`` really registers, the
decimal-string field map against the ``ContractValidation`` calls each view
really runs, and every restated literal against the transport source.

Usage::

    python scripts/export_api_contract.py            # rewrite the contract
    python scripts/export_api_contract.py --check    # fail if it is stale

``--check`` is what CI and ``tests/control_plane/test_api_contract_snapshot.py``
use. Output is deterministic: keys are sorted, collections are ordered, and no
timestamp, host, credential, or environment value is written.
"""

from __future__ import annotations

import argparse
import dataclasses
import inspect
import json
import secrets
import sys
import types
import typing
from collections.abc import Mapping, Sequence
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, TypeAlias, cast

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.app_config import AppConfig  # noqa: E402
from domain.detection import DetectionMode  # noqa: E402
from integrations.control_plane import http as http_module  # noqa: E402
from integrations.control_plane import security as security_module  # noqa: E402
from integrations.control_plane import serialization as serialization_module  # noqa: E402
from integrations.control_plane.contracts import ControlPlaneDependencies  # noqa: E402
from integrations.control_plane.http import API_PREFIX, ControlPlaneHttpApplication  # noqa: E402
from integrations.control_plane.security import (  # noqa: E402
    ALL_CONTROL_PLANE_PERMISSIONS,
    READ_ONLY_CONTROL_PLANE_PERMISSIONS,
    BearerCredential,
    ControlPlaneAuthenticator,
    ControlPlaneHttpConfig,
)
from integrations.control_plane.serialization import ControlPlaneErrorMapper  # noqa: E402
from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory  # noqa: E402
from orchestrators.control_plane_reads import ControlPlaneRuntimeReads  # noqa: E402
from scripts.api_contract_introspection import (  # noqa: E402
    DecimalFieldExtractor,
    TypeRenderer,
    blank_payload,
    outcome_values,
    resolve_vocabulary,
    summary_line,
)
from scripts.api_contract_tables import (  # noqa: E402
    BULK_ITEMS_MAX,
    CURSOR_MAX_CHARS,
    DECIMAL_FIELDS,
    FIELD_VOCABULARIES,
    HTTP_MODULE,
    PAGE_LIMIT_DEFAULT,
    PAGE_LIMIT_MAX,
    PAGE_LIMIT_MIN,
    PAYLOAD_FIELD_PLACEHOLDERS,
    ROUTES,
    SOURCE_GUARDS,
    TELEMETRY_LIMIT_MAX,
    TRANSPORT_ERRORS,
    WEBSOCKET_ERROR_CODES,
    WEBSOCKET_HEARTBEAT_SECONDS,
    WEBSOCKET_MAX_INBOUND_MESSAGE_BYTES,
    RouteSpec,
)
from services.control_plane import contracts as rest_contracts  # noqa: E402
from services.control_plane import contracts_reads as rest_read_contracts  # noqa: E402
from services.control_plane.contracts import ControlPlaneError, ControlPlaneReason  # noqa: E402
from services.telemetry import contracts as telemetry_contracts  # noqa: E402
from services.telemetry.contracts import (  # noqa: E402
    WSS_STREAM_NAMES,
    TelemetryEvent,
    TelemetryPayload,
    event_type_for_payload,
)

JsonValue: TypeAlias = Any

CONTRACT_PATH = PROJECT_ROOT / "docs" / "ui" / "api_contract.json"
"""Committed machine-readable contract this script owns."""

SCHEMA_VERSION = 1
"""Version of the JSON layout itself, not of the API it describes."""

_STALE_MESSAGE = "run scripts/export_api_contract.py to refresh it"


def _unused_capability(*args: object, **kwargs: object) -> object:
    """Fail loudly if route registration ever calls a probed collaborator."""

    del args, kwargs
    raise RuntimeError("the contract exporter never invokes control-plane services")


class _AllCapabilityDependency:
    """Answer every capability probe so every conditional route registers.

    Route registration asks only whether an injected collaborator exposes a
    named callable; it never calls one. Answering every probe positively means
    a route added behind a brand-new collaborator method still registers here,
    so the cross-check reports it as *missing from* ``ROUTES`` instead of
    silently dropping it from the published contract.
    """

    def __getattr__(self, name: str) -> Any:
        """Return a placeholder callable for any probed capability name.

        Args:
            name: Attribute the adapter probed.

        Returns:
            A callable that raises if it is ever actually invoked.

        Raises:
            AttributeError: For dunder lookups, so normal object protocols
                keep working.
        """

        if name.startswith("__"):
            raise AttributeError(name)
        return _unused_capability


class ApiContractExporter:
    """Build the deterministic machine-readable control-plane API contract."""

    def __init__(
        self,
        *,
        routes: Sequence[RouteSpec] = ROUTES,
        decimal_fields: Mapping[str, tuple[str, ...]] = DECIMAL_FIELDS,
        field_vocabularies: Mapping[str, Mapping[str, str]] = FIELD_VOCABULARIES,
    ) -> None:
        """Store the hand-written halves of the contract.

        Args:
            routes: Static route table cross-checked against the application.
            decimal_fields: Curated decimal-string field map, cross-checked
                against the validators each view runs.
            field_vocabularies: Closed vocabularies behind string-typed view
                fields, named as ``module:symbol`` and resolved at build time.
        """

        self._routes = tuple(routes)
        self._decimal_fields = {name: tuple(fields) for name, fields in decimal_fields.items()}
        self._field_vocabularies = {
            view: dict(fields) for view, fields in field_vocabularies.items()
        }
        self._config = ControlPlaneHttpConfig()
        self._payload_types = tuple(typing.get_args(TelemetryPayload))
        self._renderer = TypeRenderer(
            union_aliases={
                frozenset(payload.__name__ for payload in self._payload_types): "TelemetryPayload"
            }
        )
        self._extractor = DecimalFieldExtractor()

    # ------------------------------------------------------------------
    # Cross-checks
    # ------------------------------------------------------------------
    def registered_routes(self) -> frozenset[tuple[str, str]]:
        """Return every non-HEAD route an all-capabilities application registers."""

        application = ControlPlaneHttpApplication(
            config=self._config,
            authenticator=_export_authenticator(),
            dependencies=_export_dependencies(),
        )
        return frozenset(
            (route.method, route.resource.canonical)
            for route in application.app.router.routes()
            if route.method != "HEAD" and route.resource is not None
        )

    def derived_decimal_fields(self) -> dict[str, tuple[str, ...]]:
        """Return decimal fields recovered from the view validators themselves."""

        derived: dict[str, tuple[str, ...]] = {}
        for name, view in self._view_types().items():
            fields = self._extractor.fields_for(view)
            if fields:
                derived[name] = fields
        return derived

    def verify(self) -> tuple[str, ...]:
        """Return every cross-check problem; empty means the contract is sound.

        Returns:
            Human-readable problem descriptions in a stable order.
        """

        problems: list[str] = []
        problems.extend(self._route_problems())
        problems.extend(self._decimal_problems())
        problems.extend(self._vocabulary_problems())
        problems.extend(self._source_guard_problems())
        return tuple(problems)

    def _route_problems(self) -> tuple[str, ...]:
        registered = self.registered_routes()
        declared = {(route.method, route.path) for route in self._routes}
        problems = [
            f"route registered but missing from ROUTES: {method} {path}"
            for method, path in sorted(registered - declared)
        ]
        problems.extend(
            f"route declared in ROUTES but not registered: {method} {path}"
            for method, path in sorted(declared - registered)
        )
        if len(declared) != len(self._routes):
            problems.append("ROUTES contains a duplicate method/path pair")
        permissions = {permission.value for permission in ALL_CONTROL_PLANE_PERMISSIONS}
        problems.extend(
            f"route {route.method} {route.path} names unknown permission {route.permission}"
            for route in self._routes
            if route.permission not in permissions
        )
        capabilities = set(self.capability_vocabulary())
        problems.extend(
            f"route {route.method} {route.path} names unknown capability {route.capability}"
            for route in self._routes
            if route.capability is not None and route.capability not in capabilities
        )
        return tuple(problems)

    def _decimal_problems(self) -> tuple[str, ...]:
        problems: list[str] = []
        views = self._view_types()
        for name, fields in self._decimal_fields.items():
            view = views.get(name)
            if view is None:
                problems.append(f"DECIMAL_FIELDS names unknown view {name}")
                continue
            declared = {item.name for item in dataclasses.fields(cast(Any, view))}
            problems.extend(
                f"DECIMAL_FIELDS names unknown field {name}.{field}"
                for field in fields
                if field not in declared
            )
        derived = self.derived_decimal_fields()
        for name in sorted(set(derived) | set(self._decimal_fields)):
            if derived.get(name) != self._decimal_fields.get(name):
                problems.append(
                    f"DECIMAL_FIELDS[{name}] is {self._decimal_fields.get(name)} but the view "
                    f"validates {derived.get(name)}"
                )
        return tuple(problems)

    def resolved_vocabularies(self) -> dict[str, dict[str, tuple[str, ...]]]:
        """Return the members behind every declared string-field vocabulary.

        Returns:
            ``{view_name: {field_name: (value, ...)}}`` with values in the
            order the source enum or literal tuple declares them.
        """

        resolved: dict[str, dict[str, tuple[str, ...]]] = {}
        for view, fields in self._field_vocabularies.items():
            for field, target in fields.items():
                resolved.setdefault(view, {})[field] = resolve_vocabulary(target)
        return resolved

    def _vocabulary_problems(self) -> tuple[str, ...]:
        problems: list[str] = []
        views = self._view_types()
        for view, fields in self._field_vocabularies.items():
            owner = views.get(view)
            if owner is None:
                problems.append(f"FIELD_VOCABULARIES names unknown view {view}")
                continue
            hints = typing.get_type_hints(owner)
            declared = {item.name for item in dataclasses.fields(cast(Any, owner))}
            for field, target in sorted(fields.items()):
                if field not in declared:
                    problems.append(f"FIELD_VOCABULARIES names unknown field {view}.{field}")
                    continue
                if hints[field] is not str:
                    problems.append(
                        f"FIELD_VOCABULARIES[{view}.{field}] is not a plain string field"
                    )
                try:
                    members = resolve_vocabulary(target)
                except (ImportError, AttributeError, TypeError, ValueError) as error:
                    problems.append(
                        f"FIELD_VOCABULARIES[{view}.{field}] target {target!r} "
                        f"does not resolve: {type(error).__name__}"
                    )
                    continue
                if not members:
                    problems.append(
                        f"FIELD_VOCABULARIES[{view}.{field}] target {target!r} is empty"
                    )
        return tuple(problems)

    @staticmethod
    def _source_guard_problems() -> tuple[str, ...]:
        sources = {
            HTTP_MODULE: inspect.getsource(http_module),
            "integrations/control_plane/security.py": inspect.getsource(security_module),
            "integrations/control_plane/serialization.py": inspect.getsource(serialization_module),
        }
        joined = "\n".join(sources.values())
        problems = [
            f"literal {literal!r} no longer present in {where}"
            for literal, where in SOURCE_GUARDS
            if literal not in sources[where]
        ]
        problems.extend(
            f"transport error code {code!r} no longer present in the transport source"
            for code in sorted(TRANSPORT_ERRORS)
            if f'"{code}"' not in joined
        )
        problems.extend(
            f"websocket error code {code!r} no longer present in the transport source"
            for code in sorted(WEBSOCKET_ERROR_CODES)
            if f'"{code}"' not in joined
        )
        return tuple(problems)

    # ------------------------------------------------------------------
    # Contract assembly
    # ------------------------------------------------------------------
    @staticmethod
    def capability_vocabulary() -> tuple[str, ...]:
        """Return every capability name the composition root can advertise.

        The names come from the composition root itself, evaluated across both
        trading modes and every detection mode with all optional reads and
        commands composed, so the published vocabulary is exactly the union a
        real deployment can report.
        """

        stub = _AllCapabilityDependency()
        reads = ControlPlaneRuntimeReads(
            accounts=cast(Any, _unused_capability),
            detection_counters=cast(Any, stub),
            readiness=cast(Any, stub),
            metrics=cast(Any, stub),
            pnl_trackers={"export": cast(Any, stub)},
            positions=cast(Any, stub),
        )
        names: set[str] = set()
        for trading_mode in ("paper", "live"):
            for mode in DetectionMode:
                config = cast(
                    AppConfig,
                    SimpleNamespace(
                        execution=SimpleNamespace(trading_mode=trading_mode),
                        detection=SimpleNamespace(detection_mode=mode),
                    ),
                )
                names.update(
                    ControlPlaneRuntimeFactory._capabilities(
                        config,
                        task_commands_enabled=True,
                        discovery_commands_enabled=True,
                        portfolio_enabled=True,
                        reads=reads,
                    )
                )
        return tuple(sorted(names))

    def build(self) -> dict[str, JsonValue]:
        """Return the complete contract as JSON-compatible data.

        Returns:
            A deterministic mapping ready for :func:`json.dumps`.

        Raises:
            RuntimeError: When a cross-check fails, so a stale or dishonest
                contract can never be written.
        """

        problems = self.verify()
        if problems:
            raise RuntimeError("; ".join(problems))
        return {
            "api_prefix": API_PREFIX,
            "authentication": self._authentication(),
            "capabilities": list(self.capability_vocabulary()),
            "decimal_fields": {name: list(fields) for name, fields in self._decimal_fields.items()},
            "enums": self._rest_enums(),
            "envelope": self._envelope(),
            "errors": {
                "application": self._application_errors(),
                "transport": {
                    code: {"http_status": status, "meaning": meaning}
                    for code, (status, meaning) in sorted(TRANSPORT_ERRORS.items())
                },
            },
            "limits": self._limits(),
            "permissions": {
                "all": sorted(permission.value for permission in ALL_CONTROL_PLANE_PERMISSIONS),
                "viewer": sorted(
                    permission.value for permission in READ_ONLY_CONTROL_PLANE_PERMISSIONS
                ),
            },
            "routes": [route.to_json() for route in self._sorted_routes()],
            "schema_version": SCHEMA_VERSION,
            "telemetry": self._telemetry(),
            "views": self._views(),
            "websocket": self._websocket(),
        }

    def render(self) -> str:
        """Return the exact text the committed contract file must contain."""

        return json.dumps(self.build(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"

    def write(self, path: Path = CONTRACT_PATH) -> None:
        """Write the contract, creating the documentation directory if needed.

        Args:
            path: Destination file.
        """

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(), encoding="utf-8")

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------
    def _sorted_routes(self) -> tuple[RouteSpec, ...]:
        return tuple(sorted(self._routes, key=lambda route: (route.path, route.method)))

    @staticmethod
    def _envelope() -> dict[str, JsonValue]:
        return {
            "error": {"error": {"code": "string", "correlation_id": "string", "message": "string"}},
            "notes": (
                "Every success body is {data, correlation_id}; every failure body is "
                "{error:{code,message,correlation_id}}. The same id is echoed in the "
                "X-Correlation-ID response header, and a client may propose one with an "
                "X-Correlation-ID request header."
            ),
            "success": {"correlation_id": "string", "data": "<response>"},
        }

    @staticmethod
    def _authentication() -> dict[str, JsonValue]:
        return {
            "header": "Authorization: Bearer <credential>",
            "principals": {
                "operator": "Every permission, including every mutation.",
                "viewer": "Every *.read permission plus telemetry.stream; no mutation.",
            },
            "unauthenticated_status": 401,
            "unauthorized_status": 403,
        }

    def _limits(self) -> dict[str, JsonValue]:
        return {
            "_note": (
                "Transport limits are the ControlPlaneHttpConfig defaults, which a deployment "
                "may override in config/control_plane.yml; treat them as the expected shape, "
                "not as constants. websocket_max_inbound_message_bytes bounds client-to-server "
                "frames only - server-to-client frames such as telemetry_snapshot are not "
                "capped by it."
            ),
            "bulk_items_max": BULK_ITEMS_MAX,
            "bulk_items_min": 1,
            "command_rate_limit": self._config.command_rate_limit,
            "command_rate_scope": "subject:action",
            "command_rate_window_seconds": self._config.command_rate_window_seconds,
            "cursor_max_chars": CURSOR_MAX_CHARS,
            "page_limit_default": PAGE_LIMIT_DEFAULT,
            "page_limit_max": PAGE_LIMIT_MAX,
            "page_limit_min": PAGE_LIMIT_MIN,
            "request_max_bytes": self._config.request_max_bytes,
            "telemetry_limit_default": self._config.telemetry_snapshot_limit,
            "telemetry_limit_max": TELEMETRY_LIMIT_MAX,
            "telemetry_limit_min": 0,
            "websocket_heartbeat_seconds": WEBSOCKET_HEARTBEAT_SECONDS,
            "websocket_max_inbound_message_bytes": WEBSOCKET_MAX_INBOUND_MESSAGE_BYTES,
            "websocket_queue_capacity": self._config.websocket_queue_capacity,
        }

    @staticmethod
    def _application_errors() -> dict[str, JsonValue]:
        mapper = ControlPlaneErrorMapper()
        errors: dict[str, JsonValue] = {}
        for reason in ControlPlaneReason:
            response = mapper.response(ControlPlaneError(reason), correlation_id="contract")
            errors[reason.value] = {
                "http_status": int(response.status),
                "message": reason.public_message,
            }
        return errors

    def _rest_enums(self) -> dict[str, JsonValue]:
        """Return REST enums plus every externally-declared field vocabulary.

        A view that projects a domain enum through ``.value`` annotates the
        field as ``str``, so the vocabulary lives outside both contract
        modules. Publishing it here keeps the contract self-sufficient: a
        generator can emit one shared union type instead of a bare string.
        """

        enums = self._enums(rest_contracts)
        for fields in self._field_vocabularies.values():
            for target in fields.values():
                name = target.rpartition(":")[2]
                members = list(resolve_vocabulary(target))
                existing = enums.get(name)
                if existing is not None and existing != members:
                    raise RuntimeError(f"vocabulary name {name!r} resolves two different ways")
                enums[name] = members
        return dict(sorted(enums.items()))

    def _views(self) -> dict[str, JsonValue]:
        vocabularies = self.resolved_vocabularies()
        return {
            name: {
                "doc": summary_line(view),
                "fields": self._fields(
                    view,
                    self._decimal_fields.get(name, ()),
                    vocabularies.get(name),
                ),
            }
            for name, view in self._view_types().items()
        }

    def _telemetry(self) -> dict[str, JsonValue]:
        payloads: dict[str, JsonValue] = {
            payload.__name__: {
                "category": payload.__dataclass_fields__["category"].default.value,
                "doc": summary_line(payload),
                "fields": self._fields(payload, ()),
            }
            for payload in self._payload_types
        }
        return {
            "enums": self._enums(telemetry_contracts),
            "event": {
                "doc": summary_line(TelemetryEvent),
                "fields": self._fields(TelemetryEvent, ()),
            },
            "event_type_by_payload": self._event_types(),
            "payloads": payloads,
            "wss_stream_names": sorted(WSS_STREAM_NAMES),
        }

    def _event_types(self) -> dict[str, JsonValue]:
        mapping: dict[str, JsonValue] = {}
        for payload in self._payload_types:
            outcomes = outcome_values(payload)
            if outcomes is None:
                mapping[payload.__name__] = event_type_for_payload(
                    cast(Any, blank_payload(payload, placeholders=PAYLOAD_FIELD_PLACEHOLDERS))
                ).value
                continue
            by_outcome = {
                outcome.value: event_type_for_payload(
                    cast(
                        Any,
                        blank_payload(
                            payload,
                            outcome=outcome,
                            placeholders=PAYLOAD_FIELD_PLACEHOLDERS,
                        ),
                    )
                ).value
                for outcome in outcomes
            }
            distinct = set(by_outcome.values())
            mapping[payload.__name__] = (
                next(iter(distinct)) if len(distinct) == 1 else dict(sorted(by_outcome.items()))
            )
        return mapping

    def _fields(
        self,
        owner: type,
        decimals: Sequence[str],
        vocabularies: Mapping[str, tuple[str, ...]] | None = None,
    ) -> list[JsonValue]:
        hints = typing.get_type_hints(owner)
        vocabulary_by_field = vocabularies or {}
        rendered: list[JsonValue] = []
        for item in dataclasses.fields(cast(Any, owner)):
            type_name, nullable = self._renderer.render(hints[item.name])
            members = vocabulary_by_field.get(item.name)
            rendered.append(
                {
                    "decimal": item.name in decimals,
                    "name": item.name,
                    "nullable": nullable,
                    "type": type_name,
                    "vocabulary": list(members) if members is not None else None,
                }
            )
        return rendered

    @staticmethod
    def _enums(module: types.ModuleType) -> dict[str, JsonValue]:
        return {
            name: [str(member.value) for member in value]
            for name, value in sorted(vars(module).items())
            if isinstance(value, type)
            and issubclass(value, Enum)
            and value.__module__ == module.__name__
        }

    @staticmethod
    def _view_types() -> dict[str, type]:
        views: dict[str, type] = {}
        for module in (rest_contracts, rest_read_contracts):
            for name, value in vars(module).items():
                if (
                    isinstance(value, type)
                    and dataclasses.is_dataclass(value)
                    and value.__module__ == module.__name__
                ):
                    views[name] = value
        return dict(sorted(views.items()))

    @staticmethod
    def _websocket() -> dict[str, JsonValue]:
        return {
            "client_frames": {"ping": {"type": "ping"}, "resync": {"type": "resync"}},
            "error_codes": dict(sorted(WEBSOCKET_ERROR_CODES.items())),
            "path": f"{API_PREFIX}/stream",
            "server_frames": {
                "error": {
                    "code": "string",
                    "retry_after_seconds": (
                        "integer, present only on the rate_limited variant, which is "
                        "immediately followed by a close"
                    ),
                    "type": "error",
                },
                "pong": {"type": "pong"},
                "resync_required": {"dropped_count": "integer", "type": "resync_required"},
                "stream_ready": {"type": "stream_ready"},
                "telemetry_event": {"data": "TelemetryEvent", "type": "telemetry_event"},
                "telemetry_snapshot": {
                    "data": "TelemetrySnapshot | null",
                    "type": "telemetry_snapshot",
                },
            },
            "text_only": True,
        }


class ApiContractCli:
    """Command-line entry point for writing or checking the contract."""

    def __init__(self, exporter: ApiContractExporter | None = None) -> None:
        """Store the exporter to run.

        Args:
            exporter: Contract builder; a default one is used when omitted.
        """

        self._exporter = exporter if exporter is not None else ApiContractExporter()

    def run(self, argv: Sequence[str] | None = None) -> int:
        """Write or verify ``docs/ui/api_contract.json``.

        Args:
            argv: Command-line arguments excluding the program name.

        Returns:
            ``0`` on success, ``1`` when a cross-check failed or the committed
            contract is stale.
        """

        options = self._parse(argv)
        try:
            rendered = self._exporter.render()
        except RuntimeError as error:
            print(f"FAIL contract cross-check: {error}", file=sys.stderr)
            return 1

        destination = cast(Path, options.output)
        if options.check:
            if not destination.exists():
                print(f"FAIL {destination} is missing; {_STALE_MESSAGE}", file=sys.stderr)
                return 1
            if destination.read_text(encoding="utf-8") != rendered:
                print(f"FAIL {destination} is out of date; {_STALE_MESSAGE}", file=sys.stderr)
                return 1
            print(f"OK {destination} matches the live control plane")
            return 0

        self._exporter.write(destination)
        print(f"wrote {destination}")
        return 0

    @staticmethod
    def _parse(argv: Sequence[str] | None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Export the control-plane API contract.")
        parser.add_argument(
            "--check",
            action="store_true",
            help="exit non-zero when the committed contract is stale",
        )
        parser.add_argument(
            "--output",
            type=Path,
            default=CONTRACT_PATH,
            help="destination file (defaults to docs/ui/api_contract.json)",
        )
        return parser.parse_args(list(argv) if argv is not None else None)


def _export_authenticator() -> ControlPlaneAuthenticator:
    """Return an authenticator holding one throwaway all-permissions credential.

    The application constructor refuses to build without a credential when
    authentication is required. The token is generated per run and never
    published, so no secret material enters the repository.
    """

    return ControlPlaneAuthenticator(
        credentials=(
            BearerCredential(
                token=secrets.token_hex(32),
                subject="contract-export",
                permissions=ALL_CONTROL_PLANE_PERMISSIONS,
            ),
        )
    )


def _export_dependencies() -> ControlPlaneDependencies:
    """Return dependencies that satisfy every conditional route registration."""

    dependency = _AllCapabilityDependency()
    values: dict[str, Any] = {
        item.name: dependency
        for item in dataclasses.fields(ControlPlaneDependencies)
        if item.name != "settings_environment"
    }
    values["settings_environment"] = "export"
    return ControlPlaneDependencies(**values)


if __name__ == "__main__":  # pragma: no cover - operator entry point
    raise SystemExit(ApiContractCli().run())
