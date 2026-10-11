"""Exercise generated wire descriptions against serializers and strict task input."""

from __future__ import annotations

import ast
import dataclasses
import inspect
import re
import textwrap
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from integrations.control_plane.http import ControlPlaneHttpApplication
from integrations.control_plane.serialization import ControlPlaneJsonSerializer
from integrations.control_plane.task_decoder import CopyTaskPayloadDecoder
from scripts.export_api_contract import ApiContractExporter
from services.control_plane.contracts import TaskDetailView
from services.copy_tasks.domain import CopyTask
from services.metrics import HistogramSnapshot
from services.telemetry.contracts import (
    TaskDecisionDetails,
    TelemetryEvent,
    TelemetryProducer,
    TelemetrySeverity,
    TradeEvent,
    TradeOutcome,
    TradeSide,
    event_type_for_payload,
)
from services.telemetry.query import TelemetryQueryService


@pytest.fixture
def contract() -> dict[str, Any]:
    """Build the proposed contract without reading deployment configuration."""

    return ApiContractExporter().build()


def test_all_routes_publish_resolved_wire_schemas(contract: dict[str, Any]) -> None:
    """No accepted command or returned nested record is an unresolved pseudo-type."""

    definitions = contract["json_schema"]["$defs"]
    for route in contract["routes"]:
        assert "request_schema" in route
        assert "response_schema" in route
        if route["action"]:
            assert route["request_schema"] is not None
        if route["response"] != "WebSocket":
            assert route["response_schema"] is not None

    def inspect_schema(value: Any) -> None:
        if isinstance(value, dict):
            if "$ref" in value:
                assert value["$ref"].startswith("#/$defs/")
                assert value["$ref"].removeprefix("#/$defs/") in definitions
            for child in value.values():
                inspect_schema(child)
        elif isinstance(value, list):
            for child in value:
                inspect_schema(child)

    inspect_schema(contract)
    assert "CopyTask" not in definitions
    assert "TaskCreateCommand" not in definitions


def test_task_input_is_the_decoder_payload_not_the_internal_command(
    contract: dict[str, Any],
) -> None:
    """Lifecycle and actor fields must not be generated into browser task input."""

    definitions = contract["json_schema"]["$defs"]
    create = definitions["CopyTaskCreatePayload"]
    update = definitions["CopyTaskUpdatePayload"]
    serialized = ControlPlaneJsonSerializer().to_jsonable(
        TaskDetailView.from_domain(
            CopyTask(
                task_id="task-a", name="A", leader_wallet="0x" + "1" * 40, follower_account="a"
            )
        )
    )
    assert isinstance(serialized, dict)
    task = serialized["task"]
    assert isinstance(task, dict)
    payload = {name: task[name] for name in create["properties"] if name != "configuration"}
    payload["configuration"] = serialized["configuration"]
    decoded = CopyTaskPayloadDecoder().decode(payload, task_id=None)
    assert decoded.task_id == "task-a"
    assert set(create["required"]) == set(payload)
    assert set(update["required"]) == set(payload) - {"task_id"}
    assert create["additionalProperties"] is False
    assert not {"actor", "status", "version"} & set(create["properties"])
    configuration = definitions["CopyTaskConfigurationPayload"]
    assert "protective_risk" not in configuration["required"]
    assert set(configuration["properties"]) == set(serialized["configuration"])


def test_concurrency_types_financial_precision_and_apply_semantics(
    contract: dict[str, Any],
) -> None:
    """Do not coerce settings hashes, unknown amounts, or restart metadata."""

    definitions = contract["json_schema"]["$defs"]
    assert (
        definitions["SettingsWriteRequest"]["properties"]["expected_revision"]["type"] == "string"
    )
    assert definitions["TaskVersionRequest"]["properties"]["expected_version"]["type"] == "integer"
    assert (
        definitions["RuntimeControlRequest"]["properties"]["expected_revision"]["type"] == "integer"
    )
    assert definitions["TaskVersionRequest"]["properties"]["expected_version"]["minimum"] == 1
    assert definitions["RuntimeControlRequest"]["properties"]["expected_revision"]["minimum"] == 1
    assert definitions["TaskSizingView"]["properties"]["fixed_amount"]["x-decimal"] is True
    assert {"type": "null"} in definitions["FillView"]["properties"]["fee"]["anyOf"]
    metadata = definitions["SettingsApplyMetadata"]["properties"]
    assert metadata["restart_required"]["const"] is True
    assert metadata["live_applied"]["const"] is False
    version = definitions["TaskView"]["properties"]["version"]
    assert {"type": "integer", "minimum": -(2**53 - 1), "maximum": 2**53 - 1} in version["anyOf"]


def test_request_text_and_decimal_patterns_accept_actual_wire_values(
    contract: dict[str, Any],
) -> None:
    """JSON encoding must preserve regular-expression escapes across client generators."""

    definitions = contract["json_schema"]["$defs"]
    reason = definitions["RuntimeControlRequest"]["properties"]["reason_code"]
    assert re.search(reason["pattern"], "operator_pause") is not None
    assert re.search(reason["pattern"], "   ") is None
    amount = definitions["TaskSizingView"]["properties"]["fixed_amount"]
    for value in ("0.005", "125.005", "-1.23", "0", "1e-8"):
        assert re.fullmatch(amount["pattern"], value)
    for value in ("NaN", "Infinity", "1/2", "true"):
        assert re.fullmatch(amount["pattern"], value) is None


def test_request_field_schemas_match_fields_read_by_real_handlers(contract: dict[str, Any]) -> None:
    """Catch a handler body field added without regenerating its request schema."""

    definitions = contract["json_schema"]["$defs"]
    handlers = {
        "TaskUpdateRequest": "_update_task",
        "TaskVersionRequest": "_task_version_action",
        "TaskDuplicateRequest": "_duplicate_task",
        "BulkTaskRequest": "_bulk_tasks",
        "DiscoveryRunRequest": "_run_discovery",
        "RuntimeControlRequest": "_runtime_action",
        "SettingsWriteRequest": "_put_settings",
        "LockResetRequest": "_reset_lock",
        "LeaderPromoteRequest": "_promote_leader",
    }
    for schema_name, handler in handlers.items():
        source = inspect.getsource(getattr(ControlPlaneHttpApplication, handler))
        tree = ast.parse(textwrap.dedent(source))
        names = {
            node.args[0].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "body"
            and node.func.attr == "get"
            and isinstance(node.args[0], ast.Constant)
        }
        assert set(definitions[schema_name]["properties"]) == names
        assert "actor" not in names


def test_metrics_schema_matches_live_histogram_boundary_strings(contract: dict[str, Any]) -> None:
    """Histogram infinity is a boundary label, never a JSON numeric infinity."""

    class Metrics:
        """Provide one deterministic runtime histogram projection."""

        def snapshot(self) -> tuple[dict[str, float], dict[str, float]]:
            """Supply deterministic counters and gauges."""
            return {"received": 1.0}, {"depth": 0.0}

        def histogram_snapshots(self) -> tuple[HistogramSnapshot, ...]:
            """Supply actual typed histogram bucket boundary strings."""
            return (
                HistogramSnapshot(
                    name="latency",
                    labels=(),
                    count=1,
                    total=0.005,
                    buckets=(("0.01", 1), ("+Inf", 1)),
                    sample_count=1,
                    p50=0.005,
                    p95=0.005,
                    p99=0.005,
                ),
            )

    class Clock:
        """Return a fixed query observation time."""

        def now(self) -> datetime:
            """Supply an aware timestamp without reading wall time."""
            return datetime(2026, 9, 17, tzinfo=timezone.utc)

    query = object.__new__(TelemetryQueryService)
    query._metrics = Metrics()
    query._clock = Clock()
    snapshot = query.metrics_snapshot()
    definition = contract["json_schema"]["$defs"]["MetricsSnapshot"]
    assert set(snapshot) == set(definition["properties"])
    histogram = definition["properties"]["histograms"]["items"]
    assert set(snapshot["histograms"][0]) == set(histogram["properties"])
    bucket = histogram["properties"]["buckets"]["items"]
    assert bucket["properties"]["le"]["type"] == "string"
    assert snapshot["histograms"][0]["buckets"][-1]["le"] == "+Inf"


def test_stream_schema_keys_match_actual_emitted_frame_literals(contract: dict[str, Any]) -> None:
    """Detect dropped stream metadata using the real adapter's frame construction."""

    definitions = contract["json_schema"]["$defs"]
    frames = contract["websocket"]["server_frame_schemas"]
    tree = ast.parse(textwrap.dedent(inspect.getsource(ControlPlaneHttpApplication)))
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        literal = {
            key.value: value
            for key, value in zip(node.keys, node.values, strict=True)
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        kind = literal.get("type")
        if not isinstance(kind, ast.Constant) or kind.value not in frames:
            continue
        found.add(kind.value)
        schema = definitions[frames[kind.value]["$ref"].removeprefix("#/$defs/")]
        candidates = schema.get("anyOf", [schema])
        assert any(set(candidate["properties"]) == set(literal) for candidate in candidates)
        assert set(contract["websocket"]["server_frames"][kind.value]) >= set(literal)
    assert found == set(frames)


def test_snapshot_event_schema_matches_real_projection_separately_from_live(
    contract: dict[str, Any],
) -> None:
    """The bounded snapshot currently omits decision data carried by live events."""

    now = datetime(2026, 9, 17, tzinfo=timezone.utc)
    payload = TradeEvent(
        outcome=TradeOutcome.SUBMITTED,
        task_ref="task-a",
        side=TradeSide.BUY,
        decision=TaskDecisionDetails(
            account_ref="a",
            task_version="1",
            reason_code="allowed",
            source_price=Decimal("0.005"),
            source_shares=Decimal("3.1"),
            source_timestamp=None,
            evaluated_at=now,
            max_signal_age_seconds=1,
            price_limit=Decimal("0.0075"),
        ),
    )
    event = TelemetryEvent(
        event_id="event-a",
        event_type=event_type_for_payload(payload),
        timestamp=now,
        severity=TelemetrySeverity.INFO,
        correlation_id=None,
        payload=payload,
        producer=next(iter(TelemetryProducer)),
        sequence=1,
    )
    live = ControlPlaneJsonSerializer().to_jsonable(event)
    snapshot = TelemetryQueryService._event_to_json(event)
    definitions = contract["json_schema"]["$defs"]
    assert set(definitions["TradeEvent"]["properties"]) == set(live["payload"])
    assert set(definitions["TelemetrySnapshotTradeEvent"]["properties"]) == set(snapshot["payload"])
    assert "decision" not in definitions["TelemetrySnapshotTradeEvent"]["properties"]
    assert set(definitions["TaskDecisionDetails"]["properties"]) == {
        field.name for field in dataclasses.fields(TaskDecisionDetails)
    }


@pytest.mark.asyncio
async def test_snapshot_schema_describes_actual_adapter_frame(contract: dict[str, Any]) -> None:
    """Exercise a real sender path without opening a network connection."""

    class Socket:
        """Capture a wire frame in memory."""

        def __init__(self) -> None:
            self.frames: list[dict[str, Any]] = []

        async def send_json(self, data: dict[str, Any]) -> None:
            """Store serialized frame data."""
            self.frames.append(data)

    application = object.__new__(ControlPlaneHttpApplication)
    application._dependencies = SimpleNamespace(telemetry_query=None, telemetry_hub=None)
    application._serializer = ControlPlaneJsonSerializer()
    socket = Socket()

    class Sender:
        """Delegate directly, avoiding wall-clock timeout machinery in the fixture."""

        async def send(self, target: Socket, payload: dict[str, Any]) -> None:
            """Publish one frame to the captured sink."""
            await target.send_json(payload)

    application._stream_sender = Sender()
    await application._send_snapshot(socket)
    frame = socket.frames[0]
    ref = contract["websocket"]["server_frame_schemas"]["telemetry_snapshot"]["$ref"]
    schema = contract["json_schema"]["$defs"][ref.removeprefix("#/$defs/")]
    assert set(schema["properties"]) == set(frame)
    assert frame["data"] is None
    assert frame["authoritative_financial_state"] is False


def test_native_trader_projection_exports_closed_mode_and_restart_vocabulary(
    contract: dict[str, Any],
) -> None:
    """Configured and effective source states have separate stable wire vocabularies."""
    fields = contract["json_schema"]["$defs"]["TraderDetectionState"]["properties"]
    assert fields["desired_mode"]["enum"] == ["mined", "pending"]
    assert fields["effective_mode"]["enum"] == ["mined", "pending", "paused"]
    assert fields["provider"]["enum"] == ["alchemy", "infura"]
    assert fields["restart_required"]["enum"] == [True]
