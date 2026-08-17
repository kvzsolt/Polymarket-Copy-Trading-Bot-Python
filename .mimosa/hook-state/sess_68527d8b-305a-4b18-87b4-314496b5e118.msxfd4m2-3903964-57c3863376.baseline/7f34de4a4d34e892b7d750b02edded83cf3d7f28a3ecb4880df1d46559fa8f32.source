"""Strict frontend task payload decoding tests."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal

import pytest

from integrations.control_plane.serialization import ControlPlaneJsonSerializer
from services.control_plane.contracts import (
    ControlPlaneError,
    ControlPlaneReason,
    TaskDetailView,
)
from services.copy_tasks.domain import CopyTask, CopyTaskStatus, TaskSpendPolicy


def domain_task() -> CopyTask:
    """Return one task with exact non-binary financial values."""

    return CopyTask(
        task_id="task-a",
        name="Primary",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        spend=TaskSpendPolicy(max_total=Decimal("125.005")),
    )


def frontend_payload() -> dict[str, object]:
    """Build the documented write shape from the typed read projection."""

    serialized = ControlPlaneJsonSerializer().to_jsonable(TaskDetailView.from_domain(domain_task()))
    assert isinstance(serialized, dict)
    summary = serialized["task"]
    configuration = serialized["configuration"]
    assert isinstance(summary, dict)
    return {
        "task_id": summary["task_id"],
        "name": summary["name"],
        "leader_wallet": summary["leader_wallet"],
        "follower_account": summary["follower_account"],
        "monitoring_mode": summary["monitoring_mode"],
        "sizing_mode": summary["sizing_mode"],
        "notifications_muted": summary["notifications_muted"],
        "configuration": configuration,
    }


def test_decoder_round_trips_typed_configuration_with_exact_decimals() -> None:
    """Decode the frontend write contract without binary-float conversion."""

    from integrations.control_plane.task_decoder import CopyTaskPayloadDecoder

    decoded = CopyTaskPayloadDecoder().decode(frontend_payload(), task_id=None)

    assert decoded.task_id == "task-a"
    assert decoded.status is CopyTaskStatus.PAUSED
    assert decoded.spend.max_total == Decimal("125.005")
    assert decoded.version == 1


@pytest.mark.parametrize(
    "mutator",
    (
        lambda value: value.update({"status": "active"}),
        lambda value: value.update({"private_key": "forbidden"}),
        lambda value: value["configuration"].update({"unknown": {}}),
        lambda value: value["configuration"]["spend"].update({"unknown": "1"}),
    ),
)
def test_decoder_rejects_unknown_or_lifecycle_fields(mutator: object) -> None:
    """Reject generic patching, secret-shaped keys, and hidden activation."""

    from integrations.control_plane.task_decoder import CopyTaskPayloadDecoder

    payload = deepcopy(frontend_payload())
    mutator(payload)  # type: ignore[operator]

    with pytest.raises(ControlPlaneError) as captured:
        CopyTaskPayloadDecoder().decode(payload, task_id=None)

    assert captured.value.reason is ControlPlaneReason.INVALID_REQUEST


def test_decoder_requires_path_identity_to_match_optional_payload_identity() -> None:
    """Prevent a task update body from targeting a second task identity."""

    from integrations.control_plane.task_decoder import CopyTaskPayloadDecoder

    with pytest.raises(ControlPlaneError) as captured:
        CopyTaskPayloadDecoder().decode(frontend_payload(), task_id="task-b")

    assert captured.value.reason is ControlPlaneReason.INVALID_REQUEST
