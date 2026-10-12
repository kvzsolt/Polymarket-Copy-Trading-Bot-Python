"""Task-specific protective settings survive durable policy serialization."""

from decimal import Decimal

from services.copy_tasks.domain import CopyTask
from services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from storage.postgres.copy_task_repository import CopyTaskCodec


def test_task_protective_policy_round_trip():
    """An explicit per-fill policy is restored without numeric coercion."""
    task = CopyTask(
        task_id="protected",
        name="Protected",
        follower_account="account-a",
        leader_wallet="0x" + "1" * 40,
        protective_risk=ProtectiveRiskPolicy(take_profit_price=Decimal("0.7")),
    )
    codec = CopyTaskCodec()
    restored = codec.decode_row(
        {
            "task_id": task.task_id,
            "name": task.name,
            "follower_account": task.follower_account,
            "leader_wallet": task.leader_wallet,
            "status": "active",
            "monitoring_mode": "normal",
            "sizing_mode": "proportional",
            "notifications_muted": False,
            "version": 1,
            "configuration": codec.encode_configuration(task),
        }
    )
    assert restored.protective_risk == task.protective_risk
