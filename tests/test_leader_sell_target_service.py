"""Regression tests for one-shot leader SELL target re-drive generations."""

from __future__ import annotations

from decimal import Decimal
from dataclasses import replace

import pytest

from pmcopy.domain.leader_sell_target import LeaderSellTarget
from pmcopy.services.execution.coordinator import ExecutionCoordinatorResult, ExecutionIntent
from pmcopy.services.execution.leader_sell_target_service import LeaderSellTargetService


def test_recovered_sell_preserves_task_and_protective_lot_ownership() -> None:
    """Restart serialization must retain the reservation's ownership scope."""
    intent = replace(_intent("scoped"), task_id="task-a", protective_lot_id="lot-a")
    restored = LeaderSellTargetService._deserialize(LeaderSellTargetService._serialize(intent))
    assert restored.task_id == "task-a"
    assert restored.protective_lot_id == "lot-a"


class _TargetRepository:
    """Keep deterministic target generations and durable release claims."""

    def __init__(self) -> None:
        self.targets: dict[tuple[str, str, str, str], LeaderSellTarget] = {}
        self.claims: set[tuple[str, str, str, str, int, str]] = set()

    async def upsert_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        source_signal_id: str,
        intent_payload: dict[str, object],
        task_id: str | None = None,
    ) -> LeaderSellTarget:
        """Increment and return one target generation."""

        key = (user_id, leader_wallet, condition_id, token_id, task_id)
        generation = self.targets[key].generation + 1 if key in self.targets else 1
        target = LeaderSellTarget(
            user_id=user_id,
            leader_wallet=leader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            generation=generation,
            source_signal_id=source_signal_id,
            intent_payload=intent_payload,
            task_id=task_id,
        )
        self.targets[key] = target
        return target

    async def get_target(
        self,
        *,
        user_id: str,
        leader_wallet: str,
        condition_id: str,
        token_id: str,
        task_id: str | None = None,
    ) -> LeaderSellTarget | None:
        """Return the latest target."""

        return self.targets.get((user_id, leader_wallet, condition_id, token_id, task_id))

    async def claim_redrive(
        self,
        *,
        target: LeaderSellTarget,
        released_reservation_id: str,
    ) -> bool:
        """Claim each target/release pair once."""

        key = (
            target.user_id,
            target.leader_wallet,
            target.condition_id,
            target.token_id,
            target.generation,
            released_reservation_id,
        )
        if key in self.claims:
            return False
        self.claims.add(key)
        return True


class _PositionReader:
    """Return a deterministic current follower position."""

    def __init__(self, size: Decimal) -> None:
        self.size = size

    async def position_size(
        self,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> Decimal | None:
        """Return current held shares."""

        del user_id, condition_id, token_id
        return self.size


class _Coordinator:
    """Capture target re-drive intents without venue I/O."""

    def __init__(self) -> None:
        self.intents: list[ExecutionIntent] = []

    async def submit_copy_trade(self, intent: ExecutionIntent) -> ExecutionCoordinatorResult:
        """Capture and accept one re-drive intent."""

        self.intents.append(intent)
        return ExecutionCoordinatorResult(
            accepted=True,
            status="submitted_unconfirmed",
            order_id="redrive-order",
        )


def _intent(signal_id: str, *, fraction: str = "1") -> ExecutionIntent:
    """Build one protected follower SELL intent."""

    return ExecutionIntent(
        user_id="user-1",
        leader_wallet="0xleader",
        condition_id="condition-1",
        token_id="123",
        side="SELL",
        size=Decimal("62.29"),
        price=Decimal("0.21"),
        notional_usdc=Decimal("13.0809"),
        signal_id=signal_id,
        idempotency_key=signal_id,
        source="copy_trade",
        reason="leader_sell",
        recovery_metadata={"price_limit": "0.21"},
        price_limit=Decimal("0.21"),
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=False,
        leader_sell_fraction=Decimal(fraction),
        live_position_size=Decimal("62.2902"),
        sub_min_sell_policy="round_up",
    )


def _released_record(
    signal_id: str,
    reservation_id: str,
    *,
    automatic: bool = False,
    generation: int = 1,
    reserved_size: str = "54.17783035269195928218",
) -> dict[str, object]:
    """Build one terminal no-fill order record."""

    return {
        "user_id": "user-1",
        "leader_id": "0xleader",
        "condition_id": "condition-1",
        "asset": "123",
        "order_id": "old-order",
        "request": {
            "side": "SELL",
            "signal_id": signal_id,
            "recovery_metadata": {
                "reservation_id": reservation_id,
                "leader_sell_target_auto_redrive": automatic,
                "leader_sell_target_generation": generation,
                "reserved_sell_size": reserved_size,
            },
        },
    }


@pytest.mark.asyncio
async def test_older_cancel_re_drives_latest_full_exit_once_for_newly_free_shares() -> None:
    """A canceled older reservation re-drives the latest target exactly once."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("54.17783035269195928218")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.869765"))
    latest = await service.record_fresh_target(_intent("leader-sell-2", fraction="1"))

    first = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-1"),
        reason="cancelled",
    )
    repeated = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-1"),
        reason="cancelled",
    )

    assert first is True
    assert repeated is False
    assert latest.recovery_metadata["leader_sell_target_generation"] == 2
    [redrive] = coordinator.intents
    assert redrive.size == Decimal("54.17783035269195928218")
    assert redrive.live_position_size == Decimal("54.17783035269195928218")
    assert redrive.leader_sell_fraction is None
    assert redrive.signal_id != "leader-sell-2"
    assert redrive.recovery_metadata["leader_sell_target_auto_redrive"] is True
    assert redrive.recovery_metadata["leader_sell_target_release_id"] == "reservation-1"
    assert redrive.recovery_metadata["leader_sell_target_fraction"] == "1"


@pytest.mark.asyncio
async def test_partial_target_re_drives_only_fraction_of_newly_freed_older_size() -> None:
    """A newer partial SELL corrects only the capacity freed by an older cancel."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("100")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.4"))
    await service.record_fresh_target(_intent("leader-sell-2", fraction="0.5"))

    attempted = await service.redrive_after_terminal_no_fill(
        _released_record(
            "leader-sell-1",
            "reservation-1",
            generation=1,
            reserved_size="40",
        ),
        reason="cancelled",
    )

    assert attempted is True
    [redrive] = coordinator.intents
    assert redrive.size == Decimal("20")
    assert redrive.notional_usdc == Decimal("4.20")
    assert redrive.leader_sell_fraction is None
    assert redrive.live_position_size == Decimal("100")
    assert redrive.recovery_metadata["leader_sell_target_fraction"] == "0.5"


@pytest.mark.asyncio
async def test_proof_based_ambiguous_release_can_re_drive_newer_target() -> None:
    """The reconciler's actual auto-release reason is eligible for correction."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("62.2902")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.8"))
    await service.record_fresh_target(_intent("leader-sell-2", fraction="1"))

    attempted = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-ambiguous", generation=1),
        reason="auto_release_after_ambiguous_timeout",
    )

    assert attempted is True
    assert len(coordinator.intents) == 1


@pytest.mark.asyncio
async def test_malformed_released_size_fails_closed_without_claim_or_submission() -> None:
    """Corrupt recovery metadata cannot widen or trigger an automatic SELL."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("62.2902")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.8"))
    await service.record_fresh_target(_intent("leader-sell-2", fraction="1"))

    attempted = await service.redrive_after_terminal_no_fill(
        _released_record(
            "leader-sell-1",
            "reservation-corrupt",
            generation=1,
            reserved_size="not-a-decimal",
        ),
        reason="cancelled",
    )

    assert attempted is False
    assert repository.claims == set()
    assert coordinator.intents == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    [
        "manual_resolution:canceled",
        "manual_resolution:expired",
        "manual_resolution:confirmed_absent",
    ],
)
async def test_manual_resolution_no_fill_reasons_are_eligible(reason: str) -> None:
    """Operator-applied terminal no-fill proof re-drives the newer target once."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("62.2902")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.8"))
    await service.record_fresh_target(_intent("leader-sell-2", fraction="1"))

    attempted = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-manual", generation=1),
        reason=reason,
    )

    assert attempted is True
    assert len(coordinator.intents) == 1


@pytest.mark.asyncio
async def test_manual_resolution_filled_is_never_eligible() -> None:
    """A manually confirmed fill frees no shares and must never re-drive."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("62.2902")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-1", fraction="0.8"))
    await service.record_fresh_target(_intent("leader-sell-2", fraction="1"))

    attempted = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-filled", generation=1),
        reason="manual_resolution:filled",
    )

    assert attempted is False
    assert repository.claims == set()
    assert coordinator.intents == []


@pytest.mark.asyncio
async def test_latest_or_automatic_cancellation_never_loops_same_target() -> None:
    """The target's own cancellation and an auto attempt cannot repost it."""

    repository = _TargetRepository()
    coordinator = _Coordinator()
    service = LeaderSellTargetService(
        repository=repository,
        position_reader=_PositionReader(Decimal("54")),
        coordinator=coordinator,
    )
    await service.record_fresh_target(_intent("leader-sell-2"))

    latest = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-2", "reservation-latest", generation=1),
        reason="cancelled",
    )
    automatic = await service.redrive_after_terminal_no_fill(
        _released_record("automatic-attempt", "reservation-auto", automatic=True),
        reason="cancelled",
    )
    filled = await service.redrive_after_terminal_no_fill(
        _released_record("leader-sell-1", "reservation-filled"),
        reason="confirmed",
    )

    assert latest is False
    assert automatic is False
    assert filled is False
    assert coordinator.intents == []
