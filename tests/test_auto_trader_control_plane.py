"""Authenticated revision-bound promotion of persisted paper evidence."""

from decimal import Decimal

from domain.auto_trader import AutoRoster, Trader, TraderStatus
from integrations.control_plane import ControlPlaneDependencies, ControlPlanePermission
from services.control_plane.auto_traders import AutoTraderControlService
from tests.control_plane_http.test_http_app import _client, _headers
from tests.test_auto_trader_lifecycle import performance
from tests.test_auto_trader_service import A, B, NOW, SCOPE, MemoryRepository, service


def controller():
    paper = performance()
    repo = MemoryRepository(
        AutoRoster(
            initialized=True,
            traders=(
                Trader(A, status=TraderStatus.ACTIVE),
                Trader(
                    B, score=Decimal(".8"), observed_at=NOW, qualification_streak=3, paper=paper
                ),
            ),
        )
    )
    return AutoTraderControlService(services={SCOPE: service(repo)}, clock=lambda: NOW), repo, paper


async def test_performance_read_reports_exact_evidence_and_promotion_preserves_incumbent():
    control, repo, paper = controller()
    client = await _client(
        dependencies=ControlPlaneDependencies(auto_traders=control),
        permissions=(ControlPlanePermission.LEADER_READ, ControlPlanePermission.LEADER_PROMOTE),
    )
    try:
        response = await client.get(
            f"/api/v1/leaders/{B}/performance?follower_account=follower", headers=_headers()
        )
        assert response.status == 200
        body = (await response.json())["data"]
        assert body["paper"]["evaluation_id"] == paper.evaluation_id
        assert body["qualification_reasons"] == []
        assert isinstance(body["paper"]["realized_pnl"], str)
        command = {
            "follower_account": "follower",
            "expected_revision": 1,
            "evaluation_id": paper.evaluation_id,
            "request_id": "promote-b",
        }
        response = await client.post(
            f"/api/v1/leaders/{B}/promote", json=command, headers=_headers()
        )
        assert response.status == 200
        assert repo.roster.traders[0].status is TraderStatus.ACTIVE
        assert repo.roster.traders[1].status is TraderStatus.RESERVE
        assert repo.events[-1] == ("paper_promoted", "operator-a")
        response = await client.post(
            f"/api/v1/leaders/{B}/promote", json=command, headers=_headers()
        )
        assert response.status == 200
        assert len(repo.events) == 1
    finally:
        await client.close()


async def test_viewer_cannot_promote_and_unknown_account_cannot_read():
    control, _, paper = controller()
    client = await _client(
        dependencies=ControlPlaneDependencies(auto_traders=control),
        permissions=(ControlPlanePermission.LEADER_READ,),
    )
    try:
        response = await client.post(f"/api/v1/leaders/{B}/promote", json={}, headers=_headers())
        assert response.status == 403
        response = await client.get(
            f"/api/v1/leaders/{B}/performance?follower_account=unknown", headers=_headers()
        )
        assert response.status == 404
    finally:
        await client.close()


async def test_existing_roster_read_uses_saved_automatic_membership():
    from services.control_plane.contracts import PageRequest

    control, _, _ = controller()
    page = await control.list_discovery_roster(PageRequest(limit=1))
    assert page.items[0].wallet == A and page.items[0].active
    reserve = await control.list_discovery_roster(PageRequest(limit=1, cursor=page.next_cursor))
    assert reserve.items[0].wallet == B and not reserve.items[0].active


async def test_provider_entry_block_is_visible_and_prevents_manual_promotion():
    from dataclasses import replace
    import pytest
    from services.control_plane.contracts import ControlPlaneError

    control, repo, paper = controller()
    repo.roster = replace(repo.roster, entry_block_reason="falcon_unavailable")
    view = await control.performance("follower", B)
    assert view.entry_block_reason == "falcon_unavailable"
    with pytest.raises(ControlPlaneError):
        await control.promote(
            "follower",
            B,
            expected_revision=1,
            evaluation_id=paper.evaluation_id,
            request_id="blocked",
            actor="operator",
        )


async def test_performance_includes_bounded_saved_evaluation_history():
    from unittest.mock import AsyncMock

    control, repo, paper = controller()
    repo.history = AsyncMock(return_value=(repo.roster,))
    view = await control.performance("follower", B)
    assert view.history[0].paper.evaluation_id == paper.evaluation_id
    assert view.history[0].falcon_score == "0.8"
    repo.history.assert_awaited_once_with(SCOPE, limit=50)


async def test_existing_leader_listing_reads_automatic_membership_contract():
    from orchestrators.control_plane_auto_traders import AutomaticLeaderRosterView

    control, _, _ = controller()
    rows = await AutomaticLeaderRosterView(control).list_leader_roster(10)
    assert rows[0].wallet == A
    assert rows[0].tier == "live_follow"
    assert rows[1].wallet == B
    assert rows[1].score == "0.8"
