"""Local durable trader evidence and revision-bound operator promotion."""

from collections.abc import Callable, Mapping
from datetime import datetime
from dataclasses import replace

from domain.auto_trader import (
    AutoRoster,
    AutoTraderScope,
    AutoTraderValidation,
    CopyPerformance,
    TraderStatus,
)
from services.auto_trader import AutoTraderConflict, AutoTraderService, PromotionRejected
from services.control_plane.contracts import (
    ContractValidation,
    ControlPlaneError,
    ControlPlaneReason,
    Page,
    PageRequest,
    DiscoveryRosterView,
)
from services.control_plane.contracts_reads import (
    AutoTraderPerformanceView,
    AutoTraderEvaluationView,
    CopyPerformanceView,
)


class AutoTraderControlService:
    """Read saved evidence and delegate audited commands without provider requests."""

    def __init__(
        self,
        *,
        services: Mapping[AutoTraderScope, AutoTraderService],
        clock: Callable[[], datetime],
    ) -> None:
        """Bind the runtime's explicit account/mode scopes and current clock."""
        self._services = dict(services)
        self._scopes = {scope.follower_account: scope for scope in services}
        self._clock = clock

    async def list_discovery_roster(self, page: PageRequest) -> Page[DiscoveryRosterView]:
        """Project automatic membership into the existing paged roster contract."""
        rows: dict[str, DiscoveryRosterView] = {}
        for scope, service in self._services.items():
            roster = await service.repository.load(scope)
            for row in roster.traders:
                active = row.status is TraderStatus.ACTIVE
                current = rows.get(row.wallet)
                if current is not None and current.active and not active:
                    continue
                rows[row.wallet] = DiscoveryRosterView(
                    row.wallet,
                    "live_follow" if active and scope.trading_mode == "live" else "paper_follow",
                    active,
                    str(row.score),
                    row.observed_at or row.selected_at or self._clock(),
                )
        wallets = sorted(wallet for wallet in rows if page.cursor is None or wallet > page.cursor)
        selected = wallets[: page.limit]
        return Page(
            tuple(rows[wallet] for wallet in selected),
            selected[-1] if len(wallets) > page.limit else None,
        )

    async def performance(self, follower_account: str, wallet: str) -> AutoTraderPerformanceView:
        """Read persisted evidence; never run discovery or simulation in a GET."""
        scope, service = self._service(follower_account)
        view = self._view(scope, await service.repository.load(scope), wallet)
        history = await service.repository.history(scope, limit=50)
        rows = tuple(
            AutoTraderEvaluationView(
                row.revision,
                row.status.value,
                str(row.score),
                row.observed_at,
                row.degradation_reason,
                self._performance(row.paper),
                self._performance(row.live),
            )
            for snapshot in history
            for row in snapshot.traders
            if row.wallet == view.leader_wallet
        )
        return replace(view, history=rows)

    async def promote(
        self,
        follower_account: str,
        wallet: str,
        *,
        expected_revision: int,
        evaluation_id: str,
        request_id: str,
        actor: str,
    ) -> AutoTraderPerformanceView:
        """Approve exactly the reviewed paper evaluation under optimistic locking."""
        scope, service = self._service(follower_account)
        ContractValidation.require_count(expected_revision)
        ContractValidation.require_text(evaluation_id)
        ContractValidation.require_text(request_id)
        try:
            wallet = AutoTraderValidation.wallet(wallet)
            roster = await service.promote(
                scope,
                wallet,
                expected_revision=expected_revision,
                evaluation_id=evaluation_id,
                actor=actor,
                request_id=request_id,
            )
        except AutoTraderConflict as exc:
            raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH) from exc
        except PromotionRejected as exc:
            raise ControlPlaneError(ControlPlaneReason.CONFLICT) from exc
        except ValueError as exc:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from exc
        return self._view(scope, roster, wallet)

    def _service(self, follower_account: str) -> tuple[AutoTraderScope, AutoTraderService]:
        scope = self._scopes.get(follower_account)
        if scope is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        return scope, self._services[scope]

    def _view(
        self, scope: AutoTraderScope, roster: AutoRoster, wallet: str
    ) -> AutoTraderPerformanceView:
        try:
            wallet = AutoTraderValidation.wallet(wallet)
        except ValueError as exc:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from exc
        row = next((row for row in roster.traders if row.wallet == wallet), None)
        if row is None:
            raise ControlPlaneError(ControlPlaneReason.NOT_FOUND)
        policy = self._services[scope].policy
        reasons = (
            policy.paper_qualification(row, row.paper, self._clock())
            if row.paper
            else ("no_paper_evidence",)
        )
        return AutoTraderPerformanceView(
            scope.follower_account,
            scope.trading_mode,
            wallet,
            row.status.value,
            row.revision,
            policy.target_count,
            sum(row.status is TraderStatus.ACTIVE for row in roster.traders),
            row.activation_source,
            roster.entry_block_reason,
            str(row.score),
            row.observed_at,
            row.degradation_reason,
            reasons,
            self._performance(row.paper),
            self._performance(row.live),
        )

    @staticmethod
    def _performance(value: CopyPerformance | None) -> CopyPerformanceView | None:
        if value is None:
            return None
        return CopyPerformanceView(
            value.evaluation_id,
            value.observed_at,
            value.started_at,
            value.closed_episodes,
            str(value.realized_pnl),
            str(value.recent_pnl),
            None if value.drawdown is None else str(value.drawdown),
            value.complete,
        )
