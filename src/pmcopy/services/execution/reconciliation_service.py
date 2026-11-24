"""Canonical reconciliation service (Issue #71).

Sole owner of terminal order settlement and startup convergence.
Decomposed into ``reconciliation_models``, ``reconciliation_fill_confirmer``,
``reconciliation_probes``, ``reconciliation_trade``, and ``reconciliation_risk``
while preserving the canonical ``ReconciliationService`` facade.
"""

from __future__ import annotations

import random  # noqa: F401
import time  # noqa: F401
from typing import Any

from pmcopy.services.execution.fill_accounting import AccountedFillReader
from pmcopy.services.execution.reconciliation_fill_confirmer import (
    _ReconciliationFillConfirmerMixin,
)
from pmcopy.services.execution.reconciliation_models import (
    _FINAL_FILL_STATUSES_RISK,  # noqa: F401
    _FINAL_FILL_STATUSES_TRADE,  # noqa: F401
    _MARKET_ATTRIBUTION_MIN_AGE_SECONDS,  # noqa: F401
    _MARKET_ATTRIBUTION_POST_WINDOW_SECONDS,  # noqa: F401
    _MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS,  # noqa: F401
    _MAX_PROBE_ORDER_ID_CANDIDATES,  # noqa: F401
    _RESTING_ORDER_STATUSES,  # noqa: F401
    _TERMINAL_FILLED_SUBMISSION_STATUSES,  # noqa: F401
    _TERMINAL_NO_FILL_STATUSES,  # noqa: F401
    _VENUE_NON_TERMINAL,  # noqa: F401
    _VENUE_ORDER_NOT_FOUND_THRESHOLD,  # noqa: F401
    _VENUE_TO_LIFECYCLE,  # noqa: F401
    ClobGateway,  # noqa: F401
    ConfirmFillCallable,  # noqa: F401
    FillsRepo,  # noqa: F401
    LeaderSellTargetRedriver,
    OrderRecord,  # noqa: F401
    OrdersRepo,  # noqa: F401
    PositionsRepo,  # noqa: F401
    ReconciliationResult,  # noqa: F401
    ReservationService,  # noqa: F401
    _decimal_or_zero,  # noqa: F401
    _fills_cover_order,  # noqa: F401
    _matched_amount_from_response,  # noqa: F401
    _normalize_trade_status,  # noqa: F401
    _normalize_venue_status,  # noqa: F401
    _order_id_candidates,  # noqa: F401
    _order_id_set,  # noqa: F401
    _order_is_fully_filled,  # noqa: F401
    _order_is_resting,  # noqa: F401
    _probe_order_id_candidates,  # noqa: F401
    _record_created_at,  # noqa: F401
    _state_filled_size,  # noqa: F401
    _state_original_size,  # noqa: F401
    _state_value,  # noqa: F401
)
from pmcopy.services.execution.reconciliation_probes import _ReconciliationProbesMixin
from pmcopy.services.execution.reconciliation_risk import _ReconciliationRiskMixin
from pmcopy.services.execution.reconciliation_trade import _ReconciliationTradeMixin
from pmcopy.services.execution.recovery_inventory import RecoveryInventoryReader
from pmcopy.services.execution.user_order_evidence import UserOrderEvidenceParser
from pmcopy.services.telemetry import ExecutionTelemetryEmitter
from pmcopy.utils.logger import get_logger

_log = get_logger("ReconciliationService")


class ReconciliationService(
    _ReconciliationFillConfirmerMixin,
    _ReconciliationProbesMixin,
    _ReconciliationTradeMixin,
    _ReconciliationRiskMixin,
):
    """Sole owner of terminal order settlement.

    Constructed once per user runtime; safe to share across
    ``trade_pipeline`` and ``riskMonitor`` callers. The ``orders_repo`` /
    ``fills_repo`` / ``positions_repo`` arguments accept either the
    Protocol-based mocks (used by ``reconcile_order``) or the concrete
    ``storage.postgres.*`` repos (used by ``reconcile_trade_record`` and
    ``reconcile_risk_record``).

    The optional ``telemetry`` emitter publishes the three operator-visible
    reconciliation transitions — an applied matched-size delta (the only
    *observed fill* the event stream reports, since a submission is never a
    fill), a negative delta (drift), and an auto-released ambiguous
    submission. It is synchronous, never awaited, and never raises; ``None``
    disables all three.
    """

    def __init__(
        self,
        orders_repo: Any,
        fills_repo: Any,
        positions_repo: Any,
        reservation_service: Any | None = None,
        clob_gateway: Any | None = None,
        db: Any | None = None,
        audit_logger: Any | None = None,
        risk_blocks_repo: Any | None = None,
        risk_triggers_repo: Any | None = None,
        funder_address: str | None = None,
        order_confirmation_cfg: Any | None = None,
        task_budget_settlement: Any | None = None,
        telemetry: ExecutionTelemetryEmitter | None = None,
        user_order_evidence_parser: UserOrderEvidenceParser | None = None,
        leader_sell_target_service: LeaderSellTargetRedriver | None = None,
        accounted_fill_reader: AccountedFillReader | None = None,
        recovery_inventory: RecoveryInventoryReader | None = None,
    ):
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.reservation_service = reservation_service
        self.clob_gateway = clob_gateway
        self.db = db
        self.audit_logger = audit_logger
        self.risk_blocks_repo = risk_blocks_repo
        self.risk_triggers_repo = risk_triggers_repo
        self.order_confirmation_cfg = order_confirmation_cfg
        self.task_budget_settlement = task_budget_settlement
        # Operator telemetry for post-submission truth: observed fills, drift,
        # and ambiguous-release resolutions. ``None`` disables all three; the
        # emitter itself is synchronous and never raises.
        self._telemetry = telemetry
        self._user_order_evidence_parser = (
            user_order_evidence_parser
            if user_order_evidence_parser is not None
            else UserOrderEvidenceParser()
        )
        self.leader_sell_target_service = leader_sell_target_service
        self._accounted_fill_reader = accounted_fill_reader
        self._recovery_inventory = recovery_inventory
        # Funder wallet for the user_activities write-back. When set, every
        # terminal "filled_reconciled" emission also writes a bot=true row
        # into user_activities so the manual-sync detector has a single
        # source of truth for tx_hash dedup. None preserves legacy behaviour
        # for bot_cli / test contexts that don't need the write-back.
        self.funder_address = funder_address.lower() if funder_address else None
        self._venue_poll_backoffs: dict[str, dict[str, Any]] = {}
        self._last_order_poll_at: dict[str, float] = {}
        # Per-order 404 streak used only for throttled observability. Missing
        # lookup evidence never settles or unlocks an order.
        self._venue_not_found_counts: dict[str, int] = {}
        # Per-order auto-release pacing: (next_probe_at_monotonic, interval).
        # An ambiguous order that cannot be released (venue outage, probe
        # errors) is re-probed on an escalating schedule instead of every
        # reconcile tick.
        self._auto_release_next_probe_at: dict[str, tuple[float, float]] = {}
        # Per-order abort streak, used to throttle abort logs/audits.
        self._auto_release_abort_counts: dict[str, int] = {}
        # Per-order monotonic timestamp of the last "stalled" audit emission.
        self._ambiguous_stalled_last_audit: dict[str, float] = {}

