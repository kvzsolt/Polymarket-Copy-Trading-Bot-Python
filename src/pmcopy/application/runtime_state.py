"""Typed account-scoped runtime state and legacy ``extra`` bridge for ``TradingContext``.

``TradingContext`` is constructed per ``(account, leader)`` follow binding (plus one
protective risk context per account), while execution coordination, reconciliation
backoff, venue concurrency semaphores, coarse 24-hour ``MAX_POSITION_PCT`` balance
snapshots, and protective risk-monitor caches are **account-scoped** concerns owned
by :class:`pmcopy.orchestrators.bootstrap.runtime_bootstrap.UserRuntimeState`.

Separating :class:`AccountExecutionRuntimeState` and :class:`AccountRiskRuntimeState`
from per-binding ``TradingContext`` fields ensures:

1. Production code never reads or writes untyped ``context.extra[...]`` keys.
2. Multi-leader accounts share one ``ReconciliationService`` and one 24-hour
   ``PositionPctCapSnapshot`` per follower account while keeping separate follower
   accounts strictly isolated.
3. Existing unit tests that construct ``TradingContext(..., extra={...})`` or
   lightweight ``SimpleNamespace(extra={...})`` doubles continue to work unchanged
   through :class:`ContextExtraBridge` and typed resolver helpers.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Final, override

if TYPE_CHECKING:
    from pmcopy.interfaces.clob_gateway import ClobGateway
    from pmcopy.interfaces.repositories import AbstractReservationRepository
    from pmcopy.services.execution.pending_order_reconciliation import (
        PendingOrderReconciliationQueryService,
    )
    from pmcopy.services.execution.reconciliation_service import ReconciliationService
    from pmcopy.services.execution.reservation_service import ReservationService
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import (
        LeaderSellBuyScopeGuard,
    )

POSITION_PCT_CAP_REFRESH_SECONDS: Final[float] = 24.0 * 60.0 * 60.0


def _coerce_optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _coerce_optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _coerce_optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no", ""}:
        return False
    return None


@dataclass
class PositionPctCapSnapshot:
    """Coarse 24-hour wallet balance snapshot backing ``MAX_POSITION_PCT``."""

    balance_usdc: Decimal | None = None
    captured_at: float | None = None
    wallet: str = ""

    def seed(
        self,
        balance_usdc: Decimal,
        *,
        wallet: str,
        captured_at: float | None = None,
    ) -> None:
        """Record a non-negative balance snapshot for ``wallet``."""
        self.balance_usdc = max(Decimal("0"), balance_usdc)
        self.captured_at = float(captured_at) if captured_at is not None else time.time()
        self.wallet = str(wallet or "").strip()

    def is_fresh(self, *, wallet: str, now: float | None = None) -> bool:
        """Return ``True`` when a valid snapshot for ``wallet`` is within 24 hours."""
        if self.balance_usdc is None or self.balance_usdc < 0 or self.captured_at is None:
            return False
        cached_wallet = str(self.wallet or "").strip()
        target_wallet = str(wallet or "").strip()
        wallet_matches = not cached_wallet or cached_wallet.lower() == target_wallet.lower()
        current = time.time() if now is None else float(now)
        age_seconds = max(0.0, current - float(self.captured_at))
        return wallet_matches and age_seconds < POSITION_PCT_CAP_REFRESH_SECONDS


@dataclass
class AccountExecutionRuntimeState:
    """Account-scoped execution & reconciliation collaborators shared per follower."""

    reconciliation_service: ReconciliationService | None = None
    pending_order_query_service: PendingOrderReconciliationQueryService | None = None
    trade_analysis_semaphore: asyncio.Semaphore | None = None
    reconcile_unconfirmed_semaphore: asyncio.Semaphore | None = None
    position_pct_cap_snapshot: PositionPctCapSnapshot = field(
        default_factory=PositionPctCapSnapshot
    )
    reconciler_callback_bound: bool = False
    last_prefetched_metadata_by_token: dict[str, Any] = field(default_factory=dict)


@dataclass
class AccountRiskRuntimeState:
    """Account-scoped risk-monitor caches, cooldowns, and pre-reconcile backoffs."""

    venue_circuit_breaker: Any | None = None
    price_cache: Any | None = None
    check_interval_seconds: float = 0.25
    enable_balance_tiered_policy: bool | None = None
    warned_invalid_tokens: set[str] = field(default_factory=set)
    active_reservation_skip_logged_at: dict[str, float] = field(default_factory=dict)
    no_fill_prune_attempted_at: dict[str, float] = field(default_factory=dict)
    exit_reject_cooldown_until: dict[str, float] = field(default_factory=dict)
    pre_reconcile_backoff: Any | None = None


_NULLABLE_BRIDGED_KEYS: Final[frozenset[str]] = frozenset(
    {
        "clob_gateway",
        "accounted_fill_reader",
        "reservation_service",
        "reservations_repo",
        "leader_sell_target_service",
        "leader_sell_scope_guard",
        "task_budget_settlement",
        "audit_logger",
        "_reconciliation_service",
        "_pending_order_query_service",
        "_trade_analysis_semaphore",
        "_reconcile_unconfirmed_semaphore",
        "_max_position_pct_balance_snapshot_usdc",
        "_max_position_pct_balance_snapshot_at",
        "venue_circuit_breaker",
        "risk_price_cache",
        "enable_balance_tiered_policy",
        "risk_pre_reconcile_backoff",
    }
)

_ALL_BRIDGED_KEYS: Final[frozenset[str]] = _NULLABLE_BRIDGED_KEYS | frozenset(
    {
        "slippage_tolerance_bps",
        "leader_wallet",
        "_copy_execution_metadata_by_token",
        "_max_position_pct_balance_snapshot_wallet",
        "risk_check_interval_seconds",
        "warned_invalid_tokens",
        "risk_active_reservation_skip_logged_at",
        "risk_no_fill_prune_attempted_at",
        "risk_exit_reject_cooldown_until",
    }
)


class ContextExtraBridge(dict[str, Any]):
    """Bidirectional ``dict`` view mapping legacy ``extra`` keys to typed fields.

    Subclasses ``dict`` so ``isinstance(ctx.extra, dict)`` and
    ``isinstance(ctx.extra, Mapping)`` hold for legacy callers/tests while
    keeping typed fields on ``TradingContext``, ``AccountExecutionRuntimeState``,
    and ``AccountRiskRuntimeState`` authoritative.
    """

    def __init__(
        self,
        context: Any,
        initial: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self._context = context
        if initial:
            for key, value in initial.items():
                self[key] = value
        self._sync_all_from_typed()

    def _exec_state(self) -> AccountExecutionRuntimeState:
        state = getattr(self._context, "account_execution_state", None)
        if not isinstance(state, AccountExecutionRuntimeState):
            state = AccountExecutionRuntimeState()
            self._context.account_execution_state = state
        return state

    def _risk_state(self) -> AccountRiskRuntimeState:
        state = getattr(self._context, "risk_state", None)
        if not isinstance(state, AccountRiskRuntimeState):
            state = AccountRiskRuntimeState()
            self._context.risk_state = state
        return state

    def _read_typed(self, key: str) -> tuple[bool, Any]:
        ctx = self._context
        exec_state = self._exec_state()
        risk_state = self._risk_state()
        snapshot = exec_state.position_pct_cap_snapshot

        if key == "slippage_tolerance_bps":
            return True, int(getattr(ctx, "slippage_tolerance_bps", 200))
        if key == "leader_wallet":
            val: Any = str(getattr(ctx, "leader_wallet", "") or "")
            return (bool(val) or super().__contains__(key)), val
        if key in {
            "clob_gateway",
            "accounted_fill_reader",
            "reservation_service",
            "reservations_repo",
            "leader_sell_target_service",
            "leader_sell_scope_guard",
            "task_budget_settlement",
            "audit_logger",
        }:
            val = getattr(ctx, key, None)
            return val is not None, val
        if key == "_reconciliation_service":
            val = exec_state.reconciliation_service
            return val is not None, val
        if key == "_pending_order_query_service":
            val = exec_state.pending_order_query_service
            return val is not None, val
        if key == "_trade_analysis_semaphore":
            val = exec_state.trade_analysis_semaphore
            return val is not None, val
        if key == "_reconcile_unconfirmed_semaphore":
            val = exec_state.reconcile_unconfirmed_semaphore
            return val is not None, val
        if key == "_copy_execution_metadata_by_token":
            return True, exec_state.last_prefetched_metadata_by_token
        if key == "_max_position_pct_balance_snapshot_usdc":
            val = snapshot.balance_usdc
            return val is not None, val
        if key == "_max_position_pct_balance_snapshot_at":
            val = snapshot.captured_at
            return val is not None, val
        if key == "_max_position_pct_balance_snapshot_wallet":
            val = snapshot.wallet
            return bool(val), val
        if key == "venue_circuit_breaker":
            val = risk_state.venue_circuit_breaker
            return val is not None, val
        if key == "risk_price_cache":
            val = risk_state.price_cache
            return val is not None, val
        if key == "risk_check_interval_seconds":
            return True, risk_state.check_interval_seconds
        if key == "enable_balance_tiered_policy":
            val = risk_state.enable_balance_tiered_policy
            if val is None:
                return False, None
            return True, ("true" if val else "false")
        if key == "warned_invalid_tokens":
            return True, risk_state.warned_invalid_tokens
        if key == "risk_active_reservation_skip_logged_at":
            return True, risk_state.active_reservation_skip_logged_at
        if key == "risk_no_fill_prune_attempted_at":
            return True, risk_state.no_fill_prune_attempted_at
        if key == "risk_exit_reject_cooldown_until":
            return True, risk_state.exit_reject_cooldown_until
        if key == "risk_pre_reconcile_backoff":
            val = risk_state.pre_reconcile_backoff
            return val is not None, val
        return False, None

    def _write_typed(self, key: str, value: Any) -> bool:
        ctx = self._context
        exec_state = self._exec_state()
        risk_state = self._risk_state()
        snapshot = exec_state.position_pct_cap_snapshot

        if key == "slippage_tolerance_bps":
            try:
                ctx.slippage_tolerance_bps = int(float(value))
            except (TypeError, ValueError):
                ctx.slippage_tolerance_bps = 200
            return True
        if key == "leader_wallet":
            ctx.leader_wallet = str(value or "").strip()
            return True
        if key in {
            "clob_gateway",
            "accounted_fill_reader",
            "reservation_service",
            "reservations_repo",
            "leader_sell_target_service",
            "leader_sell_scope_guard",
            "task_budget_settlement",
            "audit_logger",
        }:
            setattr(ctx, key, value)
            return True
        if key == "_reconciliation_service":
            exec_state.reconciliation_service = value
            return True
        if key == "_pending_order_query_service":
            exec_state.pending_order_query_service = value
            return True
        if key == "_trade_analysis_semaphore":
            exec_state.trade_analysis_semaphore = value
            return True
        if key == "_reconcile_unconfirmed_semaphore":
            exec_state.reconcile_unconfirmed_semaphore = value
            return True
        if key == "_copy_execution_metadata_by_token":
            exec_state.last_prefetched_metadata_by_token = (
                dict(value) if isinstance(value, Mapping) else {}
            )
            return True
        if key == "_max_position_pct_balance_snapshot_usdc":
            snapshot.balance_usdc = _coerce_optional_decimal(value)
            return True
        if key == "_max_position_pct_balance_snapshot_at":
            snapshot.captured_at = _coerce_optional_float(value)
            return True
        if key == "_max_position_pct_balance_snapshot_wallet":
            snapshot.wallet = str(value or "").strip()
            return True
        if key == "venue_circuit_breaker":
            risk_state.venue_circuit_breaker = value
            return True
        if key == "risk_price_cache":
            risk_state.price_cache = value
            return True
        if key == "risk_check_interval_seconds":
            parsed = _coerce_optional_float(value)
            risk_state.check_interval_seconds = parsed if parsed and parsed > 0 else 0.25
            return True
        if key == "enable_balance_tiered_policy":
            risk_state.enable_balance_tiered_policy = _coerce_optional_bool(value)
            return True
        if key == "warned_invalid_tokens":
            risk_state.warned_invalid_tokens = set(value) if isinstance(value, set) else set()
            return True
        if key == "risk_active_reservation_skip_logged_at":
            risk_state.active_reservation_skip_logged_at = (
                dict(value) if isinstance(value, dict) else {}
            )
            return True
        if key == "risk_no_fill_prune_attempted_at":
            risk_state.no_fill_prune_attempted_at = dict(value) if isinstance(value, dict) else {}
            return True
        if key == "risk_exit_reject_cooldown_until":
            risk_state.exit_reject_cooldown_until = dict(value) if isinstance(value, dict) else {}
            return True
        if key == "risk_pre_reconcile_backoff":
            risk_state.pre_reconcile_backoff = value
            return True
        return False

    def _sync_key_from_typed(self, key: str) -> None:
        if key not in _ALL_BRIDGED_KEYS:
            return
        present, value = self._read_typed(key)
        if present:
            super().__setitem__(key, value)
        else:
            super().pop(key, None)

    def _sync_all_from_typed(self) -> None:
        for key in _ALL_BRIDGED_KEYS:
            self._sync_key_from_typed(key)

    @override
    def __contains__(self, key: object) -> bool:
        if isinstance(key, str) and key in _ALL_BRIDGED_KEYS:
            self._sync_key_from_typed(key)
        return super().__contains__(key)

    @override
    def __getitem__(self, key: str) -> Any:
        if key in _ALL_BRIDGED_KEYS:
            self._sync_key_from_typed(key)
        return super().__getitem__(key)

    @override
    def __setitem__(self, key: str, value: Any) -> None:
        if key in _ALL_BRIDGED_KEYS:
            self._write_typed(key, value)
            if key == "leader_wallet" and value is not None:
                super().__setitem__(key, str(value))
                return
            self._sync_key_from_typed(key)
            return
        super().__setitem__(key, value)

    @override
    def __delitem__(self, key: str) -> None:
        if key in _ALL_BRIDGED_KEYS:
            self._write_typed(key, None)
        super().__delitem__(key)

    @override
    def get(self, key: str, default: Any = None) -> Any:
        if key in _ALL_BRIDGED_KEYS:
            self._sync_key_from_typed(key)
        return super().get(key, default)

    @override
    def setdefault(self, key: str, default: Any = None) -> Any:
        if key in self:
            return self[key]
        self[key] = default
        return self[key]

    @override
    def pop(self, key: str, *args: Any) -> Any:
        if key in _ALL_BRIDGED_KEYS:
            self._sync_key_from_typed(key)
            present = super().__contains__(key)
            val = super().pop(key, *args)
            if present:
                self._write_typed(key, None)
            return val
        return super().pop(key, *args)

    @override
    def update(self, *args: Any, **kwargs: Any) -> None:
        if args:
            other = args[0]
            if isinstance(other, Mapping):
                for key, value in other.items():
                    self[key] = value
            else:
                for key, value in other:
                    self[key] = value
        for key, value in kwargs.items():
            self[key] = value


def _ensure_legacy_double_bound(context: Any) -> None:
    """Attach typed state & ``ContextExtraBridge`` onto synthetic test doubles."""
    has_exec = isinstance(
        getattr(context, "account_execution_state", None),
        AccountExecutionRuntimeState,
    )
    has_risk = isinstance(getattr(context, "risk_state", None), AccountRiskRuntimeState)
    raw_extra = getattr(context, "extra", None)
    if has_exec and has_risk and (raw_extra is None or isinstance(raw_extra, ContextExtraBridge)):
        return
    try:
        if not has_exec:
            context.account_execution_state = AccountExecutionRuntimeState()
        if not has_risk:
            context.risk_state = AccountRiskRuntimeState()
        if isinstance(raw_extra, Mapping) and not isinstance(raw_extra, ContextExtraBridge):
            context.extra = ContextExtraBridge(context, raw_extra)
    except AttributeError:
        # Frozen/slotted test doubles without mutable attributes: leave untouched.
        return


def resolve_account_execution_state(context: object) -> AccountExecutionRuntimeState:
    """Return the account-scoped execution state for ``context``."""
    _ensure_legacy_double_bound(context)
    state = getattr(context, "account_execution_state", None)
    if isinstance(state, AccountExecutionRuntimeState):
        return state
    return AccountExecutionRuntimeState()


def resolve_risk_state(context: object) -> AccountRiskRuntimeState:
    """Return the account-scoped risk-monitor state for ``context``."""
    _ensure_legacy_double_bound(context)
    state = getattr(context, "risk_state", None)
    if isinstance(state, AccountRiskRuntimeState):
        return state
    return AccountRiskRuntimeState()


def resolve_slippage_tolerance_bps(context: object, default: int = 200) -> int:
    """Return the configured slippage tolerance in basis points."""
    _ensure_legacy_double_bound(context)
    raw = getattr(context, "slippage_tolerance_bps", None)
    if raw is None:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def resolve_leader_wallet(context: object) -> str:
    """Return the normalized leader wallet bound to ``context``, if any."""
    _ensure_legacy_double_bound(context)
    for attr in ("leader_wallet", "leader_id", "leader_address"):
        val = str(getattr(context, attr, "") or "").strip().lower()
        if val:
            return val
    return ""


def resolve_clob_gateway(context: object) -> ClobGateway | None:
    """Return the typed ``ClobGateway`` bound to ``context``, if any."""
    _ensure_legacy_double_bound(context)
    return getattr(context, "clob_gateway", None)


def resolve_accounted_fill_reader(context: object) -> Any | None:
    """Return the accounted fill reader bound to ``context``, if any."""
    _ensure_legacy_double_bound(context)
    return getattr(context, "accounted_fill_reader", None)


def resolve_reservation_service(context: Any) -> ReservationService | None:
    """Return the account-owned ``ReservationService`` for ``context``."""
    _ensure_legacy_double_bound(context)
    svc = getattr(context, "reservation_service", None)
    if svc is not None:
        return svc
    coordinator = getattr(context, "execution_coordinator", None)
    svc = getattr(coordinator, "reservation_service", None)
    if svc is not None:
        try:
            context.reservation_service = svc
        except AttributeError:
            pass
    return svc


def resolve_reservations_repo(context: Any) -> AbstractReservationRepository | None:
    """Return the account-owned ``AbstractReservationRepository`` for ``context``."""
    _ensure_legacy_double_bound(context)
    repo = getattr(context, "reservations_repo", None)
    if repo is not None:
        return repo
    coordinator = getattr(context, "execution_coordinator", None)
    repo = getattr(coordinator, "reservations_repo", None)
    if repo is None:
        svc = resolve_reservation_service(context)
        repo = getattr(svc, "reservations_repo", None)
    if repo is not None:
        try:
            context.reservations_repo = repo
        except AttributeError:
            pass
    return repo


def resolve_leader_sell_target_service(context: object) -> Any | None:
    """Return the account-owned ``LeaderSellTargetService`` for ``context``."""
    _ensure_legacy_double_bound(context)
    svc = getattr(context, "leader_sell_target_service", None)
    if svc is not None:
        return svc
    coordinator = getattr(context, "execution_coordinator", None)
    return getattr(coordinator, "leader_sell_target_service", None)


def resolve_leader_sell_scope_guard(context: object) -> LeaderSellBuyScopeGuard | None:
    """Return the ``LeaderSellBuyScopeGuard`` bound to ``context``, if any."""
    _ensure_legacy_double_bound(context)
    return getattr(context, "leader_sell_scope_guard", None)


def resolve_task_budget_settlement(context: object) -> Any | None:
    """Return the V1 ``TaskBudgetReconciliationService`` bound to ``context``."""
    _ensure_legacy_double_bound(context)
    return getattr(context, "task_budget_settlement", None)


def bind_task_budget_settlement(
    context: Any,
    settlement: Any,
    *,
    overwrite: bool = True,
) -> bool:
    """Bind ``settlement`` onto ``context`` and its active ``ReconciliationService``."""
    _ensure_legacy_double_bound(context)
    existing = getattr(context, "task_budget_settlement", None)
    if existing is not None and not overwrite:
        return False
    try:
        context.task_budget_settlement = settlement
    except AttributeError:
        return False
    exec_state = resolve_account_execution_state(context)
    if exec_state.reconciliation_service is not None:
        exec_state.reconciliation_service.task_budget_settlement = settlement
    return True


def resolve_enable_balance_tiered_policy(context: object) -> bool:
    """Resolve ``enable_balance_tiered_policy`` from explicit state or ``AppConfig.risk``."""
    risk_state = resolve_risk_state(context)
    if risk_state.enable_balance_tiered_policy is not None:
        return risk_state.enable_balance_tiered_policy
    config = getattr(context, "config", None)
    risk_cfg = getattr(config, "risk", None) if config is not None else None
    configured = getattr(risk_cfg, "enable_balance_tiered_policy", None)
    if isinstance(configured, bool):
        return configured
    if isinstance(configured, str):
        return configured.strip().lower() in {"1", "true", "yes"}
    return False


__all__ = [
    "AccountExecutionRuntimeState",
    "AccountRiskRuntimeState",
    "ContextExtraBridge",
    "POSITION_PCT_CAP_REFRESH_SECONDS",
    "PositionPctCapSnapshot",
    "bind_task_budget_settlement",
    "resolve_account_execution_state",
    "resolve_accounted_fill_reader",
    "resolve_clob_gateway",
    "resolve_enable_balance_tiered_policy",
    "resolve_leader_sell_scope_guard",
    "resolve_leader_sell_target_service",
    "resolve_leader_wallet",
    "resolve_reservation_service",
    "resolve_reservations_repo",
    "resolve_risk_state",
    "resolve_slippage_tolerance_bps",
    "resolve_task_budget_settlement",
]
