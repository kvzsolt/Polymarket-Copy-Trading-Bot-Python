"""TradingContext assembly, startup reconciliation, drift check, and Gamma market hydration."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

from pmcopy.application.runtime_state import resolve_reservations_repo
from pmcopy.application.trading_context import TradingContext
from pmcopy.config.app_config import AppConfig
from pmcopy.integrations.polymarket.data_api_positions import DataApiPositionSizeReader
from pmcopy.orchestrators.bootstrap.reservation_wiring import (
    _bind_reservation_runtime,
    _release_expired_reservations_for_context,
)
from pmcopy.orchestrators.bootstrap.user_state import (
    UserRuntimeState,
    _ensure_user_state_account_containers,
    _user_state_telemetry,
)
from pmcopy.services.accounts.multi_user_config import RuntimeUser, UserFollowConfig
from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.positions.position_coordinator import PositionCoordinator
from pmcopy.services.risk.risk_monitor import (
    reconcile_unconfirmed_risk_orders,
    rehydrate_position_manager,
)
from pmcopy.services.trading.entry_permission_service import EntryPermissionService
from pmcopy.services.trading.leader_scoring_service import LeaderScoringService
from pmcopy.utils.fetch_data import _get_http_client, fetch_data
from pmcopy.utils.logger import get_logger

logger = get_logger("RuntimeBootstrap")


def _bootstrap_facade_attr(name: str, default: Any) -> Any:
    facade = sys.modules.get("pmcopy.orchestrators.bootstrap.runtime_bootstrap")
    return getattr(facade, name, default) if facade is not None else default


async def startup_reconcile_unconfirmed(
    contexts: list[TradingContext],
    *,
    failures_out: list[str] | None = None,
) -> dict[str, int]:
    """Reconcile all submitted_unconfirmed orders before starting runtime loops.

    Must run AFTER build_context_maps() and BEFORE risk_monitor / trade_monitor
    tasks.  This closes the non-deterministic restart window where:

    * BUY fills that landed on-chain during shutdown are invisible to the
      risk monitor (position never created locally).
    * SELL fills that landed during shutdown leave stale positions that the
      risk monitor or next trade cycle could act on.

    After reconciliation, the in-memory PositionManager is re-rehydrated so
    all downstream consumers observe converged state.

    Returns a dict mapping user_id → number of orders reconciled. Users
    whose per-user reconcile raised an exception are NOT in the result.
    Callers must inspect ``failures_out`` (when supplied) to detect
    partial failure and refuse to advertise the bot as ready.

    Parameters
    ----------
    contexts:
        Per-leader TradingContext list. Per-user uniqueness is enforced
        internally so duplicate contexts for the same user run once.
    failures_out:
        Optional list the function appends each failed ``user_id`` to.
        Pre-fix, exceptions were swallowed at WARNING with no observable
        side effect and the bot proceeded to advertise readiness on a
        stale position snapshot. Pass an empty list and refuse to set
        ``reconciler_ok`` when it ends non-empty.
    """
    from pmcopy.orchestrators.pipeline.trade_pipeline import reconcile_submitted_unconfirmed

    reconciled_by_user: dict[str, int] = {}
    seen_users: set[str] = set()

    for context in contexts:
        if context.user_id in seen_users:
            continue
        seen_users.add(context.user_id)

        if context.execution_service is None:
            continue

        try:
            trade_n = await reconcile_submitted_unconfirmed(context, startup_recovery=True)
            risk_reconcile = _bootstrap_facade_attr(
                "reconcile_unconfirmed_risk_orders", reconcile_unconfirmed_risk_orders
            )
            risk_n = await risk_reconcile(context, startup_recovery=True)
            reservation_n = await _release_expired_reservations_for_context(context)
            total_n = trade_n + risk_n + reservation_n
            if total_n > 0:
                # Position state changed — re-rehydrate so the risk monitor
                # and trade loops observe the converged snapshot.
                await _bootstrap_facade_attr("rehydrate_position_manager", rehydrate_position_manager)(context)
                reconciled_by_user[context.user_id] = total_n
                logger.info(
                    "startup_reconcile_complete | user=%s reconciled=%d "
                    "(trade=%d risk=%d reservations=%d)",
                    context.user_id,
                    total_n,
                    trade_n,
                    risk_n,
                    reservation_n,
                )
        except Exception as exc:
            logger.warning(
                "startup_reconcile_error | user=%s error=%s",
                context.user_id,
                repr(exc),
            )
            if failures_out is not None:
                failures_out.append(context.user_id)

    if reconciled_by_user:
        total = sum(reconciled_by_user.values())
        logger.info(
            "startup_reconcile_summary | users=%d total_reconciled=%d",
            len(reconciled_by_user),
            total,
        )
    else:
        logger.info("startup_reconcile_summary | no pending orders to reconcile")

    return reconciled_by_user



async def build_trading_context(
    user_state: UserRuntimeState,
    follow: UserFollowConfig,
    config: AppConfig,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> TradingContext:
    """Build the venue-neutral trading context for one follower binding."""
    if leaders_repo is not None and hasattr(leaders_repo, "for_account"):
        leaders_repo = leaders_repo.for_account(user_state.user.user_id)
    rank_multiplier = Decimal("1")
    roster_score: float | None = None
    if leaders_repo is not None and hasattr(leaders_repo, "get_roster"):
        roster_entry = await leaders_repo.get_roster(follow.leader_id)
        if roster_entry is not None:
            from pmcopy.services.trading.order_sizing import longitudinal_sizing_multiplier

            longitudinal = config.discovery.longitudinal
            roster_score = float(roster_entry.score)
            rank_multiplier = longitudinal_sizing_multiplier(
                roster_entry.longitudinal_score
                if roster_entry.longitudinal_score is not None
                else roster_entry.score,
                minimum=longitudinal.sizing_multiplier_min,
                maximum=longitudinal.sizing_multiplier_max,
                exponent=longitudinal.sizing_multiplier_exponent,
            )
    leader_scorer = LeaderScoringService(
        config=config,
        roster_score=roster_score,
    )
    account_exec_state, account_risk_state = _ensure_user_state_account_containers(user_state)
    context = TradingContext(
        user_id=user_state.user.user_id,
        wallet_address=user_state.user.wallet.funder_address,
        # Risk monitor reads ``context.client.get_price(token_id, side)``; the
        # CCXT gateway exposes exactly that surface, so the venue boundary is
        # the single read authority. The raw CCXT client does not.
        client=user_state.clob_gateway,
        positions_repo=user_state.positions_repo,
        fills_repo=user_state.fills_repo,
        orders_repo=user_state.orders_repo,
        risk_blocks_repo=user_state.risk_blocks_repo,
        pnl_tracker=user_state.pnl_tracker,
        leader_performance=user_state.leader_performance,
        risk_engine=user_state.risk_engine,
        accounting_service=user_state.accounting_service,
        position_manager=user_state.position_manager,
        config=config,
        # A04 — the ONE account-owned PositionCoordinator from
        # ``build_user_state``. Leader bindings are immutable policy
        # references; they must never mint or re-point coordination state.
        # The fallback only serves foreign/test states constructed without
        # ``build_user_state`` (the TradingContext default factory exists for
        # direct test construction; production always passes this explicitly).
        position_coordinator=(
            user_state.position_coordinator
            if getattr(user_state, "position_coordinator", None) is not None
            else PositionCoordinator()
        ),
        entry_permission_service=EntryPermissionService(user_state.risk_blocks_repo),
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        leader_scorer=leader_scorer,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=config.sizing.buy_min_amount,
        # A06 — compare exact Decimal to exact Decimal: the follow cap is
        # Decimal (strict loader) and the typed AppConfig float is converted
        # through its shortest exact decimal text. The winner flows on as a
        # Decimal; ``copyStrategy._to_decimal`` passes Decimals through
        # untouched, so sizing math stays exact end-to-end.
        buy_max_amount=min(Decimal(str(config.sizing.buy_max_amount)), follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        # Duck-typed tolerance mirrors ``follows_explicit``: foreign/test
        # follows constructed before the flag keep legacy semantics.
        market_exposure_cap_explicit=bool(getattr(follow, "market_exposure_cap_explicit", False)),
        rank_size_multiplier=rank_multiplier,
        readiness=readiness or ReadinessTracker(),
        market_data_service=getattr(user_state, "market_data_service", None),
        market_quotes=getattr(user_state, "market_store", None),
        holdings_reader=DataApiPositionSizeReader(_get_http_client),
        balance_regime=getattr(user_state, "balance_regime", None),
        telemetry=_user_state_telemetry(user_state),
        leader_wallet=follow.leader_id,
        slippage_tolerance_bps=int(config.execution.slippage_tolerance * 100),
        clob_gateway=user_state.clob_gateway,
        accounted_fill_reader=user_state.fills_repo,
        account_execution_state=account_exec_state,
        risk_state=account_risk_state,
    )
    if user_state.risk_triggers_repo is not None:
        context.risk_triggers_repo = user_state.risk_triggers_repo
    # PR6 — bind the account-owned execution coordinator. A04: the
    # coordinator is BORN with the account-owned position_coordinator (see
    # ``build_user_state`` / ``_build_execution_coordinator``); per-binding
    # rewiring of ``coord.position_coordinator`` is forbidden — it let the
    # last-built leader binding win the shared coordinator while earlier
    # bindings kept private lock maps and intent registries.
    coord = getattr(user_state, "execution_coordinator", None)
    if coord is not None:
        context.execution_coordinator = coord
        _bind_reservation_runtime(context, coord)
    # Phase 4 / A04 — seed the ACCOUNT-owned IntentRegistry from durable
    # reservation rows on restart. Must run after _bind_reservation_runtime so
    # ``context.reservations_repo`` is populated. The registry is shared
    # by every leader binding of this account, so recovery runs ONCE per
    # account (guarded by ``user_state.intents_recovered``) and is scoped to
    # this account's rows: intents from all leaders land in the one registry,
    # and another account's ownership never does. A missing reservations_repo
    # (test contexts, no-reservation deployments) is silently skipped — the
    # registry starts empty which is safe.
    _reservations_repo = resolve_reservations_repo(context)
    if _reservations_repo is not None and not getattr(user_state, "intents_recovered", False):
        try:
            from pmcopy.services.execution.intent_recovery import recover_intents_from_db

            _recovered = await recover_intents_from_db(
                registry=context.position_coordinator.intent_registry,
                reservations_repo=_reservations_repo,
                account_user_id=context.user_id,
            )
            user_state.intents_recovered = True
            if _recovered:
                logger.info(
                    "runtime_bootstrap.intent_recovery user_id=%s recovered=%d",
                    context.user_id,
                    _recovered,
                )
        except Exception as _exc:
            context.readiness.update(reconciler_ok=False)
            logger.warning(
                "runtime_bootstrap.intent_recovery_failed user_id=%s error_type=%s",
                context.user_id,
                type(_exc).__name__,
            )
            raise RuntimeError("reservation ownership recovery unavailable") from None
    inventory_refresh = getattr(user_state, "inventory_refresh", None)
    if inventory_refresh is not None:
        inventory_refresh.bind_readiness(context.readiness)
    # PR7 — the per-binding context exposes the account-owned drift service
    # for scope-pause checks. The shared ExecutionCoordinator was BORN with
    # the same instance (see ``build_user_state``); per-binding rewiring of
    # the coordinator attribute is forbidden.
    drift_svc = getattr(user_state, "position_drift_service", None)
    if drift_svc is not None:
        context.position_drift_service = drift_svc
    await _bootstrap_facade_attr("rehydrate_position_manager", rehydrate_position_manager)(context)
    return context


async def startup_drift_check(
    *,
    runtime_users,
    user_states: dict,
    config: AppConfig,
    readiness,
) -> dict[str, bool]:
    """Run position drift sweep for every user before monitors start.

    Updates ``readiness.drift_ok`` to the overall result and returns a per
    user dict for observability. Each unresolved scope pauses through the
    PositionDriftService's internal state — the coordinator gate honours it.
    """
    all_clean = True
    per_user: dict[str, bool] = {}

    if not config.reconciliation.position_drift.check_on_startup:
        readiness.update(drift_ok=True)
        return per_user

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        drift_svc = getattr(user_state, "position_drift_service", None)
        if drift_svc is None:
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        try:
            report = await drift_svc.scan_user(
                user_id=runtime_user.user_id,
                wallet=runtime_user.wallet.funder_address,
            )
        except Exception as exc:
            logger.warning(
                "startup_drift_check_failed | user=%s error=%r",
                runtime_user.user_id,
                exc,
            )
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        per_user[runtime_user.user_id] = bool(report.clean)
        if not report.clean:
            all_clean = False
            logger.warning(
                "startup_drift_check_unresolved | user=%s paused_scopes=%d",
                runtime_user.user_id,
                len(report.paused_scopes),
            )

    for user_id, clean in per_user.items():
        readiness.update_account_drift(user_id, clean)
    if not per_user:
        readiness.update(drift_ok=all_clean)
    return per_user


async def hydrate_resolved_market_state(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
) -> dict[str, int]:
    """Boot-time gate: REST-query Polymarket's ``/markets/{condition_id}``
    for every open position and mark resolved markets in the per-user
    MarketStateStore.

    Why:
        The WSS ``market_resolved`` event only fires on live resolution.
        A market that resolved while the bot was offline never re-emits the
        event on reconnect, so the position would otherwise poll ``/price``
        forever (the venue happily returns ``price=0`` indefinitely on a
        resolved binary outcome).

    The result feeds ``services/riskMonitor._is_token_resolved`` so neither
    the periodic risk refresh nor the per-position stop-loss probe issues
    HTTP requests for terminal positions.

    Returns a dict mapping user_id → count of (token, condition) pairs
    flagged resolved. Tokens already marked resolved by an earlier path
    (WSS push, prior boot) are still incremented — the operation is
    idempotent so the count reflects the boot survey, not a delta.
    """
    summary: dict[str, int] = {}

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        market_store = getattr(user_state, "market_store", None)
        positions_repo = getattr(user_state, "positions_repo", None)
        if market_store is None or positions_repo is None:
            continue

        try:
            positions = list(await positions_repo.list_open_positions(runtime_user.user_id) or [])
        except Exception as exc:
            logger.warning(
                "hydrate_resolved_market_state_list_failed | user=%s error=%r",
                runtime_user.user_id,
                exc,
            )
            continue

        tokens_by_condition: dict[str, list[str]] = {}
        for pos in positions:
            cond = str(getattr(pos, "condition_id", "") or "")
            token = str(getattr(pos, "token_id", "") or "")
            if not cond or not token:
                continue
            tokens_by_condition.setdefault(cond, []).append(token)

        resolved_count = 0
        # Gamma market metadata via the official public REST API (allowed by
        # the CCXT-only execution boundary). condition_ids are unique so this
        # is bounded by len(positions) — cheap and one-shot at boot.
        for cond, tokens in tokens_by_condition.items():
            lookup = await _lookup_gamma_market(cond)
            if lookup.unavailable:
                # Transient outage or unexpected payload shape: missing
                # evidence must never mark a position resolved — doing so
                # would disable price probes, stop-loss, and protective exits
                # for the lifetime of the process. Leave monitoring intact;
                # the WSS market_resolved event or the next boot retries.
                continue
            if lookup.market is None:
                # Gamma answered and does not index the condition: every token
                # under it is retired. Mark resolved so the risk monitor stops
                # probing them.
                for token in tokens:
                    try:
                        market_store.mark_resolved(token, cond)
                        resolved_count += 1
                    except Exception:
                        logger.debug("mark_resolved_failed", exc_info=True)
                continue
            market = lookup.market
            # Terminal signals per Polymarket OpenAPI:
            #   - ``closed: true``  → resolved binary outcome decided.
            #   - ``active: false`` → market suspended / not tradeable.
            #   - ``archived: true`` → historical market (documented on
            #     /simplified-markets responses; defensively check here too).
            closed = bool(market.get("closed"))
            inactive = market.get("active") is False
            archived = bool(market.get("archived"))
            if not (closed or inactive or archived):
                continue
            for token in tokens:
                try:
                    market_store.mark_resolved(token, cond)
                    resolved_count += 1
                except Exception:
                    logger.debug("mark_resolved_failed", exc_info=True)

        if resolved_count:
            logger.info(
                "hydrate_resolved_market_state | user=%s resolved=%d",
                runtime_user.user_id,
                resolved_count,
            )
        summary[runtime_user.user_id] = resolved_count

    return summary


_GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"


@dataclass(frozen=True)
class _GammaLookup:
    """Result of one Gamma market-metadata lookup.

    Exactly one of three states:
    * ``market`` set — an authoritative market document;
    * ``market`` None — Gamma answered and does not index the condition
      (authoritative terminal signal);
    * ``unavailable`` True — the lookup itself failed (transport error or
      unexpected payload shape). Never evidence of anything.
    """

    market: dict[str, Any] | None = None
    unavailable: bool = False


async def _lookup_gamma_market(condition_id: str) -> _GammaLookup:
    """Look up one Gamma market document by its 0x condition id.

    Gamma's ``/markets/{id}`` route accepts only the numeric Gamma market id
    (a condition id there returns 422), so this uses the ``condition_ids``
    filter, which resolves condition ids directly.
    """

    cond = str(condition_id or "").strip()
    if not cond:
        return _GammaLookup(unavailable=True)
    query = urlencode((("condition_ids", cond), ("limit", "1")))
    try:
        payload = await _bootstrap_facade_attr("fetch_data", fetch_data)(f"{_GAMMA_MARKETS_URL}?{query}", silent=True)
    except Exception as exc:
        logger.debug(
            "hydrate_resolved_gamma_unavailable | cond=%s error=%r",
            cond,
            exc,
        )
        return _GammaLookup(unavailable=True)
    if not isinstance(payload, list):
        logger.debug(
            "hydrate_resolved_gamma_unexpected_payload | cond=%s type=%s",
            cond,
            type(payload).__name__,
        )
        return _GammaLookup(unavailable=True)
    if not payload:
        return _GammaLookup(market=None)
    market = payload[0]
    if not isinstance(market, dict):
        logger.debug(
            "hydrate_resolved_gamma_malformed_row | cond=%s type=%s",
            cond,
            type(market).__name__,
        )
        return _GammaLookup(unavailable=True)
    return _GammaLookup(market=market)


async def build_context_maps(
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    active_leaders: list[str],
    config: AppConfig,
    *,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> tuple[dict[str, list[TradingContext]], dict[str, TradingContext]]:
    """Index trading contexts by account and leader identity."""
    build_context = _bootstrap_facade_attr("build_trading_context", build_trading_context)
    contexts_by_leader: dict[str, list[TradingContext]] = {leader: [] for leader in active_leaders}
    risk_contexts_by_user: dict[str, TradingContext] = {}

    for runtime_user in runtime_users:
        user_state = user_states[runtime_user.user_id]
        for leader in active_leaders:
            follow = runtime_user.follows.get(leader)
            if not follow or not follow.enabled:
                continue
            context = await build_context(
                user_state,
                follow,
                config,
                leaders_repo=leaders_repo,
                readiness=readiness,
            )
            contexts_by_leader[leader].append(context)
            if context.user_id not in risk_contexts_by_user:
                risk_contexts_by_user[context.user_id] = context

    # P1-i: a user holding open copied positions must still be risk-monitored
    # even when rotation/discovery leaves them with no enabled follow on any
    # active leader (empty active_leaders, or a follow set that no longer
    # intersects the active leaders). Otherwise their open positions go
    # unmonitored and protective exits (stop-loss / trailing) never run. The
    # risk-only context is added to ``risk_contexts_by_user`` ONLY — never to
    # ``contexts_by_leader`` — so it drives protective exits without enabling
    # any new copy entries.
    for runtime_user in runtime_users:
        if runtime_user.user_id in risk_contexts_by_user:
            continue
        risk_user_state = user_states.get(runtime_user.user_id)
        if risk_user_state is None:
            continue
        try:
            open_positions = (
                await risk_user_state.positions_repo.list_open_positions(runtime_user.user_id) or []
            )
        except Exception:
            logger.warning(
                "risk_only_context_open_check_failed | user=%s",
                runtime_user.user_id,
                exc_info=True,
            )
            continue
        if not open_positions:
            continue
        follow = next(iter(runtime_user.follows.values()), None)
        if follow is None:
            follow = UserFollowConfig(
                user_id=runtime_user.user_id,
                leader_id="",
                enabled=False,
                allocation_pct=Decimal("0"),
                max_trade_usdc=Decimal("0"),
                max_market_exposure_usdc=Decimal(str(config.risk.max_market_exposure_usdc)),
            )
        context = await build_context(
            risk_user_state,
            follow,
            config,
            leaders_repo=leaders_repo,
            readiness=readiness,
        )
        risk_contexts_by_user[runtime_user.user_id] = context
        logger.info(
            "risk_only_context_built | user=%s open_positions=%d (no active-leader follow)",
            runtime_user.user_id,
            len(open_positions),
        )

    return contexts_by_leader, risk_contexts_by_user
