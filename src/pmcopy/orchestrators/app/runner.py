"""Supervised application lifecycle runner: startup, streams, detection, and shutdown."""

from __future__ import annotations

import asyncio
import contextlib
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext

from pmcopy.bootstrap import build_app as _default_build_app
from pmcopy.config.app_config import EnvValidationError, load_config as _default_load_config
from pmcopy.config.env import is_valid_eth_address
from pmcopy.domain.account_identity import CopierAccountId
from pmcopy.integrations.polymarket.venue_runtime import (
    build_polymarket_runtime as _default_build_polymarket_runtime,
)
from pmcopy.orchestrators.app.builders import (
    _app_facade_attr,
    _attach_market_data_runtime_to_contexts,
    _build_leader_sell_fraction_resolver,
    _build_telemetry_emitters,
    _cold_start_monitored_after_reconcile,
    _copy_task_contexts,
    _detection_connection_probe,
    _discovery_refresh_loop,
    _reap_reservations_once,
    _refresh_contexts_for_new_leaders,
    _refresh_onchain_watched_wallets,
    _reject_duplicate_user_state,
    _should_wait_for_discovery_roster,
    _shutdown_runtime,
    _start_market_data_runtimes,
    _start_telemetry_runtime as _default_start_telemetry_runtime,
    _start_user_data_runtimes,
    _subscribe_context_tokens,
    _to_decimal_balance,
    _validate_detection_confidence_floor as _default_validate_detection_confidence_floor,
    _wait_for_active_leaders,
    _wallets_blocked_by_reconcile_failures,
)
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import (
    UserRuntimeState,
    build_context_maps,
    build_user_state as _default_build_user_state,
    ensure_runtime_follows,
    fetch_live_positions as _default_fetch_live_positions,
    hydrate_resolved_market_state,
    startup_reconcile_unconfirmed,
)
from pmcopy.orchestrators.discovery.leader_registry import LeaderRegistryState
from pmcopy.orchestrators.discovery.runtime_discovery import (
    startup_refresh_targets as _startup_refresh_targets,
)
from pmcopy.orchestrators.lifecycle.account_inventory_startup import InitialInventorySync
from pmcopy.orchestrators.lifecycle.app_lifecycle import (
    HealthServerResource as _HealthServerResource,
    SupervisedTaskSpec as _Supervised,
    TaskSupervisor,
    flatten_contexts as _flatten_contexts,
)
from pmcopy.orchestrators.lifecycle.runtime_summary import print_startup_summary
from pmcopy.orchestrators.lifecycle.startup_health import (
    perform_startup_health_check as _default_perform_startup_health_check,
)
from pmcopy.orchestrators.lifecycle.trader_startup_policy import TraderStartupPolicy
from pmcopy.orchestrators.streaming.market_data_runtime import MarketDataRuntime
from pmcopy.orchestrators.streaming.user_data_runtime import UserDataRuntime
from pmcopy.services.accounts.account_lease import (
    AccountLeaseManager as _default_account_lease_manager,
    account_lease_renewal_loop,
)
from pmcopy.services.accounts.copier_config import select_runtime_copiers
from pmcopy.services.accounts.multi_user_config import load_runtime_users
from pmcopy.services.execution.circuit_breaker import CircuitBreaker
from pmcopy.services.observability.health_server import start_health_server
from pmcopy.services.observability.metrics import metrics
from pmcopy.utils.log_redaction import register_config_secrets
from pmcopy.utils.logger import (
    configure_logging as _default_configure_logging,
    get_logger,
    print_error as _default_print_error,
    redact_error_detail,
)
from pmcopy.utils.wallet_balance import get_my_balance as _default_get_my_balance

logger = get_logger("Main")


async def _run_application(
    client_registry: list[Any],
    *,
    startup_policy: TraderStartupPolicy | None = None,
    evidence_runtime: Any | None = None,
    copier: str | None = None,
) -> int:
    """Run one fully composed application under a leader startup policy."""
    configure_logging = _app_facade_attr("configure_logging", _default_configure_logging)
    load_config = _app_facade_attr("load_config", _default_load_config)
    print_error = _app_facade_attr("print_error", _default_print_error)
    _validate_detection_confidence_floor = _app_facade_attr(
        "_validate_detection_confidence_floor", _default_validate_detection_confidence_floor
    )
    build_app = _app_facade_attr("build_app", _default_build_app)
    _start_telemetry_runtime = _app_facade_attr(
        "_start_telemetry_runtime", _default_start_telemetry_runtime
    )
    AccountLeaseManager = _app_facade_attr("AccountLeaseManager", _default_account_lease_manager)
    build_polymarket_runtime = _app_facade_attr(
        "build_polymarket_runtime", _default_build_polymarket_runtime
    )
    build_user_state = _app_facade_attr("build_user_state", _default_build_user_state)
    get_my_balance = _app_facade_attr("get_my_balance", _default_get_my_balance)
    fetch_live_positions = _app_facade_attr("fetch_live_positions", _default_fetch_live_positions)
    perform_startup_health_check = _app_facade_attr(
        "perform_startup_health_check", _default_perform_startup_health_check
    )
    startup_policy = startup_policy or TraderStartupPolicy()
    configure_logging(level="INFO")

    try:
        startup_config = load_config()
        startup_policy.validate(startup_config)
        startup_config = startup_policy.effective_config(startup_config)
        _validate_detection_confidence_floor(startup_config)
    except EnvValidationError as exc:
        print_error(str(exc))
        return 1

    # Populate the logging secret-value registry from the validated config
    # (wallet private key, DB DSN, Falcon/relayer tokens).
    # This is the single registration seam: it never logs what it collects,
    # and logging stays safe by default via key/pattern redaction even when
    # it registers nothing (e.g. tests or pre-config logging).
    register_config_secrets(startup_config)

    configure_logging(
        level=startup_config.logging.level,
        log_dir=startup_config.logging.log_dir,
        jsonl_file=startup_config.logging.app_log_jsonl_file,
        max_bytes=startup_config.logging.log_max_bytes,
        backup_count=startup_config.logging.log_backup_count,
    )

    from pmcopy.orchestrators.bootstrap.provider_runtime import ProviderRuntime
    from pmcopy.domain.provider_budget import ProviderConfigurationError

    try:
        provider_runtime = ProviderRuntime.from_config(startup_config)
    except ProviderConfigurationError:
        print_error(
            "Approved provider budgets and endpoint configuration must be verified before startup."
        )
        return 1
    client_registry.append(provider_runtime)

    from pmcopy.orchestrators.evidence.evidence_runtime import EvidenceRuntime
    from pmcopy.domain.evidence import EvidenceError

    # ``evidence.enabled`` is the operator's only opt-out. It defaults to true in
    # ``EvidenceConfig``, so a deployment that never sets it still fails closed on
    # a missing private credential. Turning it off only disables durable capture
    # and remote publication: native-owner detection still composes and
    # dispatches, discarding raw observations instead of pausing. Pending
    # evaluation genuinely needs a durable sink and fails closed in ``compose``.
    evidence_settings = startup_config.evidence
    if evidence_runtime is None and not evidence_settings.enabled:
        if evidence_settings.history_journal_path is not None:
            print_error(
                "Durable evidence is disabled while evidence.history_journal_path is "
                "configured. Enable evidence, or unset history_journal_path."
            )
            return 1
    else:
        try:
            if evidence_runtime is None:
                evidence_runtime = EvidenceRuntime.from_config(startup_config)
            client_registry.append(evidence_runtime)
            await evidence_runtime.start()
        except (EvidenceError, OSError):
            print_error(
                "Durable local evidence storage and private receiver configuration are required."
            )
            return 1

    import sqlite3

    from pmcopy.orchestrators.evidence.history_evidence_drain_runtime import HistoryEvidenceDrainRuntime

    if evidence_runtime is not None:
        try:
            history_drain = HistoryEvidenceDrainRuntime.from_config(
                startup_config.evidence, evidence_runtime
            )
            if history_drain is not None:
                client_registry.append(history_drain)
                await history_drain.start()
        except (ValueError, OSError, sqlite3.Error):
            print_error("The configured historical evidence journal could not be opened safely.")
            return 1

    from pmcopy.bot_cli.commands.doctor import run_doctor_checks

    mode = "paper" if startup_config.execution.paper_trade_only else "live"

    if mode == "live":
        doctor_result = await run_doctor_checks(
            "live",
            config=startup_config,
            auto_trader=startup_policy.auto_trader,
            provider_runtime=provider_runtime,
        )
        if doctor_result.get("status") == "blocked":
            print_error("Live mode doctor check failed. Refusing to start.")
            blocked = [
                c
                for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "blocked"
            ]
            for check in blocked:
                print_error(f"  ❌ {check.get('name')}: {check.get('detail', '')}")
                fix = check.get("fix")
                if fix:
                    print_error(f"     fix: {fix}")
            import json

            print(json.dumps(doctor_result, indent=2))
            return 1
        if doctor_result.get("status") == "warning":
            warnings = [
                c
                for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "warning"
            ]
            for check in warnings:
                logger.warning(
                    "doctor_warning | name=%s detail=%s fix=%s",
                    check.get("name"),
                    check.get("detail"),
                    check.get("fix"),
                )
    application = await build_app(config=startup_config, provider_runtime=provider_runtime)
    telemetry_runtime = await _start_telemetry_runtime(
        application=application,
        client_registry=client_registry,
    )
    telemetry_emitters = _build_telemetry_emitters(telemetry_runtime)
    if application.falcon_client is not None:
        client_registry.append(application.falcon_client)

    application.readiness.update(
        wss_market_ok=False,
        wss_market_connecting=False,
        wss_user_ok=False,
        wss_user_connecting=False,
        market_data_ready=False,
    )

    # Start observability before any discovery-only promotion wait. A fresh
    # roster can require multiple longitudinal cycles; operators must still be
    # able to inspect a fail-closed readiness payload during that warm-up.
    _slowest_loop_interval = max(
        float(application.config.detection.fetch_interval),
        float(application.config.risk.risk_check_interval),
    )
    application.readiness.configure_heartbeat_budget(
        staleness_seconds=_slowest_loop_interval * 3.0,
    )
    heartbeat_state: dict[str, Any] = {}
    metrics_runner: _HealthServerResource | None = None
    if application.config.app.enable_metrics_server:
        metrics_runner = _HealthServerResource(
            await start_health_server(
                application.config.app.metrics_port,
                tracker=application.readiness,
                host=application.config.app.metrics_bind_host,
                telemetry_query=telemetry_runtime.query if telemetry_runtime is not None else None,
                telemetry_hub=telemetry_runtime.hub if telemetry_runtime is not None else None,
                detection_telemetry=lambda: (
                    det.telemetry_snapshot()
                    if (det := heartbeat_state.get("detection")) is not None
                    and hasattr(det, "telemetry_snapshot")
                    else None
                ),
            )
        )
        client_registry.append(metrics_runner)

    configured = startup_policy.configured_leaders(application.config)
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]

    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex (0x + 40 hex digits).'
            )
        return 1

    auto_traders = None
    auto_runtime_users = None
    if startup_policy.auto_trader:
        from pmcopy.orchestrators.discovery.auto_trader_runtime import AutoTraderRuntime
        from pmcopy.storage.postgres.connection import get_pool as get_auto_trader_pool

        # Task 22 — the --copier selector limits routing AND runtime
        # ownership, so it is applied at BOTH roster load sites before any
        # account runtime or lease exists.
        try:
            auto_runtime_users = select_runtime_copiers(
                load_runtime_users([], application.config), copier
            )
        except ValueError as exc:
            print_error(str(exc))
            return 1
        auto_traders = AutoTraderRuntime.create(
            application=application, pool=await get_auto_trader_pool(), users=auto_runtime_users
        )
        application.auto_traders = auto_traders
        automatic_wallets = await auto_traders.startup(
            legacy_wallets=await application.leaders_repo.list_active()
        )
        registry_state = LeaderRegistryState(active_monitored=automatic_wallets, mode="automatic")
    else:
        registry_state = await application.registry.refresh(
            configured,
            force_discovery=startup_policy.force_discovery,
            target_wallets=await _startup_refresh_targets(
                application.registry,
                force_discovery=startup_policy.force_discovery,
            ),
        )
    active_leaders = registry_state.active_monitored

    invalid_active = [wallet for wallet in active_leaders if not is_valid_eth_address(wallet)]
    if invalid_active:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "invalid_active_leaders",
                "invalid_wallets": invalid_active,
                "scheduler_enabled": application.config.discovery.scheduler.enabled,
                "falcon_enabled": application.config.discovery.falcon.enabled,
                "force_discovery": startup_policy.force_discovery,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error("No active leaders available. Auto-selected leaders were invalid.")
        return 1

    if not active_leaders and auto_traders is None:
        if _should_wait_for_discovery_roster(
            application.config,
            force_discovery=startup_policy.force_discovery,
        ):
            logger.info(
                startup_policy.startup_roster_event,
                extra={
                    "rediscover_seconds": application.config.discovery.scheduler.rediscover_seconds,
                    "required_cycles": application.config.discovery.hysteresis.min_tenure_cycles,
                },
            )
            active_leaders = await _wait_for_active_leaders(
                application.registry,
                configured,
                application.config.discovery.scheduler.rediscover_seconds,
                config=application.config,
                initial_state=registry_state,
                force_discovery=startup_policy.force_discovery,
            )
        else:
            # Fall through to the fail-closed branch below rather than waiting
            # startup_wait_timeout_seconds for a roster that cannot arrive.
            logger.info(
                "startup_roster_wait_skipped",
                extra={
                    "reason": "roster cannot supply startup wallets",
                    "discovery_enabled": (application.config.discovery.scheduler.enabled),
                    "start_from_roster": (application.config.discovery.scheduler.start_from_roster),
                    "discovery_has_token": application.config.discovery.api.has_token,
                },
            )

    if not active_leaders and auto_traders is None:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "manual_and_roster_empty",
                "scheduler_enabled": application.config.discovery.scheduler.enabled,
                "falcon_enabled": application.config.discovery.falcon.enabled,
                "force_discovery": startup_policy.force_discovery,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error(
            "No active leaders available. No enabled traders in "
            "config/traders/*.yml and auto trader detection did not find "
            "eligible candidates."
        )
        return 1

    try:
        runtime_users = select_runtime_copiers(
            (
                auto_runtime_users
                if auto_runtime_users is not None
                else load_runtime_users(active_leaders, application.config)
            ),
            copier,
        )
    except ValueError as exc:
        # An unknown --copier id must fail closed here: never start an
        # unexpected subset of accounts (addon §7 partial-startup policy).
        print_error(str(exc))
        return 1
    if not runtime_users:
        print_error("No runtime users configured.")
        return 1

    application.readiness.update(runtime_user_count=len(runtime_users))

    application.readiness.update(db_ok=True)

    # Greptile P1 (PR #107): the bot used to reach
    # ``DiscoveryCaptureService.create()`` without ever calling
    # ``connect_db()`` — that service internally calls ``get_pool()``
    # which would raise ``RuntimeError`` and crash startup. Call
    # ``connect_db()`` here so the asyncpg pool exists before any
    # downstream component asks for it.
    from pmcopy.config.db import connect_db as _connect_db

    await _connect_db()

    from pmcopy.storage.postgres.connection import get_pool

    # Acquire the pool first so it's available to the per-user wiring below
    # even when the (non-critical, post-lease) purge step fails. Previously
    # `_pool` was scoped inside the try/except, so a degraded purge would
    # NameError on the subsequent `build_user_state(..., _pool, ...)`
    # reference.
    _pool = await get_pool()
    from pmcopy.adapters.time.clock import SystemReceiptClock
    from pmcopy.orchestrators.evidence.evidence_drain_runtime import EvidenceDrainRuntime
    from pmcopy.storage.postgres.evidence_drain import PostgresEvidenceDrain
    from pmcopy.storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    evidence_projection = PostgresFillEvidenceProjection(
        clock=SystemReceiptClock(),
        execution_mode=application.config.execution.trading_mode,
    )
    # The database-to-spool drain has nothing to append to without a local
    # evidence owner, so it is composed only alongside one. Composing it against
    # a missing sink would leave a worker retrying forever instead of reporting
    # an honest absence.
    evidence_drain = None
    if evidence_runtime is not None:
        evidence_drain = EvidenceDrainRuntime(
            PostgresEvidenceDrain(_pool, evidence_runtime, batch_size=25)
        )
        client_registry.append(evidence_drain)
        # Final-review fix: the worker itself starts only AFTER every
        # account lease is acquired below. The drain reads the shared
        # database outbox; a doomed second process (lease refused) must
        # never drain it. The registry entry stays here so shutdown closes
        # the drain on every path (aclose is a no-op when never started).
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import LeaderSellScopeGuard
    from pmcopy.storage.postgres.leader_sell_scope_repo import LeaderSellSafetyScopeRepo

    leader_sell_scope_guard = LeaderSellScopeGuard(
        store=LeaderSellSafetyScopeRepo(_pool),
    )

    leaders_repo = auto_traders if auto_traders is not None else application.leaders_repo
    registry = application.registry
    refresh_interval = application.config.discovery.scheduler.rediscover_seconds

    # Follows are explicit per-copier whitelists (A05). This call applies
    # the documented legacy default — the single account whose follows did
    # NOT come from explicit configuration (the config-less fallback)
    # auto-follows the active leaders. That default re-applies at every
    # dynamic refresh too, so the lone config-less account keeps
    # auto-following newly discovered leaders. Every other deployment
    # copies exactly what its follows configuration lists.
    ensure_runtime_follows(runtime_users, active_leaders, application.config)

    user_states: dict[str, UserRuntimeState] = {}

    # Task 21 — cross-process account lease: acquire every account's lease
    # BEFORE any venue runtime or user state exists, so a second process
    # configured for the same wallet+database fails closed here instead of
    # becoming a second active writer. ONE lease per account regardless of
    # paper/live mode: both modes write money-shaped rows (reservations,
    # orders, positions) to this database. Renewal is a supervised loop
    # below; graceful shutdown releases via ``_shutdown_runtime`` (and the
    # client registry on exception paths).
    account_lease_manager = AccountLeaseManager(pool=_pool)
    for runtime_user in runtime_users:
        lease_outcome = await account_lease_manager.acquire(
            CopierAccountId.from_runtime_user(runtime_user),
            mode=mode,
        )
        if not lease_outcome.acquired:
            held_by = lease_outcome.held_by
            print_error(
                "refusing to start a second writer for this account: lease for "
                f"{runtime_user.user_id!r} is held by another active process "
                f"(holder host={getattr(held_by, 'holder_host', '?')}, "
                f"expires_at={getattr(held_by, 'expires_at', '?')})."
            )
            await account_lease_manager.release_all()
            return 1
    client_registry.append(account_lease_manager)
    logger.info(
        "startup_stage | stage=account_leases_acquired users=%d mode=%s",
        len(runtime_users),
        mode,
    )
    # Leases held — now the shared-outbox drain worker may start (see the
    # composition note above: composition precedes the lease loop, the
    # worker start deliberately does not).
    if evidence_drain is not None:
        await evidence_drain.start()

    # Follow-up fix: the retention purge is a shared-database WRITE, so it
    # runs only AFTER every account lease is acquired (mirroring the drain
    # ordering above): a doomed second process (lease refused) must never
    # delete rows from the shared database. Skipping the purge on the
    # refused path needs no cleanup — stale rows simply survive until the
    # next lease-holding start.
    from pmcopy.storage.postgres.orders_repo import OrdersRepo as _PgOrdersRepo

    _max_age = float(application.config.app.retention_max_age_seconds)
    try:
        purged = await _PgOrdersRepo(_pool).purge_old_processed_trades(_max_age)
    except RuntimeError:
        # Defensive: should not fire now that connect_db() runs above,
        # but kept so a degraded PG init does not block startup on a
        # non-critical purge.
        purged = 0
    if purged:
        logger.info("Startup cleanup: purged %s stale records", purged)

    venue_circuit_breaker = CircuitBreaker.for_venue_config(
        application.config,
        on_state_change=lambda st: application.readiness.update(circuit_breaker_state=st.value),
    )

    for runtime_user in runtime_users:
        venue_runtime = await build_polymarket_runtime(
            runtime_user=runtime_user,
            config=application.config,
            latency=telemetry_emitters.latency if telemetry_emitters is not None else None,
        )
        # The CCXT runtime is the single venue owner; close it on shutdown.
        client_registry.append(venue_runtime)
        # A06 — wallet binding above may have re-keyed this user onto a
        # resolved Deposit Wallet address; a collision with an already-built
        # state must fail fast instead of silently overwriting it.
        _reject_duplicate_user_state(user_states, runtime_user.user_id)
        user_states[runtime_user.user_id] = build_user_state(
            runtime_user,
            venue_runtime.client,
            _pool,
            application.config,
            clob_gateway=venue_runtime.gateway,
            telemetry=telemetry_emitters,
            copy_admission=auto_traders,
            provider_runtime=provider_runtime,
            evidence_projection=evidence_projection,
            venue_circuit_breaker=venue_circuit_breaker,
        )

    logger.info(
        "startup_stage | stage=clob_clients_ready users=%d",
        len(user_states),
    )
    application.readiness.update(
        gateway_ok=True,
        circuit_breaker_state="closed",
    )

    logger.info(
        "startup_stage | stage=balance_checks_started leaders=%d users=%d",
        len(active_leaders),
        len(runtime_users),
    )
    leader_balances_raw = await asyncio.gather(
        *(get_my_balance(wallet, provider_runtime=provider_runtime) for wallet in active_leaders)
    )
    user_balances_raw = await asyncio.gather(
        *(
            get_my_balance(runtime_user.wallet.funder_address, provider_runtime=provider_runtime)
            for runtime_user in runtime_users
        )
    )

    # INF-01 (CLAUDE.md no-float-money): keep balances as ``Decimal`` for
    # the entire startup health / cap / threshold pass; downstream
    # consumers cast to float only at display boundaries.
    user_balances: dict[str, Decimal] = {
        runtime_user.user_id: _to_decimal_balance(balance)
        for runtime_user, balance in zip(runtime_users, user_balances_raw, strict=True)
    }

    api_ok = all(value is not None for value in leader_balances_raw) and all(
        value is not None for value in user_balances_raw
    )
    logger.info(
        "startup_stage | stage=balance_checks_completed api_ok=%s",
        api_ok,
    )
    application.readiness.update(api_ok=api_ok)

    live_positions_by_user: dict[str, list[dict]] = {}
    health_reports_by_user = {}

    for runtime_user in runtime_users:
        logger.info(
            "startup_stage | stage=live_positions_fetch_started user=%s",
            runtime_user.user_id,
        )
        # None == fetch failed (fail-closed); the inventory inspection treats it
        # distinctly from a genuinely-empty snapshot. Store [] for downstream
        # consumers but pass the raw value (None-aware) to the health check.
        live_positions = await fetch_live_positions(runtime_user.wallet.funder_address)
        live_positions_by_user[runtime_user.user_id] = live_positions or []
        logger.info(
            "startup_stage | stage=live_positions_fetch_completed user=%s positions=%s",
            runtime_user.user_id,
            "fetch_failed" if live_positions is None else len(live_positions),
        )

        logger.info(
            "startup_stage | stage=startup_health_started user=%s",
            runtime_user.user_id,
        )
        health_reports_by_user[runtime_user.user_id] = await perform_startup_health_check(
            wallet=runtime_user.wallet.funder_address,
            user_id=runtime_user.user_id,
            pool=_pool,
            positions_repo=user_states[runtime_user.user_id].positions_repo,
            # PR #105 review: must pass orders_repo so the atomic-lock gate
            # actually probes the repo. Without it the check fails closed
            # (was a silent warning previously).
            orders_repo=user_states[runtime_user.user_id].orders_repo,
            live_positions=live_positions,
            available_cash=user_balances.get(runtime_user.user_id, Decimal("0")),
            database_ok=True,
            rpc_url=application.config.wallet.polygon_rpc_url,
            clob_gateway=user_states[runtime_user.user_id].clob_gateway,
            require_live_clob=not application.config.dry_run,
            provider_runtime=provider_runtime,
        )
        _health_report = health_reports_by_user[runtime_user.user_id]
        logger.info(
            "startup_stage | stage=startup_health_completed user=%s status=%s healthy=%s",
            runtime_user.user_id,
            getattr(_health_report, "status", "unknown"),
            getattr(_health_report, "healthy", False),
        )

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        runtime_users,
        user_states,
        active_leaders,
        application.config,
        leaders_repo=leaders_repo,
        readiness=application.readiness,
    )
    for context in {
        id(candidate): candidate
        for candidate in (
            *risk_contexts_by_user.values(),
            *(item for values in contexts_by_leader.values() for item in values),
        )
    }.values():
        context.leader_sell_scope_guard = leader_sell_scope_guard
    from pmcopy.orchestrators.pipeline.trade_pipeline import seed_position_pct_cap_snapshot

    seeded_user_ids: set[str] = set()
    for context in risk_contexts_by_user.values():
        seed_position_pct_cap_snapshot(
            context,
            user_balances.get(context.user_id, Decimal("0")),
        )
        seeded_user_ids.add(context.user_id)
    for contexts in contexts_by_leader.values():
        for context in contexts:
            if context.user_id in seeded_user_ids:
                continue
            seed_position_pct_cap_snapshot(
                context,
                user_balances.get(context.user_id, Decimal("0")),
            )
            seeded_user_ids.add(context.user_id)

    monitored_leaders = [leader for leader, contexts in contexts_by_leader.items() if contexts]
    application.readiness.update(monitored_leader_count=len(monitored_leaders))

    if not monitored_leaders:
        logger.warning(
            "no_leaders_monitored",
            extra={"reason": "no_user_follows_active_leaders"},
        )
        metrics.inc("no_leaders_monitored_total")

    # Prepare the task lifecycle before startup reconciliation so orders-table
    # evidence can settle retained task budgets. Workers remain stopped until
    # account state and drift have converged and no detection source exists yet.
    from pmcopy.orchestrators.copy_tasks.v1_runtime_bootstrap import start_v1_copy_task_runtime

    logger.info("startup_stage | stage=v1_copy_task_restoration_started")
    initial_v1_contexts = _copy_task_contexts(contexts_by_leader, risk_contexts_by_user)
    v1_runtime = await start_v1_copy_task_runtime(
        pool=_pool,
        config=application.config,
        copy_tasks_config=application.config.copy_trading.copy_tasks,
        contexts=initial_v1_contexts,
        provider_runtime=provider_runtime,
        start_workers=False,
        buy_scope_guard=leader_sell_scope_guard,
        decision_observer=telemetry_emitters.trade,
        latency=telemetry_emitters.latency,
    )
    if v1_runtime is not None:
        client_registry.append(v1_runtime)
    logger.info(
        "startup_stage | stage=v1_copy_task_restoration_completed enabled=%s",
        v1_runtime is not None,
    )

    # CLAUDE.md non-negotiable: startup reconciliation MUST run before risk /
    # trade loops AND before the user WSS attaches its on_reconcile_signal
    # callback. Pre-fix the WSS runtime started first and could fire
    # reconcile-wake events while startup_reconcile_unconfirmed was still
    # running on the same orders, double-applying fills under the race.
    reconcile_failures: list[str] = []
    logger.info(
        "startup_stage | stage=startup_reconcile_started users=%d",
        len(risk_contexts_by_user),
    )
    startup_reconciled = await startup_reconcile_unconfirmed(
        list(risk_contexts_by_user.values()),
        failures_out=reconcile_failures,
    )
    from pmcopy.orchestrators.lifecycle.trading_permission_runtime import TradingPermissionRuntime

    trading_permission_runtime = TradingPermissionRuntime(
        permissions={
            user_id: state.trading_permission
            for user_id, state in user_states.items()
            if state.trading_permission is not None
        }
    )
    control_failures = await trading_permission_runtime.initialize(
        blocked_user_ids=set(reconcile_failures)
    )
    reconcile_failures.extend(
        user_id for user_id in control_failures if user_id not in reconcile_failures
    )
    logger.info(
        "startup_stage | stage=startup_reconcile_completed failures=%d",
        len(reconcile_failures),
    )

    if reconcile_failures:
        # Refuse to advertise readiness; downstream monitors will see
        # reconciler_ok=False and stay closed. Operators must inspect the
        # WARNING logs emitted by startup_reconcile_unconfirmed for the
        # affected users.
        logger.error(
            "startup_reconcile_partial_failure | failed_users=%s — "
            "reconciler_ok stays False, monitors for affected wallets blocked",
            reconcile_failures,
        )
        application.readiness.update(reconciler_ok=False)

        # Fail closed: actually remove the affected wallets from the monitored
        # set. Recording reconciler_ok=False is not enough — no monitor reads
        # it, so without this filter the leader monitors would still spawn and
        # re-buy on un-reconciled state (B4-01 / P0-4).
        _before = len(monitored_leaders)
        monitored_leaders = _cold_start_monitored_after_reconcile(
            monitored_leaders, contexts_by_leader, reconcile_failures
        )
        if len(monitored_leaders) != _before:
            logger.error(
                "startup_reconcile_block_monitors | kept=%d of %d wallets "
                "(blocked wallets had a user with a failed startup reconcile)",
                len(monitored_leaders),
                _before,
            )
            application.readiness.update(monitored_leader_count=len(monitored_leaders))
    else:
        application.readiness.update(reconciler_ok=True)

    # Seed inventory before drift checks and market/WSS startup so all loops
    # start against reconciled holdings, including positive zero-mark shares.
    inventory_startup = InitialInventorySync(application.readiness)
    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        manual_sync = user_state.manual_sync_service if user_state else None
        await inventory_startup.initialize(manual_sync)

    # PR7 — proactive position drift sweep. Must complete before monitors
    # start AND before the user WSS attaches so its first auth-triggered
    # manual_sync sees the prune timestamps.
    from pmcopy.orchestrators.bootstrap.runtime_bootstrap import startup_drift_check

    logger.info(
        "startup_stage | stage=startup_drift_check_started users=%d",
        len(runtime_users),
    )
    drift_per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=application.config,
        readiness=application.readiness,
    )
    logger.info(
        "startup_stage | stage=startup_drift_check_completed unresolved=%d",
        sum(1 for ok in drift_per_user.values() if not ok),
    )

    # Boot-time resolved-market gate. Catches markets that resolved /
    # archived / went inactive while the bot was offline (WSS
    # ``market_resolved`` only fires on live resolution). Runs AFTER
    # initial_sync + drift_check so the position set is stable.
    # Best-effort: a failure here logs and the runtime continues; the
    # dormant-token cache + 404 sentinel still bound traffic.
    try:
        logger.info("startup_stage | stage=hydrate_resolved_market_state_started")
        await hydrate_resolved_market_state(
            runtime_users=runtime_users,
            user_states=user_states,
        )
        logger.info("startup_stage | stage=hydrate_resolved_market_state_completed")
    except Exception as exc:
        logger.warning(
            "hydrate_resolved_market_state_failed",
            extra={"error": redact_error_detail(exc)},
        )



    monitor_tasks: list[asyncio.Task] = []
    monitor_wallet_by_task: dict[asyncio.Task, str] = {}
    monitored_set: set[str] = set(monitored_leaders)
    supervised: list[_Supervised] = []
    market_data_runtimes: dict[str, MarketDataRuntime] = {}
    user_data_runtimes: list[UserDataRuntime] = []
    onchain_runtime = None
    pending_copy_tracker = None

    # WP-B: enter the deterministic shutdown guard BEFORE starting background
    # workers, market/user WSS streams, paper evaluation, or supervised loops
    # so any mid-startup exception tears down every started runtime cleanly.
    try:
        if v1_runtime is not None:
            await v1_runtime.start()

        # Start market-data WSS only after portfolio/database convergence.
        live_positions_by_user = {}
        for runtime_user in runtime_users:
            logger.info(
                "startup_stage | stage=live_positions_refresh_started user=%s",
                runtime_user.user_id,
            )
            _refreshed = await fetch_live_positions(runtime_user.wallet.funder_address)
            live_positions_by_user[runtime_user.user_id] = _refreshed or []
            logger.info(
                "startup_stage | stage=live_positions_refresh_completed user=%s positions=%s",
                runtime_user.user_id,
                "fetch_failed" if _refreshed is None else len(_refreshed),
            )

        logger.info("startup_stage | stage=market_data_runtimes_starting")
        market_data_runtimes = await _start_market_data_runtimes(
            runtime_users=runtime_users,
            user_states=user_states,
            live_positions_by_user=live_positions_by_user,
            contexts_by_leader=contexts_by_leader,
            readiness=application.readiness,
            config=application.config,
            quote_sink=v1_runtime.quote_sink if v1_runtime is not None else None,
            telemetry=telemetry_emitters.wss,
            latency=telemetry_emitters.latency,
            evidence_runtime=evidence_runtime,
            runtimes_out=market_data_runtimes,
        )
        logger.info(
            "startup_stage | stage=market_data_runtimes_started users=%d",
            len(market_data_runtimes),
        )

        _attach_market_data_runtime_to_contexts(
            contexts_by_leader=contexts_by_leader,
            risk_contexts_by_user=risk_contexts_by_user,
            runtimes_by_user=market_data_runtimes,
        )

        # User WSS runtimes are started AFTER reconciler readiness is set AND
        # after drift_check populated prune timestamps.
        logger.info("startup_stage | stage=user_data_runtimes_starting")
        user_data_runtimes = await _start_user_data_runtimes(
            runtime_users=runtime_users,
            user_states=user_states,
            readiness=application.readiness,
            risk_contexts_by_user=risk_contexts_by_user,
            market_data_runtimes=market_data_runtimes,
            telemetry=telemetry_emitters.wss,
            runtimes_out=user_data_runtimes,
        )
        logger.info(
            "startup_stage | stage=user_data_runtimes_started users=%d",
            len(user_data_runtimes),
        )
        if drift_per_user and not all(drift_per_user.values()):
            unresolved = [u for u, ok in drift_per_user.items() if not ok]
            logger.warning(
                "startup_drift_unresolved | users=%s — affected scopes are paused",
                unresolved,
            )

        if startup_reconciled:
            application.readiness.update(
                startup_reconciled_orders=sum(startup_reconciled.values()),
            )

        first_user = runtime_users[0]
        live_positions = live_positions_by_user.get(first_user.user_id, [])
        health_report = health_reports_by_user[first_user.user_id]

        live_positions_value: Decimal = sum(
            (_to_decimal_balance(position.get("currentValue")) for position in live_positions),
            start=Decimal("0"),
        )
        available_cash: Decimal = user_balances.get(first_user.user_id, Decimal("0"))

        all_health_reports = list(health_reports_by_user.values())
        startup_health_healthy = all(report.healthy for report in all_health_reports)
        startup_health_status = "healthy" if startup_health_healthy else "unhealthy"
        startup_pruned_positions = sum(
            report.position_sync.pruned_positions for report in all_health_reports
        )
        startup_purged_failed_orders = sum(
            report.order_cleanup.purged_failed for report in all_health_reports
        )

        for runtime_user in runtime_users[1:]:
            secondary_report = health_reports_by_user.get(runtime_user.user_id)
            if secondary_report is not None and not secondary_report.healthy:
                logger.warning(
                    "secondary_startup_health_unhealthy",
                    extra={
                        "user_id": runtime_user.user_id,
                        "wallet": runtime_user.wallet.funder_address,
                        "status": secondary_report.status,
                    },
                )

        application.readiness.update(
            startup_health_status=startup_health_status,
            startup_health_healthy=startup_health_healthy,
            startup_pruned_positions=startup_pruned_positions,
            startup_purged_failed_orders=startup_purged_failed_orders,
        )

        print_startup_summary(
            wallet=first_user.wallet.funder_address,
            available_cash=available_cash,
            total_portfolio=available_cash + live_positions_value,
            positions=live_positions,
            active_leaders=monitored_leaders,
            config=application.config,
            health_report=health_report,
        )

        async def _on_new_leaders(new_wallets: list[str]) -> list[str]:
            started_wallets = await _refresh_contexts_for_new_leaders(
                new_wallets=new_wallets,
                runtime_users=runtime_users,
                user_states=user_states,
                contexts_by_leader=contexts_by_leader,
                leaders_repo=leaders_repo,
                config=application.config,
                readiness=application.readiness,
            )

            monitored_leader_count = len(
                [wallet for wallet, contexts in contexts_by_leader.items() if contexts]
            )
            telemetry_emitters.detection.roster_updated(monitored_leader_count)

            await _subscribe_context_tokens(
                market_data_runtimes=market_data_runtimes,
                contexts_by_leader=contexts_by_leader,
            )

            new_contexts: list["TradingContext"] = []
            for wallet in started_wallets:
                new_contexts.extend(contexts_by_leader.get(wallet, []))
            dynamic_recon_failures: list[str] = []
            if new_contexts:
                for context in new_contexts:
                    context.leader_sell_scope_guard = leader_sell_scope_guard
                await startup_reconcile_unconfirmed(
                    new_contexts,
                    failures_out=dynamic_recon_failures,
                )

            blocked_wallets = _wallets_blocked_by_reconcile_failures(
                started_wallets,
                contexts_by_leader,
                dynamic_recon_failures,
            )
            if blocked_wallets:
                logger.error(
                    "dynamic_leader_reconcile_failed | blocked_leaders=%s "
                    "failed_users=%s — monitors will NOT start for these "
                    "leaders; operators must inspect and re-trigger",
                    blocked_wallets,
                    dynamic_recon_failures,
                )
                blocked_set = set(blocked_wallets)
                for blocked_wallet in blocked_set:
                    contexts_by_leader.pop(blocked_wallet, None)
                started_wallets = [wallet for wallet in started_wallets if wallet not in blocked_set]

            _attach_market_data_runtime_to_contexts(
                contexts_by_leader=contexts_by_leader,
                risk_contexts_by_user=risk_contexts_by_user,
                runtimes_by_user=market_data_runtimes,
            )

            unblocked_contexts = [
                context
                for wallet in started_wallets
                for context in contexts_by_leader.get(wallet, ())
            ]
            if v1_runtime is not None and unblocked_contexts:
                v1_runtime.add_contexts(unblocked_contexts)

            if started_wallets:
                for wallet in started_wallets:
                    if wallet not in monitored_set:
                        monitored_leaders.append(wallet)
                        monitored_set.add(wallet)
                if onchain_runtime is not None:
                    onchain_runtime.update_watched_wallets(monitored_set)
                logger.info(
                    "alchemy_detection_roster_updated | added=%d total=%d",
                    len(started_wallets),
                    len(monitored_set),
                )
            application.readiness.update(
                monitored_leader_count=len(onchain_runtime.active_wallets())
                if onchain_runtime is not None
                else len(monitored_set)
            )
            return started_wallets

        async def _on_retired_leaders(wallets: list[str]) -> None:
            """Retire only monitors already proven free of outstanding exit ownership."""
            retired = set(wallets)
            tasks = [task for task, wallet in monitor_wallet_by_task.items() if wallet in retired]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                monitor_wallet_by_task.pop(task, None)
                if task in monitor_tasks:
                    monitor_tasks.remove(task)
            for wallet in retired:
                contexts_by_leader.pop(wallet, None)
                if v1_runtime is not None:
                    await v1_runtime.retire_leader(wallet)
            monitored_leaders[:] = [wallet for wallet in monitored_leaders if wallet not in retired]
            monitored_set.difference_update(retired)
            _refresh_onchain_watched_wallets(onchain_runtime, monitored_set)
            application.readiness.update(monitored_leader_count=len(monitored_set))
            telemetry_emitters.detection.roster_updated(len(monitored_set))

        recovery_services = {
            user_id: getattr(state, "ambiguous_recovery", None)
            for user_id, state in user_states.items()
        }
        recovery_services = {
            user_id: service for user_id, service in recovery_services.items() if service is not None
        }
        if recovery_services:
            from pmcopy.orchestrators.bootstrap.runtime_bootstrap import ambiguous_recovery_loop

            _recovery_interval = float(
                application.config.reconciliation.ambiguous_recovery.interval_seconds
            )
            supervised.append(
                _Supervised(
                    name="ambiguous-recovery",
                    factory=lambda: ambiguous_recovery_loop(
                        recovery_services, interval_seconds=_recovery_interval
                    ),
                )
            )
            logger.info(
                "ambiguous_recovery_wired | users=%d interval_seconds=%s",
                len(recovery_services),
                _recovery_interval,
            )

        paper_evaluation = None
        if auto_traders is not None:
            from pmcopy.orchestrators.discovery.auto_trader_paper_runtime import AutoTraderPaperRuntime

            paper_evaluation = AutoTraderPaperRuntime.create(
                lifecycle=auto_traders,
                pool=_pool,
                states=user_states,
                config=application.config,
                contexts=contexts_by_leader,
                seed_funds=user_balances,
                clock=AutoTraderRuntime.now,
            )
            await paper_evaluation.prepare()
            supervised.append(_Supervised(name="auto-trader-paper", factory=paper_evaluation.run))

        if not application.config.execution.paper_trade_only:
            control_refresh_interval = float(
                application.config.reconciliation.reconcile_interval_seconds
            )
            supervised.append(
                _Supervised(
                    name="trading-control-refresh",
                    factory=lambda: trading_permission_runtime.run(
                        interval_seconds=control_refresh_interval
                    ),
                )
            )

        if auto_traders is not None:
            supervised.append(
                _Supervised(
                    name="auto-trader-evaluation",
                    factory=lambda: auto_traders.run(
                        evaluation_seconds=application.config.discovery.scheduler.rescore_seconds,
                        falcon_seconds=application.config.discovery.scheduler.rediscover_seconds,
                        on_new_leaders=_on_new_leaders,
                        monitored=monitored_set,
                        on_retired_leaders=_on_retired_leaders,
                        evaluate_results=paper_evaluation.evaluate_results
                        if paper_evaluation is not None
                        else None,
                    ),
                )
            )
        elif startup_policy.discovery_loop_enabled(application.config) and not configured:
            dynamic_monitoring_enabled = bool(
                application.config.discovery.scheduler.start_from_roster
            )
            if not dynamic_monitoring_enabled:
                logger.info(
                    "discovery_roster_hot_add_disabled",
                    extra={
                        "roster_hot_add_enabled": False,
                        "configured_value": False,
                        "effect": "background discovery will refresh DB only and will not hot-add monitors",
                    },
                )
            supervised.append(
                _Supervised(
                    name="discovery-refresh",
                    factory=lambda: _discovery_refresh_loop(
                        registry,
                        configured,
                        refresh_interval,
                        on_new_leaders=(_on_new_leaders if dynamic_monitoring_enabled else None),
                        already_monitored=monitored_set,
                        force_discovery=startup_policy.force_discovery,
                    ),
                )
            )
        elif configured:
            logger.info(
                "discovery_skipped_manual_leaders",
                extra={"configured_leader_count": len(configured)},
            )

        if application.config.risk.enable_risk_mgmt:
            from pmcopy.services.risk.risk_monitor import risk_monitor_loop_for_contexts

            supervised.append(
                _Supervised(
                    name="risk-monitor",
                    factory=lambda: risk_monitor_loop_for_contexts(
                        list(risk_contexts_by_user.values()),
                        readiness_tracker=application.readiness,
                        config=application.config,
                    ),
                )
            )

        if not application.config.risk.enable_risk_mgmt:
            _reaper_interval = max(
                30.0, float(application.config.reconciliation.reconcile_interval_seconds)
            )

            async def _reservation_reaper_loop() -> None:
                while True:
                    await _reap_reservations_once(_flatten_contexts(contexts_by_leader))
                    await asyncio.sleep(_reaper_interval)

            supervised.append(
                _Supervised(name="reservation-reaper", factory=_reservation_reaper_loop)
            )

        supervised.append(
            _Supervised(
                name="account-lease-renewal",
                factory=lambda: account_lease_renewal_loop(account_lease_manager),
            )
        )

        from pmcopy.orchestrators.lifecycle.liveness_heartbeat import (
            DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
            bot_heartbeat_loop,
        )

        def _heartbeat_snapshot() -> dict[str, object]:
            """Read live supervisor state; never mutates and never blocks on I/O."""
            detection = heartbeat_state.get("detection")
            ready, _missing = application.readiness.is_ready(strict=False)
            readiness = application.readiness.snapshot()
            return {
                "detection_connected": (
                    bool(detection.connected) if detection is not None else None
                ),
                "active_leaders": (
                    len(detection.active_wallets()) if detection is not None else None
                ),
                "monitored_leaders": len(monitored_leaders),
                "ready": ready,
                "wss_user_ok": readiness.get("wss_user_ok"),
                "wss_market_ok": readiness.get("wss_market_ok"),
                "reconciler_ok": readiness.get("reconciler_ok"),
                "db_ok": readiness.get("db_ok"),
                "api_ok": readiness.get("api_ok"),
            }

        supervised.append(
            _Supervised(
                name="bot-heartbeat",
                factory=lambda: bot_heartbeat_loop(
                    _heartbeat_snapshot,
                    logger=logger,
                    interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                ),
            )
        )

        supervisor = TaskSupervisor(
            supervised=supervised,
            monitor_tasks=monitor_tasks,
            monitor_wallet_by_task=monitor_wallet_by_task,
            readiness=application.readiness,
            refresh_interval=float(refresh_interval),
            metrics_registry=metrics,
            logger=logger,
        )
        supervisor.spawn_all()

        from pmcopy.orchestrators.detection.alchemy_detection_runtime import (
            AlchemyDetectionRuntime,
        )
        from pmcopy.orchestrators.detection.onchain_signal_dispatch import OnchainSignalDispatchAdapter
        from pmcopy.orchestrators.pipeline.trade_pipeline import execute_trades as _execute_trade_pipeline

        def _alchemy_source_changed() -> None:
            active_owners = onchain_runtime.active_wallets() if onchain_runtime is not None else ()
            application.readiness.update(monitored_leader_count=len(active_owners))
            telemetry_emitters.detection.roster_updated(len(active_owners))

        _alchemy_source_changed()
        try:
            _onchain_signal_sink = OnchainSignalDispatchAdapter(
                contexts_by_leader=contexts_by_leader,
                v1_runtime=v1_runtime,
                observer=paper_evaluation.offer if paper_evaluation is not None else None,
                execute_trades=_execute_trade_pipeline,
                telemetry=telemetry_emitters.detection,
                sell_fraction_resolver=_build_leader_sell_fraction_resolver(
                    application.config,
                    buy_scope_guard=leader_sell_scope_guard,
                    provider_runtime=provider_runtime,
                ),
            )
            onchain_runtime = AlchemyDetectionRuntime.compose(
                config=application.config,
                wallets=monitored_leaders,
                sink=_onchain_signal_sink,
                status_changed=_alchemy_source_changed,
                provider_runtime=provider_runtime,
            )
            await onchain_runtime.start()
            if v1_runtime is not None and monitored_leaders:
                with contextlib.suppress(TimeoutError, Exception):
                    async with asyncio.timeout(1.5):
                        await v1_runtime.warm_leader_balances(monitored_leaders)
            heartbeat_state["detection"] = onchain_runtime
            telemetry_emitters.detection.attach_connection_probe(
                _detection_connection_probe(onchain_runtime)
            )
        except Exception as exc:
            logger.error(
                "alchemy_detection_paused | reason=composition_unavailable error=%s",
                redact_error_detail(exc),
            )
            if onchain_runtime is not None:
                await onchain_runtime.stop()
            onchain_runtime = None
            _alchemy_source_changed()

        return await supervisor.run_until_stopped()

    finally:
        await _shutdown_runtime(
            monitor_tasks=monitor_tasks,
            supervised=supervised,
            market_data_runtimes=market_data_runtimes,
            user_data_runtimes=user_data_runtimes,
            metrics_runner=metrics_runner,
            onchain_runtime=onchain_runtime,
            pending_copy_tracker=pending_copy_tracker,
            v1_runtime=v1_runtime,
            evidence_drain=evidence_drain,
            account_lease_manager=account_lease_manager,
        )

