"""Composition root (Phase 5).

Wires the top-level shared graph — config, readiness tracker, DB, shared
repositories and the leader registry — into a single :class:`Application`
container. Per-user state (CLOB client + execution service + repos keyed
on ``user_id``) still requires an async startup path with real network
resources, so it is assembled in ``app.py::main`` from the shared pieces
returned here.

This module has **no import-time side effects**. ``build_app`` is the sole
factory; it reads the supplied env mapping (or ``os.environ`` when ``None``)
and returns a fresh graph each call. Two invocations produce independent
objects with no shared mutable state.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from config.app_config import AppConfig, load_config
from decimal import Decimal
from integrations.falcon.client import FalconClient
from integrations.polymarket.history_client import PolymarketHistoryClient
from orchestrators.leader_registry import LeaderRegistry
from services.healthServer import ReadinessTracker
from services.discovery.candidate_classifier import CandidateClassifier
from services.discovery.candidate_scoring import CandidateScoringService
from services.discovery.balance_provider import build_wallet_balance_provider
from services.discovery.copyability_replay import CopyabilityReplayService
from services.discovery.falcon_candidate_service import FalconCandidateService
from services.discovery.falcon_recent_state_service import FalconRecentStateService
from services.discovery.ingestion_service import IngestionService
from services.discovery.leader_trust_store import LeaderTrustStore
from services.discovery.polymarket_history_backfill import PolymarketHistoryBackfill
from services.discovery.signal_grouper import SignalGrouper
from services.traderDiscoveryService import TraderDiscoveryService
from storage.postgres.connection import init_pool
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.leaders_repo import LeadersRepo
from storage.postgres.migrations import apply_migrations as apply_pg_migrations
from utils.logger import get_logger

__all__ = ["Application", "build_app"]


@dataclass
class Application:
    """Shared top-level wiring produced by :func:`build_app`."""

    config: AppConfig
    readiness: ReadinessTracker
    leaders_repo: LeadersRepo
    discovery_service: TraderDiscoveryService
    trust_store: LeaderTrustStore
    registry: LeaderRegistry


async def build_app(env: Mapping[str, str] | None = None) -> Application:
    """Construct the shared application graph.

    Parameters
    ----------
    env:
        Environment mapping. Defaults to ``os.environ`` when ``None`` — the
        same default used by :func:`config.app_config.load_config`.

    Returns
    -------
    Application
        A freshly wired, fully typed graph. Successive calls yield
        independent instances (no module-level caching).

    Notes
    -----
    PG-A5: builds an async ``LeadersRepo`` against the singleton asyncpg
    pool. The pool is brought online here (``init_pool`` is idempotent —
    a second caller returns the same pool) and the canonical schema is
    applied before any repo touches it. Sqlite legacy initialisation
    continues to run via ``config.db.connect_db()`` at the runtime
    entrypoint until the remaining sqlite callers migrate.
    """
    config = load_config(env)

    pool = await init_pool(
        config.database.dsn,
        min_size=config.database.pool_min_size,
        max_size=config.database.pool_max_size,
        command_timeout_seconds=config.database.command_timeout_seconds,
    )
    async with pool.acquire() as conn:
        await apply_pg_migrations(conn)

    readiness = ReadinessTracker()
    readiness.update(db_ok=True)

    leaders_repo = LeadersRepo(pool)
    discovery_repo = DiscoveryRepo(pool)
    trust_store = LeaderTrustStore()
    await trust_store.refresh_from_repo(discovery_repo)
    ingestion = IngestionService(discovery_repo)
    falcon_client = FalconClient(
        api_url=config.discovery.falcon_api_url,
        token=config.discovery.falcon_api_token,
    )
    history_client = PolymarketHistoryClient()
    discovery_service = TraderDiscoveryService(
        config=config,
        falcon_candidates=FalconCandidateService(falcon_client, config),
        history_backfill=PolymarketHistoryBackfill(
            client=history_client,
            ingestion=ingestion,
            config=config,
        ),
        discovery_repo=discovery_repo,
        evaluation_repo=discovery_repo,
        grouper=SignalGrouper(
            window_seconds=config.discovery.discovery_signal_group_window_seconds,
            max_gap_seconds=config.discovery.discovery_signal_group_max_gap_seconds,
            min_group_notional_usdc=Decimal(
                str(config.discovery.discovery_min_group_notional_usdc)
            ),
        ),
        classifier=CandidateClassifier(config),
        replay_service=CopyabilityReplayService(
            config=config,
            balance_provider=build_wallet_balance_provider(config),
        ),
        scoring_service=CandidateScoringService(config),
        recent_state_service=FalconRecentStateService(falcon_client),
        trust_store=trust_store,
    )
    registry = LeaderRegistry(
        discovery_service=discovery_service,
        leaders_repo=leaders_repo,
        readiness_tracker=readiness,
        config=config,
        logger_instance=get_logger("LeaderRegistry"),
    )

    return Application(
        config=config,
        readiness=readiness,
        leaders_repo=leaders_repo,
        discovery_service=discovery_service,
        trust_store=trust_store,
        registry=registry,
    )
