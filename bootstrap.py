"""Application composition root with no import-time side effects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from config.app_config import AppConfig, load_config
from integrations.falcon.client import FalconClient
from orchestrators.leader_registry import LeaderRegistry
from services.discovery.backfill import LongitudinalBackfillService
from services.discovery.funnel import DiscoveryFunnel
from services.discovery.scoring import CompositeScorer
from services.healthServer import ReadinessTracker
from storage.postgres.connection import init_pool
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.leaders_repo import LeadersRepo
from storage.postgres.migrations import apply_migrations
from utils.logger import get_logger

__all__ = ["Application", "build_app"]


@dataclass(slots=True)
class Application:
    config: AppConfig
    readiness: ReadinessTracker
    leaders_repo: LeadersRepo
    discovery_repo: DiscoveryRepo
    discovery_service: DiscoveryFunnel
    registry: LeaderRegistry
    falcon_client: FalconClient

    async def aclose(self) -> None:
        await self.falcon_client.aclose()


async def build_app(
    overrides: Mapping[str, Any] | None = None,
    *,
    config: AppConfig | None = None,
) -> Application:
    if config is not None and overrides is not None:
        raise ValueError("provide either config or overrides, not both")
    if config is None:
        config = load_config(overrides)
    pool = await init_pool(
        config.database.dsn,
        min_size=config.database.pool_min_size,
        max_size=config.database.pool_max_size,
        command_timeout_seconds=config.database.command_timeout_seconds,
    )
    async with pool.acquire() as connection:
        await apply_migrations(connection)

    readiness = ReadinessTracker()
    readiness.update(db_ok=True)
    leaders_repo = LeadersRepo(pool)
    discovery_repo = DiscoveryRepo(pool)
    falcon_client = FalconClient(config.discovery.api)
    funnel = DiscoveryFunnel(
        client=falcon_client,
        config=config.discovery,
        scorer=CompositeScorer(),
        repository=discovery_repo,
    )
    funnel.backfill_service = LongitudinalBackfillService(
        client=falcon_client,
        repository=discovery_repo,
        config=config.discovery.longitudinal,
        config_hash=funnel.config_hash,
    )
    registry = LeaderRegistry(
        discovery_service=funnel,
        leaders_repo=leaders_repo,
        config=config,
        readiness_tracker=readiness,
        logger_instance=get_logger("LeaderRegistry"),
    )
    return Application(
        config=config,
        readiness=readiness,
        leaders_repo=leaders_repo,
        discovery_repo=discovery_repo,
        discovery_service=funnel,
        registry=registry,
        falcon_client=falcon_client,
    )
