"""Application composition root with no import-time side effects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from config.app_config import AppConfig, load_config
from integrations.heisenberg.client import HeisenbergClient
from orchestrators.leader_registry import LeaderRegistry
from orchestrators.discovery_funds import build_discovery_funds_callable
from services.discovery.heisenberg_candidate_service import (
    HeisenbergCandidateService,
)
from services.discovery.heisenberg_discovery_service import (
    HeisenbergDiscoveryService,
)
from services.discovery.models import discovery_config_hash
from services.discovery.policy_config import discovery_policy_from_config
from services.healthServer import ReadinessTracker
from storage.postgres.connection import init_pool
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.leaders_repo import LeadersRepo
from storage.postgres.migrations import apply_migrations
from utils.getMyBalance import get_my_balance
from utils.logger import get_logger

__all__ = ["Application", "build_app"]


@dataclass(slots=True)
class Application:
    config: AppConfig
    readiness: ReadinessTracker
    leaders_repo: LeadersRepo
    discovery_repo: DiscoveryRepo
    discovery_service: HeisenbergDiscoveryService
    registry: LeaderRegistry
    heisenberg_client: HeisenbergClient | None

    async def aclose(self) -> None:
        if self.heisenberg_client is not None:
            await self.heisenberg_client.close()


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

    heisenberg_cfg = config.discovery.heisenberg
    token = heisenberg_cfg.api_key.get_secret_value() if heisenberg_cfg.enabled else ""
    heisenberg_client: HeisenbergClient | None = None
    discovery_service: HeisenbergDiscoveryService
    if heisenberg_cfg.enabled and token:
        heisenberg_client = HeisenbergClient(
            api_key=token,
            base_url=config.discovery.api.base_url,
            timeout_seconds=config.discovery.api.timeout_seconds,
        )
        policy = discovery_policy_from_config(config)
        candidate_service = HeisenbergCandidateService(
            heisenberg_client,
            policy=policy,
        )
    else:
        # Fail closed: a disabled/missing Heisenberg key yields a discovery
        # service that returns no candidates rather than silently substituting
        # a weaker source. The funds callable is read-only and harmless when
        # uncalled.
        candidate_service = None  # type: ignore[assignment]

    funder_address = str(getattr(config.wallet, "funder_address", "") or "").strip().lower()
    get_funds = build_discovery_funds_callable(
        funder_address,
        get_balance=get_my_balance,
    )
    discovery_service = HeisenbergDiscoveryService(
        service=candidate_service,  # type: ignore[arg-type]
        repository=discovery_repo,
        config=config,
        get_funds=get_funds,
    )

    registry = LeaderRegistry(
        discovery_service=discovery_service,
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
        discovery_service=discovery_service,
        registry=registry,
        heisenberg_client=heisenberg_client,
    )
