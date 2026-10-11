"""Application composition root with no import-time side effects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any
from orchestrators.auto_trader_runtime import AutoTraderRuntime

from config.app_config import AppConfig, load_config
from integrations.falcon.client import FalconClient
from orchestrators.leader_registry import LeaderRegistry
from orchestrators.discovery_funds import build_discovery_funds_callable
from services.discovery.falcon_candidate_service import (
    FalconCandidateService,
)
from services.discovery.falcon_discovery_service import (
    FalconDiscoveryService,
)
from services.discovery.policy_config import discovery_policy_from_config
from services.healthServer import ReadinessTracker
from storage.postgres.connection import init_pool
from storage.postgres.discovery_repo import DiscoveryRepo
from storage.postgres.leaders_repo import LeadersRepo
from storage.postgres.migrations import apply_migrations
from utils.getMyBalance import configure_from_appconfig as configure_balance_from_appconfig
from utils.getMyBalance import get_my_balance
from utils.logger import get_logger

__all__ = ["Application", "build_app"]


@dataclass(slots=True)
class Application:
    """Own the composed runtime services and closeable Falcon client."""

    config: AppConfig
    readiness: ReadinessTracker
    leaders_repo: LeadersRepo
    discovery_repo: DiscoveryRepo
    discovery_service: FalconDiscoveryService
    registry: LeaderRegistry
    falcon_client: FalconClient | None
    auto_traders: AutoTraderRuntime | None = None
    provider_runtime: Any = None

    async def aclose(self) -> None:
        """Close application-owned external clients idempotently."""
        if self.falcon_client is not None:
            await self.falcon_client.close()


async def build_app(
    overrides: Mapping[str, Any] | None = None,
    *,
    config: AppConfig | None = None,
    provider_runtime: Any = None,
) -> Application:
    """Build one application object graph from validated configuration.

    Args:
        overrides: Optional configuration overlay used primarily by tests.
        config: An already validated configuration instance.

    Returns:
        A fully composed application with database-backed discovery services.

    Raises:
        ValueError: If both ``overrides`` and ``config`` are supplied.
    """
    if config is not None and overrides is not None:
        raise ValueError("provide either config or overrides, not both")
    if config is None:
        config = load_config(overrides)
    configure_balance_from_appconfig(config)
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

    falcon_cfg = config.discovery.falcon
    token = config.discovery.api.token.get_secret_value() if falcon_cfg.enabled else ""
    falcon_client: FalconClient | None = None
    candidate_service: FalconCandidateService | None
    discovery_service: FalconDiscoveryService
    if falcon_cfg.enabled and token:
        falcon_client = FalconClient(
            api_key=token,
            base_url=config.discovery.api.base_url,
            timeout_seconds=config.discovery.api.timeout_seconds,
            max_attempts=config.discovery.api.max_retries,
            max_backoff_seconds=config.discovery.api.retry_max_delay_seconds,
        )
        policy = discovery_policy_from_config(config)
        candidate_service = FalconCandidateService(
            falcon_client,
            policy=policy,
        )
    else:
        # Fail closed: a disabled/missing Falcon key yields a discovery
        # service that returns no candidates rather than silently substituting
        # a weaker source. The funds callable is read-only and harmless when
        # uncalled.
        candidate_service = None

    funder_address = str(getattr(config.wallet, "funder_address", "") or "").strip().lower()
    get_funds = build_discovery_funds_callable(
        funder_address,
        get_balance=partial(get_my_balance, provider_runtime=provider_runtime)
        if provider_runtime is not None
        else get_my_balance,
    )
    discovery_service = FalconDiscoveryService(
        service=candidate_service,
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
        falcon_client=falcon_client,
        provider_runtime=provider_runtime,
    )
