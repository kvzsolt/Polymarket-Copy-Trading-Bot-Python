from __future__ import annotations

from dataclasses import dataclass

from config.app_config import AppConfig
from services.metrics import metrics
from utils.logger import get_logger, redact_error_detail

logger = get_logger("AutoTraderBootstrap")


@dataclass(frozen=True)
class AutoTraderBootstrapResult:
    active_leaders: list[str]
    mode: str
    discovery_ran: bool
    reason: str


async def resolve_startup_leaders(
    *,
    configured_leaders: list[str],
    config: AppConfig,
    selector,
    discovery_service,
) -> AutoTraderBootstrapResult:
    configured_clean = [
        str(wallet).strip().lower()
        for wallet in configured_leaders
        if str(wallet).strip()
    ]

    if configured_clean:
        logger.info(
            "auto_trader_bootstrap_skipped_manual_leaders",
            extra={
                "configured_count": len(configured_clean),
                "allow_auto_trader_detection": config.discovery.allow_auto_trader_detection,
            },
        )
        return AutoTraderBootstrapResult(
            active_leaders=configured_clean,
            mode="configured",
            discovery_ran=False,
            reason="manual_leaders_configured",
        )

    if not config.discovery.allow_auto_trader_detection:
        logger.warning(
            "auto_trader_bootstrap_disabled",
            extra={
                "configured_count": 0,
                "allow_auto_trader_detection": False,
                "effect": "no_live_leaders",
            },
        )
        return AutoTraderBootstrapResult(
            active_leaders=[],
            mode="no_configured_leaders",
            discovery_ran=False,
            reason="auto_detection_disabled",
        )

    max_count = int(config.discovery.max_auto_traders_count or 0)
    logger.info(
        "auto_trader_bootstrap_started",
        extra={
            "configured_count": 0,
            "allow_auto_trader_detection": True,
            "max_auto_traders_count": max_count,
            "discovery_auto_write_leaders": config.discovery.discovery_auto_write_leaders,
            "discovery_dynamic_monitoring_enabled": (
                config.discovery.discovery_dynamic_monitoring_enabled
            ),
        },
    )
    metrics.inc("auto_trader_bootstrap_started_total")

    first = await selector.select_from_db()
    if first.wallets:
        metrics.set_gauge("auto_trader_bootstrap_selected", float(len(first.wallets)))
        logger.info(
            "auto_trader_bootstrap_selected_from_db",
            extra={
                "selected_count": len(first.wallets),
                "selected_wallets": first.wallets,
                "db_candidate_count": first.db_candidate_count,
                "reason": first.reason,
            },
        )
        return AutoTraderBootstrapResult(
            active_leaders=first.wallets,
            mode="auto_detected",
            discovery_ran=False,
            reason="db_candidates_available",
        )

    if max_count <= 0:
        logger.error(
            "auto_trader_bootstrap_failed_no_candidates",
            extra={
                "max_auto_traders_count": max_count,
                "discovery_ran": False,
                "reason": first.reason,
                "effect": "clob_runtime_startup_blocked",
            },
        )
        metrics.inc("auto_trader_bootstrap_no_candidates_total")
        metrics.set_gauge("auto_trader_bootstrap_selected", 0.0)
        return AutoTraderBootstrapResult(
            active_leaders=[],
            mode="no_live_candidates",
            discovery_ran=False,
            reason=first.reason,
        )

    logger.info(
        "auto_trader_bootstrap_no_db_candidates_running_discovery",
        extra={
            "configured_count": 0,
            "allow_auto_trader_detection": True,
            "max_auto_traders_count": max_count,
            "reason": first.reason,
            "effect": "running_sync_discovery_before_clob_startup",
        },
    )
    metrics.inc("auto_trader_bootstrap_discovery_runs_total")

    try:
        await discovery_service.discover_candidates()
    except Exception as exc:
        logger.error(
            "auto_trader_bootstrap_failed_no_candidates",
            extra={
                "max_auto_traders_count": max_count,
                "discovery_ran": True,
                "reason": "discovery_bootstrap_failed",
                "error": redact_error_detail(exc),
                "effect": "clob_runtime_startup_blocked",
            },
        )
        metrics.inc("auto_trader_bootstrap_no_candidates_total")
        metrics.set_gauge("auto_trader_bootstrap_selected", 0.0)
        return AutoTraderBootstrapResult(
            active_leaders=[],
            mode="no_live_candidates",
            discovery_ran=True,
            reason="discovery_bootstrap_failed",
        )

    second = await selector.select_from_db()
    if second.wallets:
        metrics.set_gauge(
            "auto_trader_bootstrap_selected",
            float(len(second.wallets)),
        )
        logger.info(
            "auto_trader_bootstrap_selected_after_discovery",
            extra={
                "selected_count": len(second.wallets),
                "selected_wallets": second.wallets,
                "db_candidate_count": second.db_candidate_count,
            },
        )
        return AutoTraderBootstrapResult(
            active_leaders=second.wallets,
            mode="auto_detected_bootstrapped",
            discovery_ran=True,
            reason="discovery_bootstrap_found_candidates",
        )

    logger.error(
        "auto_trader_bootstrap_failed_no_candidates",
        extra={
            "max_auto_traders_count": max_count,
            "discovery_ran": True,
            "db_candidate_count": second.db_candidate_count,
            "effect": "clob_runtime_startup_blocked",
        },
    )
    metrics.inc("auto_trader_bootstrap_no_candidates_total")
    metrics.set_gauge("auto_trader_bootstrap_selected", 0.0)

    return AutoTraderBootstrapResult(
        active_leaders=[],
        mode="no_live_candidates",
        discovery_ran=True,
        reason="no_candidates_after_discovery_bootstrap",
    )
