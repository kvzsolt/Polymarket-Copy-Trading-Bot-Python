"""Immutable application configuration facade.

Decomposed into ``schemas_core``, ``schemas_discovery``, ``validators``,
and ``loader`` while preserving the canonical ``pmcopy.config.app_config``
public contract.
"""

from __future__ import annotations

import os  # noqa: F401
from pathlib import Path

from pmcopy.config.addresses import (
    ALL_VERIFIED_EXCHANGE_ADDRESSES,  # noqa: F401
    POLYGON_USDCE_ADDRESS,
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
    verified_polymarket_exchange_addresses,  # noqa: F401
)
from pmcopy.config.auto_follow import AutoFollowConfig  # noqa: F401
from pmcopy.config.env import EnvValidationError, SignatureType, WalletConfig, secret_value  # noqa: F401
from pmcopy.config.evidence import EvidenceConfig, EvidenceSecretBoundary  # noqa: F401
from pmcopy.config.loader import (
    SERVICE_CONFIG_FILES,
    _DEFAULT_ENVIRONMENT,  # noqa: F401
    _LayeredModel,  # noqa: F401
    _OBSOLETE_CLOB_CREDENTIAL_KEYS,  # noqa: F401
    _OBSOLETE_CONFIG_SECTIONS,  # noqa: F401
    _OBSOLETE_VENUE_BACKEND_KEYS,  # noqa: F401
    _RUNTIME_LOCAL_YAML,  # noqa: F401
    _SECRETS_ENV,  # noqa: F401
    _deep_merge,  # noqa: F401
    _read_secrets_contents,  # noqa: F401
    _read_yaml,  # noqa: F401
    _resolve_deployment_root,  # noqa: F401
    _resolve_environment,  # noqa: F401
    _translate_legacy_payload,  # noqa: F401
    dump_config_payload,
    load_config,
    packaged_config_dir,  # noqa: F401
)
from pmcopy.config.native_detection import NativeDetectionConfig  # noqa: F401
from pmcopy.config.provider_budget import ProviderBudgetConfig  # noqa: F401
from pmcopy.config.redemption import RedemptionConfig, RedemptionSecretBoundary  # noqa: F401
from pmcopy.config.schemas_core import (
    AmbiguousRecoveryConfig,
    AppServiceConfig,
    CopyTradingConfig,
    DatabaseConfig,
    DetectionConfig,
    ExecutionConfig,
    LeaderCopySettings,
    LeaderOverrideConfig,
    LeaderWalletConfig,
    LoggingConfig,
    OnchainDetectionConfig,
    OrderConfirmationConfig,
    PolymarketConfig,
    PositionDriftConfig,
    ProtectiveExitsConfig,
    ReconciliationConfig,
    RiskConfig,
    SizingConfig,
    _DETECTION_MODE_VALUES,  # noqa: F401
    _ONCHAIN_DETECTION_CONFIDENCE_VALUES,  # noqa: F401
    _PENDING_MODE_CONFIDENCE_VALUES,  # noqa: F401
    _alchemy_key_from_url,  # noqa: F401
    logger,  # noqa: F401
)
from pmcopy.config.schemas_discovery import (
    AppConfig,
    DiscoveryConfig,
    DiscoverySchedulerConfig,  # noqa: F401
    FalconApiConfig,  # noqa: F401
    FalconBankrollConfig,  # noqa: F401
    FalconDiscoveryConfig,  # noqa: F401
    FalconQualityWeights,  # noqa: F401
    HysteresisConfig,  # noqa: F401
    LongitudinalConfig,  # noqa: F401
    LongitudinalWeights,  # noqa: F401
    MarketDataConfig,
    MarketQualityConfig,  # noqa: F401
    RecencyConfig,  # noqa: F401
    RuntimeSignalWeights,  # noqa: F401
    ScoringConfig,  # noqa: F401
    ScoringWeights,  # noqa: F401
    ShortlistConfig,  # noqa: F401
    SocialConfig,  # noqa: F401
    Wallet360Config,  # noqa: F401
)
from pmcopy.config.trader_config import (
    TraderConfig,
    TraderTaskConfig,
    TraderTaskMarketFilterConfig,
    TraderTaskPriceConfig,
    TraderTaskReactionConfig,
    TraderTaskScheduleConfig,
    TraderTaskSizingConfig,
    TraderTaskSpendConfig,
    TraderTaskTriggerConfig,
)
from pmcopy.config.validators import (
    _DETECTION_MODE_WARNINGS,  # noqa: F401
    AppConfigSemanticValidator,
    DetectionModeResolver,
    DetectionModeWarning,
    DetectionModeWarningEmitter,
    _runtime_python_supported,  # noqa: F401
    _validate_detection_mode,  # noqa: F401
    _validate_entry_price_drift,  # noqa: F401
    _validate_semantics,  # noqa: F401
    sys,  # noqa: F401
)

_CONFIG_ROOT: Path = packaged_config_dir()

__all__ = [
    "AppConfig",
    "AppConfigSemanticValidator",
    "AppServiceConfig",
    "CopyTradingConfig",
    "DatabaseConfig",
    "DetectionConfig",
    "DetectionModeResolver",
    "DetectionModeWarning",
    "DetectionModeWarningEmitter",
    "DiscoveryConfig",
    "EnvValidationError",
    "ExecutionConfig",
    "LeaderCopySettings",
    "LeaderOverrideConfig",
    "LeaderWalletConfig",
    "LoggingConfig",
    "MarketDataConfig",
    "OnchainDetectionConfig",
    "AmbiguousRecoveryConfig",
    "OrderConfirmationConfig",
    "PolymarketConfig",
    "PositionDriftConfig",
    "ProtectiveExitsConfig",
    "ReconciliationConfig",
    "RedemptionConfig",
    "RiskConfig",
    "SignatureType",
    "SizingConfig",
    "TraderConfig",
    "TraderTaskConfig",
    "TraderTaskMarketFilterConfig",
    "TraderTaskPriceConfig",
    "TraderTaskReactionConfig",
    "TraderTaskScheduleConfig",
    "TraderTaskSizingConfig",
    "TraderTaskSpendConfig",
    "TraderTaskTriggerConfig",
    "WalletConfig",
    "POLYGON_USDCE_ADDRESS",
    "POLYMARKET_CTF_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_PUSD_ADDRESS",
    "SERVICE_CONFIG_FILES",
    "dump_config_payload",
    "load_config",
]
