"""Local CCXT Polymarket compatibility layer.

Everything CCXT-specific that exists to work around upstream limitations or
to describe the transport lives under this package:

* ``tick_size`` — venue-neutral arbitrary tick-size support;
* ``precision`` — instance-local arbitrary-tick raw-amount patch;
* ``credentials`` — L2 credential extraction from a CCXT exchange;
* ``capabilities`` — CCXT-only health/telemetry terminology;
* ``version_guard`` — fail-closed installed-version gate;
* ``exceptions`` — transport-level exceptions.

Nothing here may leak CCXT structures, dictionaries, or exceptions past
``interfaces.clob_gateway.ClobGateway``.
"""

from pmcopy.integrations.polymarket.ccxt_compat.credentials import (
    UserWsCredentials,
    extract_user_ws_credentials,
)
from pmcopy.integrations.polymarket.ccxt_compat.exceptions import (
    CcxtPredictionConfigurationError,
    CcxtPredictionUnavailable,
)
from pmcopy.integrations.polymarket.ccxt_compat.tick_size import (
    InvalidTickSize,
    PolymarketTickSizeResolver,
    PublicClobTickSizeSource,
    TickRounding,
    TickSize,
    TickSizeResolver,
    TickSizeUnavailable,
    quantize_price_to_tick,
)
from pmcopy.integrations.polymarket.ccxt_compat.version_guard import (
    ensure_supported_ccxt_version,
    parse_ccxt_version,
)

__all__ = [
    "CcxtPredictionConfigurationError",
    "CcxtPredictionUnavailable",
    "InvalidTickSize",
    "PolymarketTickSizeResolver",
    "PublicClobTickSizeSource",
    "TickRounding",
    "TickSize",
    "TickSizeResolver",
    "TickSizeUnavailable",
    "UserWsCredentials",
    "ensure_supported_ccxt_version",
    "extract_user_ws_credentials",
    "parse_ccxt_version",
    "quantize_price_to_tick",
]
