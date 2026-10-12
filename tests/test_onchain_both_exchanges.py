"""Both-exchange coverage tests (PR2 Phase 8/Phase 10).

Asserts that the on-chain detection subscription surfaces and the
``ONCHAIN_EXCHANGE_ADDRESSES`` constant cover BOTH the CTF Exchange V2 and
the Neg Risk CTF Exchange V2, so detection never silently misses one venue.
"""

from __future__ import annotations

from pmcopy.config.addresses import (
    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
)
from pmcopy.services.onchain_detection.order_filled_detector import (
    ONCHAIN_EXCHANGE_ADDRESSES,
    log_subscription_params,
)
from pmcopy.services.onchain_detection.pending_tx_detector import (
    pending_subscription_params,
)


def test_config_addresses_has_both_exchanges() -> None:
    assert (
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() == "0xe111180000d2663c0091e4f400237545b87b996b"
    )
    assert (
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower()
        == "0xe2222d279d744050d28e00520010520000310f59"
    )


def test_onchain_constant_covers_both() -> None:
    addresses = set(ONCHAIN_EXCHANGE_ADDRESSES)
    assert POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() in addresses
    assert POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower() in addresses
    assert len(addresses) == 2


def test_log_subscription_covers_both() -> None:
    params = log_subscription_params()
    assert isinstance(params, list) and params[0] == "logs"
    addresses = {a.lower() for a in params[1]["address"]}
    assert POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() in addresses
    assert POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower() in addresses


def test_pending_subscription_covers_both() -> None:
    params = pending_subscription_params()
    assert isinstance(params, list) and params[0] == "alchemy_pendingTransactions"
    addresses = {a.lower() for a in params[1]["toAddress"]}
    assert POLYMARKET_CTF_EXCHANGE_V2_ADDRESS.lower() in addresses
    assert POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS.lower() in addresses
