"""Canonical Polygon / Polymarket contract addresses (single source of truth).

This module is intentionally **dependency-free** so it can be imported by both
``config.env`` (legacy ``ENV`` surface) and ``config.app_config`` (typed config)
without creating an import cycle — ``app_config`` already imports from ``env``,
so the addresses cannot live in either of those modules without one importing
the other.

If an address ever changes (chain migration, contract upgrade), change it here
and every default — typed config, legacy env, docs-generation, and tests that
import these constants — moves with it.
"""

from __future__ import annotations

# USDC.e (bridged USDC) on Polygon PoS — Polymarket settlement collateral.
POLYGON_USDCE_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Polymarket pUSD collateral token.
POLYMARKET_PUSD_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# Polymarket CTF Exchange (V2).
POLYMARKET_CTF_EXCHANGE_V2_ADDRESS = "0xE111180000d2663C0091e4f400237545B87B996B"

# Polymarket negative-risk CTF Exchange (V2).
POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS = "0xe2222d279d744050d28e00520010520000310F59"

__all__ = [
    "POLYGON_USDCE_ADDRESS",
    "POLYMARKET_PUSD_ADDRESS",
    "POLYMARKET_CTF_EXCHANGE_V2_ADDRESS",
    "POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS",
]
