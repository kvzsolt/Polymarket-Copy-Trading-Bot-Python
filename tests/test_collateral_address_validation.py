"""Regression: collateral_readiness must fail-closed on known-wrong addresses.

Pre-fix the function carried hardcoded defaults:
  pusd_address          = 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB  # Synthetix SNX
  ctf_exchange_address  = 0xE111180000d2663C0091e4f400237545B87B996B  # not Polymarket's

If the operator forgot to set the env values, the readiness probe would
read the SNX balance (≈0 for a Polymarket trader) and report
``ready=False`` for the wrong reason, OR — worse — if a checked address
happened to be configured but pointing at a different contract, the
probe could silently pass with a stale value.

This test pins:
  * The function refuses to accept the known-bad SNX address and the
    placeholder CTF address as either parameter.
  * The contract addresses are required (no implicit defaults).
"""
from __future__ import annotations

import asyncio

import pytest

from services.collateral_readiness import (
    KNOWN_INCORRECT_ADDRESSES,
    check_collateral_readiness,
)


SNX_BAD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PLACEHOLDER_CTF = "0xE111180000d2663C0091e4f400237545B87B996B"


def test_known_incorrect_addresses_set_populated():
    assert SNX_BAD.lower() in {a.lower() for a in KNOWN_INCORRECT_ADDRESSES}
    assert PLACEHOLDER_CTF.lower() in {a.lower() for a in KNOWN_INCORRECT_ADDRESSES}


def test_check_rejects_snx_as_pusd_address():
    async def _run():
        with pytest.raises(ValueError, match=r"known-incorrect"):
            await check_collateral_readiness(
                wallet_address="0x1111111111111111111111111111111111111111",
                rpc_url="https://rpc.example",
                pusd_address=SNX_BAD,
                ctf_exchange_address="0x2222222222222222222222222222222222222222",
            )

    asyncio.run(_run())


def test_check_rejects_placeholder_ctf_address():
    async def _run():
        with pytest.raises(ValueError, match=r"known-incorrect"):
            await check_collateral_readiness(
                wallet_address="0x1111111111111111111111111111111111111111",
                rpc_url="https://rpc.example",
                pusd_address="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                ctf_exchange_address=PLACEHOLDER_CTF,
            )

    asyncio.run(_run())


def test_addresses_are_required_kwargs():
    """No defaults — caller must pass explicit addresses sourced from typed config."""
    import inspect

    sig = inspect.signature(check_collateral_readiness)
    pusd = sig.parameters.get("pusd_address")
    ctf = sig.parameters.get("ctf_exchange_address")
    assert pusd is not None and pusd.default is inspect.Parameter.empty, (
        f"pusd_address must be required; default={pusd.default!r}"
    )
    assert ctf is not None and ctf.default is inspect.Parameter.empty, (
        f"ctf_exchange_address must be required; default={ctf.default!r}"
    )
