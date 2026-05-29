"""Regression: collateral_readiness must require explicit addresses.

Historical context: an earlier guard treated two real Polymarket-v2
addresses on Polygon as "known incorrect" because they collided with
unrelated Ethereum-mainnet contract names (Synthetix SNX, etc.). The
guard was wrong about chain context — the same hex string maps to
different deployments on different chains. Per py-clob-client-v2
``get_contract_config(137)``:

  * 0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB → Polymarket pUSD on Polygon
  * 0xE111180000d2663C0091e4f400237545B87B996B → Polymarket exchange_v2 on Polygon

``KNOWN_INCORRECT_ADDRESSES`` is now empty; the primary guard is the
required-kwarg surface (no implicit defaults). If a future need to
hard-reject specific bogus addresses arises, re-populate the set with
addresses confirmed against the SDK's chain config.
"""
from __future__ import annotations

import inspect

from services.collateral_readiness import (
    KNOWN_INCORRECT_ADDRESSES,
    check_collateral_readiness,
)


def test_known_incorrect_addresses_set_is_empty_by_design():
    """No known-incorrect entries — the required-kwarg surface is the guard."""
    assert KNOWN_INCORRECT_ADDRESSES == frozenset()


def test_addresses_are_required_kwargs():
    """No defaults — caller must pass explicit addresses sourced from typed config."""
    sig = inspect.signature(check_collateral_readiness)
    pusd = sig.parameters.get("pusd_address")
    ctf = sig.parameters.get("ctf_exchange_address")
    assert pusd is not None and pusd.default is inspect.Parameter.empty, (
        f"pusd_address must be required; default={pusd.default!r}"
    )
    assert ctf is not None and ctf.default is inspect.Parameter.empty, (
        f"ctf_exchange_address must be required; default={ctf.default!r}"
    )
