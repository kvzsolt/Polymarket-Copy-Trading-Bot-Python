#!/usr/bin/env python3
"""V2 smoke test — verifies CLOB V2 SDK connectivity without placing orders.

Usage:
    .venv/bin/python scripts/smoke_v2.py

Exits 0 on full pass, 1 on any failure.
Requires: py-clob-client-v2 installed (pip install py-clob-client-v2).
"""

from __future__ import annotations

import sys

# Known Polymarket token IDs for smoke-testing public endpoints.
# USDC "Yes" token on a popular active market.
KNOWN_TOKEN_ID = "21742633143463906290569050155826241533067272736897614950488156847949938836455"
CLOB_HOST = "https://clob.polymarket.com"


def _pass(name: str) -> None:
    print(f"  PASS  {name}")


def _fail(name: str, detail: str) -> None:
    print(f"  FAIL  {name} — {detail}")


def main() -> int:
    from py_clob_client_v2.client import ClobClient

    failures = 0

    # Read-only client — no auth needed for public endpoints.
    client = ClobClient(CLOB_HOST, chain_id=137)

    # 1. get_version
    print("[1/4] CLOB version")
    try:
        version = client.get_version()
        _pass(f"get_version → {version}")
    except Exception as exc:
        _fail("get_version", str(exc)[:200])
        failures += 1

    # 2. get_tick_size
    print("[2/4] Tick size")
    try:
        ts = client.get_tick_size(KNOWN_TOKEN_ID)
        _pass(f"get_tick_size → {ts}")
    except Exception as exc:
        _fail("get_tick_size", str(exc)[:200])
        failures += 1

    # 3. get_neg_risk
    print("[3/4] Neg-risk flag")
    try:
        nr = client.get_neg_risk(KNOWN_TOKEN_ID)
        _pass(f"get_neg_risk → {nr}")
    except Exception as exc:
        _fail("get_neg_risk", str(exc)[:200])
        failures += 1

    # 4. get_order_book
    print("[4/4] Order book")
    try:
        ob = client.get_order_book(KNOWN_TOKEN_ID)
        bids = list(getattr(ob, "bids", []) or [])
        asks = list(getattr(ob, "asks", []) or [])
        _pass(f"get_order_book → {len(bids)} bids, {len(asks)} asks")
    except Exception as exc:
        _fail("get_order_book", str(exc)[:200])
        failures += 1

    print()
    if failures:
        print(f"FAILED {failures}/4 checks ❌")
        return 1
    print("ALL CHECKS PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
