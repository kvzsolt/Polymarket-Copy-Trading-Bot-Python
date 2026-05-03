"""V2 collateral readiness — pUSD balance and allowance checks.

Verifies that the bot's wallet has pUSD collateral and CTF exchange
allowance before allowing live trading.  All checks are read-only; no
automatic fund wrapping or movement is performed.

Called during startup via ``check_collateral_readiness()`` which logs a
structured summary and returns a boolean.  When the check fails the bot
should NOT start live trading — the operator must resolve the shortfall
or explicitly acknowledge via ``CLOB_V2_READY=true``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from web3 import Web3

logger = logging.getLogger("collateral_readiness")

_ERC20_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]

@dataclass(frozen=True)
class CollateralStatus:
    """Structured result of collateral readiness checks."""

    pusd_balance: Decimal
    pusd_allowance_ctf: Decimal
    pusd_allowance_neg_risk: Decimal
    wallet_address: str
    rpc_url: str
    ready: bool
    failures: tuple[str, ...]


def _erc20_read(
    *,
    rpc_url: str,
    contract_address: str,
    method: str,
    args: list[Any],
) -> int:
    """Synchronous ERC-20 read call."""
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    checksum_contract = Web3.to_checksum_address(contract_address)
    contract = w3.eth.contract(address=checksum_contract, abi=_ERC20_ABI)
    func = getattr(contract.functions, method)
    return func(*args).call()


def _check_sync(
    *,
    wallet_address: str,
    rpc_url: str,
    pusd_address: str,
    ctf_exchange_address: str,
    neg_risk_exchange_address: str,
    min_pusd_balance: Decimal,
) -> CollateralStatus:
    """Run all collateral checks synchronously (call from thread)."""
    failures: list[str] = []
    checksum_wallet = Web3.to_checksum_address(wallet_address)
    pusd_contract = Web3.to_checksum_address(pusd_address)

    # 1. pUSD balance
    try:
        raw_balance = _erc20_read(
            rpc_url=rpc_url,
            contract_address=pusd_contract,
            method="balanceOf",
            args=[checksum_wallet],
        )
        pusd_balance = Decimal(raw_balance) / Decimal(10**6)
    except Exception as exc:
        pusd_balance = Decimal("0")
        failures.append(f"pUSD balance check failed: {exc}")

    if pusd_balance < min_pusd_balance:
        failures.append(
            f"pUSD balance {pusd_balance:.2f} < minimum {min_pusd_balance}"
        )

    # 2. CTF Exchange allowance
    try:
        raw_allowance = _erc20_read(
            rpc_url=rpc_url,
            contract_address=pusd_contract,
            method="allowance",
            args=[checksum_wallet, Web3.to_checksum_address(ctf_exchange_address)],
        )
        pusd_allowance_ctf = Decimal(raw_allowance) / Decimal(10**6)
    except Exception as exc:
        pusd_allowance_ctf = Decimal("0")
        failures.append(f"CTF exchange allowance check failed: {exc}")

    if pusd_allowance_ctf <= 0:
        failures.append(
            "CTF Exchange pUSD allowance is 0 — approve pUSD spending for "
            f"{ctf_exchange_address}"
        )

    # 3. Neg Risk Exchange allowance
    try:
        raw_nr = _erc20_read(
            rpc_url=rpc_url,
            contract_address=pusd_contract,
            method="allowance",
            args=[checksum_wallet, Web3.to_checksum_address(neg_risk_exchange_address)],
        )
        pusd_allowance_neg_risk = Decimal(raw_nr) / Decimal(10**6)
    except Exception as exc:
        pusd_allowance_neg_risk = Decimal("0")
        failures.append(f"Neg Risk exchange allowance check failed: {exc}")

    if pusd_allowance_neg_risk <= 0:
        failures.append(
            "Neg Risk Exchange pUSD allowance is 0 — approve pUSD spending for "
            f"{neg_risk_exchange_address}"
        )

    ready = len(failures) == 0
    return CollateralStatus(
        pusd_balance=pusd_balance,
        pusd_allowance_ctf=pusd_allowance_ctf,
        pusd_allowance_neg_risk=pusd_allowance_neg_risk,
        wallet_address=wallet_address,
        rpc_url=rpc_url,
        ready=ready,
        failures=tuple(failures),
    )


async def check_collateral_readiness(
    *,
    wallet_address: str,
    rpc_url: str,
    pusd_address: str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
    ctf_exchange_address: str = "0xE111180000d2663C0091e4f400237545B87B996B",
    neg_risk_exchange_address: str = "0xe2222d279d744050d28e00520010520000310F59",
    min_pusd_balance: Decimal = Decimal("1.0"),
) -> CollateralStatus:
    """Run pUSD collateral readiness checks.

    Returns a ``CollateralStatus`` with structured details.
    Logs a summary and any failures at WARNING level.
    """
    status = await asyncio.to_thread(
        _check_sync,
        wallet_address=wallet_address,
        rpc_url=rpc_url,
        pusd_address=pusd_address,
        ctf_exchange_address=ctf_exchange_address,
        neg_risk_exchange_address=neg_risk_exchange_address,
        min_pusd_balance=min_pusd_balance,
    )

    if status.ready:
        logger.info(
            "V2 collateral readiness OK — pUSD balance=%.2f, "
            "CTF allowance=%.2f, NegRisk allowance=%.2f",
            status.pusd_balance,
            status.pusd_allowance_ctf,
            status.pusd_allowance_neg_risk,
        )
    else:
        for failure in status.failures:
            logger.warning("V2 collateral readiness FAILURE: %s", failure)
        logger.warning(
            "Live trading should NOT start until these issues are resolved. "
            "Set CLOB_V2_READY=true only after manually confirming readiness."
        )

    return status
