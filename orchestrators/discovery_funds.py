"""Resolve the operator bankroll used by the Heisenberg-backed live discovery.

Discovery is global (one roster for the whole app), but funds are
per-runtime-user. Per the operator decision, the global discovery uses the
**first runtime user's** funder-address USDC balance as the single bankroll
input. The balance is read via the existing read-only
``utils.getMyBalance.get_my_balance`` (an on-chain ``balanceOf`` call) — no
mutation, no order, no CCXT/SDK authenticated client.

In paper mode the funder address is required (see ``multiUserConfig``), so
this is safe. In live mode the funder may be derived from the SDK; if it is
empty the funds callable returns ``None`` and the discovery cycle fails
closed rather than ranking against an unknown bankroll.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal
from typing import Any

__all__ = ["build_discovery_funds_callable", "resolve_first_funder_address"]


def resolve_first_funder_address(
    runtime_users: Iterable[Any] | None,
    config: Any,
) -> str:
    """Return the first runtime user's funder address, else the single-user funder."""
    if runtime_users is not None:
        for user in runtime_users:
            funder = getattr(getattr(user, "wallet", None), "funder_address", "")
            if funder:
                return str(funder).strip().lower()
    single = getattr(getattr(config, "wallet", None), "funder_address", "")
    return str(single or "").strip().lower()


def build_discovery_funds_callable(
    funder_address: str,
    *,
    get_balance: Callable[[str], Awaitable[Decimal | None]],
) -> Callable[[], Awaitable[Decimal | None]]:
    """Build the read-only ``get_funds`` callable for the discovery adapter.

    Returns ``None`` when the funder address is missing or the balance read
    fails; the adapter treats ``None`` as a fail-closed bankroll-unavailable
    cycle. The callable never raises into the discovery loop on a balance
    error — ``get_my_balance`` already swallows transport errors and
    returns ``None``.
    """

    async def get_funds() -> Decimal | None:
        if not funder_address:
            return None
        return await get_balance(funder_address)

    return get_funds