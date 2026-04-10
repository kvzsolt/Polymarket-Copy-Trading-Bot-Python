from __future__ import annotations

from config.env import ENV
from utils.logger import console, shorten_addr


def print_startup_summary(
    *,
    wallet: str,
    available_cash: float,
    total_portfolio: float,
    positions: list[dict],
    active_leaders: list[str],
) -> None:
    console.print("[bold cyan]Startup summary[/bold cyan]")
    console.print(
        f"Portfolio: wallet={shorten_addr(wallet)} cash=${available_cash:.2f} "
        f"total=${total_portfolio:.2f} open_positions={len(positions)}"
    )

    if positions:
        for position in positions[:8]:
            market = str(position.get("title") or position.get("market") or position.get("condition_id") or "?")
            side = str(position.get("outcome") or position.get("side") or "?")
            size = float(position.get("size") or position.get("size_tokens") or 0.0)
            value = float(position.get("currentValue") or position.get("value") or 0.0)
            console.print(f"  - {market[:60]} | {side} | size={size:.2f} | value=${value:.2f}")
        remaining = len(positions) - 8
        if remaining > 0:
            console.print(f"  - ... and {remaining} more position(s)")
    else:
        console.print("  - No open positions")

    leader_list = ", ".join(shorten_addr(wallet_addr) for wallet_addr in active_leaders[:8]) or "none"
    if len(active_leaders) > 8:
        leader_list = f"{leader_list}, +{len(active_leaders) - 8} more"
    console.print(f"Copied traders ({len(active_leaders)}): {leader_list}")

    console.print(
        "Runtime: "
        f"sizing={ENV.get('SIZING_MODE', 'proportional')} "
        f"copy={ENV.get('PERCENTAGE')}% "
        f"buy_min=${ENV.get('BUY_MIN_AMOUNT')} "
        f"buy_max=${ENV.get('BUY_MAX_AMOUNT')} "
        f"max_exposure=${ENV.get('MAX_MARKET_EXPOSURE_USDC')} "
        f"sl={ENV.get('STOP_LOSS_PCT')}% "
        f"tp={ENV.get('TAKE_PROFIT_PCT')}% "
        f"slippage<={ENV.get('SLIPPAGE_TOLERANCE')}%"
    )
    console.print()
