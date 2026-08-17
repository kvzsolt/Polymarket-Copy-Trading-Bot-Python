"""Persistent day-start equity tracker.

Stores the equity snapshot at the start of each trading day in a JSON file.
Used by the account-cap policy to compute daily drawdown limits.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from utils.logger import get_logger

logger = get_logger("DayEquity")

_STATE_FILE = Path("data/day_equity.json")


def _today_str() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text())
    except Exception as e:
        logger.warning(f"Failed to load day equity state: {e}")
    return {}


def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        logger.warning(f"Failed to save day equity state: {e}")


def get_day_start_equity(current_equity: float | Decimal) -> Decimal:
    """Return today's starting equity, recording it if this is the first call today.

    On a new day (or first-ever run), records the current equity as the day's
    starting equity and returns it. On subsequent calls the same day, returns
    the stored value regardless of current balance changes.
    """
    today = _today_str()
    state = _load_state()

    if state.get("date") == today and state.get("equity") is not None:
        # Persisted as string for lossless Decimal round-trip; Decimal(str())
        # also handles the legacy float-shaped payload safely.
        return Decimal(str(state["equity"]))

    # New day or first run — snapshot current equity as Decimal.
    equity_dec = (
        current_equity if isinstance(current_equity, Decimal) else Decimal(str(current_equity))
    )
    state = {
        "date": today,
        # Stored as string so the JSON round-trip never coerces Decimal
        # through float and loses cent precision on large values.
        "equity": str(equity_dec),
        "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    _save_state(state)
    logger.info("Day-start equity recorded: $%s for %s", equity_dec, today)
    return equity_dec


def get_daily_loss(current_equity: float | Decimal) -> Decimal:
    """Return how much equity has been lost today (positive = loss, 0 = no loss)."""
    day_start = get_day_start_equity(current_equity)
    current = Decimal(str(current_equity))
    loss = day_start - current
    return max(Decimal("0"), loss)
