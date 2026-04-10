from __future__ import annotations

import aiohttp

from utils.fetchData import fetch_data
from utils.logger import get_logger

logger = get_logger("Compliance")


GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


async def check_geoblock() -> tuple[bool, str]:
    """Return (allowed, reason)."""
    try:
        data = await fetch_data(GEOBLOCK_URL, silent=True)
    except aiohttp.ClientResponseError as exc:
        if int(getattr(exc, "status", 0) or 0) > 0:
            return True, f"geoblock_check_http_{exc.status}"
        logger.warning(f"Geoblock check failed open: {exc}")
        return True, "geoblock_check_failed_open"
    except Exception as exc:
        logger.warning(f"Geoblock check failed open: {exc}")
        return True, "geoblock_check_failed_open"

    if not isinstance(data, dict):
        logger.warning("Geoblock check failed open: invalid payload type")
        return True, "geoblock_check_failed_open"

    blocked = bool(data.get("blocked", False))
    if blocked:
        return False, "region_blocked_by_polymarket"
    return True, "ok"
