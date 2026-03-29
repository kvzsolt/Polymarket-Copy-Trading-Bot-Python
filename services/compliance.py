from __future__ import annotations

import aiohttp

from utils.logger import get_logger

logger = get_logger("Compliance")


GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


async def check_geoblock() -> tuple[bool, str]:
    """Return (allowed, reason)."""
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(GEOBLOCK_URL) as resp:
                if resp.status != 200:
                    return True, f"geoblock_check_http_{resp.status}"
                data = await resp.json()
    except Exception as exc:
        logger.warning(f"Geoblock check failed open: {exc}")
        return True, "geoblock_check_failed_open"

    blocked = bool(data.get("blocked", False))
    if blocked:
        return False, "region_blocked_by_polymarket"
    return True, "ok"
