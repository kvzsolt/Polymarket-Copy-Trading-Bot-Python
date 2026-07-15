from __future__ import annotations

import asyncio


def new_event_loop() -> asyncio.AbstractEventLoop:
    """Use uvloop when installed; preserve a portable stdlib fallback."""
    try:
        import uvloop
    except ImportError:
        return asyncio.new_event_loop()
    return uvloop.new_event_loop()


__all__ = ["new_event_loop"]
