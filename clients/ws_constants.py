"""WebSocket protocol constants.

Phase 2 spec mandates a 5-second heartbeat cadence and a 10-second pong
timeout for both market and user streams; missing a pong within the window
must trigger a reconnect (handled by ``MarketWebSocket``/``UserWebSocket``).
"""

MARKET_HEARTBEAT_INTERVAL: int = 5
USER_HEARTBEAT_INTERVAL: int = 5
PONG_TIMEOUT_SECONDS: float = 10.0

# Backwards-compat alias; prefer the named constants above
HEARTBEAT_INTERVAL = MARKET_HEARTBEAT_INTERVAL
