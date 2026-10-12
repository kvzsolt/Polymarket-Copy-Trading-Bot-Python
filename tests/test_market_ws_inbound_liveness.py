"""P1-d regression: WSS connection liveness must be anchored on INBOUND
activity only.

``wss_connection_age_seconds`` previously returned ``now - max(last_heartbeat,
last_pong)``. ``last_heartbeat`` is refreshed by our own outbound PING every
~5s, so a silent / half-open feed (PINGs still queue into the socket but no
server frame returns) was reported as "fresh" until the in-loop pong detector
finally closed it ~10-15s later — long enough to serve a stale book past the
2s book-age budget. Only a frame *from* the server proves the pipe is alive.
"""

import time

from pmcopy.cache.market_state_store import MarketStateStore


def test_outbound_heartbeat_alone_does_not_prove_liveness():
    store = MarketStateStore()
    # Only the self-sent PING has fired; nothing inbound from the server yet.
    store.on_market_ws_heartbeat(time.monotonic())
    assert store.wss_connection_age_seconds() is None


def test_inbound_activity_proves_liveness():
    store = MarketStateStore()
    store.on_market_ws_pong(time.monotonic())
    age = store.wss_connection_age_seconds()
    assert age is not None
    assert age < 1.0


def test_recent_outbound_heartbeat_does_not_mask_stale_inbound():
    store = MarketStateStore()
    now = time.monotonic()
    store.on_market_ws_pong(now - 30.0)  # last genuine inbound was 30s ago
    store.on_market_ws_heartbeat(now)  # but we kept PINGing every 5s
    age = store.wss_connection_age_seconds()
    assert age is not None
    assert age >= 29.0  # anchored on inbound, not on the outbound heartbeat
