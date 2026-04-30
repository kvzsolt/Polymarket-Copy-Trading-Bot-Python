# Phase 2 Implementation Prompt

Repo: `/home/anon/Bots/Polymarket-Copy-Trading-Bot-Python`
Base branch: latest `develop` after Phase 1 merge
Working branch: `phase/02-price-cache-wss-heartbeat`

Implement **Phase 2 only**.

## Mission
1. Add a position/token-scoped market data cache for quote/tick/meta.
2. Add TTL + invalidation.
3. Add market websocket client.
4. Add user websocket client.
5. Implement **5-second heartbeat ping**.
6. Implement reconnect behavior with REST snapshot bootstrap followed by WSS deltas.

## Required invalidation triggers
- own fill
- price change
- tick size change
- reconcile

## Required websocket rules
- market subscribe uses `assets_ids`
- heartbeat is every **5 seconds**
- if heartbeat/reply fails, reconnect

## Required code shape
```python
async def _heartbeat(ws):
    while True:
        await asyncio.sleep(5)
        await ws.send("PING")
```

```python
await ws.send(json.dumps({
    "assets_ids": asset_ids,
    "type": "market",
    "custom_feature_enabled": True,
}))
```

## Constraints
- Do not do Phase 3 sizing work here.
- Preserve logging shape.
- Use additive changes.

## Required tests
- cache reuse
- cache invalidation on fill/tick change
- heartbeat interval behavior
- reconnect/resync behavior
- parsing basic market/user lifecycle events

## Required output
- code changes
- tests passing for this slice
- short implementation summary
- git commit for the phase
