import ccxt
import asyncio

async def test():
    exc = ccxt.RequestTimeout("timeout")
    print(dir(exc))
    print(getattr(exc, 'request', None))
    print(getattr(exc, 'response', None))

asyncio.run(test())
