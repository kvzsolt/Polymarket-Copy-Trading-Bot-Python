#!/usr/bin/env python3
"""Read-only smoke probe for the pinned unified Polymarket SDK."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import argparse
import asyncio

from polymarket import AsyncPublicClient


async def _main(token_id: str) -> int:
    async with AsyncPublicClient() as client:
        book = await client.get_order_book(token_id=token_id)
        print(
            {
                "token_id": str(book.token_id),
                "tick_size": str(book.tick_size),
                "min_order_size": str(book.min_order_size),
                "neg_risk": book.neg_risk,
                "bids": len(book.bids),
                "asks": len(book.asks),
            }
        )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("token_id")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_main(args.token_id)))
