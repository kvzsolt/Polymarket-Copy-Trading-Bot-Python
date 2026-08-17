#!/usr/bin/env python3
"""Fixture recorder for Polymarket Market WSS events.

Read-only diagnostic tool. No trading dependencies, no execution services.
Connects to the Polymarket market WebSocket and captures events to a JSONL file.

Usage:
    python scripts/capture_market_wss_events.py \
        --tokens TOKEN_A,TOKEN_B \
        --duration-seconds 120 \
        --output tests/fixtures/market_wss/live_capture.jsonl \
        --target-event book \
        --target-event price_change
"""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
except ImportError:
    sys.exit("websockets package required: pip install websockets")

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HEARTBEAT_INTERVAL = 10


async def _heartbeat(ws: Any, deadline: float) -> None:
    while time.monotonic() < deadline:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            await ws.send("PING")
        except Exception:
            return


async def capture(
    tokens: list[str],
    duration_seconds: float,
    output_path: Path,
    target_events: set[str] | None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + duration_seconds
    event_count = 0

    print(
        f"Connecting to {MARKET_WS_URL} | tokens={len(tokens)} duration={duration_seconds}s",
        file=sys.stderr,
    )

    async with websockets.connect(
        MARKET_WS_URL,
        ping_interval=None,
        close_timeout=5,
    ) as ws:
        subscribe = {
            "type": "market",
            "assets_ids": tokens,
            "initial_dump": True,
            "level": 2,
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(subscribe))
        print(f"Subscribed. Capturing to {output_path} ...", file=sys.stderr)

        heartbeat_task = asyncio.create_task(_heartbeat(ws, deadline))
        try:
            with output_path.open("a", encoding="utf-8") as fh:
                while time.monotonic() < deadline:
                    remaining = max(0.1, deadline - time.monotonic())
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    except ConnectionClosed:
                        print("Connection closed by server.", file=sys.stderr)
                        break

                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    if raw in ("PONG", "pong", ""):
                        continue
                    if raw in ("PING", "ping"):
                        try:
                            await ws.send("PONG")
                        except Exception:
                            pass
                        continue

                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    events = data if isinstance(data, list) else [data]
                    for event in events:
                        if not isinstance(event, dict):
                            continue
                        event_type = str(event.get("event_type") or event.get("type") or "")
                        if target_events and event_type not in target_events:
                            continue
                        record = {
                            "captured_at_monotonic": time.monotonic(),
                            "captured_at_unix": time.time(),
                            "event_type": event_type,
                            "raw": event,
                        }
                        fh.write(json.dumps(record) + "\n")
                        fh.flush()
                        event_count += 1
                        print(f"  [{event_count}] {event_type}", file=sys.stderr)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    print(f"Done. Captured {event_count} event(s) → {output_path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture Polymarket Market WSS events to a JSONL fixture file."
    )
    parser.add_argument(
        "--tokens",
        required=True,
        help="Comma-separated token/asset IDs to subscribe to",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=120,
        metavar="N",
        help="How long to capture (seconds, default: 120)",
    )
    parser.add_argument(
        "--output",
        default="tests/fixtures/market_wss/live_capture.jsonl",
        help="Output JSONL file path (appended if exists)",
    )
    parser.add_argument(
        "--target-event",
        action="append",
        dest="target_events",
        metavar="EVENT_TYPE",
        help="Record only these event types (repeatable; omit to capture all)",
    )
    args = parser.parse_args()

    tokens = [t.strip() for t in args.tokens.split(",") if t.strip()]
    if not tokens:
        parser.error("--tokens must provide at least one token ID")

    target_events: set[str] | None = set(args.target_events) if args.target_events else None

    asyncio.run(
        capture(
            tokens=tokens,
            duration_seconds=args.duration_seconds,
            output_path=Path(args.output),
            target_events=target_events,
        )
    )


if __name__ == "__main__":
    main()
