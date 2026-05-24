"""Boot-time position snapshot service.

Fetches current positions for every followed leader wallet **and** the
bot's own wallet from the Polymarket data API, normalises into
``PositionSnapshotRecord`` objects, persists to
``discovery_position_snapshots`` tagged with ``wallet_role`` so the
bot accumulates a time-series, and returns the in-memory snapshot
dict so the bootup banner can render without re-fetching.

Fail-soft: any fetch error logs a WARNING and returns an empty list
for that wallet — bootup must not block on data-api availability.

Reuses:
  * ``services.tradeMonitor._guarded_leader_fetch`` for per-leader
    rate-limiting (avoids hammering the data-api on a watchlist of
    20+ leaders during boot).
  * ``services.discovery.ingestion_service._normalise_position`` for
    the canonical payload shape handling. Lifted into a
    module-level ``normalise_position`` here so the snapshot
    service does not have to instantiate the ingestion-service
    class just to call one method.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from domain.discovery_models import PositionSnapshotRecord
from storage.postgres.discovery_repo import DiscoveryRepo
from utils.logger import shorten_addr

logger = logging.getLogger("PositionSnapshotService")

DATA_API_POSITIONS_URL = "https://data-api.polymarket.com/positions?user={wallet}"


@dataclass(frozen=True)
class SnapshotResult:
    """In-memory result returned for banner rendering."""

    leader_snapshots: dict[str, list[PositionSnapshotRecord]]
    own_snapshots: list[PositionSnapshotRecord]
    leaders_failed: list[str]
    own_failed: bool


def _safe_decimal(value: Any, default: Decimal) -> Decimal:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def normalise_position(
    wallet: str, raw: dict[str, Any], snapshot_ts: int
) -> PositionSnapshotRecord | None:
    """Convert a single data-api position row into a snapshot record.

    Returns ``None`` if the row lacks the identity fields we require
    (``conditionId`` + ``tokenID``). Mirrors
    ``services.discovery.ingestion_service._normalise_position`` —
    intentional duplication so a future ingestion-service refactor
    doesn't accidentally drift the snapshot shape.
    """
    try:
        cond_id = raw.get("conditionId") or raw.get("condition_id") or ""
        token_id = (
            raw.get("tokenID") or raw.get("token_id") or raw.get("asset") or ""
        )
        if not cond_id or not token_id:
            return None
        return PositionSnapshotRecord(
            trader_wallet=wallet.lower().strip(),
            condition_id=str(cond_id),
            token_id=str(token_id),
            size=_safe_decimal(raw.get("size"), Decimal("0")),
            avg_price=_safe_decimal(
                raw.get("avgPrice") or raw.get("avg_price"), Decimal("0")
            ),
            current_value=_safe_decimal(
                raw.get("currentValue") or raw.get("current_value"),
                Decimal("0"),
            ),
            current_price=_safe_decimal(
                raw.get("curPrice")
                or raw.get("cur_price")
                or raw.get("currentPrice"),
                Decimal("0"),
            ),
            realized_pnl=_safe_decimal(
                raw.get("realizedPnl") or raw.get("realized_pnl"),
                Decimal("0"),
            ),
            snapshot_ts=snapshot_ts,
            raw_json=json.dumps(raw, default=str),
        )
    except (TypeError, ValueError, KeyError) as exc:
        logger.debug(
            "position_normalise_failed wallet=%s err=%r raw_keys=%s",
            shorten_addr(wallet),
            exc,
            list(raw.keys()) if isinstance(raw, dict) else "<non-dict>",
        )
        return None


async def _fetch_wallet_positions(
    wallet: str,
    fetcher: Callable[[str], Awaitable[Any]],
) -> list[dict[str, Any]] | None:
    """Fetch positions for a single wallet via the supplied async callable.

    The callable is supplied by the caller so the bot's main runtime
    can wire in the rate-limited ``_guarded_leader_fetch`` from
    tradeMonitor while tests can inject a stub.

    cubic P2: returns ``None`` on fetch error or unexpected payload
    shape (distinguishable failure) and ``[]`` only when the API
    actually returns an empty list (valid empty result). Callers can
    then tell "wallet has zero positions" apart from "fetch crashed".
    """
    url = DATA_API_POSITIONS_URL.format(wallet=wallet)
    try:
        raw = await fetcher(url)
    except Exception as exc:
        logger.warning(
            "snapshot_fetch_failed wallet=%s err=%r",
            shorten_addr(wallet),
            exc,
        )
        return None
    if not isinstance(raw, list):
        logger.warning(
            "snapshot_fetch_unexpected_shape wallet=%s type=%s",
            shorten_addr(wallet),
            type(raw).__name__,
        )
        return None
    return raw


async def snapshot_followed_leaders(
    *,
    leader_wallets: list[str],
    own_wallets: list[str],
    repo: DiscoveryRepo,
    fetcher: Callable[[str], Awaitable[Any]],
    snapshot_ts: int | None = None,
) -> SnapshotResult:
    """Snapshot every followed leader + every own wallet.

    Multiple own wallets are supported because the multi-user runtime
    can be configured with N user wallets; each is tagged
    ``wallet_role='own'``.

    Persists each snapshot to ``discovery_position_snapshots`` with the
    appropriate role tag. Returns the in-memory snapshot tree so the
    banner can render without re-fetching.

    Fail-soft: every fetch is independent — a single failed wallet does
    not poison the whole snapshot.
    """
    ts = snapshot_ts or int(time.time())
    leader_snapshots: dict[str, list[PositionSnapshotRecord]] = {}
    own_snapshots: list[PositionSnapshotRecord] = []
    leaders_failed: list[str] = []
    own_failed = False

    # Leaders.
    for leader in leader_wallets:
        leader_lc = leader.lower().strip()
        raw_rows = await _fetch_wallet_positions(leader_lc, fetcher)
        if raw_rows is None:
            # cubic P2: ``None`` is the explicit fetch-failure signal.
            # An empty list ``[]`` means the wallet legitimately has no
            # open positions and should NOT be tagged as failed.
            leaders_failed.append(leader_lc)
            leader_snapshots[leader_lc] = []
            continue
        snapshots: list[PositionSnapshotRecord] = []
        for raw in raw_rows:
            snap = normalise_position(leader_lc, raw, ts)
            if snap is None:
                continue
            # Persist; UniqueViolationError → None (already snapshotted
            # at this exact ts) — surface to in-memory list either way.
            try:
                await repo.insert_position_snapshot(snap, wallet_role="leader")
            except Exception as exc:
                logger.warning(
                    "snapshot_persist_failed wallet=%s cond=%s err=%r",
                    shorten_addr(leader_lc),
                    snap.condition_id,
                    exc,
                )
            snapshots.append(snap)
        leader_snapshots[leader_lc] = snapshots

    # Own wallets.
    for own in own_wallets:
        own_lc = own.lower().strip()
        raw_rows = await _fetch_wallet_positions(own_lc, fetcher)
        if raw_rows is None:
            own_failed = True
            continue
        for raw in raw_rows:
            snap = normalise_position(own_lc, raw, ts)
            if snap is None:
                continue
            try:
                await repo.insert_position_snapshot(snap, wallet_role="own")
            except Exception as exc:
                logger.warning(
                    "snapshot_persist_failed wallet=%s cond=%s err=%r",
                    shorten_addr(own_lc),
                    snap.condition_id,
                    exc,
                )
            own_snapshots.append(snap)

    logger.info(
        "boot_snapshot_complete leaders=%d own=%d leaders_failed=%d own_failed=%s",
        sum(len(v) for v in leader_snapshots.values()),
        len(own_snapshots),
        len(leaders_failed),
        own_failed,
    )
    return SnapshotResult(
        leader_snapshots=leader_snapshots,
        own_snapshots=own_snapshots,
        leaders_failed=leaders_failed,
        own_failed=own_failed,
    )
