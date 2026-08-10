"""Composition helpers for the branch's existing immutable SizingConfig."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from services.copy_sizing_v5.addon import CopySizingV5Addon
from services.copy_sizing_v5.balance_provider import CachedPusdBalanceProvider
from services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from services.copy_sizing_v5.engine import CopySizingEngine
from services.copy_sizing_v5.policy import CopySizingPolicy, copy_sizing_policy_from_config


@dataclass(frozen=True, slots=True)
class CopySizingV5Runtime:
    policy: CopySizingPolicy
    engine: CopySizingEngine
    addon: CopySizingV5Addon
    balances: CachedPusdBalanceProvider | None = None

    async def size_buy(self, request: CopySizingRequest) -> CopySizingDecision:
        hydrated = request
        if self.balances is not None:
            try:
                follower, leader = await self.balances.balances_for(
                    request.leader_wallet
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Never leak provider payloads or credentials into the money-path
                # decision record; the exception class is enough for metrics/audit.
                return self.engine.reject(
                    request,
                    f"balance_unavailable:{type(exc).__name__}",
                )
            hydrated = request.with_updates(
                follower_pusd_balance=follower,
                leader_pusd_balance=leader,
            )
        return self.engine.size(hydrated)


def build_copy_sizing_v5_runtime(
    config: Any,
    *,
    balances: CachedPusdBalanceProvider | None = None,
    overrides: dict[str, Any] | None = None,
) -> CopySizingV5Runtime:
    """Build the V5 sizing runtime from ``config.sizing`` (the V5 envelope
    fields were folded into ``config/sizing.yml``; the standalone
    ``copy_sizing_v5.yaml`` file is gone)."""
    source = getattr(config, "sizing", config)
    policy = copy_sizing_policy_from_config(source, dict(overrides or {}))
    engine = CopySizingEngine(policy)
    return CopySizingV5Runtime(
        policy=policy,
        engine=engine,
        addon=CopySizingV5Addon(engine),
        balances=balances,
    )


__all__ = ["CopySizingV5Runtime", "build_copy_sizing_v5_runtime"]
