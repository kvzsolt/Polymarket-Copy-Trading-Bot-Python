"""Strategy profiler + env tuner (PR3).

Profiles a followed Polymarket trader's historical trade/position history,
classifies it into one of six :class:`StrategyArchetype` values, and proposes
a set of :class:`EnvPatch` changes to ``config/runtime.local.env`` that tune
the bot's copy parameters for that archetype.

Dry-run by default. ``--apply`` is required to mutate the env file, and
protected keys (secrets, RPC, leaders) are never mutated.
"""
from __future__ import annotations

from services.strategy_profile.env_patch_writer import EnvPatchWriter
from services.strategy_profile.leader_history_loader import (
    LeaderHistoryLoader,
    LoadedHistory,
)
from services.strategy_profile.strategy_classifier import classify_profile
from services.strategy_profile.strategy_env_tuner import tune_strategy
from services.strategy_profile.strategy_profiler import compute_features, profile_leader

__all__ = [
    "EnvPatchWriter",
    "LeaderHistoryLoader",
    "LoadedHistory",
    "classify_profile",
    "compute_features",
    "profile_leader",
    "tune_strategy",
]