"""Framework-neutral money-path addon for V5 sizing."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Awaitable, Callable, Generic, TypeVar

from services.copy_sizing_v5.domain import CopySizingDecision, CopySizingRequest
from services.copy_sizing_v5.engine import CopySizingEngine

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True, slots=True)
class CopySizingEnvelope(Generic[T]):
    payload: T
    request: CopySizingRequest


@dataclass(frozen=True, slots=True)
class CopySizingAdmission(Generic[T]):
    payload: T
    decision: CopySizingDecision


@dataclass(frozen=True, slots=True)
class CopySizingSkip(Generic[T]):
    payload: T
    decision: CopySizingDecision


class CopySizingV5Addon:
    def __init__(self, engine: CopySizingEngine) -> None:
        self._engine = engine

    async def handle(
        self,
        envelope: CopySizingEnvelope[T],
        *,
        next_handler: Callable[[CopySizingAdmission[T]], R | Awaitable[R]],
    ) -> R | CopySizingSkip[T]:
        decision = self._engine.size(envelope.request)
        if not decision.allowed:
            return CopySizingSkip(payload=envelope.payload, decision=decision)
        result = next_handler(CopySizingAdmission(payload=envelope.payload, decision=decision))
        if inspect.isawaitable(result):
            return await result
        return result


__all__ = [
    "CopySizingEnvelope",
    "CopySizingAdmission",
    "CopySizingSkip",
    "CopySizingV5Addon",
]
