"""Fail-closed live-submission permission check."""

from __future__ import annotations

from storage.postgres.trading_control_repo import TradingControlRepo, TradingControlState


class TradingPausedError(RuntimeError):
    pass


class TradingPermissionService:
    def __init__(self, repo: TradingControlRepo, *, live_mode: bool) -> None:
        self._repo = repo
        self._live_mode = live_mode

    async def require_submission_allowed(self) -> TradingControlState | None:
        if not self._live_mode:
            return None
        try:
            state = await self._repo.get()
        except Exception as exc:
            raise TradingPausedError(
                "trading control unavailable; live submission blocked"
            ) from exc
        if not state.enabled:
            raise TradingPausedError(
                f"live trading paused (generation={state.generation}, reason={state.reason})"
            )
        return state


__all__ = ["TradingPausedError", "TradingPermissionService"]
