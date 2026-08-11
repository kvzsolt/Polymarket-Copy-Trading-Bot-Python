from __future__ import annotations

import time
import re
import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from aiohttp import web

from services.metrics import metrics


# Subsystem keys whose truthiness is required for /health/ready (strict).
_STRICT_REQUIRED_KEYS: tuple[str, ...] = (
    "db_ok",
    "api_ok",
    "gateway_ok",
    "wss_market_ok",
    "wss_user_ok",
    "reconciler_ok",
    "market_data_ready",
    "drift_ok",
)

# Conservative fail-safe staleness budget (seconds) for a monitored loop's
# heartbeat when no interval-derived budget has been wired. Used by the strict
# readiness check so a silently-dead trade-poll / risk loop flips /health/ready
# to 503 instead of leaving it green. Deliberately generous so a single slow
# data-api cycle does not flap the Docker HEALTHCHECK target.
_DEFAULT_HEARTBEAT_STALENESS_SECONDS: float = 120.0

# Floor applied to any interval-derived budget so a sub-second poll interval
# (FETCH_INTERVAL defaults to 0.25s) cannot produce a knife-edge budget that
# flaps on normal jitter.
_MIN_HEARTBEAT_STALENESS_SECONDS: float = 30.0

# Heartbeat timestamp keys monitored by strict readiness, mapped to a stable,
# clearly-named failure token surfaced in the /health/ready ``failed`` list.
#
# IMPORTANT (fail-closed WITHOUT flapping): a heartbeat is only treated as
# *stale* once the loop has ticked at least once (ts > 0). A ts of 0 means
# "loop has not ticked yet" — which is the legitimate state both (a) during
# boot before the monitor tasks spawn (the metrics server starts first) and
# (b) for a loop that is config-disabled and will never tick (e.g. the risk
# loop when enable_risk_mgmt is False). Failing readiness on ts==0 would wedge
# /health/ready closed forever in those cases. The real P2-g failure mode — a
# loop that WAS alive and then silently died — is still caught because its ts
# is non-zero and stops advancing.
_HEARTBEAT_KEYS: tuple[tuple[str, str], ...] = (
    ("last_trade_poll_ts", "trade_poll_stale"),
    ("last_risk_check_ts", "risk_check_stale"),
)

_SAFE_CHECK_CODE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    """Immutable keyed readiness state safe for operator snapshots."""

    key: str
    ready: bool
    required: bool
    reason: str | None = None


def _validate_check_code(name: str, value: str) -> None:
    if not _SAFE_CHECK_CODE_RE.fullmatch(value):
        raise ValueError(f"{name} must be a bounded lowercase code")


class ReadinessTracker:
    """Injectable readiness state — one instance per Application.

    Replaces the old module-level ``_readiness_state`` dict. Services that
    need to update readiness accept a ``ReadinessTracker`` instance via
    their constructor; the composition root builds one at startup and
    hands it to every consumer.

    New subsystem fields default to False/0 so any unwired subsystem fails
    the strict readiness check (fail-closed). Live mode must wire them.
    """

    def __init__(
        self,
        initial: dict[str, Any] | None = None,
        *,
        heartbeat_staleness_seconds: float | None = None,
    ):
        self._lock = threading.RLock()
        # Per-loop heartbeat staleness budget used by the strict readiness
        # check. The composition root derives this from the trade-poll /
        # risk-loop intervals (``configure_heartbeat_budget``); absent that it
        # falls back to a conservative named constant. Always floored so a
        # sub-second poll interval cannot produce a flappy budget.
        self._heartbeat_staleness_seconds: float = (
            _DEFAULT_HEARTBEAT_STALENESS_SECONDS
            if heartbeat_staleness_seconds is None
            else max(
                _MIN_HEARTBEAT_STALENESS_SECONDS,
                float(heartbeat_staleness_seconds),
            )
        )
        self._state: dict[str, Any] = {
            "db_ok": False,
            "api_ok": False,
            "last_trade_poll_ts": 0,
            "last_risk_check_ts": 0,
            "last_successful_order_ts": 0,
            "monitored_leader_count": 0,
            "runtime_user_count": 0,
            "mode": "configured",
            # Subsystem readiness (issue #76)
            "gateway_ok": False,
            "wss_market_ok": False,
            "wss_user_ok": False,
            "market_data_ready": False,
            "reconciler_ok": False,
            "circuit_breaker_state": "unknown",
            "pending_intents_count": 0,
            "active_reservations_count": 0,
            "stale_locks_count": 0,
            "last_reconcile_ts": 0,
            "last_wss_message_ts": 0,
            # PR7 — position drift sweep status.
            "drift_ok": False,
            # Background-task supervisor health. True until a supervised aux
            # loop (risk / discovery) exhausts its
            # restart budget and the process fail-closes. Deliberately NOT in
            # _STRICT_REQUIRED_KEYS: a transient restart should not flap
            # /health/ready (the Docker HEALTHCHECK target) and trigger an
            # orchestrator to kill the container mid-recovery. Surfaced via the
            # snapshot + the ``supervised_task_restart_total`` metric instead.
            "supervisor_ok": True,
        }
        if initial:
            self._state.update(initial)
        self._checks: dict[str, ReadinessCheck] = {}

    def update(self, **kwargs: Any) -> None:
        with self._lock:
            self._state.update(kwargs)

    def register_check(
        self,
        key: str,
        *,
        required: bool,
        ready: bool = False,
        reason: str | None = None,
    ) -> ReadinessCheck:
        """Register one uniquely keyed readiness producer.

        Args:
            key: Stable low-cardinality subsystem key.
            required: Whether a false state blocks strict readiness.
            ready: Initial readiness state.
            reason: Optional bounded machine-readable reason code.

        Returns:
            The immutable registered state.

        Raises:
            ValueError: If the key exists or a code is unsafe/unbounded.
        """
        _validate_check_code("key", key)
        if reason is not None:
            _validate_check_code("reason", reason)
        with self._lock:
            if key in self._checks:
                raise ValueError(f"readiness check {key} is already registered")
            check = ReadinessCheck(
                key=key,
                ready=ready,
                required=required,
                reason=reason,
            )
            self._checks[key] = check
            return check

    def set_check(
        self,
        key: str,
        *,
        ready: bool,
        reason: str | None = None,
    ) -> ReadinessCheck:
        """Replace one registered check while preserving its required flag.

        Args:
            key: Previously registered subsystem key.
            ready: New readiness state.
            reason: Optional bounded machine-readable reason code.

        Returns:
            The immutable replacement state.

        Raises:
            KeyError: If the key was not registered.
            ValueError: If a reason is unsafe or unbounded.
        """
        if reason is not None:
            _validate_check_code("reason", reason)
        with self._lock:
            current = self._checks.get(key)
            if current is None:
                raise KeyError(f"readiness check {key} is not registered")
            check = ReadinessCheck(
                key=key,
                ready=ready,
                required=current.required,
                reason=reason,
            )
            self._checks[key] = check
            return check

    def remove_check(self, key: str) -> ReadinessCheck:
        """Remove and return one keyed check during deterministic shutdown."""
        with self._lock:
            try:
                return self._checks.pop(key)
            except KeyError as exc:
                raise KeyError(f"readiness check {key} is not registered") from exc

    def configure_heartbeat_budget(self, *, staleness_seconds: float) -> None:
        """Set the strict-readiness heartbeat staleness budget (seconds).

        Called by the composition root once the loop intervals are known
        (e.g. ``max(fetch_interval, risk_check_interval) * N``). Floored by
        ``_MIN_HEARTBEAT_STALENESS_SECONDS`` so a sub-second poll interval
        cannot wedge readiness into a flappy knife-edge budget.
        """
        with self._lock:
            self._heartbeat_staleness_seconds = max(
                _MIN_HEARTBEAT_STALENESS_SECONDS,
                float(staleness_seconds),
            )

    def _stale_heartbeats(self, snapshot: dict[str, Any], now: int) -> list[str]:
        """Return failure tokens for heartbeats that ticked then went stale.

        Fail-closed but flap-safe: a heartbeat that has never ticked (ts <= 0)
        is NOT reported (see ``_HEARTBEAT_KEYS`` rationale). Only a heartbeat
        whose recorded timestamp has fallen outside the staleness budget — i.e.
        a loop that was alive and silently died — is reported.
        """
        budget = self._heartbeat_staleness_seconds
        stale: list[str] = []
        for ts_key, fail_token in _HEARTBEAT_KEYS:
            try:
                last_ts = int(snapshot.get(ts_key) or 0)
            except (TypeError, ValueError):
                # A non-numeric heartbeat value is itself a corruption signal;
                # treat it as stale rather than ignoring it.
                stale.append(fail_token)
                continue
            if last_ts <= 0:
                continue  # never ticked — boot or config-disabled loop
            if (now - last_ts) > budget:
                stale.append(fail_token)
        return stale

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot = deepcopy(self._state)
            snapshot["checks"] = {
                key: {
                    "ready": check.ready,
                    "required": check.required,
                    "reason": check.reason,
                }
                for key, check in sorted(self._checks.items())
            }
            return snapshot

    def get_all(self) -> dict[str, Any]:
        return self.snapshot()

    def is_ready(self, *, strict: bool = False) -> tuple[bool, list[str]]:
        """Return (ok, failed_keys).

        - strict=False: legacy semantics — db_ok AND api_ok must be true.
        - strict=True: every key in ``_STRICT_REQUIRED_KEYS`` must be truthy,
          ``circuit_breaker_state == "closed"``, AND every monitored loop
          heartbeat that has ticked at least once must be fresh (within the
          configured staleness budget). Used by /health/ready and the
          live-mode doctor gate. The heartbeat check (P2-g) flips readiness to
          NOT-ready when a trade-poll / risk loop silently dies and its
          timestamp stops advancing.
        """
        with self._lock:
            snapshot = deepcopy(self._state)
            checks = tuple(self._checks.values())
        if not strict:
            ok = bool(snapshot.get("db_ok", False)) and bool(snapshot.get("api_ok", False))
            failed_legacy = [k for k in ("db_ok", "api_ok") if not snapshot.get(k)]
            return ok, failed_legacy

        failed: list[str] = []
        for key in _STRICT_REQUIRED_KEYS:
            if not snapshot.get(key):
                failed.append(key)
        cb_state = snapshot.get("circuit_breaker_state", "unknown")
        if cb_state != "closed":
            failed.append("circuit_breaker_state")
        # P2-g: a monitored loop can die silently while every subsystem flag
        # above stays True (they were set once at startup). Catch that by
        # asserting the loop heartbeats are still advancing.
        failed.extend(self._stale_heartbeats(snapshot, int(time.time())))
        failed.extend(
            check.key
            for check in sorted(checks, key=lambda check: check.key)
            if check.required and not check.ready
        )
        return (len(failed) == 0), failed


async def metrics_handler(_request: web.Request) -> web.Response:
    body = metrics.render_prometheus()
    return web.Response(text=body, content_type="text/plain; version=0.0.4")


def _readiness_payload(tracker: ReadinessTracker | None) -> dict[str, Any]:
    if tracker is None:
        return {"db_ok": False, "api_ok": False}
    return tracker.snapshot()


async def start_health_server(
    port: int = 9102,
    *,
    tracker: ReadinessTracker | None = None,
    host: str = "127.0.0.1",
) -> web.AppRunner:
    if tracker is not None:
        tracker.update(started_ts=int(time.time()))
    app = web.Application()

    async def health_handler(_request: web.Request) -> web.Response:
        # Legacy /health: db_ok AND api_ok must be true.
        readiness = _readiness_payload(tracker)
        if tracker is None:
            ok = False
        else:
            ok, _ = tracker.is_ready(strict=False)
        return web.json_response({"ok": ok, "readiness": readiness})

    async def ready_handler(_request: web.Request) -> web.Response:
        # Strict /health/ready — used by ops + the live-mode gate.
        readiness = _readiness_payload(tracker)
        if tracker is None:
            return web.json_response(
                {"ok": False, "failed": ["tracker_unset"], "readiness": readiness},
                status=503,
            )
        ok, failed = tracker.is_ready(strict=True)
        return web.json_response(
            {"ok": ok, "failed": failed, "readiness": readiness},
            status=200 if ok else 503,
        )

    async def live_handler(_request: web.Request) -> web.Response:
        # Reaching this handler proves the process event loop and HTTP server
        # are alive. Dependency state belongs exclusively to readiness.
        return web.json_response({"ok": True, "status": "alive"})

    app.router.add_get("/health", health_handler)
    app.router.add_get("/health/ready", ready_handler)
    app.router.add_get("/health/live", live_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
