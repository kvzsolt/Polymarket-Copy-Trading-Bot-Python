"""Authentication, authorization, bind safety, and bounded action throttling."""

from __future__ import annotations

import ipaddress
import math
import secrets
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Protocol
from urllib.parse import urlsplit


class ControlPlaneTransportError(RuntimeError):
    """Stable transport error that never contains collaborator detail."""

    def __init__(
        self,
        *,
        code: str,
        public_message: str,
        status: int,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.code = code
        self.public_message = public_message
        self.status = status
        self.headers = dict(headers or {})
        super().__init__(public_message)


class ControlPlanePermission(StrEnum):
    """Explicit low-cardinality permissions assigned to operator principals."""

    SYSTEM_READ = "system.read"
    TASK_READ = "task.read"
    TASK_WRITE = "task.write"
    POSITION_READ = "position.read"
    ORDER_READ = "order.read"
    ACTIVITY_READ = "activity.read"
    DISCOVERY_READ = "discovery.read"
    DISCOVERY_RUN = "discovery.run"
    LOCK_READ = "lock.read"
    LOCK_RESET = "lock.reset"
    RESERVATION_READ = "reservation.read"
    SETTINGS_READ = "settings.read"
    SETTINGS_WRITE = "settings.write"
    PORTFOLIO_READ = "portfolio.read"
    RUNTIME_READ = "runtime.read"
    RUNTIME_WRITE = "runtime.write"
    TELEMETRY_READ = "telemetry.read"
    TELEMETRY_STREAM = "telemetry.stream"


ALL_CONTROL_PLANE_PERMISSIONS = frozenset(ControlPlanePermission)


@dataclass(frozen=True, slots=True)
class ControlPlanePrincipal:
    """Authenticated operator identity and immutable permission set."""

    subject: str
    permissions: frozenset[ControlPlanePermission]

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise ValueError("principal subject is required")
        if not isinstance(self.permissions, frozenset):
            raise TypeError("principal permissions must be a frozenset")
        if any(not isinstance(item, ControlPlanePermission) for item in self.permissions):
            raise TypeError("principal permissions contain an invalid value")
        object.__setattr__(self, "subject", self.subject.strip())


@dataclass(frozen=True, slots=True)
class BearerCredential:
    """One bearer secret mapped to a safe operator identity and permissions."""

    token: str = field(repr=False)
    subject: str
    permissions: frozenset[ControlPlanePermission]

    def __post_init__(self) -> None:
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("bearer token is required")
        if len(self.token) > 1024:
            raise ValueError("bearer token is too long")
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise ValueError("credential subject is required")
        if not isinstance(self.permissions, frozenset):
            raise TypeError("credential permissions must be a frozenset")
        if any(not isinstance(item, ControlPlanePermission) for item in self.permissions):
            raise TypeError("credential permissions contain an invalid value")
        object.__setattr__(self, "subject", self.subject.strip())


class ControlPlaneAuthenticator:
    """Authenticate bearer credentials using constant-time comparison.

    Every configured credential is compared even after a match so credential
    position does not create an early-return timing signal.  No token appears
    in object representations or public failures.
    """

    def __init__(
        self,
        *,
        credentials: tuple[BearerCredential, ...] = (),
        allow_unauthenticated_loopback: bool = False,
        loopback_permissions: frozenset[ControlPlanePermission] = frozenset(),
        compare_digest: Callable[[str, str], bool] = secrets.compare_digest,
    ) -> None:
        if not isinstance(credentials, tuple):
            raise TypeError("credentials must be a tuple")
        if len(credentials) > 32:
            raise ValueError("at most 32 bearer credentials are supported")
        if len({credential.token for credential in credentials}) != len(credentials):
            raise ValueError("duplicate bearer credentials are forbidden")
        if allow_unauthenticated_loopback and not loopback_permissions:
            raise ValueError("unauthenticated loopback access requires explicit permissions")
        self._credentials = credentials
        self._allow_unauthenticated_loopback = bool(allow_unauthenticated_loopback)
        self._loopback_permissions = loopback_permissions
        self._compare_digest = compare_digest

    @property
    def credentials(self) -> tuple[BearerCredential, ...]:
        """Return immutable credential metadata; token reprs remain redacted."""

        return self._credentials

    @property
    def has_credentials(self) -> bool:
        """Whether at least one bearer credential is configured."""

        return bool(self._credentials)

    def authenticate_header(
        self,
        authorization: str | None,
        *,
        remote_address: str | None,
    ) -> ControlPlanePrincipal:
        """Authenticate one Authorization header or explicit local principal."""

        if authorization is None or not authorization.strip():
            if self._allow_unauthenticated_loopback and is_loopback_host(remote_address):
                return ControlPlanePrincipal(
                    subject="loopback-operator",
                    permissions=self._loopback_permissions,
                )
            raise ControlPlaneTransportError(
                code="authentication_required",
                public_message="Authentication is required.",
                status=401,
                headers={"WWW-Authenticate": "Bearer"},
            )

        scheme, separator, presented = authorization.partition(" ")
        if (
            separator != " "
            or scheme.casefold() != "bearer"
            or not presented
            or presented.strip() != presented
            or len(presented) > 1024
        ):
            raise self._invalid_token()

        matched: BearerCredential | None = None
        for credential in self._credentials:
            is_match = self._compare_digest(presented, credential.token)
            if is_match and matched is None:
                matched = credential
        if matched is None:
            raise self._invalid_token()
        return ControlPlanePrincipal(
            subject=matched.subject,
            permissions=matched.permissions,
        )

    @staticmethod
    def _invalid_token() -> ControlPlaneTransportError:
        return ControlPlaneTransportError(
            code="invalid_bearer_token",
            public_message="The bearer token is invalid.",
            status=401,
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )


class ControlPlanePermissionPolicy:
    """Fail-closed permission policy used explicitly by every handler."""

    def require(
        self,
        principal: ControlPlanePrincipal,
        permission: ControlPlanePermission,
    ) -> None:
        """Reject a principal that lacks the exact route permission."""

        if permission not in principal.permissions:
            raise ControlPlaneTransportError(
                code="permission_denied",
                public_message="The authenticated operator lacks this permission.",
                status=403,
            )


class MonotonicClock(Protocol):
    """Monotonic clock seam for deterministic throttling."""

    def monotonic(self) -> float:
        """Return monotonic seconds."""

        ...


class SystemMonotonicClock:
    """Production monotonic clock."""

    def monotonic(self) -> float:
        """Return process-local monotonic seconds."""

        return time.monotonic()


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    """Result of one bounded sliding-window admission check."""

    allowed: bool
    retry_after_seconds: int = 0


class InProcessActionRateLimiter:
    """Bounded per-principal/action sliding-window limiter.

    The limiter is intentionally process-local.  It limits accidental or
    malicious command bursts but is not a distributed authorization system.
    Both key count and timestamps retained per key are hard bounded.
    """

    def __init__(
        self,
        *,
        max_actions: int = 30,
        window_seconds: float = 60.0,
        max_keys: int = 1024,
        clock: MonotonicClock | None = None,
    ) -> None:
        if not 1 <= max_actions <= 10_000:
            raise ValueError("max_actions must be between 1 and 10000")
        if not math.isfinite(window_seconds) or window_seconds <= 0:
            raise ValueError("window_seconds must be finite and positive")
        if not 1 <= max_keys <= 100_000:
            raise ValueError("max_keys must be between 1 and 100000")
        self._max_actions = int(max_actions)
        self._window_seconds = float(window_seconds)
        self._max_keys = int(max_keys)
        self._clock = clock if clock is not None else SystemMonotonicClock()
        self._windows: OrderedDict[str, deque[float]] = OrderedDict()

    @property
    def tracked_key_count(self) -> int:
        """Return the current bounded key count."""

        return len(self._windows)

    def check(self, key: str) -> RateLimitDecision:
        """Admit or reject one action without sleeping or blocking."""

        if not isinstance(key, str) or not key or len(key) > 256:
            raise ValueError("rate-limit key must contain 1 to 256 characters")
        now = float(self._clock.monotonic())
        if not math.isfinite(now):
            raise RuntimeError("monotonic clock returned a non-finite value")

        window = self._windows.get(key)
        if window is None:
            if len(self._windows) >= self._max_keys:
                self._windows.popitem(last=False)
            window = deque(maxlen=self._max_actions)
            self._windows[key] = window
        else:
            self._windows.move_to_end(key)

        cutoff = now - self._window_seconds
        while window and window[0] <= cutoff:
            window.popleft()
        if len(window) >= self._max_actions:
            retry_after = max(1, math.ceil(window[0] + self._window_seconds - now))
            return RateLimitDecision(False, retry_after)
        window.append(now)
        return RateLimitDecision(True, 0)


@dataclass(frozen=True, slots=True)
class ControlPlaneHttpConfig:
    """Bounded network configuration for the aiohttp adapter."""

    host: str = "127.0.0.1"
    port: int = 9103
    require_authentication: bool = True
    request_max_bytes: int = 262_144
    websocket_queue_capacity: int = 128
    telemetry_snapshot_limit: int = 100
    command_rate_limit: int = 30
    command_rate_window_seconds: float = 60.0
    rate_limit_max_keys: int = 1024
    shutdown_timeout_seconds: float = 5.0
    cors_allowed_origins: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.host, str)
            or not self.host.strip()
            or len(self.host) > 255
            or "\x00" in self.host
        ):
            raise ValueError("host is required")
        object.__setattr__(self, "host", self.host.strip())
        if not isinstance(self.require_authentication, bool):
            raise TypeError("require_authentication must be a boolean")
        if isinstance(self.port, bool) or not isinstance(self.port, int):
            raise TypeError("port must be an integer")
        if not 0 <= self.port <= 65_535:
            raise ValueError("port must be between 0 and 65535")
        _require_int("request_max_bytes", self.request_max_bytes)
        if not 1_024 <= self.request_max_bytes <= 16 * 1024 * 1024:
            raise ValueError("request_max_bytes must be between 1024 and 16777216")
        _require_int("websocket_queue_capacity", self.websocket_queue_capacity)
        if not 1 <= self.websocket_queue_capacity <= 4096:
            raise ValueError("websocket_queue_capacity must be between 1 and 4096")
        _require_int("telemetry_snapshot_limit", self.telemetry_snapshot_limit)
        if not 0 <= self.telemetry_snapshot_limit <= 1000:
            raise ValueError("telemetry_snapshot_limit must be between 0 and 1000")
        _require_int("command_rate_limit", self.command_rate_limit)
        if not 1 <= self.command_rate_limit <= 10_000:
            raise ValueError("command_rate_limit must be between 1 and 10000")
        _require_number("command_rate_window_seconds", self.command_rate_window_seconds)
        if (
            not math.isfinite(self.command_rate_window_seconds)
            or self.command_rate_window_seconds <= 0
        ):
            raise ValueError("command_rate_window_seconds must be finite and positive")
        _require_int("rate_limit_max_keys", self.rate_limit_max_keys)
        if not 1 <= self.rate_limit_max_keys <= 100_000:
            raise ValueError("rate_limit_max_keys must be between 1 and 100000")
        _require_number("shutdown_timeout_seconds", self.shutdown_timeout_seconds)
        if not math.isfinite(self.shutdown_timeout_seconds) or self.shutdown_timeout_seconds <= 0:
            raise ValueError("shutdown_timeout_seconds must be finite and positive")
        if not isinstance(self.cors_allowed_origins, tuple):
            raise TypeError("cors_allowed_origins must be a tuple")
        if len(self.cors_allowed_origins) > 32:
            raise ValueError("at most 32 CORS origins are supported")
        for origin in self.cors_allowed_origins:
            _validate_cors_origin(origin)
        if not self.require_authentication and not is_loopback_host(self.host):
            raise ValueError("non-loopback control-plane binds require authentication")


def is_loopback_host(host: str | None) -> bool:
    """Return whether a bind or peer address is explicitly loopback."""

    if host is None:
        return False
    normalized = str(host).strip().strip("[]").casefold()
    if normalized == "localhost":
        return True
    # aiohttp may render an IPv4-mapped IPv6 peer.
    if normalized.startswith("::ffff:"):
        normalized = normalized.removeprefix("::ffff:")
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def _validate_cors_origin(origin: str) -> None:
    if not isinstance(origin, str) or not origin or origin == "*" or len(origin) > 512:
        raise ValueError("CORS origins must be explicit HTTP(S) origins")
    parsed = urlsplit(origin)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise ValueError("CORS origins must be explicit HTTP(S) origins")


def _require_int(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")


def _require_number(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")


__all__ = [
    "ALL_CONTROL_PLANE_PERMISSIONS",
    "BearerCredential",
    "ControlPlaneAuthenticator",
    "ControlPlaneHttpConfig",
    "ControlPlanePermission",
    "ControlPlanePermissionPolicy",
    "ControlPlanePrincipal",
    "ControlPlaneTransportError",
    "InProcessActionRateLimiter",
    "MonotonicClock",
    "RateLimitDecision",
    "SystemMonotonicClock",
    "is_loopback_host",
]
