"""Atomic, revision-safe repository for environment service settings.

Only explicitly allowlisted ``config/environments/<env>/<section>.yml`` files
are accessible.  Reads and writes reject symbolic links, share a directory
file lock, and never return or persist fields classified as secrets by the
injected validator.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import os
import stat
import tempfile
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self

import yaml

from services.control_plane.contracts import (
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
    SettingsApplyMetadata,
    SettingsDocument,
    SettingsPath,
    SettingsValues,
    SettingsWriteCommand,
    SettingsWriteResult,
)


class SettingsValidator(Protocol):
    """Injected typed-schema and secret-classification boundary."""

    def allowed_keys(self, path: SettingsPath) -> frozenset[str]:
        """Return allowed top-level keys for one known section."""

        ...

    def is_secret(self, path: SettingsPath, key_path: tuple[str, ...]) -> bool:
        """Return whether one nested key path contains credential material."""

        ...

    def validate(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Return a validated, normalized section mapping."""

        ...


class Clock(Protocol):
    """Timezone-aware UTC clock boundary."""

    def now(self) -> datetime:
        """Return the current timestamp."""

        ...


class AuditSink(Protocol):
    """Append-only settings mutation audit boundary."""

    async def append(self, event: AuditEvent) -> None:
        """Append one immutable event."""

        ...


class SettingsReadPort(Protocol):
    """Narrow secret-free settings read boundary."""

    async def read(self, path: SettingsPath) -> SettingsDocument:
        """Return one immutable settings document."""

        ...


class SettingsWritePort(Protocol):
    """Narrow revision-guarded settings write boundary."""

    async def write(self, command: SettingsWriteCommand) -> SettingsWriteResult:
        """Replace one known settings section atomically."""

        ...


class _DirectoryLock:
    """File lock held on a non-symlink environment directory descriptor."""

    def __init__(self, directory: Path, *, exclusive: bool) -> None:
        self._directory = directory
        self._exclusive = exclusive
        self._fd = -1

    def __enter__(self) -> Self:
        """Open and lock the exact directory without following symlinks."""

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        self._fd = os.open(self._directory, flags)
        details = os.fstat(self._fd)
        if not stat.S_ISDIR(details.st_mode):
            os.close(self._fd)
            self._fd = -1
            raise OSError("settings environment is not a directory")
        operation = fcntl.LOCK_EX if self._exclusive else fcntl.LOCK_SH
        fcntl.flock(self._fd, operation)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the advisory lock and close its descriptor."""

        del exc_type, exc_value, traceback
        if self._fd >= 0:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = -1

    def fsync(self) -> None:
        """Durably flush directory metadata while the lock is held."""

        if self._fd < 0:
            raise OSError("settings directory lock is not held")
        os.fsync(self._fd)


class SettingsRepository:
    """Read and atomically replace known environment service sections."""

    def __init__(
        self,
        *,
        config_root: Path,
        known_paths: frozenset[SettingsPath],
        validator: SettingsValidator,
        clock: Clock,
    ) -> None:
        """Store explicit path policy and typed validation collaborators.

        Args:
            config_root: Repository ``config`` directory.
            known_paths: Exact editable environment/section pairs.
            validator: Typed schema and secret policy adapter.
            clock: Apply-metadata clock.
        """

        self._config_root = Path(os.path.abspath(config_root))
        self._known_paths = known_paths
        self._validator = validator
        self._clock = clock

    async def read(self, path: SettingsPath) -> SettingsDocument:
        """Read, validate, and omit secrets from one known section."""

        return await asyncio.to_thread(self._read_safely, path)

    async def write(self, command: SettingsWriteCommand) -> SettingsWriteResult:
        """Atomically replace one section under an expected SHA-256 revision."""

        return await asyncio.to_thread(self._write_safely, command)

    @staticmethod
    def revision_for(contents: bytes) -> str:
        """Return the lowercase SHA-256 content revision."""

        return hashlib.sha256(contents).hexdigest()

    def _read_safely(self, path: SettingsPath) -> SettingsDocument:
        """Perform a locked read and map all raw storage errors."""

        target = self._target(path)
        try:
            with _DirectoryLock(target.parent, exclusive=False):
                self._reject_target_symlink(target)
                contents = self._read_bytes(target)
                payload = self._decode(contents)
                validated = self._validate(path, payload, reject_secrets=False)
                safe = self._omit_secrets(path, validated)
                return SettingsDocument(
                    path=path,
                    values=SettingsValues.from_mapping(safe),
                    revision=self.revision_for(contents),
                )
        except ControlPlaneError:
            raise
        except OSError:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_UNAVAILABLE) from None

    def _write_safely(self, command: SettingsWriteCommand) -> SettingsWriteResult:
        """Perform one locked compare, validate, fsync, and replace sequence."""

        target = self._target(command.path)
        try:
            with _DirectoryLock(target.parent, exclusive=True) as directory_lock:
                self._reject_target_symlink(target)
                previous = self._read_bytes(target)
                previous_revision = self.revision_for(previous)
                if previous_revision != command.expected_revision:
                    raise ControlPlaneError(ControlPlaneReason.REVISION_MISMATCH)
                payload = command.values.to_dict()
                validated = self._validate(command.path, payload, reject_secrets=True)
                encoded = self._encode(validated)
                self._atomic_replace(target, encoded, directory_lock)
                revision = self.revision_for(encoded)
                safe = self._omit_secrets(command.path, validated)
                document = SettingsDocument(
                    path=command.path,
                    values=SettingsValues.from_mapping(safe),
                    revision=revision,
                )
                metadata = SettingsApplyMetadata(
                    previous_revision=previous_revision,
                    revision=revision,
                    applied_at=self._clock.now(),
                    applied_by=command.actor,
                    restart_required=True,
                    live_applied=False,
                )
                return SettingsWriteResult(document=document, metadata=metadata)
        except ControlPlaneError:
            raise
        except OSError:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_UNAVAILABLE) from None

    def _target(self, path: SettingsPath) -> Path:
        """Resolve one exact allowlisted data target without following links."""

        if path not in self._known_paths:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN)
        environments = self._config_root / "environments"
        environment = environments / path.environment
        target = environment / f"{path.section}.yml"
        try:
            if os.path.commonpath((str(self._config_root), str(target))) != str(self._config_root):
                raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN)
        except ValueError:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN) from None
        for component in (self._config_root, environments, environment):
            if component.is_symlink():
                raise ControlPlaneError(ControlPlaneReason.SETTINGS_SYMLINK_FORBIDDEN)
        return target

    @staticmethod
    def _reject_target_symlink(target: Path) -> None:
        """Reject a target swapped to a symbolic link before opening it."""

        if target.is_symlink():
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_SYMLINK_FORBIDDEN)

    @staticmethod
    def _read_bytes(target: Path) -> bytes:
        """Read one regular file without following symbolic links."""

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(target, flags)
        except FileNotFoundError:
            return b""
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise OSError("settings target is not a regular file")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            os.close(descriptor)

    @staticmethod
    def _decode(contents: bytes) -> dict[str, object]:
        """Decode one YAML mapping while hiding parser detail."""

        if not contents:
            return {}
        try:
            loaded = yaml.safe_load(contents.decode("utf-8"))
        except (UnicodeDecodeError, yaml.YAMLError):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None
        if loaded is None:
            return {}
        if not isinstance(loaded, Mapping):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
        if any(not isinstance(key, str) or not key for key in loaded):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
        return {str(key): value for key, value in loaded.items()}

    def _validate(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
        *,
        reject_secrets: bool,
    ) -> dict[str, object]:
        """Enforce extra-key and secret policy around typed validation."""

        try:
            allowed = self._validator.allowed_keys(path)
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None
        if not isinstance(allowed, frozenset) or any(
            not isinstance(key, str) or not key for key in allowed
        ):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
        if not set(payload).issubset(allowed):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
        if reject_secrets and self._contains_secret(path, payload):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN)
        try:
            normalized = self._validator.validate(path, payload)
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None
        if not isinstance(normalized, Mapping) or not set(normalized).issubset(allowed):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID)
        result = {str(key): value for key, value in normalized.items()}
        if reject_secrets and self._contains_secret(path, result):
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN)
        try:
            return SettingsValues.from_mapping(result).to_dict()
        except ControlPlaneError:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None

    def _contains_secret(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
        prefix: tuple[str, ...] = (),
    ) -> bool:
        """Return whether a nested mapping contains a classified secret key."""

        for key, value in payload.items():
            key_path = (*prefix, str(key))
            if self._is_secret(path, key_path):
                return True
            if isinstance(value, Mapping) and self._contains_secret(path, value, key_path):
                return True
            if isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    if isinstance(item, Mapping) and self._contains_secret(
                        path, item, (*key_path, str(index))
                    ):
                        return True
        return False

    def _omit_secrets(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
        prefix: tuple[str, ...] = (),
    ) -> dict[str, object]:
        """Return a fresh mapping with all classified key paths removed."""

        output: dict[str, object] = {}
        for raw_key, value in payload.items():
            key = str(raw_key)
            key_path = (*prefix, key)
            if self._is_secret(path, key_path):
                continue
            if isinstance(value, Mapping):
                output[key] = self._omit_secrets(path, value, key_path)
            elif isinstance(value, (list, tuple)):
                items: list[object] = []
                for index, item in enumerate(value):
                    if isinstance(item, Mapping):
                        items.append(self._omit_secrets(path, item, (*key_path, str(index))))
                    else:
                        items.append(item)
                output[key] = items
            else:
                output[key] = value
        return output

    def _is_secret(self, path: SettingsPath, key_path: tuple[str, ...]) -> bool:
        """Apply validator policy plus conservative credential-name defence."""

        normalized = key_path[-1].strip().casefold().replace("-", "_")
        exact = {
            "api_key",
            "api_token",
            "authorization",
            "bearer_token",
            "credential",
            "credentials",
            "password",
            "passphrase",
            "private_key",
            "refresh_token",
            "secret",
            "secret_key",
        }
        suffixes = (
            "_api_key",
            "_credential",
            "_credentials",
            "_password",
            "_passphrase",
            "_private_key",
            "_refresh_token",
            "_secret",
            "_secret_key",
        )
        try:
            classified = self._validator.is_secret(path, key_path)
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None
        return classified or normalized in exact or normalized.endswith(suffixes)

    @staticmethod
    def _encode(payload: Mapping[str, object]) -> bytes:
        """Encode deterministic safe YAML without Python-specific tags."""

        try:
            text = yaml.safe_dump(
                dict(payload),
                allow_unicode=True,
                default_flow_style=False,
                sort_keys=True,
            )
        except yaml.YAMLError:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_INVALID) from None
        return text.encode("utf-8")

    @staticmethod
    def _atomic_replace(
        target: Path,
        contents: bytes,
        directory_lock: _DirectoryLock,
    ) -> None:
        """Write, fsync, replace, and fsync the containing directory."""

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            offset = 0
            while offset < len(contents):
                offset += os.write(descriptor, contents[offset:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, target)
            directory_lock.fsync()
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


class SettingsQueryService:
    """Side-effect-free settings query service."""

    def __init__(
        self,
        repository: SettingsReadPort,
        *,
        paths: tuple[SettingsPath, ...] = (),
    ) -> None:
        """Store the repository and bounded collection path allowlist.

        Args:
            repository: Secret-free settings read boundary.
            paths: Exact paths exposed by the collection snapshot.
        """

        self._repository = repository
        self._paths = tuple(sorted(paths, key=lambda item: (item.environment, item.section)))

    async def get(self, path: SettingsPath) -> SettingsDocument:
        """Return one secret-free settings document."""

        return await self._repository.read(path)

    async def list(self) -> tuple[SettingsDocument, ...]:
        """Return every explicitly composed settings document in stable order."""

        return tuple(await asyncio.gather(*(self._repository.read(path) for path in self._paths)))


class SettingsCommandService:
    """Write revisioned settings and audit every successful mutation."""

    def __init__(
        self,
        *,
        repository: SettingsWritePort,
        audit_sink: AuditSink,
        clock: Clock,
    ) -> None:
        """Store file and audit collaborators."""

        self._repository = repository
        self._audit_sink = audit_sink
        self._clock = clock

    async def write(self, command: SettingsWriteCommand) -> SettingsWriteResult:
        """Write one restart-required section and append its audit event."""

        result = await self._repository.write(command)
        try:
            await self._audit_sink.append(
                AuditEvent.record(
                    actor=command.actor,
                    action="settings.write",
                    target_type="settings_section",
                    target_id=f"{command.path.environment}/{command.path.section}",
                    outcome="succeeded",
                    reason_code="restart_required",
                    occurred_at=self._clock.now(),
                    details=(("restart_required", "true"),),
                )
            )
        except Exception:
            raise ControlPlaneError(ControlPlaneReason.AUDIT_UNAVAILABLE) from None
        return result


__all__ = [
    "SettingsCommandService",
    "SettingsQueryService",
    "SettingsReadPort",
    "SettingsRepository",
    "SettingsValidator",
    "SettingsWritePort",
]
