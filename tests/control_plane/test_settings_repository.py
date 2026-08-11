"""Revision-safe, secret-free, atomic environment settings contracts."""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

import services.control_plane.settings as settings_module
from services.control_plane.contracts import (
    AuditEvent,
    ControlPlaneError,
    ControlPlaneReason,
    SettingsPath,
    SettingsWriteCommand,
)
from services.control_plane.settings import (
    SettingsCommandService,
    SettingsQueryService,
    SettingsRepository,
)


NOW = datetime(2026, 8, 11, 16, 0, tzinfo=UTC)


class FixedClock:
    """Return a deterministic apply time."""

    def now(self) -> datetime:
        """Return the fixed UTC timestamp."""

        return NOW


class Validator:
    """Small typed-schema adapter used by repository contract tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[SettingsPath, dict[str, object]]] = []

    def allowed_keys(self, path: SettingsPath) -> frozenset[str]:
        """Return the known top-level schema for a section."""

        if path.section == "execution":
            return frozenset({"max_retries", "trading_mode", "nested", "api_key"})
        return frozenset({"enabled"})

    def is_secret(self, path: SettingsPath, key_path: tuple[str, ...]) -> bool:
        """Classify credential paths without inspecting their values."""

        del path
        return key_path[-1] in {"api_key", "private_key", "password"}

    def validate(
        self,
        path: SettingsPath,
        payload: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Reject one deterministic invalid value and return a safe copy."""

        snapshot = dict(payload)
        self.calls.append((path, snapshot))
        if snapshot.get("max_retries") == 99:
            raise ValueError("super-secret validation detail")
        return snapshot


class BlindValidator(Validator):
    """Deliberately omit secret classification to test repository defence-in-depth."""

    def is_secret(self, path: SettingsPath, key_path: tuple[str, ...]) -> bool:
        """Claim that no key is secret."""

        del path, key_path
        return False


class ExplodingSchemaValidator(Validator):
    """Raise raw schema-discovery detail to exercise exception sanitization."""

    def allowed_keys(self, path: SettingsPath) -> frozenset[str]:
        """Fail before document validation."""

        del path
        raise RuntimeError("password=must-not-escape")


class AuditSink:
    """Capture settings mutation audit events."""

    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    async def append(self, event: AuditEvent) -> None:
        """Append one event."""

        self.events.append(event)


def build_repository(tmp_path: Path, validator: Validator | None = None) -> SettingsRepository:
    """Create an isolated repository with an explicit path allowlist."""

    config_root = tmp_path / "config"
    environment = config_root / "environments" / "paper"
    environment.mkdir(parents=True)
    return SettingsRepository(
        config_root=config_root,
        known_paths=frozenset(
            {
                SettingsPath(environment="paper", section="execution"),
                SettingsPath(environment="paper", section="risk"),
            }
        ),
        validator=validator or Validator(),
        clock=FixedClock(),
    )


def write_yaml(tmp_path: Path, section: str, payload: Mapping[str, object]) -> Path:
    """Write one initial environment override fixture."""

    target = tmp_path / "config" / "environments" / "paper" / f"{section}.yml"
    target.write_text(yaml.safe_dump(dict(payload), sort_keys=True), encoding="utf-8")
    return target


@pytest.mark.asyncio
async def test_read_returns_sha256_revision_and_omits_secrets_recursively(tmp_path: Path) -> None:
    """Credential material never appears in the immutable settings document."""

    repository = build_repository(tmp_path)
    target = write_yaml(
        tmp_path,
        "execution",
        {
            "max_retries": 3,
            "api_key": "top-level-secret",
            "nested": {"private_key": "nested-secret", "safe": True},
        },
    )

    document = await repository.read(SettingsPath("paper", "execution"))

    assert len(document.revision) == 64
    assert document.revision == repository.revision_for(target.read_bytes())
    assert document.values.to_dict() == {
        "max_retries": 3,
        "nested": {"safe": True},
    }
    assert "secret" not in repr(document)
    with pytest.raises(TypeError):
        document.values["max_retries"] = 4  # type: ignore[index]


@pytest.mark.asyncio
async def test_repository_omits_well_known_secret_names_even_if_validator_misses_them(
    tmp_path: Path,
) -> None:
    """A defective schema adapter cannot make common credentials readable."""

    validator = BlindValidator()
    repository = build_repository(tmp_path, validator)
    write_yaml(
        tmp_path,
        "execution",
        {"max_retries": 3, "api_key": "must-not-leak"},
    )

    document = await repository.read(SettingsPath("paper", "execution"))

    assert document.values.to_dict() == {"max_retries": 3}
    assert "must-not-leak" not in repr(document)


@pytest.mark.asyncio
async def test_write_requires_matching_revision_and_returns_restart_metadata(
    tmp_path: Path,
) -> None:
    """A successful write exposes both revisions and never claims live apply."""

    repository = build_repository(tmp_path)
    target = write_yaml(tmp_path, "execution", {"max_retries": 3})
    before = await repository.read(SettingsPath("paper", "execution"))

    result = await repository.write(
        SettingsWriteCommand.from_mapping(
            path=SettingsPath("paper", "execution"),
            values={"max_retries": 4, "trading_mode": "paper"},
            expected_revision=before.revision,
            actor="operator-a",
        )
    )

    assert yaml.safe_load(target.read_text(encoding="utf-8")) == {
        "max_retries": 4,
        "trading_mode": "paper",
    }
    assert result.metadata.previous_revision == before.revision
    assert result.metadata.revision == result.document.revision
    assert result.metadata.applied_at == NOW
    assert result.metadata.applied_by == "operator-a"
    assert result.metadata.restart_required is True
    assert result.metadata.live_applied is False


@pytest.mark.asyncio
async def test_stale_revision_never_validates_or_overwrites_newer_file(tmp_path: Path) -> None:
    """Revision comparison occurs under the write lock before validation."""

    validator = Validator()
    repository = build_repository(tmp_path, validator)
    target = write_yaml(tmp_path, "execution", {"max_retries": 3})
    before = await repository.read(SettingsPath("paper", "execution"))
    validator.calls.clear()
    target.write_text("max_retries: 7\n", encoding="utf-8")

    with pytest.raises(ControlPlaneError) as captured:
        await repository.write(
            SettingsWriteCommand.from_mapping(
                path=SettingsPath("paper", "execution"),
                values={"max_retries": 4},
                expected_revision=before.revision,
                actor="operator-a",
            )
        )

    assert captured.value.reason is ControlPlaneReason.REVISION_MISMATCH
    assert validator.calls == []
    assert target.read_text(encoding="utf-8") == "max_retries: 7\n"


@pytest.mark.asyncio
async def test_unknown_and_secret_keys_fail_at_boundary_without_write(tmp_path: Path) -> None:
    """Extra keys and credential keys are rejected before schema validation."""

    validator = Validator()
    repository = build_repository(tmp_path, validator)
    target = write_yaml(tmp_path, "execution", {"max_retries": 3})
    before = await repository.read(SettingsPath("paper", "execution"))
    validator.calls.clear()

    for values, expected in (
        ({"unknown": True}, ControlPlaneReason.SETTINGS_INVALID),
        ({"api_key": "do-not-store"}, ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN),
        (
            {"nested": {"private_key": "do-not-store"}},
            ControlPlaneReason.SETTINGS_SECRET_FORBIDDEN,
        ),
    ):
        with pytest.raises(ControlPlaneError) as captured:
            await repository.write(
                SettingsWriteCommand.from_mapping(
                    path=SettingsPath("paper", "execution"),
                    values=values,
                    expected_revision=before.revision,
                    actor="operator-a",
                )
            )
        assert captured.value.reason is expected

    assert validator.calls == []
    assert yaml.safe_load(target.read_text()) == {"max_retries": 3}


@pytest.mark.asyncio
async def test_validator_error_is_sanitized_and_original_file_survives(tmp_path: Path) -> None:
    """Raw validation text neither escapes nor reaches the target file."""

    repository = build_repository(tmp_path)
    target = write_yaml(tmp_path, "execution", {"max_retries": 3})
    before = await repository.read(SettingsPath("paper", "execution"))

    with pytest.raises(ControlPlaneError) as captured:
        await repository.write(
            SettingsWriteCommand.from_mapping(
                path=SettingsPath("paper", "execution"),
                values={"max_retries": 99},
                expected_revision=before.revision,
                actor="operator-a",
            )
        )

    assert captured.value.reason is ControlPlaneReason.SETTINGS_INVALID
    assert "secret" not in str(captured.value).casefold()
    assert yaml.safe_load(target.read_text()) == {"max_retries": 3}


@pytest.mark.asyncio
async def test_schema_adapter_error_is_sanitized(tmp_path: Path) -> None:
    """Schema-discovery exceptions cannot expose raw collaborator detail."""

    repository = build_repository(tmp_path, ExplodingSchemaValidator())
    write_yaml(tmp_path, "execution", {"max_retries": 3})

    with pytest.raises(ControlPlaneError) as captured:
        await repository.read(SettingsPath("paper", "execution"))

    assert captured.value.reason is ControlPlaneReason.SETTINGS_INVALID
    assert "password" not in str(captured.value).casefold()


@pytest.mark.asyncio
async def test_write_uses_same_directory_replace_and_fsyncs_file_and_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Atomic replacement leaves no temporary document behind."""

    repository = build_repository(tmp_path)
    target = write_yaml(tmp_path, "execution", {"max_retries": 3})
    before = await repository.read(SettingsPath("paper", "execution"))
    replacements: list[tuple[Path, Path]] = []
    fsync_calls: list[int] = []
    real_replace = os.replace
    real_fsync = os.fsync

    def replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        replacements.append((Path(source), Path(destination)))
        real_replace(source, destination)

    def fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(settings_module.os, "replace", replace)
    monkeypatch.setattr(settings_module.os, "fsync", fsync)

    await repository.write(
        SettingsWriteCommand.from_mapping(
            path=SettingsPath("paper", "execution"),
            values={"max_retries": 4},
            expected_revision=before.revision,
            actor="operator-a",
        )
    )

    assert len(replacements) == 1
    temporary, destination = replacements[0]
    assert temporary.parent == target.parent
    assert destination == target
    assert not temporary.exists()
    assert len(fsync_calls) >= 2


@pytest.mark.asyncio
async def test_unknown_paths_and_symlink_targets_are_rejected(tmp_path: Path) -> None:
    """Neither traversal nor a swapped symbolic-link target can be followed."""

    repository = build_repository(tmp_path)
    real_target = write_yaml(tmp_path, "risk", {"enabled": True})
    symlink = real_target.with_name("execution.yml")
    symlink.symlink_to(real_target)

    with pytest.raises(ControlPlaneError) as unknown:
        await repository.read(SettingsPath("paper", "../wallet"))
    with pytest.raises(ControlPlaneError) as linked:
        await repository.read(SettingsPath("paper", "execution"))

    assert unknown.value.reason is ControlPlaneReason.SETTINGS_PATH_FORBIDDEN
    assert linked.value.reason is ControlPlaneReason.SETTINGS_SYMLINK_FORBIDDEN


@pytest.mark.asyncio
async def test_settings_services_delegate_reads_and_audit_every_successful_mutation(
    tmp_path: Path,
) -> None:
    """The application command service records a safe apply event."""

    repository = build_repository(tmp_path)
    write_yaml(tmp_path, "execution", {"max_retries": 3})
    query = SettingsQueryService(repository)
    audit = AuditSink()
    command_service = SettingsCommandService(
        repository=repository,
        audit_sink=audit,
        clock=FixedClock(),
    )
    before = await query.get(SettingsPath("paper", "execution"))

    result = await command_service.write(
        SettingsWriteCommand.from_mapping(
            path=SettingsPath("paper", "execution"),
            values={"max_retries": 4},
            expected_revision=before.revision,
            actor="operator-a",
        )
    )

    assert result.metadata.restart_required is True
    assert len(audit.events) == 1
    assert audit.events[0].action == "settings.write"
    assert audit.events[0].details == (("restart_required", "true"),)


@pytest.mark.asyncio
async def test_settings_query_lists_only_its_bounded_known_paths(tmp_path: Path) -> None:
    """The collection snapshot cannot enumerate arbitrary config files."""

    repository = build_repository(tmp_path)
    write_yaml(tmp_path, "execution", {"max_retries": 3})
    write_yaml(tmp_path, "risk", {"enabled": True})
    query = SettingsQueryService(
        repository,
        paths=(
            SettingsPath("paper", "risk"),
            SettingsPath("paper", "execution"),
        ),
    )

    documents = await query.list()

    assert [document.path.section for document in documents] == ["execution", "risk"]
