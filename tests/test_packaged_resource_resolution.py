"""Packaged defaults must resolve through ``importlib.resources``.

The config YAML tree and the SQL migrations are READONLY packaged inputs.
They must be discovered through the import system (``importlib.resources``),
not through ``Path(__file__)`` joins, so an installed wheel serves them from
read-only site-packages and nothing ever writes into the package tree.
"""

from __future__ import annotations

import importlib.resources
from pathlib import Path

import pytest

from pmcopy.config.app_config import load_config
from pmcopy.storage.postgres.v1_migration_runner import V1MigrationCatalog

REPO_ROOT = Path(__file__).resolve().parents[1]


def _files_path(package: str) -> Path:
    resource = importlib.resources.files(package)
    assert isinstance(resource, Path), (
        f"{package} is not installed on a filesystem (zipimport is unsupported); "
        f"got {type(resource)!r}"
    )
    return resource


class TestConfigDefaultsResolveViaImportlibResources:
    def test_packaged_config_dir_helper_matches_importlib_resources(self) -> None:
        from pmcopy.config.app_config import packaged_config_dir

        assert packaged_config_dir() == _files_path("pmcopy.config")

    def test_loader_default_root_is_the_importlib_resources_dir(self) -> None:
        from pmcopy.config import app_config

        assert Path(app_config._CONFIG_ROOT) == _files_path("pmcopy.config")

    def test_loader_default_root_survives_config_root_module_replacement(self) -> None:
        """`_CONFIG_ROOT` stays a plain filesystem directory (test seam kept)."""
        from pmcopy.config import app_config

        assert isinstance(app_config._CONFIG_ROOT, Path)


class TestMigrationsResolveViaImportlibResources:
    def test_packaged_migrations_dir_helper(self) -> None:
        from pmcopy.utils.packaged_resources import packaged_resource_dir

        packaged = packaged_resource_dir("pmcopy.storage.postgres", "migrations")
        assert packaged == _files_path("pmcopy.storage.postgres") / "migrations"

    def test_catalog_default_directory_is_the_packaged_resources_dir(self) -> None:
        catalog = V1MigrationCatalog()
        assert catalog._migration_directory == (
            _files_path("pmcopy.storage.postgres") / "migrations"
        )

    def test_catalog_lists_every_packaged_sql_file(self) -> None:
        migrations = V1MigrationCatalog().migrations()
        packaged = sorted(
            path.name
            for path in (_files_path("pmcopy.storage.postgres") / "migrations").glob("*.sql")
        )
        assert packaged, "packaged migrations directory must not be empty"
        assert [m.migration_id + ".sql" for m in migrations] == packaged

    def test_explicit_directory_override_is_still_honored(self, tmp_path: Path) -> None:
        (tmp_path / "0001_probe.sql").write_text("-- probe\n", encoding="utf-8")
        catalog = V1MigrationCatalog(migration_directory=tmp_path)
        assert [m.migration_id for m in catalog.migrations()] == ["0001_probe"]


class TestPackagedDefaultsAreReadonlyInputs:
    def test_load_config_writes_nothing_into_the_packaged_tree(self, tmp_path: Path) -> None:
        packaged = _files_path("pmcopy.config")

        def snapshot() -> set[Path]:
            return {path.relative_to(packaged) for path in packaged.rglob("*") if path.is_file()}

        before = snapshot()
        load_config(
            {
                "database": {"dsn": "postgresql://probe:probe@localhost/probe"},
                "wallet": {"funder_address": "0x" + "2" * 40},
                "detection": {
                    "onchain": {
                        "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key-123"
                    }
                },
            }
        )
        assert snapshot() == before

    def test_loader_shim_and_secrets_are_external_to_the_package(self) -> None:
        """The legacy shim and secrets.env resolve under the deployment root."""
        from pmcopy.config import app_config

        assert app_config._RUNTIME_LOCAL_YAML.startswith("config/")
        assert app_config._SECRETS_ENV == "secrets.env"


class TestNoModuleRelativeDefaultRoots:
    """Forbid-scan: packaged default roots must not be `__file__`-joined."""

    @pytest.mark.parametrize(
        ("module_path", "module_name"),
        [
            (REPO_ROOT / "src" / "pmcopy" / "config" / "app_config.py", "app_config"),
            (
                REPO_ROOT / "src" / "pmcopy" / "storage" / "postgres" / "v1_migration_runner.py",
                "v1_migration_runner",
            ),
            (REPO_ROOT / "src" / "pmcopy" / "utils" / "logger.py", "logger"),
        ],
    )
    def test_module_has_no_dunder_file_default_root(
        self, module_path: Path, module_name: str
    ) -> None:
        source = module_path.read_text(encoding="utf-8")
        assert "Path(__file__)" not in source, (
            f"{module_name} must resolve packaged/default paths via importlib.resources "
            "or explicit configuration, never from the installed module location"
        )


class TestExplicitOutputGuardInInstalledMode:
    """``profile-leader --output`` must not treat the install tree as writable."""

    def test_installed_mode_rejects_writes_under_the_module_tree(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``parents[4]`` of an installed module lands inside the interpreter
        tree; a path there must be rejected (an installed run allows only the
        system temp dir as a write root)."""
        import sysconfig
        import tempfile

        from pmcopy.bot_cli.commands import profile_leader

        # A pyproject-less `_REPO_ROOT` simulates the installed distribution
        # (the directory need not exist: detection only stats for the marker).
        fake_install_root = tmp_path / "fake-venv" / "lib" / "python3.13"
        monkeypatch.setattr(profile_leader, "_REPO_ROOT", fake_install_root)
        monkeypatch.setattr(
            profile_leader, "_ALLOWED_WRITE_ROOTS", profile_leader._build_write_roots()
        )

        assert profile_leader._ALLOWED_WRITE_ROOTS == (Path(tempfile.gettempdir()),)

        purelib = Path(sysconfig.get_paths()["purelib"])
        if purelib.is_relative_to(Path(tempfile.gettempdir())):
            pytest.skip("interpreter lives under the system temp dir")
        target = purelib / "profile.json"
        with pytest.raises(ValueError):
            profile_leader._validate_writable_path(str(target))

    def test_checkout_mode_still_allows_repo_root_writes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import tempfile

        from pmcopy.bot_cli.commands import profile_leader

        checkout = tmp_path / "checkout"
        (checkout / "src" / "pmcopy").mkdir(parents=True)
        (checkout / "pyproject.toml").write_text("# probe\n", encoding="utf-8")

        monkeypatch.setattr(profile_leader, "_REPO_ROOT", checkout)
        monkeypatch.setattr(
            profile_leader, "_ALLOWED_WRITE_ROOTS", profile_leader._build_write_roots()
        )

        assert checkout in profile_leader._ALLOWED_WRITE_ROOTS
        target = checkout / "profiles" / "leader.json"
        assert profile_leader._validate_writable_path(str(target)) == target.resolve()
        assert Path(tempfile.gettempdir()) in profile_leader._ALLOWED_WRITE_ROOTS
