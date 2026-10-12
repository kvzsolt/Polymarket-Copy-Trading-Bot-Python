"""Default log directory must never derive from the installed module location.

In a wheel install the package lives inside site-packages; a module-relative
default would create ``log/`` inside the interpreter's lib directory. The
default log directory is deployment-local: ``<deployment root>/log`` where
the deployment root is ``PMCOPY_DEPLOYMENT_ROOT`` or the process CWD.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pmcopy.utils import logger as logger_module


def _module_tree() -> Path:
    return Path(logger_module.__file__).resolve().parents[1]


def test_default_log_dir_uses_deployment_root_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "deployment"
    root.mkdir()
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(root))

    assert logger_module._default_log_dir() == root / "log"


def test_default_log_dir_falls_back_to_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)
    monkeypatch.chdir(cwd)

    assert logger_module._default_log_dir() == cwd / "log"


def test_default_log_dir_is_never_module_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "deployment"
    root.mkdir()
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(root))
    default = logger_module._default_log_dir()

    package_tree = _module_tree()
    assert package_tree not in default.parents
    assert package_tree.parent not in default.parents
    assert "site-packages" not in default.parts


def test_implicit_configure_logging_writes_under_deployment_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "deployment"
    root.mkdir()
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(root))
    monkeypatch.chdir(cwd)  # ensure CWD is NOT the write target

    log = logger_module.configure_logging()
    log.info("packaging probe | secret=probe-canary-value")

    expected = root / "log" / f"{logger_module.APP_NAME}.jsonl"
    assert expected.is_file(), "JSONL sink must be created under the deployment root"
    package_tree = _module_tree()
    written = list(package_tree.parent.rglob(f"{logger_module.APP_NAME}.jsonl"))
    assert not written, "no log file may appear inside the installed package tree"
