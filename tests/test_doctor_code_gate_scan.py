"""Doctor code-gate scanning must be a real check in installed (wheel) mode.

The code gates historically scanned ``src/pmcopy/...`` roots under the
repository root. An installed wheel has no source tree, so every root was
missing and each gate reported a clean pass for modules it never scanned —
a vacuous green on the operator's readiness report.

Contracts pinned here:

* source-checkout mode: the repo roots are scanned and the ok detail says
  so (provenance + file count);
* installed mode: the same gate scans the installed ``pmcopy`` subpackages
  (the code that actually runs in that deployment) and reports the
  site-packages provenance — a forbidden reference in those files is a hit;
* when neither the source tree nor the installed package can be located,
  the gate fails closed (``_GateScanUnavailable``) instead of passing
  vacuously;
* the coordinator-alias allowlist recognises the installed-package path
  spelling (``pmcopy/services/execution/coordinator.py``).

Forbidden literals are concatenated at runtime (mirroring the doctor
source) so this file is spelled exactly like any other clean module.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from pmcopy.bot_cli.commands import doctor

_POSITION_PRICE_CACHE = "PositionPrice" + "Cache"


class _FakePackageSpec:
    """Minimal importlib spec stand-in for one installed subpackage."""

    def __init__(self, package_dir: Path) -> None:
        self.submodule_search_locations = [str(package_dir)]


def _fake_installed_packages(
    monkeypatch: pytest.MonkeyPatch, mapping: dict[str, Path | None]
) -> None:
    """Point ``importlib.util.find_spec`` at fake per-package directories.

    Names absent from ``mapping`` keep the real resolution, so unrelated
    imports inside the same window are unaffected.
    """

    real_find_spec = importlib.util.find_spec

    def find_spec(name, *args, **kwargs):
        if name in mapping:
            location = mapping[name]
            return None if location is None else _FakePackageSpec(location)
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", find_spec)


def _empty_dir(root: Path, name: str) -> Path:
    path = root / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_source_tree_mode_reports_source_provenance() -> None:
    # From the checkout the gated source roots exist and are scanned; the
    # ok detail must say WHAT was scanned, not just "no references".
    hits, provenance = doctor._code_gate_scan(
        repo_root=doctor._repo_root(),
        forbidden_substrings=(_POSITION_PRICE_CACHE,),
    )
    assert hits == []
    assert "source tree" in provenance


def test_installed_mode_without_source_tree_scans_installed_package(tmp_path: Path) -> None:
    # An empty "repo root" has no src/pmcopy/... roots: wheel-install mode.
    # The gate must fall back to the installed pmcopy subpackages (the code
    # that actually runs) instead of passing with zero files scanned.
    hits, provenance = doctor._code_gate_scan(
        repo_root=tmp_path,
        forbidden_substrings=(_POSITION_PRICE_CACHE,),
    )
    assert hits == []
    assert "installed package" in provenance


def test_installed_mode_reports_hits_from_installed_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    site = tmp_path / "site-packages"
    services = site / "pmcopy" / "services"
    services.mkdir(parents=True)
    (services / "legacy_price_cache.py").write_text(
        f"_LEGACY = {_POSITION_PRICE_CACHE!r}\n", encoding="utf-8"
    )
    mapping = {
        name: (
            services
            if name == "pmcopy.services"
            else _empty_dir(tmp_path / "empty-packages", name.replace(".", "-"))
        )
        for name in doctor._GATE_INSTALLED_PACKAGES
    }
    _fake_installed_packages(monkeypatch, mapping)

    hits, provenance = doctor._code_gate_scan(
        repo_root=tmp_path / "no-source-tree",
        forbidden_substrings=(_POSITION_PRICE_CACHE,),
    )
    assert hits == [("pmcopy/services/legacy_price_cache.py", _POSITION_PRICE_CACHE)]
    assert "installed package" in provenance


def test_gate_fails_closed_when_nothing_can_be_scanned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No source roots, and the interpreter cannot locate the installed
    # gated packages: a gate that scanned nothing must never report a pass.
    mapping = {name: None for name in doctor._GATE_INSTALLED_PACKAGES}
    _fake_installed_packages(monkeypatch, mapping)
    with pytest.raises(doctor._GateScanUnavailable):
        doctor._code_gate_scan(
            repo_root=tmp_path,
            forbidden_substrings=(_POSITION_PRICE_CACHE,),
        )


def test_alias_gate_fails_closed_when_nothing_can_be_scanned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mapping = {name: None for name in doctor._GATE_INSTALLED_PACKAGES}
    _fake_installed_packages(monkeypatch, mapping)
    with pytest.raises(doctor._GateScanUnavailable):
        doctor._code_gate_coordinator_alias(tmp_path)


def test_coordinator_alias_allowlist_covers_installed_spelling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The allowlisted guarded file keeps its exemption under the
    # installed-package path spelling; an unguarded alias elsewhere still
    # violates.
    site = tmp_path / "site-packages"
    coordinator = site / "pmcopy" / "services" / "execution"
    coordinator.mkdir(parents=True)
    (coordinator / "coordinator.py").write_text(
        "execution_service.execute(order)\n", encoding="utf-8"
    )
    orchestrators = site / "pmcopy" / "orchestrators"
    orchestrators.mkdir(parents=True)
    (orchestrators / "rogue.py").write_text("execution_service.execute(order)\n", encoding="utf-8")
    _fake_installed_packages(
        monkeypatch,
        {"pmcopy.services": site / "pmcopy" / "services", "pmcopy.orchestrators": orchestrators},
    )

    violations, provenance = doctor._code_gate_coordinator_alias(tmp_path / "no-source-tree")
    assert violations == ["pmcopy/orchestrators/rogue.py: execution_service.execute"]
    assert "installed package" in provenance
