"""Architecture contracts for inward-pointing domain dependencies."""

from __future__ import annotations

import ast
from pathlib import Path


FORBIDDEN_DOMAIN_ROOTS = frozenset(
    {
        "adapters",
        "application",
        "bot_cli",
        "clients",
        "config",
        "integrations",
        "orchestration",
        "orchestrators",
        "services",
        "storage",
    }
)


def _imported_pmcopy_subpackages(node: ast.AST) -> list[str]:
    modules: list[str] = []
    if isinstance(node, ast.Import):
        modules = [alias.name for alias in node.names]
    elif isinstance(node, ast.ImportFrom) and node.module:
        modules = [node.module]

    subpackages: list[str] = []
    for mod in modules:
        parts = mod.split(".")
        if parts[0] == "pmcopy" and len(parts) > 1:
            subpackages.append(parts[1])
        else:
            subpackages.append(parts[0])
    return subpackages


def test_domain_never_imports_outward_layers() -> None:
    """Domain modules depend only on the standard library and domain ports."""

    root = Path(__file__).resolve().parents[1]
    domain_files = sorted((root / "src" / "pmcopy" / "domain").rglob("*.py"))
    assert len(domain_files) >= 25
    violations: list[str] = []
    for path in domain_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            for imported_root in _imported_pmcopy_subpackages(node):
                if imported_root in FORBIDDEN_DOMAIN_ROOTS:
                    relative = path.relative_to(root)
                    violations.append(f"{relative}:{node.lineno} imports {imported_root}")

    assert violations == []


def test_no_camelcase_python_filenames_in_pmcopy() -> None:
    """Enforce a single strict snake_case filename convention across src/pmcopy."""
    root = Path(__file__).resolve().parents[1] / "src" / "pmcopy"
    offenders = [
        str(path.relative_to(root))
        for path in sorted(root.rglob("*.py"))
        if "__pycache__" not in path.parts and any(ch.isupper() for ch in path.stem)
    ]
    assert offenders == [], f"camelCase filenames are forbidden: {offenders}"


def test_organized_packages_have_no_loose_solo_files_or_duplicate_roots() -> None:
    """Ensure solo modules live inside cohesive subsystem subpackages."""
    root = Path(__file__).resolve().parents[1] / "src" / "pmcopy"
    for removed in ("orchestration", "ports", "models", "log"):
        assert not (root / removed).exists(), f"Obsolete directory reappeared: {removed}"
    for container in ("services", "orchestrators", "storage", "clients", "adapters"):
        loose = [
            p.name
            for p in sorted((root / container).glob("*.py"))
            if p.name != "__init__.py"
        ]
        assert loose == [], f"Loose solo files found in {container}/: {loose}"
