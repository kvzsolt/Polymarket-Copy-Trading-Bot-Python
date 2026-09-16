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


def test_domain_never_imports_outward_layers() -> None:
    """Domain modules depend only on the standard library and domain ports."""

    root = Path(__file__).resolve().parents[1]
    violations: list[str] = []
    for path in sorted((root / "domain").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            imported_roots: list[str] = []
            if isinstance(node, ast.Import):
                imported_roots = [alias.name.partition(".")[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots = [node.module.partition(".")[0]]
            for imported_root in imported_roots:
                if imported_root in FORBIDDEN_DOMAIN_ROOTS:
                    relative = path.relative_to(root)
                    violations.append(f"{relative}:{node.lineno} imports {imported_root}")

    assert violations == []
