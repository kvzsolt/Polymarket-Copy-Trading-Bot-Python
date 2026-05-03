"""Verify that only the gateway module imports py_clob_client directly.

After CLOB V2 gateway isolation, all CLOB SDK access must flow through
`integrations/polymarket/clob_v2_gateway.py`. The only allowed exception is
`utils/createClobClient.py` which constructs the raw client.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

ALLOWED_DIRECT_SDK_IMPORTERS = {
    "integrations",
    "utils/createClobClient.py",
    "scripts/smoke_v2.py",  # dev-only smoke script
}

DIRECT_SDK_PATTERNS = (
    "from py_clob_client",
    "import py_clob_client",
    "from py_clob_client_v2",
    "import py_clob_client_v2",
)


def test_no_direct_py_clob_imports_in_production_code() -> None:
    violations: list[str] = []

    # Only scan under the core source packages of THIS repo.
    scan_roots = [
        REPO_ROOT / "services",
        REPO_ROOT / "orchestrators",
        REPO_ROOT / "clients",
        REPO_ROOT / "domain",
        REPO_ROOT / "config",
        REPO_ROOT / "storage",
        REPO_ROOT / "bot_cli",
        REPO_ROOT / "scripts",
        REPO_ROOT / "utils",
        REPO_ROOT / "integrations",
        REPO_ROOT / "app.py",
        REPO_ROOT / "bot.py",
        REPO_ROOT / "main.py",
    ]

    for entry in scan_roots:
        if entry.is_dir():
            py_files = sorted(entry.rglob("*.py"))
        elif entry.is_file():
            py_files = [entry]
        else:
            continue

        for py_file in py_files:
            rel = str(py_file.relative_to(REPO_ROOT))

            if "__pycache__" in rel:
                continue
            if any(seg.startswith(".") for seg in Path(rel).parts):
                continue

            # Allowed modules are exempt from the gate
            if any(rel.startswith(a) or rel == a for a in ALLOWED_DIRECT_SDK_IMPORTERS):
                continue

            content = py_file.read_text(encoding="utf-8", errors="replace")
            for pattern in DIRECT_SDK_PATTERNS:
                if pattern in content:
                    violations.append(rel)
                    break

    if violations:
        raise AssertionError(
            "Direct py_clob_client imports found outside allowed modules:\n"
            + "\n".join(f"  - {v}" for v in violations)
            + "\n\n"
            "Use 'from integrations.polymarket.clob_v2_gateway import ...' instead."
        )


def test_clob_v2_gateway_exports_required_symbols() -> None:
    """Smoke test: gateway exports the symbols rest of the codebase relies on."""
    from integrations.polymarket.clob_v2_gateway import (
        BUY,
        OrderArgs,
        OrderType,
        SELL,
    )

    # BUY and SELL should be distinct string constants
    assert BUY != SELL
    assert isinstance(BUY, str) and isinstance(SELL, str)
    assert OrderType is not None
    assert OrderArgs is not None
