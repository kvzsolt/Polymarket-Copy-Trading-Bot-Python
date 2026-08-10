"""Replay test fixtures.

Each replay fixture is a JSON file with a list of time-ordered events.
Tests verify that the system produces correct, deterministic results
without lookahead bias.
"""

import json
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_replay_fixture(name: str) -> dict:
    """Load a named replay fixture (without .json extension)."""
    path = FIXTURES_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"replay fixture not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def list_replay_fixtures() -> list[str]:
    """List available replay fixture names (without .json extension)."""
    return sorted([p.stem for p in FIXTURES_DIR.glob("*.json")])
