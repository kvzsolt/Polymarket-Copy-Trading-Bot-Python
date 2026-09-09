"""The implementation branch must run the existing CI checks."""

from fnmatch import fnmatchcase
from pathlib import Path
import re

import pytest
import yaml


@pytest.mark.parametrize("event", ["push", "pull_request"])
def test_ci_covers_implementation_branch(event: str) -> None:
    workflow = yaml.safe_load(Path(".github/workflows/ci.yml").read_text())
    triggers = workflow.get("on", workflow.get(True))
    assert any(
        fnmatchcase("feature/hybrid-ccxt-implementation", pattern)
        for pattern in triggers[event]["branches"]
    )


def test_ci_adapter_test_files_exist() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()
    for filename in re.findall(r"tests/[\w/]+\.py", workflow):
        assert Path(filename).is_file(), filename
