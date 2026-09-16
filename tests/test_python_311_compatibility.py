"""The package syntax must support every declared interpreter version."""

import ast
from pathlib import Path


def test_reservation_repository_parses_on_python_311():
    path = Path(__file__).parents[1] / "storage/postgres/reservations_repo.py"
    ast.parse(path.read_text(), filename=str(path), feature_version=(3, 11))
