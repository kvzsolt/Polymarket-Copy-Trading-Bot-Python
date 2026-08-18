"""Keep ``docs/ui/api_contract.json`` honest against the live control plane.

The committed contract is what a UI generator reads, so it is only useful if
it cannot drift from the code. These tests regenerate it in memory and fail
when the committed bytes, the static route table, the curated decimal-field
map, or the declared string-field vocabularies no longer match what
``ControlPlaneHttpApplication`` actually registers and what the view contracts
actually validate.

The positive tests prove the committed file is current. The negative tests
prove the guards that keep it current actually fire, by feeding the exporter
mutated tables through its constructor seams — otherwise a guard that silently
stopped working would look exactly like a healthy one.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from scripts.export_api_contract import (
    CONTRACT_PATH,
    DECIMAL_FIELDS,
    FIELD_VOCABULARIES,
    ROUTES,
    ApiContractCli,
    ApiContractExporter,
)


def test_committed_contract_matches_a_fresh_export() -> None:
    committed = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert ApiContractExporter().build() == committed, "run scripts/export_api_contract.py"


def test_committed_contract_is_byte_identical_to_the_renderer() -> None:
    assert ApiContractExporter().render() == CONTRACT_PATH.read_text(encoding="utf-8"), (
        "run scripts/export_api_contract.py"
    )


def test_every_registered_route_appears_in_the_routes_table() -> None:
    declared = {(route.method, route.path) for route in ROUTES}

    missing = sorted(ApiContractExporter().registered_routes() - declared)

    assert missing == []


def test_every_routes_entry_is_registered_by_the_application() -> None:
    registered = ApiContractExporter().registered_routes()

    stale = sorted({(route.method, route.path) for route in ROUTES} - registered)

    assert stale == []


def test_the_exporter_reports_no_cross_check_problems() -> None:
    assert ApiContractExporter().verify() == ()


def test_decimal_fields_name_only_existing_string_fields() -> None:
    views = ApiContractExporter().build()["views"]
    assert isinstance(views, dict)

    for view_name, field_names in DECIMAL_FIELDS.items():
        assert view_name in views, view_name
        fields = {field["name"]: field for field in views[view_name]["fields"]}
        for field_name in field_names:
            assert field_name in fields, f"{view_name}.{field_name}"
            assert fields[field_name]["type"] == "string", f"{view_name}.{field_name}"
            assert fields[field_name]["decimal"] is True, f"{view_name}.{field_name}"


def test_decimal_fields_match_the_validators_the_views_actually_run() -> None:
    derived = ApiContractExporter().derived_decimal_fields()

    assert derived == {name: tuple(fields) for name, fields in DECIMAL_FIELDS.items()}


def test_declared_vocabularies_are_published_on_their_string_fields() -> None:
    views = ApiContractExporter().build()["views"]
    resolved = ApiContractExporter().resolved_vocabularies()
    assert isinstance(views, dict)

    for view_name, fields in FIELD_VOCABULARIES.items():
        published = {field["name"]: field for field in views[view_name]["fields"]}
        for field_name in fields:
            members = resolved[view_name][field_name]
            assert members, f"{view_name}.{field_name}"
            assert published[field_name]["vocabulary"] == list(members)


def test_check_mode_accepts_the_committed_contract() -> None:
    assert ApiContractCli().run(["--check"]) == 0


# ----------------------------------------------------------------------
# Negative tests: prove each guard fires.
# ----------------------------------------------------------------------
def test_a_registered_route_absent_from_the_table_is_reported() -> None:
    trimmed = tuple(route for route in ROUTES if route.path != "/api/v1/leaders")

    problems = ApiContractExporter(routes=trimmed).verify()

    assert problems == ("route registered but missing from ROUTES: GET /api/v1/leaders",)


def test_a_declared_route_that_is_not_registered_is_reported() -> None:
    invented = ROUTES + (replace(ROUTES[0], path="/api/v1/ghost"),)

    problems = ApiContractExporter(routes=invented).verify()

    assert problems == ("route declared in ROUTES but not registered: GET /api/v1/ghost",)


def test_a_dropped_decimal_field_is_reported_against_the_real_validators() -> None:
    mutated = dict(DECIMAL_FIELDS) | {"PositionView": ("size",)}

    problems = ApiContractExporter(decimal_fields=mutated).verify()

    assert problems == (
        "DECIMAL_FIELDS[PositionView] is ('size',) but the view validates "
        "('size', 'avg_entry_price')",
    )


def test_a_decimal_field_map_naming_an_unknown_view_is_reported() -> None:
    mutated = dict(DECIMAL_FIELDS) | {"NopeView": ("amount",)}

    problems = ApiContractExporter(decimal_fields=mutated).verify()

    assert problems == (
        "DECIMAL_FIELDS names unknown view NopeView",
        "DECIMAL_FIELDS[NopeView] is ('amount',) but the view validates None",
    )


def test_a_decimal_field_map_naming_an_unknown_field_is_reported() -> None:
    mutated = dict(DECIMAL_FIELDS) | {"PositionView": ("size", "avg_entry_price", "nope")}

    problems = ApiContractExporter(decimal_fields=mutated).verify()

    assert problems == (
        "DECIMAL_FIELDS names unknown field PositionView.nope",
        "DECIMAL_FIELDS[PositionView] is ('size', 'avg_entry_price', 'nope') but the view "
        "validates ('size', 'avg_entry_price')",
    )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (
            {"NopeView": {"status": "services.copy_tasks.domain:CopyTaskStatus"}},
            "FIELD_VOCABULARIES names unknown view NopeView",
        ),
        (
            {"TaskView": {"nope": "services.copy_tasks.domain:CopyTaskStatus"}},
            "FIELD_VOCABULARIES names unknown field TaskView.nope",
        ),
        (
            {"TaskView": {"version": "services.copy_tasks.domain:CopyTaskStatus"}},
            "FIELD_VOCABULARIES[TaskView.version] is not a plain string field",
        ),
        (
            {"TaskView": {"status": "services.copy_tasks.domain:NoSuchEnum"}},
            "FIELD_VOCABULARIES[TaskView.status] target "
            "'services.copy_tasks.domain:NoSuchEnum' does not resolve: AttributeError",
        ),
        (
            {"TaskView": {"status": "malformed"}},
            "FIELD_VOCABULARIES[TaskView.status] target 'malformed' does not resolve: ValueError",
        ),
    ],
)
def test_a_broken_field_vocabulary_declaration_is_reported(
    mutation: dict[str, dict[str, str]],
    expected: str,
) -> None:
    problems = ApiContractExporter(field_vocabularies=mutation).verify()

    assert problems == (expected,)


def test_build_refuses_to_produce_a_contract_when_a_guard_fires() -> None:
    trimmed = tuple(route for route in ROUTES if route.path != "/api/v1/leaders")

    with pytest.raises(RuntimeError, match="missing from ROUTES"):
        ApiContractExporter(routes=trimmed).build()


def test_check_mode_rejects_a_stale_file(tmp_path: Path) -> None:
    stale = tmp_path / "api_contract.json"
    stale.write_text("{}\n", encoding="utf-8")

    assert ApiContractCli().run(["--check", "--output", str(stale)]) == 1


def test_check_mode_rejects_a_missing_file(tmp_path: Path) -> None:
    absent = tmp_path / "absent.json"

    assert ApiContractCli().run(["--check", "--output", str(absent)]) == 1


def test_check_mode_accepts_a_freshly_written_file(tmp_path: Path) -> None:
    destination = tmp_path / "api_contract.json"

    assert ApiContractCli().run(["--output", str(destination)]) == 0
    assert ApiContractCli().run(["--check", "--output", str(destination)]) == 0


def test_check_mode_fails_when_a_guard_fires(tmp_path: Path) -> None:
    trimmed = tuple(route for route in ROUTES if route.path != "/api/v1/leaders")
    destination = tmp_path / "api_contract.json"

    cli = ApiContractCli(ApiContractExporter(routes=trimmed))

    assert cli.run(["--check", "--output", str(destination)]) == 1
    assert not destination.exists()
