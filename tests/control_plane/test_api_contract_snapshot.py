"""Keep ``docs/ui/api_contract.json`` honest against the live control plane.

The committed contract is what a UI generator reads, so it is only useful if
it cannot drift from the code. These tests regenerate it in memory and fail
when the committed bytes, the static route table, or the curated decimal-field
map no longer match what ``ControlPlaneHttpApplication`` actually registers and
what the view contracts actually validate.
"""

from __future__ import annotations

import json

from scripts.export_api_contract import (
    CONTRACT_PATH,
    DECIMAL_FIELDS,
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


def test_check_mode_accepts_the_committed_contract() -> None:
    assert ApiContractCli().run(["--check"]) == 0
