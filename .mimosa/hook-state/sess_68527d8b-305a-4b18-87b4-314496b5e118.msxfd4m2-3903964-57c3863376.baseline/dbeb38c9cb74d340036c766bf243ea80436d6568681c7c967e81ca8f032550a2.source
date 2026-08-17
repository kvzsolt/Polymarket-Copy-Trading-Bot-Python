from storage.postgres.migrations import (
    _DISCOVERY_DDL,
    _DISCOVERY_RESET_ID,
    _LEGACY_DISCOVERY_TABLES,
)


def test_reset_is_named_and_scoped_to_discovery_tables() -> None:
    assert _DISCOVERY_RESET_ID
    assert "orders" not in _LEGACY_DISCOVERY_TABLES
    assert "positions" not in _LEGACY_DISCOVERY_TABLES
    assert "fills" not in _LEGACY_DISCOVERY_TABLES
    assert "discovered_leaders" in _LEGACY_DISCOVERY_TABLES


def test_single_generation_schema_contains_roster_and_longitudinal_tables() -> None:
    schema = "\n".join(_DISCOVERY_DDL)
    for table in (
        "discovery_candidate_snapshots",
        "discovery_score_history",
        "leader_roster",
        "wallet_pnl_daily",
        "wallet360_snapshots",
        "wallet_rank_history",
        "wallet_metric_timeseries",
        "wallet_backfill_state",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in schema
