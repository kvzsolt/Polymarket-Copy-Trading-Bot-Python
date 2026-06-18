"""P1-h regression: the ``wallet_role`` column ALTER must precede the
role-keyed unique index in ``_CORE_DDL``.

On a database created before ``wallet_role`` existed, creating
``idx_disc_pos_snap_unique_v2`` (which references the column) *before* the
``ADD COLUMN IF NOT EXISTS wallet_role`` would raise ``undefined_column`` and
abort the whole single-transaction migration — the bot could not start. The
module comment already says the column-add "MUST run before" the index; this
test pins that ordering so the regression cannot silently return.
"""

from storage.postgres.migrations import _CORE_DDL, _DISCOVERY_DDL

# The exact order apply_migrations() executes statements in (single txn).
_APPLY_ORDER = list(_CORE_DDL) + list(_DISCOVERY_DDL)


def _first_stmt_index(needle: str) -> int:
    for i, stmt in enumerate(_APPLY_ORDER):
        if needle in stmt:
            return i
    raise AssertionError(f"no migration statement contains {needle!r}")


def test_wallet_role_column_added_before_role_keyed_indexes():
    add_col = _first_stmt_index("ADD COLUMN IF NOT EXISTS wallet_role")
    unique_index = _first_stmt_index("idx_disc_pos_snap_unique_v2")
    role_ts_index = _first_stmt_index("idx_disc_pos_snap_role_ts")

    assert add_col < unique_index, (
        "ALTER ADD COLUMN wallet_role must precede idx_disc_pos_snap_unique_v2 "
        f"(add_col={add_col}, unique_index={unique_index})"
    )
    assert add_col < role_ts_index, (
        "ALTER ADD COLUMN wallet_role must precede idx_disc_pos_snap_role_ts "
        f"(add_col={add_col}, role_ts_index={role_ts_index})"
    )
