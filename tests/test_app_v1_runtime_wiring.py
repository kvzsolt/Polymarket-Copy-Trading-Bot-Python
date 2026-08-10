"""Regression guards for the v1 copy-task runtime wiring in ``app.main()``.

Phase A found the entire v1 stack test-only: no production path loaded
``config/runtime_v1.yml``, called ``apply_v1_migrations``, or constructed
``CopyTaskV1RuntimeBuilder``. These tests pin the composition-root call sites
so the wiring cannot silently regress to dead code again.
"""

from __future__ import annotations

import inspect


def test_v1_wiring_imports_resolve() -> None:
    """The lazy imports used by app.main()'s v1 wiring must resolve."""
    from config.v1_runtime import load_v1_runtime_config
    from orchestrators.v1_runtime_bootstrap import start_v1_copy_task_runtime

    assert callable(load_v1_runtime_config)
    assert callable(start_v1_copy_task_runtime)


def test_run_application_starts_the_v1_copy_task_runtime() -> None:
    """``app._run_application`` must invoke the v1 composition seam between
    startup reconciliation/restoration and the market-data subscriptions."""
    import app

    source = inspect.getsource(app._run_application)

    assert "start_v1_copy_task_runtime" in source
    assert "load_v1_runtime_config" in source
    # Canonical startup order: restoration precedes subscriptions/detection.
    assert source.index("start_v1_copy_task_runtime") < source.index(
        "_start_market_data_runtimes("
    )


def test_startup_migrations_include_v1_schema() -> None:
    """``apply_migrations`` (the startup runner) must call the v1 runner."""
    from storage.postgres import migrations

    source = inspect.getsource(migrations.apply_migrations)

    assert "apply_v1_migrations" in source
