"""Release gates for single-authority copy-task event wiring in ``app.py``."""

from __future__ import annotations

from pathlib import Path


def test_app_injects_existing_and_dynamic_contexts_into_v1_runtime() -> None:
    """Keep task follower lookup current across startup and roster refresh."""

    source = Path("app.py").read_text(encoding="utf-8")

    assert "contexts=initial_v1_contexts" in source
    assert "start_workers=False" in source
    assert "v1_runtime.add_contexts(unblocked_contexts)" in source
    assert source.index("v1_copy_task_restoration_started") < source.index(
        "startup_reconcile_started"
    )
    assert source.index("startup_drift_check_completed") < source.index("await v1_runtime.start()")
    assert source.index("blocked_wallets = _wallets_blocked_by_reconcile_failures") < source.index(
        "v1_runtime.add_contexts(unblocked_contexts)"
    )
    assert "task_monitoring_state.add(active_owners)" in source
    assert "task_monitoring_state.remove(monitored_leaders)" in source
    assert "native_detection_roster_awaiting_restart" in source
    assert "LeaderRegistryDiscoveryRunner(" in source
    assert source.index("await discovery_command_queue.start()") < source.index(
        "await _start_control_plane_runtime("
    )


def test_app_routes_native_owner_source_to_v1_without_legacy_duplication() -> None:
    """Make v1 ingress the sole copy authority while it is enabled."""

    source = Path("app.py").read_text(encoding="utf-8")

    assert "run_leader_monitor(" not in source
    assert "v1_sink=v1_runtime.submit_raw_batch" not in source
    assert "NativeDetectionRuntime.compose(" in source
    # On-chain signals reach v1 through ``OnchainSignalDispatchAdapter``
    # (``tests/test_onchain_signal_dispatch.py::test_prefers_v1_runtime_when_present``);
    # the app must hand it the same v1 runtime instance.
    assert "OnchainSignalDispatchAdapter(" in source
    assert "v1_runtime=v1_runtime," in source
    assert "if v1_runtime is not None:" in source


def test_app_projects_market_wss_quotes_into_the_shared_v1_cache() -> None:
    """Wire the shared quote sink into every follower market-data runtime."""

    source = Path("app.py").read_text(encoding="utf-8")

    assert "quote_sink=v1_runtime.quote_sink if v1_runtime is not None else None" in source


def test_app_starts_cached_portfolio_refresh_before_control_plane() -> None:
    """GUI portfolio reads use the isolated cache composed from runtime repos."""

    source = Path("app.py").read_text(encoding="utf-8")

    assert "PortfolioRuntimeFactory(" in source
    assert "PortfolioAccountBinding(" in source
    assert "await portfolio_runtime.refresh.start()" in source
    assert "client_registry.append(portfolio_runtime.refresh)" in source
    assert "portfolio_cache=" in source
    assert source.index("await portfolio_runtime.refresh.start()") < source.index(
        "await _start_control_plane_runtime("
    )
