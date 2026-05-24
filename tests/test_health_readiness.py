from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from services.healthServer import ReadinessTracker, start_health_server


@pytest.mark.asyncio
async def test_readiness_tracker_logic():
    tracker = ReadinessTracker()
    tracker.update(db_ok=True, api_ok=True, runtime_user_count=2)
    
    snap = tracker.snapshot()
    assert snap["db_ok"] is True
    assert snap["api_ok"] is True
    assert snap["runtime_user_count"] == 2


@pytest.mark.asyncio
async def test_health_server_setup():
    tracker = ReadinessTracker()
    # Test that start_health_server correctly wires the tracker
    with patch("services.healthServer.web.TCPSite") as site_cls:
        site_instance = AsyncMock()
        site_cls.return_value = site_instance

        runner = await start_health_server(port=9102, tracker=tracker)
        
        # Verify started_ts was set
        assert tracker.snapshot().get("started_ts") is not None
        
        await runner.cleanup()


@pytest.mark.asyncio
async def test_metrics_server_uses_validated_bind_host_from_env():
    tracker = ReadinessTracker()
    with patch.dict("os.environ", {"METRICS_BIND_HOST": "127.0.0.1"}, clear=True):
        with patch("services.healthServer.web.TCPSite") as site_cls:
            site_instance = AsyncMock()
            site_cls.return_value = site_instance

            runner = await start_health_server(port=9102, tracker=tracker)

            _, kwargs = site_cls.call_args
            assert kwargs["host"] == "127.0.0.1"
            assert kwargs["port"] == 9102

            await runner.cleanup()
