"""Unit tests for ``config.app_config.DetectionModeResolver``.

The resolver maps the raw ``detection`` payload (explicit ``mode`` plus the
deprecated ``enable_onchain_*`` booleans) onto exactly one detection source
before typed validation runs.
"""

from __future__ import annotations

from config.app_config import DetectionModeResolver


def test_default_is_polling() -> None:
    mode, warning = DetectionModeResolver().resolve({})
    assert mode == "polling"
    assert warning is None

    mode, warning = DetectionModeResolver().resolve(None)
    assert mode == "polling"
    assert warning is None


def test_legacy_pending_flag_maps_to_on_chain_confirmed_with_warning() -> None:
    mode, warning = DetectionModeResolver().resolve(
        {"onchain": {"enable_onchain_pending_detection": True}}
    )
    assert mode == "on_chain_confirmed"
    assert warning is not None
    rendered = warning.render()
    assert rendered.startswith("detection_legacy_flags_deprecated")
    assert "derived_mode=on_chain_confirmed" in rendered
    assert "replace_with=detection.mode" in rendered
    # Lazy-logging shape: template + args, never pre-interpolated.
    assert "%s" in warning.template
    assert warning.args == ("on_chain_confirmed", "enable_onchain_pending_detection")


def test_legacy_mined_flag_maps_to_on_chain_confirmed() -> None:
    mode, warning = DetectionModeResolver().resolve(
        {"onchain": {"enable_onchain_mined_detection": True}}
    )
    assert mode == "on_chain_confirmed"
    assert warning is not None
    assert warning.render().startswith("detection_legacy_flags_deprecated")


def test_explicit_mode_wins_over_legacy_flags() -> None:
    mode, warning = DetectionModeResolver().resolve(
        {
            "mode": "on_chain_pending",
            "onchain": {
                "enable_onchain_pending_detection": True,
                "enable_onchain_mined_detection": True,
            },
        }
    )
    assert mode == "on_chain_pending"
    assert warning is not None
    assert warning.render().startswith("detection_legacy_flags_ignored")

    mode, warning = DetectionModeResolver().resolve({"mode": " POLLING "})
    assert mode == "polling"
    assert warning is None


def test_warning_emitter_logs_each_distinct_warning_once() -> None:
    import logging

    from config.app_config import DetectionModeWarning, DetectionModeWarningEmitter

    class _Recorder(logging.Logger):
        def __init__(self) -> None:
            super().__init__("recorder")
            self.calls: list[tuple[str, tuple]] = []

        def warning(self, msg, *args, **kwargs):  # type: ignore[override]
            self.calls.append((str(msg), tuple(args)))

    logger = _Recorder()
    emitter = DetectionModeWarningEmitter()
    first = DetectionModeWarning(template="a=%s", args=("1",))
    emitter.emit(logger, first)
    emitter.emit(logger, DetectionModeWarning(template="a=%s", args=("1",)))
    emitter.emit(logger, DetectionModeWarning(template="a=%s", args=("2",)))

    assert logger.calls == [("a=%s", ("1",)), ("a=%s", ("2",))]
    assert first.render() == "a=1"
