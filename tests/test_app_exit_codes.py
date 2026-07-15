"""Fatal startup must return a non-zero exit code (runtime-hardening).

Previously several startup-fatal branches did a bare ``return`` from ``main()``,
which ``__main__`` discarded — so a fail-closed startup exited 0 and misled
Docker / systemd / process supervisors. ``main()`` now returns ``int`` and
``__main__`` exits with it.
"""
from __future__ import annotations

import pytest

import app


@pytest.mark.asyncio
async def test_main_returns_1_on_env_validation_failure(monkeypatch):
    def _boom() -> None:
        raise app.EnvValidationError("missing PRIVATE_KEY")

    # The EnvValidationError branch is the very first thing in main(), before
    # build_app(), so this exercises the exit-code path without standing up the
    # whole application.
    monkeypatch.setattr(app, "load_config", _boom)

    result = await app.main()
    assert result == 1


def test_main_is_annotated_to_return_int():
    import inspect

    assert inspect.signature(app.main).return_annotation in ("int", int)


def test_dunder_main_exits_with_main_result():
    # __main__ must propagate main()'s return code, not swallow it.
    src = (app.__file__ and open(app.__file__, encoding="utf-8").read()) or ""
    assert "sys.exit(exit_code)" in src
    assert "loop.run_until_complete(main_task) or 0" in src
