from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent
_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.env"
_LOCAL_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.local.env"


def load_runtime_env(
    *,
    override_secrets: bool = False,
    secrets_path: str | os.PathLike[str] | None = None,
    non_sensitive_path: str | os.PathLike[str] | None = None,
    local_non_sensitive_path: str | os.PathLike[str] | None = None,
) -> None:
    """Load checked-in config, then local public overrides, then local secrets.

    Load order is intentional:
    1) config/runtime.env (checked-in, non-sensitive shared defaults)
    2) config/runtime.local.env (local, non-sensitive identity/override lane)
    3) .env (local, sensitive values)
    """
    runtime_path = Path(non_sensitive_path).expanduser() if non_sensitive_path else _NON_SENSITIVE_ENV_PATH
    runtime_local_path = (
        Path(local_non_sensitive_path).expanduser()
        if local_non_sensitive_path
        else _LOCAL_NON_SENSITIVE_ENV_PATH
    )

    if runtime_path.exists():
        load_dotenv(dotenv_path=runtime_path, override=False)

    if runtime_local_path.exists():
        load_dotenv(dotenv_path=runtime_local_path, override=True)

    load_dotenv(dotenv_path=secrets_path, override=override_secrets)
