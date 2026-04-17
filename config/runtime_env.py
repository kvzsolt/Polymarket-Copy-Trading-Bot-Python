from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.env"
_LOCAL_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.local.env"


def load_runtime_env(
    *,
    override_secrets: bool = True,
    secrets_path: str | os.PathLike[str] | None = None,
    non_sensitive_path: str | os.PathLike[str] | None = None,
    local_non_sensitive_path: str | os.PathLike[str] | None = None,
) -> None:
    """Load checked-in config, then local public overrides, then local secrets.

    Load order is intentional:
    1) config/runtime.env (checked-in, non-sensitive shared defaults)
    2) config/runtime.local.env (local, non-sensitive identity/override lane)
    3) secrets.env (local, sensitive values)
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

    # Resolve secrets path explicitly to avoid find_dotenv() frame-inspection
    # surprises.  If the caller supplies a path use that; otherwise default to
    # <repo>/.env so the lookup is deterministic regardless of call depth.
    resolved_secrets = Path(secrets_path).expanduser() if secrets_path else _REPO_ROOT / "secrets.env"
    if resolved_secrets.exists():
        # override=True so values in secrets.env always win over inherited empty env
        # vars (e.g. LEADER_WALLETS= exported in the shell would otherwise
        # silently suppress the user's wallet list).
        load_dotenv(dotenv_path=resolved_secrets, override=override_secrets)
