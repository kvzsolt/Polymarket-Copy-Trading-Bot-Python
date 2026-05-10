from __future__ import annotations

import os
import warnings
from pathlib import Path

from dotenv import dotenv_values, load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.env"
_LOCAL_NON_SENSITIVE_ENV_PATH = _REPO_ROOT / "config" / "runtime.local.env"

# Keys that are expected to live in secrets.env and should override the
# public/local runtime lanes when present.
_SECRETS_OVERRIDE_KEYS = {
    "RPC_URL",
    "CLOB_API_KEY",
    "CLOB_SECRET",
    "CLOB_PASSPHRASE",
    "PRIVATE_KEY",
    "PRIVATE_KEY_FILE",
    "FUNDER_ADDRESS",
}


def _load_env_values(path: Path) -> dict[str, str]:
    raw = dotenv_values(path)
    return {str(k): str(v) for k, v in raw.items() if k and v is not None}


def _warn_on_conflicting_public_overrides(*, local_public: dict[str, str], secrets: dict[str, str], secrets_path: Path) -> None:
    for key, local_value in local_public.items():
        secret_value = secrets.get(key)
        if not secret_value or secret_value == local_value or key in _SECRETS_OVERRIDE_KEYS:
            continue
        warnings.warn(
            f"{secrets_path.name} defines non-sensitive key '{key}' that conflicts with config/runtime.local.env. "
            f"Ignoring secrets value and keeping runtime.local.env. Move '{key}' out of secrets.env.",
            UserWarning,
        )


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
    3) secrets.env (local, sensitive values only)

    Public keys in runtime.local.env win over secrets.env. Secrets override only
    for explicitly allowlisted sensitive keys like RPC_URL and PRIVATE_KEY.
    """
    runtime_path = Path(non_sensitive_path).expanduser() if non_sensitive_path else _NON_SENSITIVE_ENV_PATH
    runtime_local_path = (
        Path(local_non_sensitive_path).expanduser()
        if local_non_sensitive_path
        else _LOCAL_NON_SENSITIVE_ENV_PATH
    )

    if runtime_path.exists():
        load_dotenv(dotenv_path=runtime_path, override=False)

    local_public_values: dict[str, str] = {}
    if runtime_local_path.exists():
        load_dotenv(dotenv_path=runtime_local_path, override=True)
        local_public_values = _load_env_values(runtime_local_path)

    # Resolve secrets path explicitly to avoid find_dotenv() frame-inspection
    # surprises. If the caller supplies a path use that; otherwise default to
    # <repo>/secrets.env so the lookup is deterministic regardless of call depth.
    resolved_secrets = Path(secrets_path).expanduser() if secrets_path else _REPO_ROOT / "secrets.env"
    if resolved_secrets.exists():
        secret_values = _load_env_values(resolved_secrets)
        _warn_on_conflicting_public_overrides(
            local_public=local_public_values,
            secrets=secret_values,
            secrets_path=resolved_secrets,
        )

        # Only set allowlisted sensitive keys from secrets.env into os.environ.
        # Non-sensitive keys must live in runtime.env / runtime.local.env.
        for key in _SECRETS_OVERRIDE_KEYS:
            value = secret_values.get(key)
            if value is not None and key not in os.environ:
                os.environ[key] = value

        # Then explicitly override sensitive keys (even if already set).
        if override_secrets:
            for key in _SECRETS_OVERRIDE_KEYS:
                value = secret_values.get(key)
                if value is not None:
                    os.environ[key] = value
