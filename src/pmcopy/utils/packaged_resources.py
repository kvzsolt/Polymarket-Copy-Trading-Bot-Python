"""Readonly packaged-resource and deployment-local path resolution.

Stdlib-only leaf module (importable from the config loader and the logging
layer without cycles). Two concerns:

- **Packaged resources** — the shipped config YAML tree and SQL migrations
  are readonly inputs resolved through ``importlib.resources``, so an
  installed wheel serves them from (possibly read-only) site-packages and no
  code path ever derives them from ``__file__``. Filesystem installs only:
  a zipimport-based deployment fails closed with a clear error instead of
  silently extracting or writing into the package tree.
- **Deployment root** — the single directory holding deployment-local state
  (``secrets.env``, the legacy ``config/runtime.local.yml`` shim, logs,
  data). Resolution is explicit: the ``PMCOPY_DEPLOYMENT_ROOT`` environment
  variable (when it names an existing directory) or the process CWD.
  Ancestor directories are NEVER walked: a leftover ``secrets.env`` in a
  parent must not become part of an unrelated process's configuration.
"""

from __future__ import annotations

import importlib.resources
import os
from pathlib import Path

DEPLOYMENT_ROOT_ENV = "PMCOPY_DEPLOYMENT_ROOT"


def packaged_dir(package: str) -> Path:
    """Return an installed package's directory as a filesystem path.

    Raises:
        RuntimeError: If the package is not backed by a real directory
            (e.g. a zip import). Such deployments are unsupported: packaged
            defaults are readonly inputs and are never extracted.
    """
    resource = importlib.resources.files(package)
    if isinstance(resource, Path):
        return resource
    raise RuntimeError(
        f"packaged resources for {package!r} are not on a filesystem "
        f"({type(resource).__name__}); install the package (wheel/sdist) to a "
        "real directory — zipimport deployments are unsupported"
    )


def packaged_resource_dir(package: str, child: str) -> Path:
    """Return a subdirectory of packaged readonly resources."""
    packaged = packaged_dir(package) / child
    if not packaged.is_dir():
        raise RuntimeError(
            f"packaged resource directory {child!r} is missing from {package!r} "
            f"(looked at {packaged}); the installed distribution is incomplete"
        )
    return packaged


def deployment_root() -> Path:
    """Return the deployment-local root: env var (existing dir) or CWD.

    Precedence is explicit and documented: ``PMCOPY_DEPLOYMENT_ROOT`` when it
    names an existing directory, otherwise the process CWD. Parent
    directories are never searched for markers or credentials.
    """
    explicit = os.getenv(DEPLOYMENT_ROOT_ENV)
    if explicit:
        root = Path(explicit).expanduser().resolve()
        if root.is_dir():
            return root
    return Path.cwd()
