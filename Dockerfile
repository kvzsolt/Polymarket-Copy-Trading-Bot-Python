# Polymarket Copy Trading Bot — CCXT-only main branch image.
# Build and operator commands: docs/docker.md.
#
# Multi-stage build (Task 20): the builder stage compiles the pure-Python
# wheel from the checkout; the runtime stage installs that wheel plus the
# locked runtime dependencies and never contains the source tree. The
# installed pmcopy package — console entry point, application modules, and
# the packaged config YAML tree — is resolved from site-packages; there is
# no PYTHONPATH shadowing anywhere in the image.
#
# Base tag policy: pinned to the exact interpreter patch the repository
# test suite runs on (Python 3.14.6, matching requires-python >=3.14,<3.15).
# The repo pins tags, not digests (same policy as postgres:16-alpine in
# compose) so Debian security updates keep flowing under the same Python
# patch. Registry digests verified 2026-10-11 via
# `docker manifest inspect python:3.14.6-slim-bookworm`:
#   linux/amd64 sha256:ff83a535339812dd72e69c93b3c48ddf7c85a324d6330af5797c82a255dbeef4
#   linux/arm64 sha256:1609c3e634a6ad5b4759f67e34c246640d41858fffde69876245fd26afd632b7

# ---------------------------------------------------------------------------
# Builder: compile the application wheel. Build tooling stays in this stage.
# ---------------------------------------------------------------------------
FROM python:3.14.6-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /build

# Wheel-build inputs only: pyproject metadata (README is its readme field)
# and src/. .dockerignore keeps secrets.env / db.env / .git / data / log /
# tests out of the build context, so they cannot reach any stage.
COPY pyproject.toml README.md ./
COPY src ./src

# Pure-Python wheel: setuptools runs inside pip's isolated build environment
# in this stage only. No compiler or build tool ever reaches the runtime.
RUN pip wheel --no-cache-dir --no-deps -w /wheels .

# ---------------------------------------------------------------------------
# Runtime: locked dependencies + the built wheel. Nothing else.
# ---------------------------------------------------------------------------
FROM python:3.14.6-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python"
LABEL org.opencontainers.image.description="CCXT-only Polymarket copy trading bot — paper & live modes"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Non-root runtime user. Contains blast radius for any future
# code-execution bug — root inside the container would otherwise be
# one Docker-socket misconfiguration away from host root.
RUN groupadd -r botuser && useradd -r -g botuser -d /app -s /usr/sbin/nologin botuser

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.runtime.txt .
# Runtime dependencies all publish manylinux/pure wheels for supported
# Python/CPU targets. Refuse source builds so compilers and development
# headers never enter the production image.
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.runtime.txt

# The built wheel is the only application artifact crossing the stage
# boundary: no source tree, no /app/src, no PYTHONPATH. Config YAML ships
# inside the wheel and is read via importlib.resources from site-packages.
COPY --from=builder /wheels/*.whl /tmp/wheels/
RUN pip install --no-cache-dir --no-deps /tmp/wheels/*.whl && pip check && rm -rf /tmp/wheels

# Ensure runtime paths are writable by the non-root user. data + log
# are the only paths the bot writes; everything else is read-only. We
# narrow the chown to those two directories so the rest of /app stays
# owned by root and ends up effectively read-only when the container
# runs as ``botuser`` (cubic P2).
RUN mkdir -p /app/data /app/log && chown -R botuser:botuser /app/data /app/log

# Verify the install. Import the Postgres connection layer too so a future
# requirements.txt drop of ``asyncpg`` (the runtime DB driver) fails the build
# rather than surfacing only at first DB use inside a running container.
# Run this outside /app so the check proves the installed console entry point
# can import the namespaced application modules from site-packages. Checking
# from /app masks missing ``pmcopy`` packaging because cwd is importable.
RUN cd /tmp && python -c "import pmcopy.app, pmcopy.bootstrap; from pmcopy.config.app_config import load_config; import pmcopy.storage.postgres.connection; print('OK')"
RUN cd /tmp && python -c "import ccxt; from ccxt.prediction import polymarket; from pmcopy.integrations.polymarket.ccxt_compat.version_guard import ensure_supported_ccxt_version; ensure_supported_ccxt_version(ccxt.__version__); assert callable(polymarket); print('CCXT prediction OK:', ccxt.__version__)"

# Fail-closed mode selection lives in AppConfig. ``execution.trading_mode``
# defaults to paper and is immutable after startup. Live submission additionally
# requires an explicit database-backed ``pmcopy trading resume`` decision.
ENV PMCOPY__APP__ENABLE_METRICS_SERVER=true
ENV PMCOPY__APP__METRICS_BIND_HOST=0.0.0.0
ENV PMCOPY__APP__METRICS_PORT=9102

# Drop privileges for the actual runtime.
USER botuser

EXPOSE 9102

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://localhost:' + os.environ.get('PMCOPY__APP__METRICS_PORT', '9102') + '/health/ready', timeout=3)" || exit 1

# Exec form so SIGTERM reaches the pmcopy process directly; compose grants a
# 60s stop_grace_period for graceful shutdown.
ENTRYPOINT ["pmcopy"]
CMD ["run"]
