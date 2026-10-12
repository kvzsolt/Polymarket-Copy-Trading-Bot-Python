# Polymarket Copy Trading Bot — CCXT-only main branch image.
# Build and operator commands: docs/docker.md.

FROM python:3.12-slim-bookworm

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
# Runtime dependencies all publish manylinux wheels for supported Python/CPU
# targets. Refuse source builds so compilers and development headers never
# enter the production image.
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.runtime.txt

# Copy the bot source. Respect .dockerignore so secrets.env / .git /
# data / log never bake into image layers.
COPY . .
RUN pip install --no-cache-dir --no-deps . && pip check

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

# The console script otherwise imports the installed config package and
# silently ignores service YAML mounted at /app/src/pmcopy/config. Set this
# only after the installed-wheel checks above so those checks cannot use the
# source tree.
ENV PYTHONPATH=/app/src

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

ENTRYPOINT ["pmcopy"]
CMD ["run"]
