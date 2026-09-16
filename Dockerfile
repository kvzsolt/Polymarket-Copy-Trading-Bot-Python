# Polymarket Copy Trading Bot — Production Docker image
# Build: docker build -t polymarket-copy-trader .
# Run:   docker run --env-file secrets.env -v ./data:/app/data polymarket-copy-trader
#
# Phase 4.17 — the run command uses secrets.env (0600, PMCOPY__* keys), not a
# stale .env. Compose consumes secrets.env via env_file (see docker-compose.yml).

FROM python:3.11-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python"
LABEL org.opencontainers.image.description="Polymarket copy trading bot — paper & live modes"

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
RUN pip install --no-cache-dir --no-deps .

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
# can import the top-level application modules from site-packages. Checking
# from /app masks missing ``py-modules`` packaging because cwd is importable.
RUN cd /tmp && python -c "import app, bootstrap; from config.app_config import load_config; import storage.postgres.connection; print('OK')"

# Fail-closed mode selection lives in AppConfig. ``execution.trading_mode``
# defaults to paper and is immutable after startup. Live submission additionally
# requires an explicit database-backed ``pmcopy trading resume`` decision.
ENV PMCOPY__NETWORK__ENABLE_METRICS_SERVER=true
ENV PMCOPY__NETWORK__METRICS_PORT=9102

# Drop privileges for the actual runtime.
USER botuser

# Expose the metrics / health server. Pre-fix this was EXPOSE 9090
# but the actual server binds to METRICS_PORT (default 9102), so
# Docker reported the container permanently unhealthy.
EXPOSE 9102

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://localhost:' + os.environ.get('PMCOPY__NETWORK__METRICS_PORT', '9102') + '/health/ready')" || exit 1

ENTRYPOINT ["pmcopy", "run"]
