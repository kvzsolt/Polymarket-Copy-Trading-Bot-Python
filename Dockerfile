# Polymarket Copy Trading Bot — Production Docker image
# Build: docker build -t polymarket-copy-trader .
# Run:   docker run --env-file .env -v ./data:/app/data polymarket-copy-trader

FROM python:3.11-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/kvzsolt/Polymarket-Copy-Trading-Bot-Python"
LABEL org.opencontainers.image.description="Polymarket copy trading bot — paper & live modes"

# System deps for building wheels + runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user. Contains blast radius for any future
# code-execution bug — root inside the container would otherwise be
# one Docker-socket misconfiguration away from host root.
RUN groupadd -r botuser && useradd -r -g botuser -d /app -s /usr/sbin/nologin botuser

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot source. Respect .dockerignore so secrets.env / .git /
# data / log never bake into image layers.
COPY . .

# Ensure runtime paths are writable by the non-root user. data + log
# are the only paths the bot writes; everything else is read-only. We
# narrow the chown to those two directories so the rest of /app stays
# owned by root and ends up effectively read-only when the container
# runs as ``botuser`` (cubic P2).
RUN mkdir -p /app/data /app/log && chown -R botuser:botuser /app/data /app/log

# Verify the install. Import the Postgres connection layer too so a future
# requirements.txt drop of ``asyncpg`` (the runtime DB driver) fails the build
# rather than surfacing only at first DB use inside a running container.
RUN python -c "from config.app_config import load_config; import storage.postgres.connection; print('OK')"

# Fail-closed live-trading gate (P0-2). Live trading is OFF unless LIVE_TRADING
# is EXPLICITLY enabled; the config layer (config/app_config.py) defaults to
# PAPER when the master switch is absent/unknown, so a deployment missing config
# can never silently go live — no Docker env bake is required for safety.
# To go live: set ``LIVE_TRADING=enabled``. (Setting ``PAPER_TRADE_ONLY=true``
# still force-overrides to paper.) The legacy ``ENV PAPER_TRADE_ONLY=true`` bake
# is removed so the master switch is the single live gate.
ENV ENABLE_METRICS_SERVER=true
ENV METRICS_PORT=9102

# Drop privileges for the actual runtime.
USER botuser

# Expose the metrics / health server. Pre-fix this was EXPOSE 9090
# but the actual server binds to METRICS_PORT (default 9102), so
# Docker reported the container permanently unhealthy.
EXPOSE 9102

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request, os; urllib.request.urlopen('http://localhost:' + os.environ.get('METRICS_PORT', '9102') + '/health/ready')" || exit 1

ENTRYPOINT ["python", "app.py"]
