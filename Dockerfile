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

# Verify the install
RUN python -c "from config.app_config import load_config; print('OK')"

# Fail-closed safety defaults. ``PAPER_TRADE_ONLY=true`` is the
# canonical kill-switch for live mode — set it here so a deployment
# missing an --env-file does NOT silently enable live trading.
# The ``CMD ["--paper"]`` token in the previous version was silently
# ignored (``app.py`` does no argv parsing), making the apparent
# "paper-by-default" comment a lie. The env var is the real switch.
ENV PAPER_TRADE_ONLY=true
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
