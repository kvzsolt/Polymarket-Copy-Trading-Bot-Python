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

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot source
COPY . .

# Verify the install
RUN python -c "from config.app_config import load_config; print('OK')"

# Expose health endpoint
EXPOSE 9090

# Run paper mode by default; override with --live in CMD
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9090/health/ready')" || exit 1

ENTRYPOINT ["python", "app.py"]
CMD ["--paper"]
