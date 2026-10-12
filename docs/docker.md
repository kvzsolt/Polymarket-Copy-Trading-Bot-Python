# Docker on the CCXT-only main branch

The image is a multi-stage build on `python:3.14.6-slim-bookworm` — the exact
interpreter patch the test suite runs on (the application targets Python 3.14
only). The builder stage compiles the pure-Python wheel from the checkout; the
runtime stage installs that wheel plus the pinned runtime requirements from
`requirements.runtime.txt` (generated from `uv.lock`), including CCXT 4.5.85.
The runtime stage never contains the source tree: `pmcopy`, its console entry
point, and the packaged config YAML tree install into site-packages and are
resolved through `importlib.resources` (no `PYTHONPATH` shadowing). Build
checks verify the installed application, the PostgreSQL driver, and
`ccxt.prediction.polymarket` from outside `/app`.

Base tag policy: the repo pins tags, not digests (same policy as
`postgres:16-alpine` in compose), so Debian security updates keep flowing
under the same Python patch. Registry digests recorded on 2026-10-11 via
`docker manifest inspect python:3.14.6-slim-bookworm`:

- linux/amd64 `sha256:ff83a535339812dd72e69c93b3c48ddf7c85a324d6330af5797c82a255dbeef4`
- linux/arm64 `sha256:1609c3e634a6ad5b4759f67e34c246640d41858fffde69876245fd26afd632b7`

## Prepare

Use Docker Engine with the Compose plugin. For a new installation only:

```bash
cp secrets.env.example secrets.env
chmod 600 secrets.env
cp db.env.example db.env
chmod 600 db.env
```

Keep existing `secrets.env` and `db.env` files when upgrading. The two secret
files are split per service: `db.env` holds only `POSTGRES_PASSWORD` and is
read only by the compose postgres service; `secrets.env` holds the bot's
credentials (wallet key, DSN, discovery token, relayer keys) and is read only
by the bot service. Set `POSTGRES_PASSWORD` privately in `db.env` and
`PMCOPY__DATABASE__DSN` privately in `secrets.env`. Inside Compose, the database hostname is
`postgres`, for example
`postgresql://bot:<URL-encoded-password>@postgres:5432/polymarket_copytrading_python`.
The password must match the existing database if its volume is already initialized;
changing `POSTGRES_PASSWORD` does not rotate an existing PostgreSQL user's password.
Populate the required wallet/RPC settings from `secrets.env.example`.

Default settings select paper mode. `PMCOPY_ENVIRONMENT` from `secrets.env` can
select an explicit environment overlay; `execution.trading_mode` remains the
authoritative paper/live control. Live submission also requires the existing
database trading control to permit it. Rebuilding does not reset that control.

Config lives inside the installed wheel: service YAML, `environments/`
overlays, and `traders/` ship as package data and compose mounts no source
tree (the wheel-only image has no `/app/src`). Runtime overrides use
`PMCOPY__*` environment variables from `secrets.env` and the compose
`environment` block; changing packaged service YAML requires rebuilding the
image (`docker compose build bot`). An existing `config/runtime.local.yml`
still participates in the loader's deprecated compatibility layer when placed
in the deployment root; migrate it to the service/environment files.

## Build and run

```bash
docker compose config --quiet
docker compose build --pull bot
docker compose up -d postgres
docker compose run --rm --no-deps bot doctor
docker compose up -d --no-build bot
docker compose logs -f bot
```

`doctor` performs configured connectivity checks. Starting the bot connects to
the configured providers and database, runs startup migrations/reconciliation,
and can submit orders if explicit live mode and trading control already permit
them. Check those settings before starting an existing live installation.

The image is named `polymarket-copy-trader:main`. Its default command is
`pmcopy run`; alternate commands replace the `run` subcommand:

```bash
docker compose run --rm --no-deps bot --help
docker compose run --rm --no-deps bot config validate
docker compose run --rm --no-deps bot trading status
docker compose exec bot pmcopy trading status
```

To pass run options, include `run`, for example
`docker compose run --rm bot run --auto-trader`. Do not run a second bot loop
against the same account/database while the bot service is already running.

## Persistence and health

The existing PostgreSQL 16 image family and named database volume
`polymarket_copytrading_python_postgres_data` are retained. Bot data and file
logs use Compose-managed `bot_data` and `bot_logs` volumes, initialized with
the image's non-root ownership. Existing host `./log` files are left in place;
new logs go to the managed volume and remain visible through
`docker compose logs`. Existing host data is not copied automatically. If you
previously mounted local data, copy the required files into the new volume
while the bot is stopped, preserving `botuser` ownership.

Metrics listen on `0.0.0.0:9102` inside the container and are published only
on `127.0.0.1:9102` on the host. PostgreSQL remains host-loopback-only too.
The healthcheck still uses `/health/ready`, which requires actual runtime
readiness; a running container alone does not prove venue or account readiness.

```bash
docker compose ps
curl --fail http://127.0.0.1:9102/health/ready
docker compose stop bot
```

The bot has 60 seconds for graceful shutdown. `docker compose down` retains
named volumes; `docker compose down --volumes` deletes durable state and must
not be used for a routine rebuild or upgrade.

## Local verification without credentials

```bash
python -m pytest -q tests/test_docker_ccxt_runtime.py
python -m pytest -q -o addopts='' tests/release_gate/test_dockerfile_safety_defaults.py
docker build -t polymarket-copy-trader:main .
docker run --rm --network none polymarket-copy-trader:main --help
docker run --rm --network none polymarket-copy-trader:main doctor --help
```

These checks build and inspect the container without enabling trading or
testing a funded account. The image's production readiness probe remains strict.
