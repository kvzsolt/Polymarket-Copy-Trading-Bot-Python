# Docker on the CCXT-only main branch

The image uses Python 3.12 and the pinned runtime requirements, including
CCXT 4.5.85. (The application itself targets Python 3.14 only — the image
base is scheduled to move to `python:3.14.x-slim` in the modernization plan's
Docker task; until then the container runtime stays on 3.12.) It builds the
current checkout; the `:main` tag does not fetch Git or install CCXT from an
upstream development branch. Build checks verify the installed application,
PostgreSQL driver, and `ccxt.prediction.polymarket`.

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

Compose mounts the checkout's `config/` read-only, including service YAML,
`config/environments/`, and `config/traders/`. Keep that directory and the image
from the same checkout because the directory also contains Python configuration
modules. The old `runtime.local.example.yml` copy step is no longer needed.
An existing `config/runtime.local.yml` still participates in the loader's
deprecated compatibility layer; migrate it to the service/environment files.

Default settings select paper mode. `PMCOPY_ENVIRONMENT` from `secrets.env` can
select an explicit environment overlay; `execution.trading_mode` remains the
authoritative paper/live control. Live submission also requires the existing
database trading control to permit it. Rebuilding does not reset that control.

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
