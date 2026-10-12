# Automatic traders

Start the durable lifecycle with either alias:

```bash
pmcopy run --auto-trader
pmcopy run --auto-follow
```

The only trader-count setting is `scheduler.max_active_leaders` in
`config/discovery.yml`:

```yaml
scheduler:
  max_active_leaders: 3
```

Restart after changing it. Each follower has at most that many active traders
and a paper reserve cohort derived from the same number. Increasing the count
fills vacancies from qualified DB candidates. Decreasing it drains the weakest
selected traders. Open positions and unfinished orders keep their exit monitors;
flat former traders leave the monitored set.

## Startup and automatic promotion

PostgreSQL migration `20260916_10_auto_trader` creates additive tables for scoped
membership, audit history, command receipts, discovery leases, paper portfolios,
and paper event receipts. Normal application database initialization applies it.
Membership is isolated by follower account and explicit execution mode. Configure
a stable follower `funder_address` before automatic startup.

An initialized roster restores directly from the DB without calling Falcon.
Legacy active leaders are imported once with `activation_source: legacy`.
An initialized empty roster remains empty and available for operator action.
A genuinely new roster performs a bounded Falcon shortlist and targeted
qualification checks. The best candidates passing the current bankroll-aware
Falcon gates are automatically selected after the configured qualification
cycles. Initial selection does not wait for paper results.

Selection never changes `execution.trading_mode` or resumes a paused trading
control. Live execution still requires the existing explicit live mode,
authentication, reconciliation, risk, and trading-permission checks. In global
paper mode, active traders and reserves execute only in isolated paper portfolios.

## Evaluation and rotation

Healthy incumbents remain selected even when a reserve has a higher score.
Routine evaluation checks known wallets; it does not request a shortlist.
`scheduler.rescore_seconds` controls local results evaluation and
`scheduler.rediscover_seconds` controls targeted Falcon checks.

Both Falcon and recorded copy results are evaluated. Either sustained failure
can degrade an incumbent:

- Falcon rejection or score below `hysteresis.demote_score` on
  `hysteresis.min_tenure_cycles` successful observations;
- negative net results over the latest 20 fully closed copy episodes on that many
  evaluations, each containing an additional newly closed episode.

The configured minimum dwell time also applies. Defaults are three observations,
three hours of tenure, promotion score `0.55`, and demotion score `0.50`.
Provider outages do not count as losses or demote the roster. They persist an
entry block until a complete successful targeted check; owned exits remain
available.

Degraded traders stop receiving new entries. Qualified saved reserves fill their
slots first. Only when a degraded incumbent leaves an unfilled slot can the
runtime request a replacement shortlist. Discovery jobs are scoped and leased:
at most three attempts for one initialization/degradation event, with one-hour
retry spacing. A successful zero-safe result remains zero. The local
`rotate-traders` CLI command can request a new operator-directed cycle.

## Paper evidence and recovery

Each observed candidate has its own durable virtual cash and inventory. The
initial allocation is the follower's recorded available balance. Public CCXT
books supply current depth, tick size, minimum size, and explicit fee metadata.
Local `PaperExecutionGateway` writes model partial fills, taker fees, price
limits, and proportional sells. The evaluator reuses follower copy sizing, entry
quality, cash headroom, and risk limits. Money and shares use decimal arithmetic.
These portfolios measure each trader independently; they are not a simulation
of the combined live roster's shared capital or task-specific overrides.

Only fully closed episodes count toward promotion. Inventory is valued against
executable bid depth after fees; missing depth or unsupported fee metadata leaves
evidence incomplete. A missing book is never interpreted as a settlement or a
zero-value liquidation. A position held through resolution without an observed
exit consequently cannot manufacture a profitable closed episode.

Signal receipts and portfolio updates commit atomically. A durable transaction
claim selects one observation feed: a polling replay cannot duplicate an on-chain
copy, and separate mined log fills remain additive. Replays cannot spend paper
cash twice. Queue overflow, missed observations, or restart gaps invalidate
the current qualification window. Existing inventory and exit ownership survive.
Once flat, an invalid portfolio starts a new measured window with its original
virtual allocation; previous snapshots remain in audit history. Therefore a
restart can delay qualification instead of claiming continuous observation while
the bot was offline. Saved live and paper results remain separate.

## Operator review

The remote HTTP review and promotion surface
(`GET/POST /api/v1/leaders/...`) was removed together with the operator
control plane; there is no bearer-token promotion command anymore. Promotion
follows the automatic path described above: evidence defaults require seven
days of observed paper evidence, at least 20 closed copy episodes, positive
net realized PnL, drawdown below 10%, and fresh passing Falcon qualification,
subject to the shipped evidence limits. Roster history and audit records
remain queryable from the database directly.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_auto_trader_*.py
.venv/bin/python -m pytest -q -o addopts= tests/test_auto_trader_*.py
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest -q -o addopts=
.venv/bin/ruff check .
pyright
```

The all-tier commands include disposable PostgreSQL containers and require
Docker. These tests exercise simulated market data and local DB state; they do
not authorize or submit live orders.
