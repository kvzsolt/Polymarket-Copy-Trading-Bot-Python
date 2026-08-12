# Trader Discovery

The project has one discovery pipeline and one PostgreSQL roster. The live
discovery service is the Falcon-backed
`services.discovery.falcon_discovery_service.FalconDiscoveryService`,
which consumes `integrations.falcon.client.FalconClient` (the
`narrative.agent.heisenberg.so` parameterized-agent endpoint) and produces
`services.discovery.models.Candidate` rows persisted by
`storage.postgres.discovery_repo.DiscoveryRepo`. There is no legacy funnel or
alternate analytics-provider client.

The paid Falcon transport currently retains the vendor hostname shown above
and the wire field `h_score`; those are external protocol details documented
by the [paid API reference](https://prediction.heisenberg.so/docs/api-reference/heisenberg-leaderboard),
not a second application integration. The runtime exposes only Falcon-named
packages, services, configuration, telemetry, and roster source values.

Polymarket's public Data API polling and the existing mined/pending on-chain
detector remain the live leader-signal sources.

## Pipeline

```text
Falcon leaderboard (agent 584)
   ↓ H-Score shortlist, EVM-address + h_score threshold gates
Wallet360 581 (1/3/7/15-day) + all-time 586
   ↓ sybil, drawdown, concentration, missing-evidence, freshness gates
Trades 556
   ↓ recency, sample-size, short-duration-market gates
Markets 574 + Market360 575
   ↓ liquidity, toxic-market, capacity gates
Realized PnL 569 + official PnL leaderboard 579
   ↓ realized-PnL corroboration, official-rank corroboration
Optional CCXT visible-depth probe (ccxt.prediction.polymarket order book)
   ↓ bankroll-compatible copy ratio
bankroll-aware composite score (0..1)
   ↓
PostgreSQL watchlist/active roster with hysteresis (LeaderRegistry)
```

The discovery service ranks wallets by **bankroll-compatible copyability**, not
raw PnL or H-Score alone. Every wallet is re-evaluated with fresh trades,
Wallet360 windows, market duration, Market360 quality, realized PnL, price-jump
risk, and a follower-specific bankroll-fit calculation.

## Bankroll source

The live discovery reads the operator's USDC bankroll each cycle via the
read-only `utils.getMyBalance.get_my_balance` on-chain `balanceOf` call,
using the **first runtime user's** funder address (single-user default:
`config.wallet.funder_address`). A missing or non-positive balance fails
closed: the cycle is recorded as `failed`, `last_cycle_degraded` is set, and
no candidates are returned — discovery never ranks against an unknown or zero
bankroll. No order, approval, or mutation is performed by the discovery path.

## Fail-closed provider semantics

- Auth, permission, schema, or configuration errors fail closed.
- Transient outage/rate-limit raises propagate as a failed cycle
  (`last_cycle_degraded=True`); the roster is not mass-demoted on missing
  evidence.
- A successful Falcon run that produces zero safe wallets returns zero
  candidates and does not silently substitute weaker candidates.
- All provider secrets and error bodies are redacted before logging.

## Roster and hysteresis

Roster promotion requires consecutive observations above `promote_score`.
Demotion requires consecutive observations below `demote_score` and the dwell
timer. The gap between the thresholds prevents score noise from churning live
monitors. At startup, active rows are accepted only when their decision-config
hash matches the current gates and formulas. A `rescore` re-ranks the last
accepted roster from stored snapshots without an API call and does not advance
hysteresis tenure. Transport rate limits and scheduler cadence do not
invalidate the roster; the `falcon` policy thresholds do.

The execution-time leader score is not a second ranking system: it blends the
persisted roster score with fresh local win-rate, realized PnL, and activity
using `scoring.runtime_weights` from the same YAML file.

## Configuration

Every discovery setting and policy threshold is in `config/discovery.yml`.
Falcon policy lives under `discovery.falcon`; its private transport lives under
`discovery.api`. The API key is supplied via env and never committed:

```dotenv
PMCOPY__DISCOVERY__API__TOKEN=...
PMCOPY__DISCOVERY__FALCON__ENABLED=true
PMCOPY__DISCOVERY__SCHEDULER__REDISCOVER_SECONDS=900
PMCOPY__DISCOVERY__HYSTERESIS__MIN_DWELL_SECONDS=21600
PMCOPY__DISCOVERY__API__REQUESTS_PER_SECOND=3
```

Inspect without running trading:

```bash
python scripts/run_discovery_only.py
python scripts/run_discovery_only.py --list-only
pmcopy rotate-traders --rescore-only
```

## Operator ranking CLI (bankroll report)

`scripts/rank_copy_wallets.py` is a standalone, read-only operator tool that
ranks Polymarket wallets for an explicitly supplied USDC bankroll and prints a
ranked report. It performs no live trading, accepts no private keys, and never
mutates venue state.

```bash
python scripts/rank_copy_wallets.py --funds 500 --top 10
python scripts/rank_copy_wallets.py --funds 2000 --top 20 --json
python scripts/rank_copy_wallets.py --funds 100 --ccxt-depth auto
```

See `docs/discovery_bankroll_ranking.md` for how to choose a bankroll and read
the report.
