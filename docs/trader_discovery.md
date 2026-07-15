# Trader Discovery

The project has one discovery pipeline and one PostgreSQL roster. There are no
generation switches, legacy evaluators, env-file rotators, or deprecated
realtime activity detector.
Polymarket's public Data API polling and the existing mined/pending on-chain
detector remain the live leader-signal sources.

## Funnel

```text
H-Score 584
   ↓ round-robin shortlist across configured sorts
Wallet 360 581 (1/3/7/15-day audit)
   ↓ bot, drawdown, diversity, stake-CV, consistency gates
Trades 556
   ↓ activity age and sample-size gates
Market Insights 575
   ↓ per-traded-market liquidity/participation pass ratio
Social Pulse 585 (optional)
   ↓
configured composite score
   ↓
decay-weighted longitudinal score + low-sample shrinkage
   ↓
PostgreSQL watchlist/active roster with hysteresis
```

One pooled HTTP/2 client serves every Heisenberg agent ID. Every parameter
value is serialized as a string; pagination limit/offset remain integers and
pagination stops when a page returns fewer rows than requested. Retries use a
bounded exponential backoff and honor numeric or HTTP-date `Retry-After`.

## Longitudinal evidence

- Wallet 360 snapshots are stored each cycle.
- Agent 569 seeds realized-only daily PnL; it does not represent open-position
  risk.
- Agent 556 seeds trade counts and turnover.
- Agent 579 stores 1/3/7/30-day ranks.
- Metric history is decay-weighted, then shrunk toward the configured prior for
  low-trade wallets.

Roster promotion requires consecutive observations above `promote_score`.
Demotion requires consecutive observations below `demote_score` and the dwell
timer. The gap between the thresholds prevents score noise from churning live
monitors. Longitudinal rank also scales copy sizing inside the configured
minimum/maximum multipliers; portfolio and per-order risk caps remain binding.
At startup, active rows are accepted only when their decision-config hash
matches the current gates and formulas. Transport rate limits and scheduler
cadence do not invalidate the roster.
The execution-time leader score is not a second ranking system: it blends the
persisted roster score with fresh local win-rate, realized PnL, and activity
using `scoring.runtime_weights` from the same YAML file.

Every setting and formula coefficient is in
`config/trader_discovery.yaml`. Process overrides use paths such as:

```dotenv
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
