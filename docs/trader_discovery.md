# Trader Discovery

The project has one discovery pipeline and one PostgreSQL roster. The live
discovery service is the Falcon-backed
`services.discovery.falcon_discovery_service.FalconDiscoveryService`,
which consumes `integrations.falcon.client.FalconClient` (the
premium `retriever.falconapi.net` parameterized-agent endpoint) and produces
`services.discovery.models.Candidate` rows persisted by
`storage.postgres.discovery_repo.DiscoveryRepo`. There is no legacy funnel or
alternate analytics-provider client.

The premium endpoint is
`https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized`.
Its request uses `Authorization: Bearer <token>` and the `data.results` response
envelope described by the [Falcon API documentation](https://polygunllc.mintlify.app).
Supply the raw token, without adding a `Bearer` prefix. An explicit
`discovery.api.base_url` override remains supported for compatible endpoints.

Premium endpoint access does not imply access to every analytics agent. In
particular, `discovery.falcon.enable_price_jump_risk: true` requires price-jump
agent **596**. A `403 AGENT_NOT_ALLOWED` stops discovery without retries or a
weaker-candidate fallback during initial qualification. The failure log preserves the
requested `falcon_agent_id`; the exception also reports `agent_id=596` for this
case, without provider error bodies or response headers. The shipped operator
configuration explicitly sets `discovery.falcon.enable_price_jump_risk: false`
because this plan does not include agent 596. This skips price-jump requests
and the excessive-price-jump rejection; the other discovery gates still run.
To restore this check, ask Falcon to enable agent 596 for the configured token
and set the flag to `true`. The application never disables an enabled check
automatically after a denial.

Polymarket's public Data API polling and the existing mined/pending on-chain
detector remain the live leader-signal sources.

## Pipeline

```text
Falcon leaderboard (agent 584), unioned across sort modes
   ↓ shortlist union, EVM-address + h_score threshold gates
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
- Initial automatic discovery propagates sanitized provider failures. Restarts
  restore initialized membership without a provider request. A failed routine
  targeted check persists an entry block while keeping owned exits available;
  it never counts as poor performance or starts a fallback sweep.
- Transient outage/rate-limit raises propagate as a failed cycle
  (`last_cycle_degraded=True`); the roster is not mass-demoted on missing
  evidence.
- A successful Falcon run that produces zero safe wallets returns zero
  candidates and does not silently substitute weaker candidates.
- All provider secrets and error bodies are redacted before logging.

Wallet360's `max_position_pct` is parsed as percentage points in `[0, 100]`
and normalized to a fraction internally: `21.225` becomes `0.21225` and
`0.3048` becomes `0.003048`. This is a field-specific conversion, never a
magnitude-based guess. Win rate, confidence, and market concentration retain
their fraction contracts. Missing values remain missing; malformed or
out-of-range values still fail closed, including in historical snapshot rows.

Wallet360's `max_drawdown` remains a finite, non-negative provider metric,
without rescaling. Falcon's [Wallet360 reference](https://falcon-bot.gitbook.io/falcon-api/market-intelligence/falcon-wallet-360)
includes values above one (for example, `3.7464`), so the parser does not impose
an upper bound of one. Such values retain the maximum existing drawdown scoring
penalty and exceed every supported `max_drawdown` rejection threshold when
`drawdown_gate_enabled` is true. Missing drawdown remains missing; negative,
boolean, non-finite, and malformed values fail closed.

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

## Shortlist size and sort-mode union

Agent 584 is hard-capped at **200 rows per sort order**: requesting offset 200
returns ranks 1-200 again with `has_more=false`. A single `sort_by="h_score"`
call therefore cannot produce a 500-wallet shortlist.

`leaderboard_sort_modes` (default `h_score, roi, win_rate, pnl, sharpe, trades`)
is queried concurrently, one call per mode with
`limit = min(candidate_limit, 200)`, and the rows are concatenated in
configuration order and de-duplicated by wallet. The first mode wins
precedence, so `h_score` stays the primary ranking signal when
`candidate_limit` truncates the union.

A sort mode that fails does not kill the cycle: its error is recorded as a
`leaderboard:<mode>:<ErrorType>` warning on the `DiscoveryResult` and the
remaining modes still populate the shortlist. If **every** mode fails the first
error is re-raised and the cycle is recorded as failed — fail closed.

`candidate_limit` ships at **50**. Each shortlisted wallet costs several
provider calls, so one sweep runs a few minutes at `candidate_concurrency: 12`,
and initial automatic qualification
(`hysteresis.min_tenure_cycles: 3`) stays bounded by that. Raise
`candidate_limit` for a wider net — cycle time scales roughly linearly with it —
keeping the provider rate limit in `discovery.api` in mind.

## Durable automatic selection

`python bot.py run --auto-trader` and `--auto-follow` select the same DB-backed
lifecycle. See [Automatic traders](auto-follow.md) for configuration and control
plane commands.

An initialized account restores its saved membership without a Falcon startup
request. Existing active legacy leaders are imported once. An empty initialized
roster stays empty; it does not trigger a sweep on each restart.

For a new account, one capped shortlist plus targeted qualification cycles
selects the best qualifying candidates automatically. Paper evidence is not
required for this initial selection. Healthy incumbents remain selected even
when another wallet ranks higher. Routine evaluation targets known wallets.
A sweep is permitted after sustained incumbent degradation when the stored
reserve cannot fill the vacancy, or through an explicit operator discovery job.

The sole count is `scheduler.max_active_leaders`. It bounds active traders and
also derives the reserve paper cohort. Outstanding exits retain monitoring.

## Hard gates and rejection reasons

Every rejected wallet carries a `;`-joined `rejected_reason` list. All gates are
policy-driven from `discovery.falcon` in `config/discovery.yml`; each threshold
below names the setting that controls it.

| Reason | Gate |
| --- | --- |
| `invalid_wallet_address` | leaderboard row is not an EVM address |
| `h_score_below_threshold` | `min_h_score` |
| `insufficient_wallet360_evidence` | `required_wallet360_windows` |
| `stale_wallet360_evidence` | `max_wallet360_snapshot_age_hours` |
| `sybil_risk` | `max_sybil_score`, provider sybil flag, or `max_similar_wallets` |
| `timing_anomaly` | provider timing-anomaly flag |
| `inactive_or_stale` | `max_last_trade_age_hours`, `activity_window_hours`, `min_activity_window_trades` |
| `insufficient_recent_trades` | `min_recent_trades` |
| `insufficient_short_duration_sample` | `min_short_trade_count` (0 = off) |
| `short_duration_share_below_threshold` | `min_short_trade_share` (0 = off) |
| `market_liquidity_below_threshold` | `min_liquidity_percentile` |
| `excessive_recent_price_jumps` | `max_recent_price_jumps`, `enable_price_jump_risk` |
| `toxic_or_inactive_market_mix` | `toxic_market_flags`, `max_toxic_market_share` |
| `wallet_market_concentration` | `max_market_concentration` |
| `drawdown_above_threshold` | `max_drawdown`, only when `drawdown_gate_enabled` |
| `missing_evidence_above_threshold` | `max_missing_field_ratio` |
| `bankroll_incompatible` | `bankroll.*` (see below) |
| `perfect_timing` | `reject_perfect_timing_flag` |
| `suspicious_win_rate` | `reject_suspicious_win_rate_flag` |
| `hyperactive_trader` | `max_trades_per_day` |
| `micro_trade_bot` | `min_avg_trade_size_usdc` |
| `excluded_category_dominant` | `excluded_categories`, `max_excluded_category_trade_share` |

### Activity window

Recent activity is a rolling window (`activity_window_hours`, default 24) rather
than a UTC calendar day, so a cycle running shortly after midnight does not
reject every wallet. `max_last_trade_age_hours` (default 36) remains the
hard staleness bound. The evidence field is `activity_window_trade_count`.

### Toxic market mix

`toxic_market_flags` selects which Market360 signals count as toxic. The allowed
names are `liquidity_risk`, `volume_collapse`, `whale_control`,
`trade_concentration`, `squeeze`, `not_active` and `closed`. The shipped default
is the first three: `trade_concentration` and `squeeze` fire on roughly two
thirds and one third of liquid markets respectively, so including them rejects
essentially every wallet. `max_toxic_market_share` (default `0.50`) caps the
share of a wallet's recently traded markets that may carry a selected flag.

### Bot and uncopyable-trader heuristics

Each heuristic depends on an optional Wallet360 field. A missing value **skips**
that heuristic rather than guessing — documented fail-open behaviour, distinct
from the core `missing_evidence_above_threshold` gate which is unchanged and
still fails closed. `sybil_risk` is additionally raised when
`similar_wallets_count` exceeds `max_similar_wallets`.

### Category exclusion

Wallet360 reports `performance_by_category` hierarchically (`"Crypto"`,
`"Crypto / Prices"`, `"Sports / Soccer"`). A parent row duplicates its children
and sibling top-level rows overlap, so `services.discovery.category_profile.CategoryProfile`
computes each share as `category_trades / snapshot.total_trades`, clamped to 1.
A wallet is rejected with `excluded_category_dominant` when any name in
`excluded_categories` (default `Crypto`) holds more than
`max_excluded_category_trade_share` (default `0.50`) of its trades. The 15-day
window is used, falling back to 7 days; an empty profile skips the gate.

### Specialist bonus

`specialist_bonus_enabled` (default on) adds `specialist_bonus_weight` (default
`0.10`) to the composite score when the **dominant specialty bucket** holds at
least `specialist_min_dominant_share` (default `0.80`) of the wallet's trades and
the recent win rate reaches `specialist_min_win_rate` (default `0.57`). The
30-day win rate is preferred, falling back to the 15-day window. A specialty
bucket is a sub-category row such as `Sports / Soccer` or `Esports / MOBA`
(deeper rows fold into it); a top-level category without sub-rows is its own
bucket. Dominance is measured below the top level because the provider files
every esports market under the `Sports` umbrella as well, so a top-level share
is near 100 % for any sports or esports trader, and it is a share rather than a
category count because almost every wallet carries a small residual bucket.
This is a soft preference and never a gate — a wallet without category or
win-rate evidence simply scores zero on the `specialist` component; metrics
`specialist_dominant_category` / `specialist_dominant_share` expose the bucket.

## Why did a cycle return zero candidates?

`FalconDiscoveryService.run` returns accepted candidates only, so
`discovery_refresh_succeeded` alone cannot explain an empty roster. Two places
carry the histogram:

- the structured `discovery_cycle_summary` log line and the
  `accepted_count` / `rejected_count` / `rejection_reasons` extras on
  `discovery_refresh_succeeded`;
- the persisted snapshots, queried per cycle:

```sql
SELECT reason, count(*)
FROM (
  SELECT unnest(string_to_array(rejected_reason, ';')) reason
  FROM discovery_candidate_snapshots
  WHERE cycle_id = (SELECT cycle_id FROM discovery_cycles ORDER BY started_at DESC LIMIT 1)
) s
GROUP BY 1
ORDER BY 2 DESC;
```

Read the dominant reason against the gate table above and loosen exactly that
setting. Note that promotion additionally needs `hysteresis.promote_score`
(default `0.55`) on `hysteresis.min_tenure_cycles` consecutive cycles, so a
freshly accepted wallet is not tradeable on its first cycle.

### Changing discovery gates

The legacy discovery roster still uses `discovery_config_hash` to invalidate
old scoring evidence. Automatic membership is stored separately and survives
configuration changes. Its next targeted evaluation applies the current gates;
provider failures block entries, while successful poor evaluations advance the
configured degradation streak. Restart to apply a changed trader count.

## Configuration

Every discovery setting and policy threshold is in `config/discovery.yml`.
Falcon policy lives under `discovery.falcon`; its private transport lives under
`discovery.api`. The API key is supplied via env and never committed:

```dotenv
PMCOPY__DISCOVERY__API__TOKEN=...
PMCOPY__DISCOVERY__API__BASE_URL=https://retriever.falconapi.net/api/v2/semantic/retrieve/parameterized
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

The standalone ranking CLI reads its token and optional endpoint override from
`PMCOPY__DISCOVERY__API__TOKEN` and `PMCOPY__DISCOVERY__API__BASE_URL`; it defaults
to the premium endpoint and does not load the bot's full runtime configuration.

```bash
python scripts/rank_copy_wallets.py --funds 500 --top 10
python scripts/rank_copy_wallets.py --funds 2000 --top 20 --json
python scripts/rank_copy_wallets.py --funds 100 --ccxt-depth auto
```

See `docs/discovery_bankroll_ranking.md` for how to choose a bankroll and read
the report.
