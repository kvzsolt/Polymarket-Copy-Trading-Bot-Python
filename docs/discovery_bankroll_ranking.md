# Bankroll-aware wallet ranking (operator guide)

This guide explains how to choose a follower bankroll and produce a ranked
wallet report using the Heisenberg discovery analytics provider. The ranking
tool is **read-only**: it performs no live trading, accepts no private keys,
and never submits, cancels, approves, redeems, splits, or merges anything.

## What the ranking does

`scripts/rank_copy_wallets.py` ranks Polymarket copy-trading wallets for a
specific follower bankroll (USDC). It uses Heisenberg H-Score as a shortlist
signal, then re-evaluates every wallet with:

- Wallet360 across 1/3/7/15-day windows and all-time statistics;
- realized PnL and win rate;
- recent trades, recency, and trade frequency;
- market concentration and strategy diversity;
- liquidity, visible order-book capacity (optional, via CCXT), and spread;
- short-duration market exposure;
- price-jump / anomaly / toxic-market filtering;
- a follower-specific bankroll-fit calculation (copy ratio, recommended stake,
  copyable trade share).

The final score optimizes for **bankroll-compatible copyability**, not raw
PnL or H-Score alone. A whale that is profitable but whose smallest trade
dwarfs your bankroll will rank below a smaller, stable wallet whose trades you
can actually reproduce.

## Prerequisites

- A Heisenberg API key in the environment:

  ```bash
  export PMCOPY_HEISENBERG_API_KEY=...   # or HEISENBERG_API_KEY
  ```

- The Python environment installed (`pip install -e ".[dev]"`), which includes
  the mandatory CCXT runtime dependency.
- Public order-book visible-depth probing remains operationally optional:
  `--ccxt-depth auto` can be disabled or will fall back to Heisenberg
  liquidity evidence if the CCXT runtime is unavailable.

## Choosing a bankroll

Pass the USDC you can actually deploy as `--funds`. The bankroll is the
denominator for every copy-ratio and recommended-stake calculation, so it must
reflect real available capital, not a notional target.

Guidance:

- **Reserve a buffer.** The default policy reserves 20% of funds
  (`bankroll.reserve_fraction`); only the usable 80% is considered for
  copy sizing.
- **Match your account.** The live discovery service uses the first runtime
  user's on-chain USDC balance each cycle. Use the same figure here so the
  CLI report matches what the live roster will produce.
- **Compare several bankrolls.** The best wallet changes with the bankroll.
  Run the ranking for a few values:

  ```bash
  python scripts/rank_copy_wallets.py --funds 100  --top 10
  python scripts/rank_copy_wallets.py --funds 500  --top 10
  python scripts/rank_copy_wallets.py --funds 2000 --top 10
  python scripts/rank_copy_wallets.py --funds 10000 --top 10
  ```

  A wallet that tops the `$100` list may disappear at `$10,000` because its
  trades saturate a small bankroll's per-position budget, while a whale may
  only become copyable at higher bankrolls.

## Reading the report

Human-readable output (default):

```text
Discovery source: heisenberg
Funds: 500 USDC

#1 0xabc... — score 0.8200 (H 82)
Recommended stake: 8.40 USDC
Copy ratio: 0.12
Copyable share: 0.71 | liquidity pctl 85 | toxic share 0.05
...
Rejections: {'bankroll_incompatible': 3, 'inactive_or_stale': 2}
```

JSON output (`--json`) is machine-readable and includes every component score
and evidence field.

Fields:

- `score` — bankroll-aware composite score (0..1); higher is better.
- `h_score` — raw Heisenberg H-Score (0..100).
- `recommended_copy_ratio` / `recommended_stake_usdc` — the copy ratio and
  per-trade stake the bankroll-fit calculation recommends.
- `copyable_trade_share` — fraction of the wallet's recent trades that stay
  above the minimum practical copy ratio at your bankroll.
- `median_liquidity_percentile` / `toxic_market_share` — market quality.
- `bankroll_reasons` — why the bankroll fit passed or failed.

## Exit codes

- `0` — at least one wallet passed every fail-closed gate.
- `2` — operator error (missing API key, CCXT required but unavailable with
  `--ccxt-depth on`, invalid funds).
- `3` — discovery completed but no wallet passed every gate. This is a policy
  result, not a transport failure; the tool does not fall back to weaker
  candidates.

## Fail-closed behavior

- Auth, permission, schema, or configuration errors fail closed.
- A successful run that yields zero safe wallets returns zero wallets and does
  not silently substitute weaker candidates.
- API keys, bearer headers, and unredacted provider errors are never logged.

## Rollback / disabling

The live discovery service uses the same Heisenberg provider. To disable it
without code changes, set `PMCOPY__DISCOVERY__HEISENBERG__ENABLED=false` (or
unset the API key) and restart; the discovery cycle then fails closed (no
candidates) rather than substituting a weaker source. This CLI is unaffected by
that setting because it always requires an explicit `--funds` and its own API
key.
