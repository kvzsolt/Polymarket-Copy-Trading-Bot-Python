# Copy-Leader Scoring Invariants

This repository ranks leaders for one purpose only:

- improve future follower execution-adjusted alpha

It does not exist to build a vanity leaderboard, simulate fake equity curves, or guess what a trader "must have meant".

## Hard bans

The copy-leader stack must not introduce any of the following:

- bankroll inference from trade size, position size, or wallet activity
- guessed inventory or guessed sell fractions when trade history is incomplete
- midpoint-as-fill assumptions or any synthetic fill price presented as execution truth
- fake Sharpe, fake equity curves, or other metrics built from reconstructed fantasy PnL
- leader ranking based on leader vanity PnL alone without follower copyability

Midpoint, spread, and orderbook descriptors may be stored for context analysis. They are never to be treated as actual fill prices.

## Scoring layers

There are three distinct layers, and they must remain separate:

1. `research score`
   Measures whether a leader has historically produced copyable edge in specific contexts. This is research-only and should be built from observed trades, market context, orderbook state, post-trade drift, and explicit confidence.

2. `operational trust`
   Measures whether the leader can be followed safely right now. This includes data quality, consistency, activity freshness, and execution-facing constraints. Existing live trust logic belongs here, not in the research score.

3. `live permission`
   Final gate for whether the bot is allowed to copy a leader or signal in production. This must consider risk, liquidity, confidence, operational health, and deployment ladder state.

No single scalar should silently blur these layers together.

## Context buckets

Research must be sliced by context, not only by aggregate totals. The canonical buckets for this repo are:

- category
- liquidity bucket
- spread bucket
- time-to-resolution bucket
- trade-size bucket

Additional buckets are allowed only if they are grounded in observed data and materially improve follower decision quality.

## Phase 1 data contract

The minimum durable research spine must preserve:

- observed leader trades as ledger facts
- market metadata captured from trusted API payloads
- position snapshots exactly as reported by source APIs
- orderbook snapshots captured near observed leader trades

Every stored record should preserve the raw payload or enough provenance to audit how it was derived.

## Confidence rules

- If history is incomplete, downgrade confidence. Do not fill gaps with invented state.
- If a source feed omits inventory, bankroll, or exit attribution, leave it unknown.
- If research and live systems need different views of the same event, store both explicitly rather than mutating one representation.

## Promotion ladder

The intended ladder is:

- watchlist
- paper follow
- live follow

Promotion should be based on copyability evidence and confidence, not on theatrical return metrics.
