# Operator Note: Polymarket Ambiguous Order Attribution Fixes

## Incident Context
On 2026-09-19, the live copy-bot created multiple orders that became parked as `ambiguous_timeout` due to an extended Polymarket matching delay. The actual venue orders were filled, but the local bot was unable to attribute the fills because the position delta probe was skewed by sibling order fills that arrived concurrently.

## Fixes Implemented
1. **Trade-history based fill attribution**: Idempotent pass of `reconcile_trade_record` was incorrectly treating previously attributed fills as `"live"`. This was fixed by ensuring `is_attributed` properly skips the downgrade and returns `"confirmed"` immediately.
2. **Clean-no-fill auto-release probe**: We extended the position delta safety check (`_run_auto_release_probes`) to gracefully handle overlapping sibling trades. The probe now matches the exact discrepancy (`delta = local_size - pre_submit_position_size`) against fills belonging to sibling orders within the `_MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS` timeframe. If sibling fills completely account for the delta, the safety check succeeds (`clean` instead of `inconclusive`).
3. **Correlation ID persistence**: The `VenueSubmitResult` data model now supports `trade_ids` and `transactions_hashes`. During submission, `ccxt_prediction_gateway` extracts these venue IDs directly from the CCXT raw payload (`value` or `info`). Both the V1 (`executionService.py`) and V2 (`coordinator.py`) paths now correctly persist these keys into the `response` attribute of the order lifecycle metadata. The reconciler (`_submission_sequences`) can now seamlessly use these persisted correlation IDs to cross-reference market fills if they are available.

## Tests Performed
- Wrote and passed `test_auto_release_clean_when_delta_matches_sibling_fills` in `test_reconciliation_market_trade_attribution.py`.
- Wrote and passed `test_reconcile_trade_record_applies_attributed_fill_once` in `test_reconciliation_market_trade_attribution.py`.
- Ran the full test suite (`python -m pytest -q tests/`) ensuring existing logic (including V1 execution pathways) wasn't broken by the newly added dataclass properties and mapping configurations.
- Verified style adherence via `ruff check .`
