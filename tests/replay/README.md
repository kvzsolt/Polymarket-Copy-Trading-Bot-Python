# Replay fixtures (inputs only — no collected tests yet)

This directory holds six JSON scenario fixtures plus a `conftest.py`
loader. They are **replay inputs**, not a runnable test suite: there is no
`test_*.py` module here, so `pytest tests/replay` collects nothing and
exits with code 5 ("no tests ran"). That exit is deliberate and expected —
it is why the verification matrix records the replay row as
`exit 5 — no tests ran (empty selection)`, not as a failure.

## Contents

- `conftest.py` — `load_replay_fixture(name)` / `list_replay_fixtures()`
  helpers over `fixtures/`.
- `fixtures/*.json` — six time-ordered event scenarios, each with a
  `_description`, `_scenario`, and `_invariant` header:
  - `delayed_buy_then_fill` — second BUY suppressed while the first is
    pending; after the fill, the reservation is released.
  - `duplicate_sell_delayed` — duplicate leader SELL signals do not
    double-sell while the first is in flight.
  - `partial_fill_then_cancel` — partial fill followed by cancel settles
    the remainder correctly.
  - `rejected_sell_after_reserved` — a venue-rejected SELL releases the
    reserved shares without losing the fill evidence.
  - `restart_with_pending_orders` — restart reconciliation of orders left
    pending by a previous process.
  - `venue_timeout_then_reconciled_fill` — a lost POST response resolves
    through the read-only evidence scans, never a blind resubmit.

## Coverage today

No production or test code imports `tests.replay.conftest` yet. The
equivalent scenarios are covered inline elsewhere — for example
`tests/test_execution_fak_repost_defer.py` embeds the
partial-fill-then-cancel scenario, and the restart/reconciliation
semantics of `restart_with_pending_orders` and
`venue_timeout_then_reconciled_fill` are exercised by the
reconciliation/recovery tests in the default tier (e.g.
`tests/test_reconciliation_service.py`,
`tests/test_ambiguous_submission_recovery.py`). This directory is
scaffolding for a future replay suite that will drive these exact JSON
inputs through the pipeline; until that suite exists, adding a
`test_*.py` here is the intended way to light the fixtures up — do not
delete them.
