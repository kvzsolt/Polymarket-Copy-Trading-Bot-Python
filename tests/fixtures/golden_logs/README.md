# Golden log fixtures — structured logging compatibility contract

These fixtures pin the observable behavior of the two logging sinks in
`utils/logger.py` — the Rich console handler and the JSONL
`RotatingFileHandler` (`JsonFormatter`) — for the twelve order-lifecycle
moments of the copy-trading pipeline. They are the **compatibility contract**
for the standalone-refactor tasks that touch logging (Tasks 5 and 26): those
tasks must keep these fixtures byte-identical for all non-secret events.

## Layout

| File | Content |
|---|---|
| `sink_contract.json` | Sink topology: handler classes, levels, rotation settings, JSONL-only routing sets |
| `<event>.console.txt` | Exactly what the Rich console renders for the event (empty file = JSONL-only routing, itself part of the contract) |
| `<event>.jsonl` | Exactly what the JSONL sink records (one JSON object per line) |

Events (all twelve lifecycle moments):

`startup`, `detection_event`, `sizing_decision`, `skip_decision`, `submit`,
`ambiguous_order`, `fill`, `cancellation`, `recovery`, `position_drift`,
`protective_exit`, `shutdown`.

Each fixture is produced by driving the **real production helpers** in
`utils/logger.py` (`log_order_lifecycle`, `log_trade_detected`,
`log_trade_decision`, `log_trade_skip`, `log_trade_submitted`,
`log_trade_execution`, `log_position_drift`, `log_risk_exit`, the
`print_*` console helpers, plus the production
`StartupHealthReport.as_log_payload` dataclass payload) with synthetic
events whose message shapes mirror the verified production call sites
(`services/tradeMonitor.py`, `orchestrators/trade_pipeline.py`,
`orchestrators/startup_health.py`, `orchestrators/runtime_summary.py`,
`services/executionService.py`, `services/execution/reconciliation_service.py`,
`services/execution/coordinator.py`, `services/position_drift_service.py`,
`app.py` shutdown teardown).

## What is contract (assertable)

- Event names: the `type=` extras and message prefixes (`LIFECYCLE`,
  `DETECTED`, `SUBMIT`, `EXECUTED`, `POSITION_DRIFT`, …).
- Log levels per record.
- Required structured fields and their JSON key names (`status`,
  `submissionState`, `venueStatus`, `skipCode`, `riskReason`, `reason`, …).
- Status/skip vocabularies (`submitted_unconfirmed`, `ambiguous_timeout`,
  `auto_released_ambiguous`, `matched`, `confirmed`, `cancelled`, …).
- Console-vs-JSONL routing: which types render a console panel and which are
  JSONL-only (see `sink_contract.json` → `jsonl_only_types` and the empty
  `sizing_decision.console.txt`).
- Rotation settings: 5,242,880 bytes (5 MiB) × 3 backups, UTF-8,
  `log/PolymarketCopyTradingBot.jsonl`.

## What is NOT contract (deliberately normalized)

- Source module / function / line metadata — JSONL `module`, `funcName`,
  `lineno` fields are replaced with `<module>`, `<funcName>`, `<lineno>`.
- Timestamps — frozen at emission and normalized to `<TS>` (both sinks).
- Latencies and order/tx/reservation ids — fixed synthetic constants, so no
  normalization beyond `<TS>` is required.
- Console terminal styling — fixtures are captured with a fixed-width (100
  columns), no-color, non-terminal Rich console; only the text layout and
  box geometry are pinned.

## Regenerating

From the repository root with the project virtualenv:

```bash
python tests/fixtures/golden_logs/regenerate.py          # rewrite fixtures in place
python tests/fixtures/golden_logs/regenerate.py --check  # verify fixtures match; exit 1 on drift
python tests/fixtures/golden_logs/regenerate.py --output-dir DIR  # write elsewhere (e.g. a temp dir)
```

The harness is deterministic (frozen clock, fixed-width console, cleared
lifecycle-dedup map, fixed synthetic identifiers), so repeated runs produce
byte-identical output. Regeneration never appends: each capture starts from
an empty sink. If a refactor intentionally changes logged output, regenerate
and note the delta in the task's report — never hand-edit a fixture.

No secret material is embedded: every identifier is an obviously synthetic
constant declared at the top of `regenerate.py`.
