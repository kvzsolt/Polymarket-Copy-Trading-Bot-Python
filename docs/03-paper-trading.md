# Paper Trading and Discovery Validation

Keep `execution.trading_mode: paper` until the full runtime is stable.

## Discovery-only

Set `PMCOPY__DISCOVERY__API__TOKEN` in mode-`0600` `secrets.env`, then run:

```bash
python scripts/run_discovery_only.py
python scripts/run_discovery_only.py --list-only
```

The command creates no CLOB client or trading monitor. Review accepted and
rejected candidate snapshots, component scores, longitudinal history, and the
watchlist/active roster. Promotion intentionally needs several cycles.

## Paper runtime

```bash
pmcopy config validate
pmcopy doctor --paper
pmcopy trading pause --reason "paper soak"
pmcopy run
```

Watch decision, sizing, copy-latency, queue-depth, reconciliation, market-data,
and drift metrics. Paper mode never submits to the venue, and the database
control should remain paused throughout the soak.
