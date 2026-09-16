# Current runtime and preserved documentation

The implementation branch replaces the runtime while preserving the existing
`main` README and documentation files. Those preserved guides may describe the
previous SDK or configuration layout. This note records the current runtime
without rewriting those files.

## Execution and configuration

Authenticated order execution uses `ccxt.prediction.polymarket` behind
`ClobGateway`. The removed `polymarket-client` / `py-clob-client` SDK is not a
runtime dependency. See [the repository architecture rules](../CLAUDE.md).

Service settings live in `config/*.yml`; environment overrides live in
`config/environments/<environment>/`. The checked-in
`config/environments/example/execution.yml` is the paper-mode execution
template. Per-leader settings live in `config/traders/*.yml`.

Keep credentials in the ignored `secrets.env` file using the typed
`PMCOPY__SECTION__FIELD` names. The explicit `execution.trading_mode` setting
controls paper versus live execution; credentials alone never enable live
trading.

## Discovery

The premium Falcon endpoint remains configured. The optional price-jump agent
596 check is disabled for the operator's plan; the other discovery gates
remain active. See [the current discovery guide](trader_discovery.md) for
provider permissions, Wallet360 numeric handling, and discovery settings.

## Verification

Run the complete repository test selection with:

```bash
python -m pytest -q -o addopts=''
```

Integration and release-gate tests require the repository's test dependencies
and Docker-backed temporary databases. Local tests and analytics reads do not
constitute live-trading activation.
