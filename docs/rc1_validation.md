# RC1 Validation

When to use: confirming the branch is ready to ship as a release candidate.

## One-shot bundle

```bash
bash scripts/rc1_check.sh
```

Exits 0 only when all four phases below pass.

## Phase 1 — compileall

```bash
python -m compileall app.py bot_cli clients orchestrators services cache domain integrations interfaces storage utils
```

Expected: no output (every module byte-compiles).

## Phase 2 — pytest

```bash
pytest
```

Expected: `1679 passed, 5 skipped, 176 deselected` or higher passed count,
zero failures. Skipped tests are the release-gate suite that requires
`--override-ini='addopts='` to opt in (see below).

Release-gate suite (run separately):

```bash
pytest tests/release_gate/test_startup_recovery.py \
       tests/release_gate/test_reconciler_terminal_ownership.py \
       tests/release_gate/test_near_one_exit.py \
       tests/release_gate/test_e2e_dry_run_buy.py \
       tests/release_gate/test_e2e_dry_run_sell.py \
       --override-ini='addopts='
```

## Phase 3 — Forbid scans

The legacy names removed in PR3 must not return in Python source:

| Token | Where removed | Acceptance |
| --- | --- | --- |
| `services.price_cache` | PR3 | zero hits under `*.py` |
| `PositionPriceCache` | PR3 | zero hits under `*.py` |
| `MarketMetaService` | PR3 | zero hits under `*.py` |
| `execution_service.execute` (unguarded) | PR6.1 | only inside coordinator.py |

Manual scan equivalents:

```bash
grep -RIn --include='*.py' 'services.price_cache' .  # expect: no output
grep -RIn --include='*.py' 'PositionPriceCache' .    # expect: no output
grep -RIn --include='*.py' 'MarketMetaService' .     # expect: no output
```

For the coordinator alias guard, run:

```bash
python - <<'PY'
from pathlib import Path
allowed = {"services/execution/coordinator.py"}
patterns = [
    "execution_service.execute",
    'getattr(execution_service, "execute"',
    "getattr(execution_service, 'execute'",
]
violations = []
for root in ["orchestrators", "services"]:
    for path in Path(root).rglob("*.py"):
        if str(path) in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        for pat in patterns:
            if pat in text and "execution_coordinator_missing" not in text:
                violations.append(f"{path}: {pat}")
if violations:
    for v in violations:
        print(v)
    raise SystemExit(1)
print("ok")
PY
```

Expected: `ok`.

## Phase 4 — doctor live

```bash
python -m bot_cli.main doctor --live
```

Expected:
- `status: ok` (preferred) or `status: warning` with every warning reviewed.
- Every check has `name`, `status`, `detail`, `fix` keys in JSON dump.
- Console summary shows `❌` lines (if any) with `fix:` text.

If `status: blocked`, do NOT ship. Read each `fix:` and address.

## RC1 sign-off

When all four phases pass, paste the summary into the release-notes PR:

```
RC1 validation
- compileall: ok
- pytest: 1679 passed, 0 failed
- forbid scans: services.price_cache absent, PositionPriceCache absent, MarketMetaService absent, no unguarded execution_service.execute
- doctor --live: status=ok|warning
```

## Related docs

- `live_runbook.md` — paper / live startup.
- `emergency_shutdown.md` — kill switch + cleanup.
- `recovery_playbook.md` — restart after crash.
- `known_limitations.md` — RC1 deferred items.
