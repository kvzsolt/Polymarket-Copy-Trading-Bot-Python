#!/usr/bin/env bash
# RC1 validation bundle (PR9).
#
# Runs compileall + pytest + Python source forbid scans + an unguarded
# execution_service.execute alias scan, plus the doctor live gate against the
# same layered config used by the application. Exits non-zero on the first
# blocker so CI / operators see the failure clearly.

set -euo pipefail

cd "$(dirname "$0")/.."

echo "[1/4] compileall..."
python -m compileall -q app.py bot_cli clients orchestrators services cache domain integrations interfaces storage utils
echo "  ok"

echo "[2/4] pytest..."
python -m pytest -q --tb=line
echo "  ok"

echo "[3/4] forbid scans..."
fail=0
# Scan production code only — tests legitimately reference legacy tokens
# while documenting them. Scope: app + bot_cli + Python service trees.
_FORBID_ROOTS=("app.py" "bot_cli" "clients" "orchestrators" "services" "cache" "domain" "integrations" "interfaces" "storage" "utils" "config")
for substr in "services.price_cache" "PositionPriceCache" "MarketMetaService"; do
    if grep -RIn --include='*.py' "$substr" "${_FORBID_ROOTS[@]}" >/dev/null 2>&1; then
        echo "  FAIL forbidden substring still present in production Python source: $substr"
        grep -RIn --include='*.py' "$substr" "${_FORBID_ROOTS[@]}" | head -5
        fail=1
    else
        echo "  ok $substr"
    fi
done

python - <<'PY'
from pathlib import Path
import sys

allowed = {"services/execution/coordinator.py"}
patterns = [
    "execution_service.execute",
    'getattr(execution_service, "execute"',
    "getattr(execution_service, 'execute'",
]
violations = []
for root in ["orchestrators", "services"]:
    for path in Path(root).rglob("*.py"):
        rel = str(path)
        if rel in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        for pat in patterns:
            if pat in text and "execution_coordinator_missing" not in text:
                violations.append(f"{rel}: {pat}")
                break
if violations:
    print("  FAIL unguarded execution fallback:")
    for v in violations:
        print("    " + v)
    sys.exit(1)
print("  ok no unguarded execution_service.execute aliases")
PY

if [ "$fail" -ne 0 ]; then
    echo "RC1 check FAILED at phase 3"
    exit 1
fi

echo "[4/4] doctor live..."
python -m bot_cli.main doctor --live

echo "RC1 check complete."
