#!/usr/bin/env bash
# Audit #103 — one-command rollback for live trading.
#
# Phases (each phase is idempotent / re-runnable):
#   1. pause trading via the kill switch (LIVE_TRADING_ENABLED=false)
#   2. snapshot SQLite + log/audit artifacts to backups/<timestamp>/
#   3. revert to the previous known-good commit (operator-supplied SHA)
#   4. run startup reconciliation (bot doctor) before resuming
#   5. run smoke checks (rc1_check.sh)
#   6. report status; resume is operator-gated (does NOT auto-flip the
#      kill switch back on — that decision belongs to a human)
#
# Usage:
#   scripts/rollback.sh <previous-good-commit-sha>      # full rollback
#   scripts/rollback.sh --dry-run [<sha>]               # log-only, no mutation
#   scripts/rollback.sh --canary <max_notional_usdc>    # enter canary mode
#
# Canary mode caps daily-spend + per-order notional for the first live
# window after a rollback. Operator flips the kill switch back on only
# once the canary window passes without alarms.

set -euo pipefail

cd "$(dirname "$0")/.."

DRY_RUN=0
CANARY_NOTIONAL=""
TARGET_SHA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1; shift ;;
    --canary)
      CANARY_NOTIONAL="${2:?--canary requires a USDC amount}"; shift 2 ;;
    *)
      TARGET_SHA="$1"; shift ;;
  esac
done

log()  { printf "[rollback] %s\n" "$*"; }
run()  { if [[ "$DRY_RUN" == 1 ]]; then log "DRY-RUN: $*"; else eval "$*"; fi }

ts="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="backups/$ts"

log "phase 1/6 — pause trading (kill switch off)"
if [[ "$DRY_RUN" == 0 ]]; then
  grep -q '^LIVE_TRADING_ENABLED=' secrets.env 2>/dev/null \
    && sed -i 's/^LIVE_TRADING_ENABLED=.*/LIVE_TRADING_ENABLED=false/' secrets.env \
    || echo "LIVE_TRADING_ENABLED=false" >> secrets.env
fi
log "  → LIVE_TRADING_ENABLED=false"

log "phase 2/6 — snapshot SQLite + logs to $BACKUP_DIR"
run "mkdir -p \"$BACKUP_DIR\""
for f in data/*.db data/*.sqlite logs/*.log audit/*.jsonl; do
  [[ -e "$f" ]] && run "cp -a \"$f\" \"$BACKUP_DIR/\""
done

if [[ -n "$TARGET_SHA" ]]; then
  log "phase 3/6 — revert working tree to $TARGET_SHA"
  run "git fetch --quiet"
  run "git checkout \"$TARGET_SHA\""
else
  log "phase 3/6 — no SHA supplied; keeping current tree (use for snapshot-only)"
fi

log "phase 4/6 — startup reconciliation (bot doctor live)"
run "python -m bot_cli.doctor --live --no-trade"

log "phase 5/6 — smoke checks"
run "scripts/rc1_check.sh"

if [[ -n "$CANARY_NOTIONAL" ]]; then
  log "phase 6/6 — entering canary mode (BUY_MAX_AMOUNT=$CANARY_NOTIONAL, MAX_DAILY_SPEND_USDC=$CANARY_NOTIONAL)"
  if [[ "$DRY_RUN" == 0 ]]; then
    # PR #105 review: ``sed -i || true`` is a no-op when the key is absent
    # from secrets.env — operator believes caps are active when they are
    # not. Mirror phase-1's pattern: replace in place when present, append
    # when missing.
    set_or_append_cap() {
      local key="$1" value="$2"
      if grep -q "^${key}=" secrets.env 2>/dev/null; then
        sed -i "s/^${key}=.*/${key}=${value}/" secrets.env
      else
        echo "${key}=${value}" >> secrets.env
      fi
    }
    set_or_append_cap BUY_MAX_AMOUNT "$CANARY_NOTIONAL"
    set_or_append_cap MAX_DAILY_SPEND_USDC "$CANARY_NOTIONAL"
  fi
else
  log "phase 6/6 — no canary requested; live trading still paused"
fi

log "DONE. Resume is operator-gated: flip LIVE_TRADING_ENABLED=true manually"
log "once doctor + smoke + canary window are all green."
