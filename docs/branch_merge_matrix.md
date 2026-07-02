# Branch & PR integration matrix

Integration branch: `integration/merge-all-prs-and-audit-fixes` (off `origin/develop`).
Generated during the PR1 stabilization pass (2026-06-30).

## Live remote state at integration time

After `git fetch --all --prune`, the only branches on `origin` were:
`develop`, `main`, `feature/leader-discovery-v2` (already merged via #119), and
`fix/risk-pre-reconcile-async-io` (open PR #122). The branches the original
inventory listed (`fix/pr115-review-fixes`, `fix/risk-pre-reconcile-executor-isolation`,
`feature/concurrency-hardening`, `chore/remove-stale-legacy-test`) had already
been **deleted from the remote** by the owner — the prune cleaned the stale local
remote-tracking refs. Their commit objects remained reachable locally, so each
was assessed before being classified.

## Matrix

| Branch / PR | Purpose | Status | Action | Resolution / notes |
|---|---|---|---|---|
| **PR #122** `fix/risk-pre-reconcile-async-io` (`95a38ba`) | Async REST venue order-status/fills so risk pre-reconcile overlaps (no threads) | OPEN, mergeable | **MERGED** (`--no-ff`) | No file overlap with the integration branch; 63 gateway tests pass post-merge. |
| PR #121 `fix/risk-pre-reconcile-executor-isolation` (`54e841f`) | Bounded executor for venue SDK calls | CLOSED, branch deleted | **SUPERSEDED** | Same root problem as #122; #122's async-REST approach replaced it. Not merged. |
| PR #116 `fix/pr115-review-fixes` (`ff1673f`,`4a199b6`,`c9f5029`) | rpc-fallback / slippage-decimal / config-decimal + near_one/redemption edits | CLOSED conflicting, branch deleted | **ABANDONED upstream** | Owner closed+deleted it; it also edits the near_one/redemption subsystem removed by #120. Not resurrected. The rpc-fallback / decimal-slippage fixes were never landed — flagged as a follow-up for owner decision rather than re-introduced against owner intent. |
| `feature/concurrency-hardening` (`7577efa`,`805bd6a`) | Reservation reaper when risk mgmt disabled | branch deleted | **ALREADY INTEGRATED** | `tests/test_reservation_reaper.py` is already present on develop — the reaper intent landed. Not re-merged. |
| `chore/remove-stale-legacy-test` (`40ad229`) | Delete a legacy test importing removed modules | branch deleted | **ALREADY INTEGRATED** | `tests/test_execution_live_fill_rejection.py` is already absent on develop. No-op. |
| Local WIP on `main` (uncommitted) | Fail-closed comment-only config parser attempt | local only | **PRESERVED → SUPERSEDED** | Committed to `wip/config-parser-main` for safety. The good parts (leading-`#` detection) were folded into the feature-aware fail-soft parser; its fail-closed `test_comment_only_value_rejected` was intentionally replaced by the Phase-5 disabled-OK contract. |

## Out of scope (documented, not actioned)
- `main` ↔ `develop` divergence (main carries docs + security-package commits never back-merged; develop carries 23 unpromoted commits).
- Local-only branches: `backup-before-cleanup`, `develop-test`, `fix/develop-runtime-hardening`.
