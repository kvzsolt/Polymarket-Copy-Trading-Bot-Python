# Archive

Historical documents preserved for reference. None of these reflect the
current architecture or operational procedure. Treat them as historical
context only — do not follow their recommendations.

The current sources of truth are:

- `audit.md` (root) — the full-rework audit findings.
- `plan.md` (root) — the full-rework remediation plan.
- `docs/ARCHITECTURE.md` — the canonical architecture overview.
- `docs/runbooks/` — operator runbooks for incident response.
- `docs/MIGRATION.md` — migration notes from `main` / `develop` to
  `full-rework`.
- `docs/full-rework-progress.md` — live progress log of the remediation.
- `docs/00-read-this-first.md` through `docs/12-release-notes.md` — the
  numbered operator guide.

## What's in here

| File | Origin | Why archived |
|---|---|---|
| `simplification-plan.md` | Pre-audit "refactor-cleanup" planning doc | Superseded by `audit.md` + `plan.md`. References `utils/postOrder.py`, which has since been removed. |
| `full-rework-review-fixes-progress.md` | Progress log for the PR review fix-up branch | Superseded by `docs/full-rework-progress.md`. Contains references to the root-level probe scripts (`test_probe*.py`, `test_doctor_*.py`) that were deleted in issue #77. |
| `usage.md` | Original repo usage / configuration guide | Replaced by `docs/02-configuration.md` and the rest of the numbered operator guide. The env-var surface listed in `usage.md` no longer matches the typed `AppConfig` schema. |
