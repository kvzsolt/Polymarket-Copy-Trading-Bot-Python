# Simplification Plan — Python Copy Trader (`refactor-cleanup`)

## Goal
Kill the accidental complexity without changing live trading semantics by surprise.

## 1. SELL path stays dumb-on-purpose
- Keep BUY as the place for scoring / trust / optional gates.
- Keep SELL as a pure mirrored exit rule: **sell the same fraction of our held shares as the leader sold from theirs**.
- Do not reintroduce `decide_sell()` into the active pipeline.
- If ratio enrichment fails, log it loudly and fall back only as a temporary compatibility path.

## 2. One config source of truth
- `config/runtime.env` = checked-in non-sensitive defaults.
- `config/runtime.local.env` = machine-specific public overrides.
- `.env` = secrets only.
- `config/env.py` and `config/app_config.py` should only mirror those defaults, not invent new ones.
- Add a small CI check that diffs known env keys against the typed config defaults.

## 3. Stop duplicating env parsing logic
- Today both `config/env.py` and `config/app_config.py` know too much.
- Medium-term target:
  - keep `app_config.py` as the typed source of truth,
  - make `config/env.py` a compatibility projection layer for legacy callers,
  - gradually replace raw `ENV[...]` reads with injected typed config.

## 4. Trim legacy execution leftovers
- Keep `legacy/` isolated and clearly marked read-only.
- Do not let active orchestration import from `utils/postOrder.py` or legacy executor paths except in tests proving parity.
- Prefer one execution service, one trade pipeline, one persistence path.

## 5. Pull side-effectful enrichment closer to ingestion boundaries
- Leader position snapshots used for SELL ratio calculation belong at monitoring/ingestion time.
- That keeps trade execution simpler and reduces decision spaghetti in the pipeline.

## 6. Logging cleanup
- Typed logging config should include level, file locations, and rotation settings.
- Runtime code should stop reading ad-hoc logging env values directly when injected config is available.
- Keep rich console output and JSONL audit logs, but make the knobs explicit.

## 7. Add guardrail tests for the important shit
- SELL 50% trim -> bot sells 50% of its held shares.
- SELL full exit -> bot exits fully.
- No held position -> clean skip.
- Config defaults in typed config and env snapshot stay aligned.
- Repo layout invariant: only `app.py` and `bootstrap.py` remain as root Python entrypoints.

## Suggested next slices
1. Add a config-alignment test that enumerates expected default keys.
2. Replace more runtime modules that read `ENV` directly with injected `AppConfig` slices.
3. Split `config/runtime.env` into smaller generated sections only if the single file becomes unmaintainable.
4. Remove sell-related dead code from `CopyDecisionService` once callers are gone.
