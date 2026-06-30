# Strategy profiler & env tuner — design intent (PR3, not yet implemented)

Design note for the deferred **PR3** module. Nothing here ships in PR1.
Rule-based (no ML) for auditability. **Dry-run by default; `--apply` required.**

## Module layout (planned)
```
services/strategy_profile/
  leader_history_loader.py
  strategy_profiler.py
  strategy_classifier.py
  strategy_env_tuner.py
  env_patch_writer.py
models/strategy_profile.py
models/env_patch.py
```

## CLI (argparse `_COMMANDS` pattern in bot_cli/main.py)
```
python bot.py profile-leader --wallet 0x... --lookback-days 30 --output strategy_profile.json
python bot.py tune-strategy  --wallet 0x... --dry-run --patch-out strategy_patch.env
python bot.py tune-strategy  --wallet 0x... --apply --target config/runtime.local.env
```

## Archetypes
`StrategyArchetype(StrEnum)` = `fast_crypto_scalper`, `low_price_lottery`,
`near_certainty_sweeper`, `event_specialist`, `balanced_swing`,
`noisy_uncopyable`. Noisy/uncopyable → recommend NOT auto-tuning.

## EnvPatch model
`@dataclass(frozen=True) EnvPatch(key, old_value: str|None, new_value: str,
reason: str, confidence: float)`.

## Hard safety rules
- Never modify secrets / `PRIVATE_KEY` / CLOB creds / RPC creds.
- Never auto-rewrite `LEADER_WALLETS`.
- Write **only** to `config/runtime.local.env` (which is gitignored — operator
  local), and only under `--apply`.
- Emit a human-readable patch explanation before mutating.

These map onto the existing `services/discovery/runtime_env_editor.py` which
already edits `runtime.local.env` for trader rotation — reuse its safe-write
mechanics rather than re-implementing.
