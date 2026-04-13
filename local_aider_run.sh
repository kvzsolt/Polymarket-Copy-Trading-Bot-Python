#!/usr/bin/env bash
OLLAMA_API_BASE=http://127.0.0.1:11434/v1 aider \
  --model openai/qwen3-coder-unsloth \
  --openai-api-base http://127.0.0.1:11434/v1 \
  --openai-api-key ollama \
  --no-show-model-warnings \
  --yes \
  --no-auto-commits \
  --no-dirty-commits \
  --message "Refactor config/env.py to produce a strongly-typed 'AppConfig' dataclass containing all loaded keys, rather than keeping them solely as a module-level dictionary alias. 1. Define a robust dataclass 'AppConfig' mapping existing keys like 'PRIVATE_KEY', 'POLYMARKET_API_KEY', 'DB_PATH', etc. 2. Implement an 'AppConfig.from_env_snapshot(snapshot: dict)' classmethod factory. 3. Create an 'APP_CONFIG' initialized instance at the bottom of the file alongside the raw 'ENV' dictionary to allow gradual migration for dependents. Do not delete the old 'ENV' logic yet." \
  config/env.py
