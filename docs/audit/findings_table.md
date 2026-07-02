# Deep-research findings → current-repo resolution

The deep-research report was based on an **uploaded older snapshot** of
`app_config.py` / `runtime.env`. develop had since evolved (it already shipped a
`float_or_auto` parser, the `POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS` constant,
and the WSS-independent REST execution path). Each finding was therefore
cross-checked against the live develop code before acting.

| ID | Finding (report) | Severity | State on develop | PR1 resolution |
|---|---|---|---|---|
| BUG-1 | `TRAILING_ACTIVATION_PCT` comment-only value crashes load_config | P0 | Partially fixed (`float_or_auto` returned None for `#`/blank/auto) | Hardened: feature-aware `optional_float_or_symbol`; `_has_inline_comment` now catches leading-`#`. |
| BUG-2 | Disabled feature still fatal-parses its knob | P0 | Present (parsed regardless of flag) | **Fixed**: `ENABLE_TRAILING_STOP` read first; disabled → `("disabled", None)`, never raises. |
| BUG-3 | `auto` mode unrepresentable (smuggled as float) | P0/doc | Partially (None, no explicit mode) | **Fixed**: explicit `RiskConfig.trailing_activation_mode: Literal["disabled","fixed","auto"]`. |
| BUG-5 | ≥20 env keys not in AppConfig | P1 | Mostly already typed; 5 real | **Fixed**: 4 typed in `ExecutionConfig`; `PYTHONUNBUFFERED` allowlisted; schema-coverage test added. |
| BUG-6 | REST-primary execution blocks on WSS | P1 | **Not present** — execution path is WSS-independent | Verified + pinned with tests; flags live only in gitignored runtime.env (operator-owned). |
| BUG-7 | Missing Neg Risk exchange surface | P1 | Constant existed, not typed | **Fixed**: typed `NetworkConfig.neg_risk_ctf_exchange_v2_address`; collateral allowance now probes both exchanges. |
| BUG-8 | Ambiguous reservation release risk | P1 | **Already hardened** (probe-clean release) | Verified (`reconciliation_service._maybe_auto_release_ambiguous`); guard test pins fail-safe defaults. |
| BUG-9 | Duplicate keys in runtime.local.env | P2 | Present (local file) | **Fixed**: deduped; duplicate-key lint added. |
| BUG-10 | Parser fails on Python 3.13 / CI matrix | P2 | CI already 3.11/3.12 | N/A — already correct. |
| (new) | `runtime.env` / `runtime.local.env` are gitignored | — | Confirmed | Tests + smoke degrade gracefully to the tracked `runtime.local.example` in CI. |

Deferred to PR2 (onchain detection — where `log_index` is produced):
`(tx_hash, log_index)` mined-fill dedupe, pending→mined reconcile, both-exchange
log/pending subscriptions.
