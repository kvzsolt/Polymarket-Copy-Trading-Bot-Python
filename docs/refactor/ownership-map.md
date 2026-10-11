# Ownership map — copy-trade lifecycle stage owners

Baseline for the standalone refactor (Task 2). One page mapping each lifecycle
stage to its owning module, so relocations (Tasks 11/12/21) and ownership
fixes (Task 10) know what moves and what must not gain a second owner.

Method: **import inspection** of the traced modules (not guessing). Every
"owns" claim below was verified by reading the module's imports/definitions
and the call sites named in the refactor audit's verified traces:
`orchestrators/trade_pipeline.py`, `orchestrators/runtime_bootstrap.py`,
`services/execution/{coordinator,reservation_service,ambiguous_recovery}.py`,
`services/executionService.py`,
`integrations/polymarket/ccxt_prediction_gateway.py`, `storage/postgres/*`.

```text
detection sources → canonical events → admission/sizing → reservation
  → submit (CCXT-only) → fills → reconciliation/recovery → persistence
```

## Composition root

| Concern | Owning module |
|---|---|
| Process entry / supervised loops / shutdown | `app.py` (`_run_application`, `_shutdown_runtime`) |
| Per-user runtime assembly | `orchestrators/runtime_bootstrap.py` (builds `ExecutionService`, `ReservationService`, `ExecutionCoordinator`, reconciliation, risk monitor, monitor loops) |
| Venue runtime (one CCXT exchange per follower wallet) | `integrations/polymarket/venue_runtime.py` (`build_polymarket_runtime`, called via `runtime_bootstrap.build_user_venue_runtime`) |
| Trading context / cross-service state bag | `application/trading_context.py` (`TradingContext`) |

## 1. Detection sources → canonical events

| Concern | Owning module |
|---|---|
| REST leader-activity polling | `services/tradeMonitor.py` (`fetch_trade_data` monitor loop; emits `print_new_trade_detected` + `log_trade_detected`; invokes the pipeline callback) |
| On-chain WSS leader detection | `clients/onchain_ws.py` (transport) → `services/onchain_detection/native_observer.py`, `pending_tx_detector.py`, `order_filled_detector.py` (decode/derive `OnchainTradeSignal` from `services/onchain_detection/trade_detection_models.py`) |
| Detection support (calldata decode, dedup, finality, scope guards) | `services/onchain_detection/` (`calldata_decoder.py`, `mined_fill_deduplicator.py`, `source_transaction_gate.py`, `leader_sell_scope_guard.py`, `leader_sell_fraction_resolver.py`) |
| Cross-source claim / dispatch to pipeline | `orchestrators/onchain_signal_dispatch.py` (`OnchainSignalDispatchAdapter`, `CrossSourceTradeClaimPolicy`) |
| Market data (public WSS + book decode + cache) | `clients/market_ws.py`, `clients/market_book_decoder.py`, `cache/` |
| Canonical copy signal (REST/classic path) | `domain/models.py::TradeSignal`, constructed in `orchestrators/trade_pipeline.py` |
| Canonical leader event (copy-task/v1 path) | `domain/detection.py::LeaderTradeEvent`, constructed only in `orchestrators/v1_event_adapters.py` |

## 2. Admission & sizing

| Concern | Owning module |
|---|---|
| Pipeline orchestration (dedup, admission order, sizing audit logs `sizing_audit`/`sizing_final`, skip emission) | `orchestrators/trade_pipeline.py` (`execute_trades`) |
| Task/market admission | `services/entryPermissionService.py` (`EntryPermissionService.check_entry`) |
| Copy decision gate (allow/skip + score + skip codes) | `services/copyDecisionService.py` (`decide_buy` / `decide_sell`) |
| Order sizing & minimum-order policy | `services/order_sizing.py` (`MinimumOrderPolicy`, `size_buy_order`, `size_sell_order`) |
| Sizing strategies (v5 / copy tasks) | `services/copy_sizing_v5/`, `services/copy_tasks/` |
| Risk headroom / account snapshot | `services/risk/risk_engine.py` (`headroom_for_buy`), `services/risk/account_snapshot.py` |

## 3. Reservation (atomic budget)

| Concern | Owning module |
|---|---|
| Intent / reservation / lifecycle domain types | `domain/order_lifecycle.py` (`OrderIntent`, `Reservation`, `OrderLifecycleStatus`) |
| Atomic reserve/release/expiry | `services/execution/reservation_service.py` (`ReservationService`) |
| Reservation persistence | `storage/postgres/reservations_repo.py` (`ReservationsRepo`) |
| Status transition legality | `services/execution/state_machine.py` (`assert_transition_allowed`) |

## 4. Submit (CCXT-only execution transport)

| Concern | Owning module |
|---|---|
| Pipeline → coordinator funnel | `orchestrators/trade_pipeline.py::_submit_copy_through_coordinator` |
| Guarded submit (reservation + idempotency + state machine + audit) | `services/execution/coordinator.py` (`ExecutionCoordinator.submit_copy_trade` / `submit_risk_exit` / `submit_protective_exit`) |
| Order build, retry policy, post-POST lifecycle emission | `services/executionService.py` (`ExecutionService.execute` → `_emit_post_lifecycle` `order_lifecycle` logs) |
| Venue-neutral execution port | `interfaces/clob_gateway.py` (`ClobGateway` protocol) |
| Live venue adapter | `integrations/polymarket/ccxt_prediction_gateway.py` (`CcxtPredictionGateway`) |
| Paper adapter (CCXT reads + local simulated writes) | `integrations/polymarket/paper_execution.py` (`PaperExecutionGateway`), `paper_market_reader.py`, `paper_evaluation_execution.py` |
| CCXT client wrapper | `integrations/polymarket/ccxt_prediction_client.py` (`CcxtPredictionClient`) → `ccxt.prediction.polymarket` |
| Tick/precision/credentials compat | `integrations/polymarket/ccxt_compat/` |
| Submission sanitization / pending-order tracking helpers | `services/execution/submission.py` |

## 5. Fills

| Concern | Owning module |
|---|---|
| User WSS transport (fills/orders; callbacks only — never mutates positions) | `clients/user_ws.py` |
| User-order evidence parsing | `services/execution/user_order_evidence.py` (`UserOrderEvidenceParser`) |
| User-data runtime (reconnect, readiness, invalidation) | `orchestrators/user_data_runtime.py` (`UserDataRuntime`) |
| Fill accounting (cumulative evidence vs accounted fills) | `services/execution/fill_accounting.py` (`AccountedOrderFill`) |
| Fill persistence + position application (dedup + atomic apply) | `storage/postgres/fills_repo.py` (`FillsRepo.append_fill_and_apply`, `append_recovered_sell`) → `storage/postgres/positions_repo.py::_apply_fill_in_conn` |
| Evidence fills / projections | `services/evidence_fills.py`, `storage/postgres/fill_evidence_projection.py` |

## 6. Reconciliation & recovery

| Concern | Owning module |
|---|---|
| Canonical order reconciliation (venue polling, `ambiguous_timeout`, `auto_released_ambiguous`, fill upsert) | `services/execution/reconciliation_service.py` (`ReconciliationService`) |
| Delayed-fill convergence for submitted-unconfirmed | `orchestrators/trade_pipeline.py::reconcile_submitted_unconfirmed` + `get_reconciliation_service` |
| Ambiguous order recovery (venue order-id reattach from private history) | `services/execution/ambiguous_recovery.py`; evidence in `integrations/polymarket/ccxt_ambiguous_evidence.py` |
| Pending/intent recovery sweeps | `services/execution/pending_order_reconciliation.py`, `intent_recovery.py`, `recovery_inventory.py` |
| Read-only exchange-side drift classification (never invents fills) | `services/reconciliation.py` |
| Startup reconcile wiring | `orchestrators/runtime_bootstrap.py`; startup health/order normalization in `orchestrators/startup_health.py` |
| Position drift (detect/pause/prune; `position_drift` logs) | `services/position_drift_service.py`; domain types `domain/position_drift.py` |
| Protective exits (risk monitor → coordinator) | `services/risk/risk_monitor.py` (`_dispatch_risk_exit` → `submit_protective_exit`/`submit_risk_exit`) |
| Manual venue↔DB parity sync | `services/manual_sync_service.py` |

## 7. Persistence

| Concern | Owning module |
|---|---|
| Process-wide asyncpg pool | `storage/postgres/connection.py` |
| Orders | `storage/postgres/orders_repo.py` (submission status, `update_submission_status`) |
| Fills / positions | `storage/postgres/fills_repo.py`, `positions_repo.py` |
| Reservations / task budgets | `storage/postgres/reservations_repo.py`, `task_budget_repository.py` |
| Leaders / cursors / leader performance | `storage/postgres/leaders_repo.py`, `cursors_repo.py`, `leader_performance_repository.py` |
| Risk tables | `storage/postgres/risk_blocks_repo.py`, `risk_triggers_repo.py`, `risk_lot_repository.py` |
| Migrations | `storage/postgres/migrations.py` + `storage/postgres/migrations/` |

## Cross-cutting invariants to preserve while moving anything above

- Only `CcxtPredictionGateway` (live) and `PaperExecutionGateway` (paper) sit
  behind `ClobGateway`; nothing else may sign or submit.
- Ambiguity after POST is owned by reconciliation (`ambiguous_timeout` →
  probes → `auto_released_ambiguous`); no blind resubmit anywhere.
- Fill evidence is push-first (user WSS) with REST polling as fallback; the
  WSS client never mutates positions.
- Every stage's operator-visible output goes through `utils/logger.py`
  helpers; the golden fixtures in `tests/fixtures/golden_logs/` pin that
  contract.
