# Architecture Overview

This document outlines the canonical architecture of the Polymarket Copy Trading Engine. The system is designed around domain-driven design principles, immutability where possible, and strict failure isolation.

## 1. Composition Root & Bootstrapping
- `bootstrap.py::build_app`: Pure, stateless dependency injection. Wires the configuration, PostgreSQL DB pool, and shared repositories without performing any I/O.
- `app.py::main`: Handles the asynchronous setup, network connections (WSS/REST), and assembles the per-user `ClobClient` and `ExecutionService`.

## 2. Auto Trader Discovery Engine (`src/pmcopy/services/discovery/`)
Responsible for finding, scoring, and rotating leaders.
- **Ingestion**: Pulls raw wallet history via Falcon (`falcon_candidate_service`) and Polymarket (`polymarket_history_backfill`).
- **Scoring**: `CandidateScoringService` evaluates Win Rate, PnL, and ROI.
- **Copyability Replay**: Simulates a leader's historical trades against the operator's actual budget to ensure viability (`copyability_replay.py`).
- **Rotation**: Automatically promotes strong candidates to active monitoring and demotes failing ones (`rotation_service.py`).

## 3. Copy Decision & Coordination Engine
Translates leader actions into local intents.
- **Position Action Coordinator**: Serializes events per-position to prevent race conditions during execution (`positionActionCoordinator.py`).
- **Copy Decision**: Reconstructs leader portfolios to determine the exact percentage of a position the leader exited, enabling flawless partial mimicry (`copyDecisionService.py`).
- **Reservation Service**: Locks specific USDC or share amounts during the intent lifecycle to prevent over-allocation.

## 4. Risk & Portfolio Engine
The ultimate authority on whether a trade fires.
- **Risk Engine**: Evaluates the `ProposedTrade` against the `RiskPolicy` (daily drawdowns, caps, kill switches) (`riskEngine.py`).
- **Balance Regime**: Tracks active exposure and available collateral (`balance_regime.py`).

## 5. Execution & Reconciliation Engine
Interacts with the blockchain and CLOB.
- **Execution Service**: The boundary layer mapping internal intents to Polymarket CLOB V2 orders (`executionService.py`).
- **Idempotency**: All intents and fills are tracked in PostgreSQL. In the event of a crash, the system resumes without duplicating orders (`idempotency.py`).
- **Reconciliation Service**: Runs at startup to align the Postgres state with the on-chain/CLOB reality (`reconciliation.py`).

## 6. Observability
- **Health Server**: Exposes `/health`, `/ready`, and `/metrics` (`healthServer.py`).
- **Audit Logs**: All decisions are emitted as structured JSONL for external analytics.