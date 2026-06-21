# Runbooks

This directory contains operational runbooks for resolving incidents, outages, and state drift.

## Table of Contents
1. **CLOB Outage**: What to do when Polymarket's CLOB API is down or rejecting orders.
2. **WSS Drops**: Handling chronic WebSocket disconnections and state recovery.
3. **Circuit Breaker**: Steps to take when the `CircuitBreaker` trips globally.
4. **Stale Locks**: How to clear hanging DB reservations using `python3 bot.py stale-locks`.
5. **Reconciliation Drift**: Fixing desyncs between DB and on-chain using `python3 bot.py drift`.
6. **Manual Drift**: What happens when you manually trade on Polymarket outside the bot.

*(Runbook details to be expanded per-incident).*