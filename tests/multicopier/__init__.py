"""Task 24 — multi-copier acceptance matrix (addon MULTI_COPIER_ADDON.md §9).

Deterministic integration/contract tests proving every §9 scenario against
the REAL production composition (``orchestrators.runtime_bootstrap`` /
``orchestrators.trade_pipeline`` paths) with fake venue gateways. The
SQL-scope/uniqueness/money-path scenarios run against disposable Postgres in
``test_multicopier_postgres.py`` (``-m integration``).
"""
