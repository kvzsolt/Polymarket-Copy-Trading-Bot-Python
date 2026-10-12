"""Service package.

Avoid importing heavy modules at package import time to prevent side effects
and circular import chains during startup/tests.
"""

__all__: list[str] = []
