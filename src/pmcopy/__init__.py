"""pmcopy — Polymarket copy-trading bot application namespace.

Single top-level package introduced by the standalone refactor (Task 18).
Every former flat top-level package and module lives under here:
``pmcopy.app``, ``pmcopy.bootstrap``, ``pmcopy.bot_cli``,
``pmcopy.services``, ... No forwarding shims remain at the old flat paths.

Entry points:

* console script: ``pmcopy`` -> ``pmcopy.bot_cli.main:main``
* ``python -m pmcopy`` -> :mod:`pmcopy.__main__` -> :mod:`pmcopy.bot_cli.main`
"""
