"""Interface package.

Hosts ``clob_gateway.py`` (Protocol for the venue gateway). The
``UserActivityInterface`` / ``UserPositionInterface`` dataclasses that
used to live here predated the Polymarket rewrite and depended on a
vendored ``bson.ObjectId`` from a MongoDB driver — both were removed
(issue #77) as part of the legacy-code kill.
"""

__all__: list[str] = []
