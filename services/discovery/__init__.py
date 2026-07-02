"""Leader discovery services package.

Keep package-level exports lazy. Several discovery services depend on the
Postgres repository, while the repository imports discovery scoring models.
Eager imports here turn normal submodule imports into circular imports.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.discovery.raw_feature_joiner import RawFeatureJoiner
    from services.discovery.reconstructed_positions_service import (
        ReconstructedPositionsService,
    )
    from services.discovery.reconstruction_service import ReconstructionService
    from services.discovery.scoring_input_service import ScoringInputAssemblerService

__all__ = [
    "ReconstructedPositionsService",
    "ReconstructionService",
    "ScoringInputAssemblerService",
    "RawFeatureJoiner",
]


def __getattr__(name: str):
    if name == "ReconstructedPositionsService":
        from services.discovery.reconstructed_positions_service import (
            ReconstructedPositionsService,
        )

        return ReconstructedPositionsService
    if name == "ReconstructionService":
        from services.discovery.reconstruction_service import ReconstructionService

        return ReconstructionService
    if name == "ScoringInputAssemblerService":
        from services.discovery.scoring_input_service import ScoringInputAssemblerService

        return ScoringInputAssemblerService
    if name == "RawFeatureJoiner":
        from services.discovery.raw_feature_joiner import RawFeatureJoiner

        return RawFeatureJoiner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
