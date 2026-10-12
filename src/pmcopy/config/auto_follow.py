"""Validated evidence settings; roster capacity remains in discovery.scheduler."""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class AutoFollowConfig(BaseModel):
    """Paper qualification and local performance sample requirements."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    paper_days: int = Field(7, ge=1)
    minimum_episodes: int = Field(20, ge=1)
    max_paper_drawdown: Decimal = Field(Decimal("0.10"), gt=0, le=1)
