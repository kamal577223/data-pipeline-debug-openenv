"""Pydantic models for the data pipeline debugging environment."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class _ActionBase(BaseModel):
    """Fallback stand-in for OpenEnv Action when openenv is unavailable."""


class _ObservationBase(BaseModel):
    """Fallback stand-in for OpenEnv Observation when openenv is unavailable."""


class _StateBase(BaseModel):
    """Fallback stand-in for OpenEnv State when openenv is unavailable."""


try:
    from openenv.core.env_server.types import Action as OpenEnvAction
    from openenv.core.env_server.types import Observation as OpenEnvObservation
    from openenv.core.env_server.types import State as OpenEnvState
except ImportError:
    OpenEnvAction = _ActionBase
    OpenEnvObservation = _ObservationBase
    OpenEnvState = _StateBase


class DataPipelineDebugAction(OpenEnvAction):
    """Action submitted by the agent for the current debugging task."""

    candidate_pipeline: str = Field(
        ...,
        description="Full repaired Python pipeline code for the current task.",
    )


class DataPipelineDebugObservation(OpenEnvObservation):
    """Observation returned to the agent after reset or step."""

    task_id: str = Field(..., description="Unique identifier for the current task.")
    difficulty: str = Field(..., description="Task difficulty: easy, medium, or hard.")
    prompt: str = Field(..., description="Debugging instructions shown to the agent.")
    broken_pipeline: str = Field(..., description="The broken input pipeline or data sample.")
    expected_contract: dict[str, Any] = Field(
        default_factory=dict,
        description="Expected schema, values, or invariants after the fix.",
    )
    last_submission: str | None = Field(
        default=None,
        description="The agent's latest submitted fix, if any.",
    )
    feedback: str | None = Field(
        default=None,
        description="Deterministic grader feedback for the latest submission.",
    )
    passed: bool = Field(default=False, description="Whether the submission passed grading.")
    reward: float = Field(default=0.0, description="Reward assigned by the environment.")
    done: bool = Field(default=False, description="Whether the episode is finished.")


class DataPipelineDebugState(OpenEnvState):
    """Session state for a pipeline debugging episode."""

    episode_id: str = Field(default="episode-0")
    step_count: int = Field(default=0)
    task_id: str | None = Field(default=None)
    difficulty: str | None = Field(default=None)
    completed: bool = Field(default=False)
