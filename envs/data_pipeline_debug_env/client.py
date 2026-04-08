"""Client for the data pipeline debugging environment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from .models import DataPipelineDebugAction, DataPipelineDebugObservation, DataPipelineDebugState

ObsT = TypeVar("ObsT")
StateT = TypeVar("StateT")
ActT = TypeVar("ActT")


@dataclass
class StepResult(Generic[ObsT]):
    observation: ObsT
    reward: float
    done: bool


try:
    from openenv.core.env_client import EnvClient
except ImportError:
    class EnvClient(Generic[ActT, ObsT, StateT]):
        """Minimal fallback to keep local imports/testing functional."""

        def __init__(self, base_url: str | None = None):
            self.base_url = base_url


class DataPipelineDebugEnv(
    EnvClient[DataPipelineDebugAction, DataPipelineDebugObservation, DataPipelineDebugState]
):
    """Typed client for the data pipeline debugging environment."""

    def _step_payload(self, action: DataPipelineDebugAction) -> dict:
        return action.model_dump()

    def _parse_result(self, payload: dict) -> StepResult[DataPipelineDebugObservation]:
        observation = DataPipelineDebugObservation(**payload["observation"])
        return StepResult(
            observation=observation,
            reward=payload.get("reward", observation.reward),
            done=payload.get("done", observation.done),
        )

    def _parse_state(self, payload: dict) -> DataPipelineDebugState:
        return DataPipelineDebugState(**payload)
